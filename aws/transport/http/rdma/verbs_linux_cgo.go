//go:build linux && cgo && rdma

package rdma

/*
#cgo LDFLAGS: -lrdmacm -libverbs

#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <poll.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
	GO_RDMA_FRAME_HEADER_SIZE = 12,
	GO_RDMA_POLL_SPINS = 64,
	GO_RDMA_POLL_SLEEP_US = 50,
};

typedef struct {
	uint8_t *buf;
	struct ibv_mr *mr;
} go_rdma_recv_slot;

typedef struct {
	struct rdma_cm_id *id;
	struct rdma_event_channel *cm_channel;
	struct ibv_pd *pd;
	struct ibv_cq *send_cq;
	struct ibv_cq *recv_cq;
	int manual_resources;
	uint8_t *send_buf;
	struct ibv_mr *send_mr;
	go_rdma_recv_slot *recv_slots;
	uint32_t recv_depth;
	uint32_t frame_cap;
	uint32_t inline_threshold;
	uint32_t last_recv_slot;
	int has_last_recv_slot;
} go_rdma_conn;

typedef struct {
	struct rdma_cm_id *listen_id;
	uint32_t frame_cap;
	uint32_t send_wr;
	uint32_t recv_wr;
	uint32_t inline_threshold;
} go_rdma_listener;

static void go_rdma_set_err(char **err_out, const char *fmt, ...) {
	if (err_out == NULL) {
		return;
	}
	*err_out = NULL;

	char stack_buf[512];
	va_list args;
	va_start(args, fmt);
	int n = vsnprintf(stack_buf, sizeof(stack_buf), fmt, args);
	va_end(args);
	if (n < 0) {
		return;
	}

	if ((size_t)n < sizeof(stack_buf)) {
		*err_out = strdup(stack_buf);
		return;
	}

	size_t need = (size_t)n + 1;
	char *buf = (char *)malloc(need);
	if (buf == NULL) {
		return;
	}

	va_start(args, fmt);
	vsnprintf(buf, need, fmt, args);
	va_end(args);
	*err_out = buf;
}

static void go_rdma_set_errno(char **err_out, const char *op) {
	go_rdma_set_err(err_out, "%s: %s", op, strerror(errno));
}

static int64_t go_rdma_now_monotonic_us(void) {
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		return -1;
	}
	return ((int64_t)ts.tv_sec * 1000000LL) + ((int64_t)ts.tv_nsec / 1000LL);
}

static int go_rdma_sleep_us(int64_t sleep_us) {
	if (sleep_us <= 0) {
		return 0;
	}

	struct timespec ts;
	ts.tv_sec = (time_t)(sleep_us / 1000000LL);
	ts.tv_nsec = (long)((sleep_us % 1000000LL) * 1000LL);

	while (nanosleep(&ts, &ts) != 0) {
		if (errno == EINTR) {
			continue;
		}
		return -1;
	}

	return 0;
}

static int go_rdma_poll_cq_with_timeout(
	struct ibv_cq *cq,
	struct ibv_wc *wc,
	int timeout_ms,
	const char *op,
	char **err_out
) {
	if (cq == NULL || wc == NULL) {
		go_rdma_set_err(err_out, "%s: invalid cq/wc", op);
		return EINVAL;
	}

	int64_t deadline_us = -1;
	if (timeout_ms >= 0) {
		int64_t now_us = go_rdma_now_monotonic_us();
		if (now_us < 0) {
			go_rdma_set_errno(err_out, "clock_gettime");
			return errno != 0 ? errno : EIO;
		}
		deadline_us = now_us + ((int64_t)timeout_ms * 1000LL);
	}

	int spins = 0;
	for (;;) {
		int n = ibv_poll_cq(cq, 1, wc);
		if (n > 0) {
			return 0;
		}
		if (n < 0) {
			go_rdma_set_errno(err_out, op);
			return errno != 0 ? errno : EIO;
		}

		if (deadline_us >= 0) {
			int64_t now_us = go_rdma_now_monotonic_us();
			if (now_us < 0) {
				go_rdma_set_errno(err_out, "clock_gettime");
				return errno != 0 ? errno : EIO;
			}
			if (now_us >= deadline_us) {
				return EAGAIN;
			}
		}

		if (spins < GO_RDMA_POLL_SPINS) {
			spins++;
			continue;
		}

		if (go_rdma_sleep_us(GO_RDMA_POLL_SLEEP_US) != 0) {
			go_rdma_set_errno(err_out, "nanosleep");
			return errno != 0 ? errno : EIO;
		}
	}
}

static int go_rdma_wait_cm_event(
	struct rdma_event_channel *channel,
	int timeout_ms,
	enum rdma_cm_event_type expected,
	const char *op,
	char **err_out
) {
	if (channel == NULL) {
		go_rdma_set_err(err_out, "%s: nil event channel", op);
		return EINVAL;
	}

	struct pollfd pfd;
	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = channel->fd;
	pfd.events = POLLIN;

	int poll_timeout = timeout_ms;
	if (poll_timeout < 0) {
		poll_timeout = -1;
	}

	int rc;
	do {
		rc = poll(&pfd, 1, poll_timeout);
	} while (rc < 0 && errno == EINTR);

	if (rc == 0) {
		go_rdma_set_err(err_out, "%s: timed out waiting cm event %s", op, rdma_event_str(expected));
		return EAGAIN;
	}
	if (rc < 0) {
		go_rdma_set_errno(err_out, "poll(cm_event)");
		return errno != 0 ? errno : EIO;
	}

	struct rdma_cm_event *event = NULL;
	rc = rdma_get_cm_event(channel, &event);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_get_cm_event");
		return errno != 0 ? errno : EIO;
	}

	enum rdma_cm_event_type actual = event->event;
	int status = event->status;
	rdma_ack_cm_event(event);

	if (actual != expected) {
		go_rdma_set_err(
			err_out,
			"%s: unexpected cm event=%s expected=%s status=%d",
			op,
			rdma_event_str(actual),
			rdma_event_str(expected),
			status
		);
		return EIO;
	}
	if (status != 0) {
		go_rdma_set_err(
			err_out,
			"%s: cm event=%s status=%d",
			op,
			rdma_event_str(actual),
			status
		);
		return EIO;
	}

	return 0;
}

static struct ibv_cq *go_rdma_send_cq(go_rdma_conn *conn) {
	if (conn == NULL || conn->id == NULL) {
		return NULL;
	}
	if (conn->send_cq != NULL) {
		return conn->send_cq;
	}
	return conn->id->send_cq;
}

static struct ibv_cq *go_rdma_recv_cq(go_rdma_conn *conn) {
	if (conn == NULL || conn->id == NULL) {
		return NULL;
	}
	if (conn->recv_cq != NULL) {
		return conn->recv_cq;
	}
	return conn->id->recv_cq;
}

static void go_rdma_free_conn(go_rdma_conn *conn) {
	if (conn == NULL) {
		return;
	}

	if (conn->id != NULL) {
		rdma_disconnect(conn->id);
	}

	if (conn->send_mr != NULL) {
		rdma_dereg_mr(conn->send_mr);
		conn->send_mr = NULL;
	}

	if (conn->recv_slots != NULL) {
		for (uint32_t i = 0; i < conn->recv_depth; i++) {
			if (conn->recv_slots[i].mr != NULL) {
				rdma_dereg_mr(conn->recv_slots[i].mr);
				conn->recv_slots[i].mr = NULL;
			}
			if (conn->recv_slots[i].buf != NULL) {
				free(conn->recv_slots[i].buf);
				conn->recv_slots[i].buf = NULL;
			}
		}
		free(conn->recv_slots);
		conn->recv_slots = NULL;
	}

	if (conn->send_buf != NULL) {
		free(conn->send_buf);
		conn->send_buf = NULL;
	}

	if (conn->id != NULL) {
		if (conn->manual_resources) {
			if (conn->id->qp != NULL) {
				rdma_destroy_qp(conn->id);
			}
			if (conn->send_cq != NULL) {
				ibv_destroy_cq(conn->send_cq);
				conn->send_cq = NULL;
			}
			if (conn->recv_cq != NULL) {
				ibv_destroy_cq(conn->recv_cq);
				conn->recv_cq = NULL;
			}
			if (conn->pd != NULL) {
				ibv_dealloc_pd(conn->pd);
				conn->pd = NULL;
			}
			rdma_destroy_id(conn->id);
		} else {
			rdma_destroy_ep(conn->id);
		}
		conn->id = NULL;
	}

	if (conn->cm_channel != NULL) {
		rdma_destroy_event_channel(conn->cm_channel);
		conn->cm_channel = NULL;
	}

	free(conn);
}

static void go_rdma_free_listener(go_rdma_listener *listener) {
	if (listener == NULL) {
		return;
	}

	if (listener->listen_id != NULL) {
		rdma_destroy_ep(listener->listen_id);
		listener->listen_id = NULL;
	}

	free(listener);
}

static int go_rdma_open(
	const char *host,
	const char *port,
	uint32_t frame_cap,
	uint32_t send_wr,
	uint32_t recv_wr,
	uint32_t inline_threshold,
	int timeout_ms,
	go_rdma_conn **out,
	char **err_out
) {
	if (out == NULL) {
		go_rdma_set_err(err_out, "go_rdma_open: out is nil");
		return EINVAL;
	}
	*out = NULL;

	if (frame_cap <= GO_RDMA_FRAME_HEADER_SIZE) {
		go_rdma_set_err(err_out, "go_rdma_open: invalid frame_cap=%u", frame_cap);
		return EINVAL;
	}
	if (send_wr == 0 || recv_wr == 0) {
		go_rdma_set_err(err_out, "go_rdma_open: queue depth must be > 0");
		return EINVAL;
	}

	struct rdma_addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_port_space = RDMA_PS_TCP;
	hints.ai_qp_type = IBV_QPT_RC;
	hints.ai_flags = 0;
#ifdef RAI_NUMERICSERV
	hints.ai_flags |= RAI_NUMERICSERV;
#endif

	struct rdma_addrinfo *res = NULL;
	int rc = rdma_getaddrinfo(host, port, &hints, &res);
	if (rc != 0) {
		go_rdma_set_err(err_out, "rdma_getaddrinfo: %s", gai_strerror(rc));
		return rc > 0 ? rc : EINVAL;
	}

	struct ibv_qp_init_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = send_wr;
	qp_attr.cap.max_recv_wr = recv_wr;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;
	qp_attr.cap.max_inline_data = inline_threshold;

	struct rdma_cm_id *id = NULL;
	rc = rdma_create_ep(&id, res, NULL, &qp_attr);
	rdma_freeaddrinfo(res);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_create_ep");
		return errno != 0 ? errno : EIO;
	}

	int reuse_addr = 1;
	(void)rdma_set_option(id, RDMA_OPTION_ID, RDMA_OPTION_ID_REUSEADDR, &reuse_addr, sizeof(reuse_addr));

	go_rdma_conn *conn = (go_rdma_conn *)calloc(1, sizeof(go_rdma_conn));
	if (conn == NULL) {
		rdma_destroy_ep(id);
		go_rdma_set_errno(err_out, "calloc go_rdma_conn");
		return errno != 0 ? errno : ENOMEM;
	}

	conn->id = id;
	conn->frame_cap = frame_cap;
	conn->recv_depth = recv_wr;
	conn->inline_threshold = inline_threshold;
	conn->has_last_recv_slot = 0;

	conn->send_buf = (uint8_t *)malloc(frame_cap);
	if (conn->send_buf == NULL) {
		go_rdma_set_errno(err_out, "malloc send buffer");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : ENOMEM;
	}

	conn->send_mr = rdma_reg_msgs(conn->id, conn->send_buf, frame_cap);
	if (conn->send_mr == NULL) {
		go_rdma_set_errno(err_out, "rdma_reg_msgs send");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	conn->recv_slots = (go_rdma_recv_slot *)calloc(recv_wr, sizeof(go_rdma_recv_slot));
	if (conn->recv_slots == NULL) {
		go_rdma_set_errno(err_out, "calloc recv slots");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : ENOMEM;
	}

	for (uint32_t i = 0; i < recv_wr; i++) {
		conn->recv_slots[i].buf = (uint8_t *)malloc(frame_cap);
		if (conn->recv_slots[i].buf == NULL) {
			go_rdma_set_errno(err_out, "malloc recv buffer");
			go_rdma_free_conn(conn);
			return errno != 0 ? errno : ENOMEM;
		}

		conn->recv_slots[i].mr = rdma_reg_msgs(conn->id, conn->recv_slots[i].buf, frame_cap);
		if (conn->recv_slots[i].mr == NULL) {
			go_rdma_set_errno(err_out, "rdma_reg_msgs recv");
			go_rdma_free_conn(conn);
			return errno != 0 ? errno : EIO;
		}

		rc = rdma_post_recv(
			conn->id,
			(void *)(uintptr_t)(i + 1),
			conn->recv_slots[i].buf,
			frame_cap,
			conn->recv_slots[i].mr
		);
		if (rc != 0) {
			go_rdma_set_errno(err_out, "rdma_post_recv");
			go_rdma_free_conn(conn);
			return errno != 0 ? errno : EIO;
		}
	}

	struct rdma_conn_param conn_param;
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;

	rc = rdma_connect(conn->id, &conn_param);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_connect");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	*out = conn;
	return 0;
}

static int go_rdma_listen(
	const char *host,
	const char *port,
	uint32_t frame_cap,
	uint32_t send_wr,
	uint32_t recv_wr,
	uint32_t inline_threshold,
	int backlog,
	go_rdma_listener **out,
	char **err_out
) {
	if (out == NULL) {
		go_rdma_set_err(err_out, "go_rdma_listen: out is nil");
		return EINVAL;
	}
	*out = NULL;

	if (frame_cap <= GO_RDMA_FRAME_HEADER_SIZE) {
		go_rdma_set_err(err_out, "go_rdma_listen: invalid frame_cap=%u", frame_cap);
		return EINVAL;
	}
	if (send_wr == 0 || recv_wr == 0) {
		go_rdma_set_err(err_out, "go_rdma_listen: queue depth must be > 0");
		return EINVAL;
	}
	if (backlog <= 0) {
		go_rdma_set_err(err_out, "go_rdma_listen: backlog must be > 0");
		return EINVAL;
	}

	struct rdma_addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_port_space = RDMA_PS_TCP;
	hints.ai_qp_type = IBV_QPT_RC;

	int flags = 0;
#ifdef RAI_PASSIVE
	flags |= RAI_PASSIVE;
#endif
#ifdef RAI_NUMERICSERV
	flags |= RAI_NUMERICSERV;
#endif
	hints.ai_flags = flags;

	struct rdma_addrinfo *res = NULL;
	int rc = rdma_getaddrinfo(host, port, &hints, &res);
	if (rc != 0) {
		go_rdma_set_err(err_out, "rdma_getaddrinfo(listen): %s", gai_strerror(rc));
		return rc > 0 ? rc : EINVAL;
	}

	struct ibv_qp_init_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = send_wr;
	qp_attr.cap.max_recv_wr = recv_wr;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;
	qp_attr.cap.max_inline_data = inline_threshold;

	struct rdma_cm_id *listen_id = NULL;
	rc = rdma_create_ep(&listen_id, res, NULL, &qp_attr);
	rdma_freeaddrinfo(res);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_create_ep(listen)");
		return errno != 0 ? errno : EIO;
	}

	rc = rdma_listen(listen_id, backlog);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_listen");
		rdma_destroy_ep(listen_id);
		return errno != 0 ? errno : EIO;
	}

	go_rdma_listener *listener = (go_rdma_listener *)calloc(1, sizeof(go_rdma_listener));
	if (listener == NULL) {
		go_rdma_set_errno(err_out, "calloc go_rdma_listener");
		rdma_destroy_ep(listen_id);
		return errno != 0 ? errno : ENOMEM;
	}

	listener->listen_id = listen_id;
	listener->frame_cap = frame_cap;
	listener->send_wr = send_wr;
	listener->recv_wr = recv_wr;
	listener->inline_threshold = inline_threshold;

	*out = listener;
	return 0;
}

static int go_rdma_accept(
	go_rdma_listener *listener,
	go_rdma_conn **out,
	char **err_out
) {
	if (out == NULL) {
		go_rdma_set_err(err_out, "go_rdma_accept: out is nil");
		return EINVAL;
	}
	*out = NULL;

	if (listener == NULL || listener->listen_id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_accept: listener closed");
		return EBADF;
	}

	struct rdma_cm_id *id = NULL;
	int rc = rdma_get_request(listener->listen_id, &id);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_get_request");
		return errno != 0 ? errno : EIO;
	}

	go_rdma_conn *conn = (go_rdma_conn *)calloc(1, sizeof(go_rdma_conn));
	if (conn == NULL) {
		go_rdma_set_errno(err_out, "calloc go_rdma_conn(accept)");
		rdma_destroy_ep(id);
		return errno != 0 ? errno : ENOMEM;
	}

	conn->id = id;
	conn->frame_cap = listener->frame_cap;
	conn->recv_depth = listener->recv_wr;
	conn->inline_threshold = listener->inline_threshold;
	conn->has_last_recv_slot = 0;

	conn->send_buf = (uint8_t *)malloc(listener->frame_cap);
	if (conn->send_buf == NULL) {
		go_rdma_set_errno(err_out, "malloc send buffer(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : ENOMEM;
	}

	conn->send_mr = rdma_reg_msgs(conn->id, conn->send_buf, listener->frame_cap);
	if (conn->send_mr == NULL) {
		go_rdma_set_errno(err_out, "rdma_reg_msgs send(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	conn->recv_slots = (go_rdma_recv_slot *)calloc(listener->recv_wr, sizeof(go_rdma_recv_slot));
	if (conn->recv_slots == NULL) {
		go_rdma_set_errno(err_out, "calloc recv slots(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : ENOMEM;
	}

	for (uint32_t i = 0; i < listener->recv_wr; i++) {
		conn->recv_slots[i].buf = (uint8_t *)malloc(listener->frame_cap);
		if (conn->recv_slots[i].buf == NULL) {
			go_rdma_set_errno(err_out, "malloc recv buffer(accept)");
			go_rdma_free_conn(conn);
			return errno != 0 ? errno : ENOMEM;
		}

		conn->recv_slots[i].mr = rdma_reg_msgs(conn->id, conn->recv_slots[i].buf, listener->frame_cap);
		if (conn->recv_slots[i].mr == NULL) {
			go_rdma_set_errno(err_out, "rdma_reg_msgs recv(accept)");
			go_rdma_free_conn(conn);
			return errno != 0 ? errno : EIO;
		}

		rc = rdma_post_recv(
			conn->id,
			(void *)(uintptr_t)(i + 1),
			conn->recv_slots[i].buf,
			listener->frame_cap,
			conn->recv_slots[i].mr
		);
		if (rc != 0) {
			go_rdma_set_errno(err_out, "rdma_post_recv(accept)");
			go_rdma_free_conn(conn);
			return errno != 0 ? errno : EIO;
		}
	}

	struct rdma_conn_param conn_param;
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;

	rc = rdma_accept(conn->id, &conn_param);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_accept");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	*out = conn;
	return 0;
}

static int go_rdma_send_frame(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t payload_len,
	uint32_t total_len,
	uint32_t offset,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_frame: conn closed");
		return EBADF;
	}

	if ((uint64_t)GO_RDMA_FRAME_HEADER_SIZE + payload_len > conn->frame_cap) {
		go_rdma_set_err(err_out, "go_rdma_send_frame: payload_len=%u exceeds frame capacity", payload_len);
		return EMSGSIZE;
	}
	if ((uint64_t)offset + payload_len > total_len) {
		go_rdma_set_err(err_out, "go_rdma_send_frame: invalid range offset=%u payload=%u total=%u", offset, payload_len, total_len);
		return EINVAL;
	}

	uint32_t total_n = htonl(total_len);
	uint32_t offset_n = htonl(offset);
	uint32_t payload_n = htonl(payload_len);
	memcpy(conn->send_buf, &total_n, sizeof(total_n));
	memcpy(conn->send_buf + 4, &offset_n, sizeof(offset_n));
	memcpy(conn->send_buf + 8, &payload_n, sizeof(payload_n));

	if (payload_len > 0 && payload != NULL) {
		memcpy(conn->send_buf + GO_RDMA_FRAME_HEADER_SIZE, payload, payload_len);
	}

	int send_flags = IBV_SEND_SIGNALED;
	if (conn->inline_threshold > 0 && (uint32_t)(GO_RDMA_FRAME_HEADER_SIZE + payload_len) <= conn->inline_threshold) {
		send_flags |= IBV_SEND_INLINE;
	}

	int rc = rdma_post_send(
		conn->id,
		NULL,
		conn->send_buf,
		(size_t)GO_RDMA_FRAME_HEADER_SIZE + payload_len,
		conn->send_mr,
		send_flags
	);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_post_send");
		return errno != 0 ? errno : EIO;
	}

	struct ibv_cq *send_cq = go_rdma_send_cq(conn);
	struct ibv_wc wc;
	rc = go_rdma_poll_cq_with_timeout(send_cq, &wc, timeout_ms, "ibv_poll_cq(send)", err_out);
	if (rc != 0) {
		return rc;
	}
	if (wc.status != IBV_WC_SUCCESS) {
		go_rdma_set_err(err_out, "rdma send completion failed: %s", ibv_wc_status_str(wc.status));
		return EIO;
	}

	return 0;
}

static int go_rdma_recv_frame(
	go_rdma_conn *conn,
	uint8_t **payload_ptr,
	uint32_t *payload_len,
	uint32_t *total_len,
	uint32_t *offset,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_recv_frame: conn closed");
		return EBADF;
	}
	if (payload_ptr == NULL || payload_len == NULL || total_len == NULL || offset == NULL) {
		go_rdma_set_err(err_out, "go_rdma_recv_frame: nil output parameter");
		return EINVAL;
	}

	struct ibv_cq *recv_cq = go_rdma_recv_cq(conn);
	struct ibv_wc wc;
	int rc = go_rdma_poll_cq_with_timeout(recv_cq, &wc, timeout_ms, "ibv_poll_cq(recv)", err_out);
	if (rc != 0) {
		return rc;
	}
	if (wc.status != IBV_WC_SUCCESS) {
		go_rdma_set_err(err_out, "rdma recv completion failed: %s", ibv_wc_status_str(wc.status));
		return EIO;
	}

	uintptr_t wr = (uintptr_t)wc.wr_id;
	if (wr == 0 || wr > conn->recv_depth) {
		go_rdma_set_err(err_out, "invalid recv wr_id=%llu", (unsigned long long)wc.wr_id);
		return EIO;
	}
	uint32_t idx = (uint32_t)(wr - 1);
	go_rdma_recv_slot *slot = &conn->recv_slots[idx];

	if (wc.byte_len < GO_RDMA_FRAME_HEADER_SIZE) {
		go_rdma_set_err(err_out, "received short frame byte_len=%u", wc.byte_len);
		return EPROTO;
	}

	uint32_t total_n = 0;
	uint32_t offset_n = 0;
	uint32_t payload_n = 0;
	memcpy(&total_n, slot->buf, 4);
	memcpy(&offset_n, slot->buf + 4, 4);
	memcpy(&payload_n, slot->buf + 8, 4);
	*total_len = ntohl(total_n);
	*offset = ntohl(offset_n);
	*payload_len = ntohl(payload_n);

	if ((uint64_t)GO_RDMA_FRAME_HEADER_SIZE + *payload_len > wc.byte_len) {
		go_rdma_set_err(err_out, "frame payload length invalid payload=%u byte_len=%u", *payload_len, wc.byte_len);
		return EPROTO;
	}
	if ((uint64_t)(*offset) + (*payload_len) > *total_len) {
		go_rdma_set_err(err_out, "frame range invalid offset=%u payload=%u total=%u", *offset, *payload_len, *total_len);
		return EPROTO;
	}

	*payload_ptr = slot->buf + GO_RDMA_FRAME_HEADER_SIZE;
	conn->last_recv_slot = idx;
	conn->has_last_recv_slot = 1;
	return 0;
}

static int go_rdma_repost_recv(go_rdma_conn *conn, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_repost_recv: conn closed");
		return EBADF;
	}
	if (!conn->has_last_recv_slot) {
		go_rdma_set_err(err_out, "go_rdma_repost_recv: no recv slot to repost");
		return EINVAL;
	}

	uint32_t idx = conn->last_recv_slot;
	conn->has_last_recv_slot = 0;

	int rc = rdma_post_recv(
		conn->id,
		(void *)(uintptr_t)(idx + 1),
		conn->recv_slots[idx].buf,
		conn->frame_cap,
		conn->recv_slots[idx].mr
	);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_post_recv(repost)");
		return errno != 0 ? errno : EIO;
	}

	return 0;
}

static void go_rdma_close(go_rdma_conn *conn) {
	go_rdma_free_conn(conn);
}

static void go_rdma_listener_close(go_rdma_listener *listener) {
	go_rdma_free_listener(listener);
}
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	verbsBackendEnabled   = true
	verbsFrameHeaderSize  = 12
	verbsOpenTimeoutMills = 3000
	verbsOpenRetryBackoff = 250 * time.Millisecond
	verbsOpenAttemptLimit = 8
)

type verbsMessageConn struct {
	mu sync.RWMutex
	cc *C.go_rdma_conn

	framePayloadSize int
	localAddr        net.Addr
	remoteAddr       net.Addr
}

var _ MessageConn = (*verbsMessageConn)(nil)

type verbsListener struct {
	mu sync.RWMutex
	cl *C.go_rdma_listener

	framePayloadSize int
	localAddr        net.Addr
}

var _ net.Listener = (*verbsListener)(nil)

func newVerbsListener(network, address string, opts VerbsListenerOptions) (net.Listener, error) {
	cfg, err := opts.VerbsOptions.normalize()
	if err != nil {
		return nil, err
	}

	host, port, err := splitHostPortListenAddress(network, address)
	if err != nil {
		return nil, err
	}

	frameCap := cfg.framePayloadSize + verbsFrameHeaderSize
	if frameCap <= verbsFrameHeaderSize {
		return nil, fmt.Errorf("rdma verbs: invalid frame capacity")
	}

	backlog := opts.Backlog
	if backlog == 0 {
		backlog = DefaultVerbsListenBacklog
	}
	if backlog < 0 {
		return nil, fmt.Errorf("rdma verbs: listen backlog must be >= 0")
	}

	var cHost *C.char
	if host != "" {
		cHost = C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
	}
	cPort := C.CString(port)
	defer C.free(unsafe.Pointer(cPort))

	var cListener *C.go_rdma_listener
	var cErr *C.char
	rc := C.go_rdma_listen(
		cHost,
		cPort,
		C.uint32_t(frameCap),
		C.uint32_t(cfg.sendQueueDepth),
		C.uint32_t(cfg.recvQueueDepth),
		C.uint32_t(cfg.inlineThreshold),
		C.int(backlog),
		&cListener,
		&cErr,
	)
	if rc != 0 {
		return nil, rdmaCError("listen", rc, cErr)
	}
	if cListener == nil {
		return nil, errors.New("rdma verbs listen returned nil listener")
	}

	localHost := host
	if localHost == "" {
		localHost = "0.0.0.0"
	}

	return &verbsListener{
		cl:               cListener,
		framePayloadSize: cfg.framePayloadSize,
		localAddr:        rdmaAddr{network: "rdma", address: net.JoinHostPort(localHost, port)},
	}, nil
}

func splitHostPortListenAddress(network, address string) (host string, port string, err error) {
	switch network {
	case "", "tcp", "tcp4", "tcp6", "rdma", "rdma4", "rdma6":
	default:
		return "", "", fmt.Errorf("rdma verbs: unsupported network %q", network)
	}

	host, port, err = net.SplitHostPort(address)
	if err != nil {
		return "", "", fmt.Errorf("rdma verbs: invalid listen address %q: %w", address, err)
	}
	if port == "" {
		return "", "", fmt.Errorf("rdma verbs: missing port in listen address %q", address)
	}
	return host, port, nil
}

func (l *verbsListener) Accept() (net.Conn, error) {
	l.mu.RLock()
	cListener := l.cl
	framePayloadSize := l.framePayloadSize
	localAddr := l.localAddr
	l.mu.RUnlock()
	if cListener == nil {
		return nil, net.ErrClosed
	}

	var cConn *C.go_rdma_conn
	var cErr *C.char
	rc := C.go_rdma_accept(cListener, &cConn, &cErr)
	if rc != 0 {
		err := rdmaCError("accept", rc, cErr)
		l.mu.RLock()
		closed := l.cl == nil
		l.mu.RUnlock()
		if closed {
			return nil, net.ErrClosed
		}
		if strings.Contains(err.Error(), "Invalid argument") {
			return nil, temporaryListenerError{err: err}
		}
		return nil, err
	}
	if cConn == nil {
		return nil, errors.New("rdma verbs accept returned nil connection")
	}

	msgConn := &verbsMessageConn{
		cc:               cConn,
		framePayloadSize: framePayloadSize,
		localAddr:        localAddr,
		remoteAddr:       rdmaAddr{network: "rdma", address: "remote"},
	}
	return NewConn(msgConn), nil
}

func (l *verbsListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cl == nil {
		return nil
	}
	C.go_rdma_listener_close(l.cl)
	l.cl = nil
	return nil
}

func (l *verbsListener) Addr() net.Addr {
	return l.localAddr
}

type temporaryListenerError struct {
	err error
}

func (e temporaryListenerError) Error() string {
	return e.err.Error()
}

func (e temporaryListenerError) Unwrap() error {
	return e.err
}

func (e temporaryListenerError) Timeout() bool {
	return false
}

func (e temporaryListenerError) Temporary() bool {
	return true
}

// Open opens a MessageConn backed by librdmacm + libibverbs.
//
// Build requirements:
//  1. Linux
//  2. CGO_ENABLED=1
//  3. build tag: rdma
//  4. system libraries: librdmacm, libibverbs
func (o VerbsOptions) Open(ctx context.Context, network, address string) (MessageConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cfg, err := o.normalize()
	if err != nil {
		return nil, err
	}

	host, port, err := splitHostPortAddress(network, address)
	if err != nil {
		return nil, err
	}

	frameCap := cfg.framePayloadSize + verbsFrameHeaderSize
	if frameCap <= verbsFrameHeaderSize {
		return nil, fmt.Errorf("rdma verbs: invalid frame capacity")
	}

	deadline, hasDeadline := ctx.Deadline()
	var lastErr error
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("rdma open canceled after %d attempts: %w (last open err: %v)", attempt, err, lastErr)
			}
			return nil, err
		}

		if !hasDeadline && attempt >= verbsOpenAttemptLimit {
			if lastErr != nil {
				return nil, fmt.Errorf("rdma open failed after %d attempts: %w", attempt, lastErr)
			}
			return nil, errors.New("rdma open failed")
		}

		attemptTimeout := 3 * time.Second
		if hasDeadline {
			remain := time.Until(deadline)
			if remain <= 0 {
				if lastErr != nil {
					return nil, fmt.Errorf("rdma open deadline exceeded after %d attempts: %w", attempt, lastErr)
				}
				return nil, context.DeadlineExceeded
			}
			if remain < attemptTimeout {
				attemptTimeout = remain
			}
		}

		attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
		cConn, openErr := openVerbsConnOnce(attemptCtx, host, port, frameCap, cfg)
		cancelAttempt()
		attempt++
		if openErr == nil {
			return &verbsMessageConn{
				cc:               cConn,
				framePayloadSize: cfg.framePayloadSize,
				localAddr:        rdmaAddr{network: "rdma", address: "local"},
				remoteAddr:       rdmaAddr{network: "rdma", address: net.JoinHostPort(host, port)},
			}, nil
		}

		lastErr = openErr
		if !isRetriableOpenErr(openErr) {
			return nil, openErr
		}

		if hasDeadline {
			remain := time.Until(deadline)
			if remain <= 0 {
				return nil, fmt.Errorf("rdma open deadline exceeded after %d attempts: %w", attempt, lastErr)
			}
			sleep := verbsOpenRetryBackoff
			if remain < sleep {
				sleep = remain
			}
			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}

		time.Sleep(verbsOpenRetryBackoff)
	}
}

type openResult struct {
	cConn *C.go_rdma_conn
	cErr  *C.char
	rc    C.int
}

func openVerbsConnOnce(ctx context.Context, host, port string, frameCap int, cfg verbsConfig) (*C.go_rdma_conn, error) {
	openTimeoutMS, err := rdmaOpenTimeoutMillis(ctx)
	if err != nil {
		return nil, err
	}

	resultCh := make(chan openResult, 1)
	go func() {
		cHost := C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
		cPort := C.CString(port)
		defer C.free(unsafe.Pointer(cPort))

		var result openResult
		result.rc = C.go_rdma_open(
			cHost,
			cPort,
			C.uint32_t(frameCap),
			C.uint32_t(cfg.sendQueueDepth),
			C.uint32_t(cfg.recvQueueDepth),
			C.uint32_t(cfg.inlineThreshold),
			openTimeoutMS,
			&result.cConn,
			&result.cErr,
		)
		resultCh <- result
	}()

	var result openResult
	select {
	case <-ctx.Done():
		go func() {
			late := <-resultCh
			if late.cConn != nil {
				C.go_rdma_close(late.cConn)
			}
			if late.cErr != nil {
				C.free(unsafe.Pointer(late.cErr))
			}
		}()
		return nil, ctx.Err()
	case result = <-resultCh:
	}

	if result.rc != 0 {
		return nil, rdmaCError("open", result.rc, result.cErr)
	}
	if result.cConn == nil {
		return nil, errors.New("rdma verbs open returned nil connection")
	}
	return result.cConn, nil
}

func isRetriableOpenErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "rdma verbs open")
}

func (c *verbsMessageConn) SendMessage(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cc == nil {
		return net.ErrClosed
	}

	total := len(payload)
	if total == 0 {
		return c.sendFrame(ctx, nil, 0, 0)
	}

	for offset := 0; offset < total; offset += c.framePayloadSize {
		if err := ctx.Err(); err != nil {
			return err
		}

		chunkLen := c.framePayloadSize
		remaining := total - offset
		if remaining < chunkLen {
			chunkLen = remaining
		}

		chunk := payload[offset : offset+chunkLen]
		if err := c.sendFrame(ctx, chunk, total, offset); err != nil {
			return err
		}
	}

	return nil
}

func (c *verbsMessageConn) sendFrame(ctx context.Context, chunk []byte, totalLen int, offset int) error {
	timeoutMS, err := rdmaContextTimeoutMillis(ctx)
	if err != nil {
		return err
	}

	var payloadPtr *C.uint8_t
	if len(chunk) > 0 {
		payloadPtr = (*C.uint8_t)(unsafe.Pointer(&chunk[0]))
	}

	var cErr *C.char
	rc := C.go_rdma_send_frame(
		c.cc,
		payloadPtr,
		C.uint32_t(len(chunk)),
		C.uint32_t(totalLen),
		C.uint32_t(offset),
		timeoutMS,
		&cErr,
	)
	runtime.KeepAlive(chunk)
	if rc != 0 {
		if rc == C.int(C.EAGAIN) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
		return rdmaCError("send", rc, cErr)
	}
	return nil
}

func (c *verbsMessageConn) RecvMessage(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cc == nil {
		return nil, net.ErrClosed
	}

	var msg []byte
	var total int
	received := 0

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var (
			payloadPtr *C.uint8_t
			payloadLen C.uint32_t
			frameTotal C.uint32_t
			frameOff   C.uint32_t
			cErr       *C.char
		)
		timeoutMS, err := rdmaContextTimeoutMillis(ctx)
		if err != nil {
			return nil, err
		}

		rc := C.go_rdma_recv_frame(c.cc, &payloadPtr, &payloadLen, &frameTotal, &frameOff, timeoutMS, &cErr)
		if rc != 0 {
			if rc == C.int(C.EAGAIN) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				continue
			}
			return nil, rdmaCError("recv", rc, cErr)
		}

		frameTotalN := int(frameTotal)
		offsetN := int(frameOff)
		payloadLenN := int(payloadLen)

		if msg == nil {
			total = frameTotalN
			if total < 0 {
				_ = c.repostRecvSlot()
				return nil, fmt.Errorf("rdma verbs: invalid total message length %d", total)
			}
			msg = make([]byte, total)
		} else if frameTotalN != total {
			_ = c.repostRecvSlot()
			return nil, fmt.Errorf("rdma verbs: inconsistent frame total %d, expected %d", frameTotalN, total)
		}

		if offsetN < 0 || payloadLenN < 0 || offsetN+payloadLenN > total {
			_ = c.repostRecvSlot()
			return nil, fmt.Errorf("rdma verbs: invalid frame range offset=%d payload=%d total=%d", offsetN, payloadLenN, total)
		}

		if payloadLenN > 0 {
			C.memcpy(
				unsafe.Pointer(&msg[offsetN]),
				unsafe.Pointer(payloadPtr),
				C.size_t(payloadLenN),
			)
		}

		if err := c.repostRecvSlot(); err != nil {
			return nil, err
		}

		received += payloadLenN
		if received == total {
			return msg, nil
		}
	}
}

func (c *verbsMessageConn) repostRecvSlot() error {
	var cErr *C.char
	rc := C.go_rdma_repost_recv(c.cc, &cErr)
	if rc != 0 {
		return rdmaCError("repost_recv", rc, cErr)
	}
	return nil
}

func (c *verbsMessageConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cc == nil {
		return net.ErrClosed
	}
	C.go_rdma_close(c.cc)
	c.cc = nil
	return nil
}

func (c *verbsMessageConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *verbsMessageConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func rdmaContextTimeoutMillis(ctx context.Context) (C.int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return C.int(-1), nil
	}

	remain := time.Until(deadline)
	if remain <= 0 {
		return 0, context.DeadlineExceeded
	}

	ms := int64(remain / time.Millisecond)
	if remain%time.Millisecond != 0 {
		ms++
	}
	if ms > 2147483647 {
		ms = 2147483647
	}

	return C.int(ms), nil
}

func rdmaOpenTimeoutMillis(ctx context.Context) (C.int, error) {
	timeoutMS, err := rdmaContextTimeoutMillis(ctx)
	if err != nil {
		return 0, err
	}
	if timeoutMS >= 0 {
		return timeoutMS, nil
	}
	return C.int(verbsOpenTimeoutMills), nil
}

func rdmaCError(op string, rc C.int, cErr *C.char) error {
	msg := takeCString(cErr)
	if msg != "" {
		return fmt.Errorf("rdma verbs %s: %s", op, msg)
	}
	if rc != 0 {
		return fmt.Errorf("rdma verbs %s failed: rc=%d", op, int(rc))
	}
	return fmt.Errorf("rdma verbs %s failed", op)
}

func takeCString(cstr *C.char) string {
	if cstr == nil {
		return ""
	}
	s := C.GoString(cstr)
	C.free(unsafe.Pointer(cstr))
	return s
}
