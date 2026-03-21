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
#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
	// Keep only a short spin window, then yield to event/low-frequency wait.
	GO_RDMA_POLL_SPINS = 2,
	GO_RDMA_POLL_SLEEP_US = 1000,
	// Bound server-side accept idle wait so listener close can stop accept
	// workers promptly without relying on a peer event to wake rdma_get_cm_event.
	GO_RDMA_ACCEPT_POLL_TIMEOUT_MS = 100,
	// Bound server-side accept handshake so one bad peer does not stall Accept.
	// Keep this reasonably large to tolerate connection bursts.
	GO_RDMA_ACCEPT_HANDSHAKE_TIMEOUT_MS = 15000,
	// Messages at or above this size switch data plane to RDMA WRITE.
	GO_RDMA_RW_THRESHOLD_BYTES = 64 * 1024,
	// Per-connection remote-write receive buffer capacity is derived from
	// frame_cap with bounds to avoid excessive MR memory footprint.
	// Defaults are tuned to cover multi-MiB payloads in offset-based zcopy path.
	GO_RDMA_RW_RECV_CAP_MIN_BYTES = 256 * 1024,
	GO_RDMA_RW_RECV_CAP_MAX_BYTES = 4 * 1024 * 1024,
	GO_RDMA_RW_RECV_CAP_FRAMES = 32,
	// Number of per-connection RW receive slots to reduce overwrite risk
	// when multiple RW messages are in-flight.
	GO_RDMA_RW_RECV_SLOTS = 8,
	GO_RDMA_RW_NOTIFY_KIND_WRITE = 1,
	GO_RDMA_RW_NOTIFY_KIND_WRITE_AT = 2,
};

#define GO_RDMA_RW_DESC_MAGIC 0x5244574445534331ULL
#define GO_RDMA_RW_NOTIFY_MAGIC 0x5244574e4f544659ULL

typedef struct {
	uint8_t *buf;
	struct ibv_mr *mr;
} go_rdma_recv_slot;

typedef struct {
	uint64_t magic;
	uint64_t addr;
	uint32_t rkey;
	uint32_t cap;
	uint32_t slots;
} __attribute__((packed)) go_rdma_rw_desc;

typedef struct {
	uint64_t magic;
	uint32_t kind;
	uint32_t length;
	uint32_t slot;
} __attribute__((packed)) go_rdma_rw_notify;

typedef struct {
	struct rdma_cm_id *id;
	struct rdma_event_channel *cm_channel;
	struct ibv_pd *pd;
	struct ibv_comp_channel *send_comp_ch;
	struct ibv_comp_channel *recv_comp_ch;
	struct ibv_cq *send_cq;
	struct ibv_cq *recv_cq;
	int manual_resources;
	uint8_t *send_buf;
	struct ibv_mr *send_mr;
	uint32_t send_depth;
	uint32_t send_completed;
	uint32_t send_signal_interval;
	uint32_t tx_prod;
	go_rdma_recv_slot *notify_slots;
	uint32_t notify_depth;
	uint32_t recv_depth;
	uint32_t frame_cap;
	uint32_t inline_threshold;
	uint32_t last_notify_slot;
	int has_last_notify_slot;
	uint8_t *rw_recv_buf;
	struct ibv_mr *rw_recv_mr;
	uint32_t rw_recv_cap;
	uint32_t rw_recv_slots;
	int rw_recv_external;
	uint64_t peer_rw_addr;
	uint32_t peer_rw_rkey;
	uint32_t peer_rw_cap;
	uint32_t peer_rw_slots;
	uint32_t rw_send_seq;
	int rw_ready;
} go_rdma_conn;

typedef struct {
	struct rdma_cm_id *listen_id;
	struct rdma_event_channel *cm_channel;
	uint32_t frame_cap;
	uint32_t send_wr;
	uint32_t recv_wr;
	uint32_t inline_threshold;
	uint32_t send_signal_interval;
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
	int notify_armed = 0;
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

		// Prefer event-driven CQ waiting when the CQ has a completion channel.
		// This keeps CPU usage low under light or bursty traffic.
		struct ibv_comp_channel *comp_ch = cq->channel;
		if (comp_ch != NULL) {
			if (!notify_armed) {
				if (ibv_req_notify_cq(cq, 0) != 0) {
					go_rdma_set_errno(err_out, "ibv_req_notify_cq");
					return errno != 0 ? errno : EIO;
				}
				notify_armed = 1;

				// Close the race between poll and arm: re-check CQ immediately.
				n = ibv_poll_cq(cq, 1, wc);
				if (n > 0) {
					return 0;
				}
				if (n < 0) {
					go_rdma_set_errno(err_out, op);
					return errno != 0 ? errno : EIO;
				}
			}

			int wait_ms = -1;
			if (deadline_us >= 0) {
				int64_t now_us = go_rdma_now_monotonic_us();
				if (now_us < 0) {
					go_rdma_set_errno(err_out, "clock_gettime");
					return errno != 0 ? errno : EIO;
				}
				if (now_us >= deadline_us) {
					return EAGAIN;
				}
				int64_t remain_us = deadline_us - now_us;
				wait_ms = (int)((remain_us + 999LL) / 1000LL);
				if (wait_ms < 1) {
					wait_ms = 1;
				}
			}

			struct pollfd pfd;
			memset(&pfd, 0, sizeof(pfd));
			pfd.fd = comp_ch->fd;
			pfd.events = POLLIN;

			int prc;
			do {
				prc = poll(&pfd, 1, wait_ms);
			} while (prc < 0 && errno == EINTR);

			if (prc == 0) {
				return EAGAIN;
			}
			if (prc < 0) {
				go_rdma_set_errno(err_out, "poll(cq_event)");
				return errno != 0 ? errno : EIO;
			}

			struct ibv_cq *event_cq = NULL;
			void *event_ctx = NULL;
			if (ibv_get_cq_event(comp_ch, &event_cq, &event_ctx) != 0) {
				go_rdma_set_errno(err_out, "ibv_get_cq_event");
				return errno != 0 ? errno : EIO;
			}
			ibv_ack_cq_events(event_cq, 1);
			notify_armed = 0;
			spins = 0;
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
	if (conn->rw_recv_mr != NULL) {
		rdma_dereg_mr(conn->rw_recv_mr);
		conn->rw_recv_mr = NULL;
	}

	if (conn->notify_slots != NULL) {
		for (uint32_t i = 0; i < conn->notify_depth; i++) {
			if (conn->notify_slots[i].mr != NULL) {
				rdma_dereg_mr(conn->notify_slots[i].mr);
				conn->notify_slots[i].mr = NULL;
			}
			if (conn->notify_slots[i].buf != NULL) {
				free(conn->notify_slots[i].buf);
				conn->notify_slots[i].buf = NULL;
			}
		}
		free(conn->notify_slots);
		conn->notify_slots = NULL;
	}

	if (conn->send_buf != NULL) {
		free(conn->send_buf);
		conn->send_buf = NULL;
	}
	if (conn->rw_recv_buf != NULL) {
		if (!conn->rw_recv_external) {
			free(conn->rw_recv_buf);
		}
		conn->rw_recv_buf = NULL;
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
				if (conn->send_comp_ch != NULL) {
					ibv_destroy_comp_channel(conn->send_comp_ch);
					conn->send_comp_ch = NULL;
				}
				if (conn->recv_comp_ch != NULL) {
					ibv_destroy_comp_channel(conn->recv_comp_ch);
					conn->recv_comp_ch = NULL;
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
		rdma_destroy_id(listener->listen_id);
		listener->listen_id = NULL;
	}
	if (listener->cm_channel != NULL) {
		rdma_destroy_event_channel(listener->cm_channel);
		listener->cm_channel = NULL;
	}

	free(listener);
}

static int go_rdma_ensure_ready(go_rdma_conn *conn, int timeout_ms, char **err_out);
static int go_rdma_exchange_rw_desc(go_rdma_conn *conn, int timeout_ms, char **err_out);
static int go_rdma_send_message(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t total_len,
	int timeout_ms,
	char **err_out
);
static int go_rdma_send_message_rw_at(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t total_len,
	uint32_t remote_offset,
	int timeout_ms,
	char **err_out
);
static int go_rdma_recv_frame(
	go_rdma_conn *conn,
	uint8_t **payload_ptr,
	uint32_t *payload_len,
	uint32_t *total_len,
	uint32_t *offset,
	int timeout_ms,
	char **err_out
);
static int go_rdma_release_last_recv_slot(go_rdma_conn *conn, char **err_out);
static int go_rdma_get_rw_recv_payload_at(
	go_rdma_conn *conn,
	uint32_t remote_offset,
	uint32_t payload_len,
	uint8_t **payload_ptr,
	char **err_out
);

static uint32_t go_rdma_choose_rw_recv_cap(uint32_t frame_cap) {
	uint64_t cap = (uint64_t)frame_cap * (uint64_t)GO_RDMA_RW_RECV_CAP_FRAMES;
	if (cap < GO_RDMA_RW_RECV_CAP_MIN_BYTES) {
		cap = GO_RDMA_RW_RECV_CAP_MIN_BYTES;
	}
	if (cap > GO_RDMA_RW_RECV_CAP_MAX_BYTES) {
		cap = GO_RDMA_RW_RECV_CAP_MAX_BYTES;
	}
	return (uint32_t)cap;
}

static int go_rdma_wait_send_completion_with_id(
	go_rdma_conn *conn,
	int timeout_ms,
	const char *op,
	uint64_t *wr_id_out,
	char **err_out
) {
	struct ibv_cq *send_cq = go_rdma_send_cq(conn);
	struct ibv_wc wc;
	int rc = go_rdma_poll_cq_with_timeout(send_cq, &wc, timeout_ms, op, err_out);
	if (rc != 0) {
		return rc;
	}
	if (wc.status != IBV_WC_SUCCESS) {
		go_rdma_set_err(err_out, "%s completion failed: %s", op, ibv_wc_status_str(wc.status));
		return EIO;
	}
	if (wr_id_out != NULL) {
		*wr_id_out = (uint64_t)wc.wr_id;
	}
	return 0;
}

static int go_rdma_wait_recv_completion(go_rdma_conn *conn, int timeout_ms, const char *op, struct ibv_wc *wc_out, char **err_out) {
	if (wc_out == NULL) {
		go_rdma_set_err(err_out, "%s: nil wc_out", op);
		return EINVAL;
	}
	struct ibv_cq *recv_cq = go_rdma_recv_cq(conn);
	int rc = go_rdma_poll_cq_with_timeout(recv_cq, wc_out, timeout_ms, op, err_out);
	if (rc != 0) {
		return rc;
	}
	if (wc_out->status != IBV_WC_SUCCESS) {
		go_rdma_set_err(err_out, "%s completion failed: %s", op, ibv_wc_status_str(wc_out->status));
		return EIO;
	}
	return 0;
}

static int go_rdma_reap_data_send_completion(go_rdma_conn *conn, int timeout_ms, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_reap_data_send_completion: conn closed");
		return EBADF;
	}

	for (;;) {
		uint64_t wr_id = 0;
		int rc = go_rdma_wait_send_completion_with_id(conn, timeout_ms, "ibv_poll_cq(send_data)", &wr_id, err_out);
		if (rc != 0) {
			return rc;
		}

		uint32_t wr32 = (uint32_t)wr_id;
		uint32_t done = wr32 + 1;
		if ((int32_t)(done - conn->send_completed) > 0) {
			conn->send_completed = done;
		}
		return 0;
	}
}

static int go_rdma_wait_local_send_slot(go_rdma_conn *conn, int timeout_ms, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_wait_local_send_slot: conn closed");
		return EBADF;
	}
	if (conn->send_depth == 0) {
		go_rdma_set_err(err_out, "go_rdma_wait_local_send_slot: send depth not initialized");
		return EINVAL;
	}

	while ((conn->tx_prod - conn->send_completed) >= conn->send_depth) {
		int rc = go_rdma_reap_data_send_completion(conn, timeout_ms, err_out);
		if (rc != 0) {
			return rc;
		}
	}
	return 0;
}

static int go_rdma_wait_send_completed_until(
	go_rdma_conn *conn,
	uint32_t target_completed,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_wait_send_completed_until: conn closed");
		return EBADF;
	}

	while ((int32_t)(target_completed - conn->send_completed) > 0) {
		int rc = go_rdma_reap_data_send_completion(conn, timeout_ms, err_out);
		if (rc != 0) {
			return rc;
		}
	}
	return 0;
}

static int go_rdma_post_notify_recv_slot(go_rdma_conn *conn, uint32_t idx, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_post_notify_recv_slot: conn closed");
		return EBADF;
	}
	if (conn->notify_slots == NULL || idx >= conn->notify_depth) {
		go_rdma_set_err(err_out, "go_rdma_post_notify_recv_slot: invalid slot idx=%u", idx);
		return EINVAL;
	}

	go_rdma_recv_slot *slot = &conn->notify_slots[idx];
	int rc = rdma_post_recv(
		conn->id,
		(void *)(uintptr_t)(idx + 1),
		slot->buf,
		conn->frame_cap,
		slot->mr
	);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_post_recv(recv_data)");
		return errno != 0 ? errno : EIO;
	}
	return 0;
}

static int go_rdma_init_notify_slots(go_rdma_conn *conn, uint32_t depth, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_init_notify_slots: conn closed");
		return EBADF;
	}
	if (depth == 0) {
		go_rdma_set_err(err_out, "go_rdma_init_notify_slots: depth must be > 0");
		return EINVAL;
	}
	if (conn->notify_slots != NULL) {
		if (conn->notify_depth == depth) {
			return 0;
		}
		go_rdma_set_err(
			err_out,
			"go_rdma_init_notify_slots: notify slots already initialized depth=%u (want=%u)",
			conn->notify_depth,
			depth
		);
		return EINVAL;
	}

	conn->notify_slots = (go_rdma_recv_slot *)calloc(depth, sizeof(go_rdma_recv_slot));
	if (conn->notify_slots == NULL) {
		go_rdma_set_errno(err_out, "calloc notify slots");
		return errno != 0 ? errno : ENOMEM;
	}
	conn->notify_depth = depth;
	if (conn->frame_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_init_notify_slots: frame_cap must be > 0");
		return EINVAL;
	}

	for (uint32_t i = 0; i < depth; i++) {
		conn->notify_slots[i].buf = (uint8_t *)malloc(conn->frame_cap);
		if (conn->notify_slots[i].buf == NULL) {
			go_rdma_set_errno(err_out, "malloc notify recv buffer");
			return errno != 0 ? errno : ENOMEM;
		}

		conn->notify_slots[i].mr = rdma_reg_msgs(conn->id, conn->notify_slots[i].buf, conn->frame_cap);
		if (conn->notify_slots[i].mr == NULL) {
			go_rdma_set_errno(err_out, "rdma_reg_msgs notify recv");
			return errno != 0 ? errno : EIO;
		}

		int rc = go_rdma_post_notify_recv_slot(conn, i, err_out);
		if (rc != 0) {
			return rc;
		}
	}

	return 0;
}

static int go_rdma_exchange_rw_desc(go_rdma_conn *conn, int timeout_ms, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_exchange_rw_desc: conn closed");
		return EBADF;
	}
	if (conn->rw_ready) {
		return 0;
	}
	if (conn->rw_recv_buf == NULL || conn->rw_recv_mr == NULL || conn->rw_recv_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_exchange_rw_desc: rw recv buffer unavailable");
		return EINVAL;
	}

	go_rdma_rw_desc local_desc;
	memset(&local_desc, 0, sizeof(local_desc));
	local_desc.magic = GO_RDMA_RW_DESC_MAGIC;
	local_desc.addr = (uint64_t)(uintptr_t)conn->rw_recv_buf;
	local_desc.rkey = conn->rw_recv_mr->rkey;
	local_desc.cap = conn->rw_recv_cap;
	local_desc.slots = conn->rw_recv_slots;

	int rc = go_rdma_send_message(
		conn,
		(const uint8_t *)&local_desc,
		(uint32_t)sizeof(local_desc),
		timeout_ms,
		err_out
	);
	if (rc != 0) {
		return rc;
	}

	uint8_t *payload_ptr = NULL;
	uint32_t payload_len = 0;
	uint32_t total_len = 0;
	uint32_t offset = 0;
	rc = go_rdma_recv_frame(
		conn,
		&payload_ptr,
		&payload_len,
		&total_len,
		&offset,
		timeout_ms,
		err_out
	);
	if (rc != 0) {
		return rc;
	}
	if (payload_len != sizeof(go_rdma_rw_desc) || total_len != payload_len || offset != 0) {
		go_rdma_set_err(
			err_out,
			"go_rdma_exchange_rw_desc: invalid peer desc frame payload=%u total=%u offset=%u",
			payload_len,
			total_len,
			offset
		);
		return EPROTO;
	}

	go_rdma_rw_desc peer_desc;
	memset(&peer_desc, 0, sizeof(peer_desc));
	memcpy(&peer_desc, payload_ptr, sizeof(peer_desc));

	rc = go_rdma_release_last_recv_slot(conn, err_out);
	if (rc != 0) {
		return rc;
	}

	if (peer_desc.magic != GO_RDMA_RW_DESC_MAGIC || peer_desc.addr == 0 || peer_desc.rkey == 0 || peer_desc.cap == 0 || peer_desc.slots == 0) {
		go_rdma_set_err(err_out, "go_rdma_exchange_rw_desc: invalid peer desc");
		return EPROTO;
	}

	conn->peer_rw_addr = peer_desc.addr;
	conn->peer_rw_rkey = peer_desc.rkey;
	conn->peer_rw_cap = peer_desc.cap;
	conn->peer_rw_slots = peer_desc.slots;
	conn->rw_ready = 1;
	return 0;
}

static int go_rdma_ensure_ready(go_rdma_conn *conn, int timeout_ms, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_ensure_ready: conn closed");
		return EBADF;
	}

	if (conn->notify_slots == NULL) {
		int rc = go_rdma_init_notify_slots(conn, conn->recv_depth, err_out);
		if (rc != 0) {
			return rc;
		}
	}
	if (!conn->rw_ready) {
		int rc = go_rdma_exchange_rw_desc(conn, timeout_ms, err_out);
		if (rc != 0) {
			return rc;
		}
	}

	return 0;
}

static int go_rdma_open(
	const char *host,
	const char *port,
	uint32_t frame_cap,
	uint32_t send_wr,
	uint32_t recv_wr,
	uint32_t inline_threshold,
	uint32_t send_signal_interval,
	uint8_t *rw_mem,
	uint32_t rw_mem_len,
	int timeout_ms,
	go_rdma_conn **out,
	char **err_out
) {
	if (out == NULL) {
		go_rdma_set_err(err_out, "go_rdma_open: out is nil");
		return EINVAL;
	}
	*out = NULL;

	if (frame_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_open: invalid frame_cap=%u", frame_cap);
		return EINVAL;
	}
	if (send_wr == 0 || recv_wr == 0) {
		go_rdma_set_err(err_out, "go_rdma_open: queue depth must be > 0");
		return EINVAL;
	}
	if (send_signal_interval == 0) {
		send_signal_interval = 1;
	}
	if (rw_mem == NULL && rw_mem_len > 0) {
		go_rdma_set_err(err_out, "go_rdma_open: rw_mem_len provided but rw_mem is nil");
		return EINVAL;
	}
	if (rw_mem != NULL && rw_mem_len < frame_cap) {
		go_rdma_set_err(
			err_out,
			"go_rdma_open: external rw memory too small rw_mem_len=%u frame_cap=%u",
			rw_mem_len,
			frame_cap
		);
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
	go_rdma_conn *conn = NULL;
	int rc = rdma_getaddrinfo(host, port, &hints, &res);
	if (rc != 0) {
		go_rdma_set_err(err_out, "rdma_getaddrinfo: %s", gai_strerror(rc));
		return rc > 0 ? rc : EINVAL;
	}

	conn = (go_rdma_conn *)calloc(1, sizeof(go_rdma_conn));
	if (conn == NULL) {
		go_rdma_set_errno(err_out, "calloc go_rdma_conn");
		rc = errno != 0 ? errno : ENOMEM;
		goto cleanup;
	}

	conn->cm_channel = rdma_create_event_channel();
	if (conn->cm_channel == NULL) {
		go_rdma_set_errno(err_out, "rdma_create_event_channel(open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	rc = rdma_create_id(conn->cm_channel, &conn->id, NULL, RDMA_PS_TCP);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_create_id(open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	struct sockaddr *src_addr = NULL;
	struct sockaddr *dst_addr = NULL;
	if (res->ai_src_addr != NULL) {
		src_addr = (struct sockaddr *)res->ai_src_addr;
	}
	if (res->ai_dst_addr != NULL) {
		dst_addr = (struct sockaddr *)res->ai_dst_addr;
	}
	if (dst_addr == NULL) {
		go_rdma_set_err(err_out, "go_rdma_open: rdma destination address is nil");
		rc = EINVAL;
		goto cleanup;
	}

	int cm_timeout_ms = timeout_ms >= 0 ? timeout_ms : 30000;

	rc = rdma_resolve_addr(conn->id, src_addr, dst_addr, cm_timeout_ms);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_resolve_addr");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}
	rc = go_rdma_wait_cm_event(conn->cm_channel, cm_timeout_ms, RDMA_CM_EVENT_ADDR_RESOLVED, "rdma_resolve_addr", err_out);
	if (rc != 0) {
		goto cleanup;
	}

	rc = rdma_resolve_route(conn->id, cm_timeout_ms);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_resolve_route");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}
	rc = go_rdma_wait_cm_event(conn->cm_channel, cm_timeout_ms, RDMA_CM_EVENT_ROUTE_RESOLVED, "rdma_resolve_route", err_out);
	if (rc != 0) {
		goto cleanup;
	}

	conn->frame_cap = frame_cap;
	conn->send_depth = send_wr;
	conn->recv_depth = recv_wr;
	conn->inline_threshold = inline_threshold;
	conn->send_signal_interval = send_signal_interval;
	conn->send_completed = 0;
	conn->has_last_notify_slot = 0;
	conn->manual_resources = 1;

	conn->pd = ibv_alloc_pd(conn->id->verbs);
	if (conn->pd == NULL) {
		go_rdma_set_errno(err_out, "ibv_alloc_pd(open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	conn->send_comp_ch = ibv_create_comp_channel(conn->id->verbs);
	if (conn->send_comp_ch == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_comp_channel(send,open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}
	conn->recv_comp_ch = ibv_create_comp_channel(conn->id->verbs);
	if (conn->recv_comp_ch == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_comp_channel(recv,open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	int cq_capacity = (int)(send_wr + recv_wr + 16);
	if (cq_capacity < 64) {
		cq_capacity = 64;
	}

	conn->send_cq = ibv_create_cq(conn->id->verbs, cq_capacity, NULL, conn->send_comp_ch, 0);
	if (conn->send_cq == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_cq(send,open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}
	conn->recv_cq = ibv_create_cq(conn->id->verbs, cq_capacity, NULL, conn->recv_comp_ch, 0);
	if (conn->recv_cq == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_cq(recv,open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	struct ibv_qp_init_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.send_cq = conn->send_cq;
	qp_attr.recv_cq = conn->recv_cq;
	qp_attr.cap.max_send_wr = send_wr;
	qp_attr.cap.max_recv_wr = recv_wr;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;
	qp_attr.cap.max_inline_data = inline_threshold;
	rc = rdma_create_qp(conn->id, conn->pd, &qp_attr);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_create_qp(open)");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	size_t send_region_size = (size_t)frame_cap * (size_t)send_wr;
	if (send_wr > 0 && send_region_size / send_wr != frame_cap) {
		go_rdma_set_err(err_out, "send ring size overflow");
		rc = EOVERFLOW;
		goto cleanup;
	}

	conn->send_buf = (uint8_t *)malloc(send_region_size);
	if (conn->send_buf == NULL) {
		go_rdma_set_errno(err_out, "malloc send buffer");
		rc = errno != 0 ? errno : ENOMEM;
		goto cleanup;
	}

	conn->send_mr = rdma_reg_msgs(conn->id, conn->send_buf, send_region_size);
	if (conn->send_mr == NULL) {
		go_rdma_set_errno(err_out, "rdma_reg_msgs send");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}

	size_t rw_recv_cap = 0;
	size_t rw_recv_slots = 0;
	size_t rw_recv_size = 0;
	if (rw_mem != NULL && rw_mem_len > 0) {
		size_t rw_mem_total = (size_t)rw_mem_len;
		conn->rw_recv_external = 1;
		if (rw_mem_total > UINT32_MAX) {
			go_rdma_set_err(
				err_out,
				"go_rdma_open: external rw memory too large rw_mem_len=%u max=%u",
				rw_mem_len,
				(uint32_t)UINT32_MAX
			);
			rc = EINVAL;
			goto cleanup;
		}
		// External shared memory is fully registered so offset-based addressing
		// can target arbitrary caller-owned regions.
		rw_recv_cap = rw_mem_total;
		rw_recv_slots = 1;
		rw_recv_size = rw_mem_total;
		conn->rw_recv_buf = rw_mem;
	} else {
		rw_recv_cap = (size_t)go_rdma_choose_rw_recv_cap(frame_cap);
		rw_recv_slots = (size_t)GO_RDMA_RW_RECV_SLOTS;
		rw_recv_size = rw_recv_cap * rw_recv_slots;
		if (rw_recv_slots > 0 && rw_recv_size / rw_recv_slots != rw_recv_cap) {
			go_rdma_set_err(err_out, "rw recv region size overflow");
			rc = EOVERFLOW;
			goto cleanup;
		}
		conn->rw_recv_buf = (uint8_t *)malloc(rw_recv_size);
		if (conn->rw_recv_buf == NULL) {
			go_rdma_set_errno(err_out, "malloc rw recv buffer");
			rc = errno != 0 ? errno : ENOMEM;
			goto cleanup;
		}
	}
	conn->rw_recv_mr = ibv_reg_mr(
		conn->pd,
		conn->rw_recv_buf,
		rw_recv_size,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
	);
	if (conn->rw_recv_mr == NULL) {
		if (conn->rw_recv_external) {
			if (errno == EFAULT) {
				go_rdma_set_err(
					err_out,
					"ibv_reg_mr rw recv(external): %s (addr=%p len=%zu). hint: prefer anonymous or memfd-backed mmap for long-term RDMA pin",
					strerror(errno),
					(void *)conn->rw_recv_buf,
					rw_recv_size
				);
			} else {
				go_rdma_set_err(
					err_out,
					"ibv_reg_mr rw recv(external): %s (addr=%p len=%zu)",
					strerror(errno),
					(void *)conn->rw_recv_buf,
					rw_recv_size
				);
			}
		} else {
			go_rdma_set_errno(err_out, "ibv_reg_mr rw recv");
		}
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}
	conn->rw_recv_cap = (uint32_t)rw_recv_cap;
	conn->rw_recv_slots = (uint32_t)rw_recv_slots;

	struct rdma_conn_param conn_param;
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;

	rc = rdma_connect(conn->id, &conn_param);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_connect");
		rc = errno != 0 ? errno : EIO;
		goto cleanup;
	}
	rc = go_rdma_wait_cm_event(conn->cm_channel, cm_timeout_ms, RDMA_CM_EVENT_ESTABLISHED, "rdma_connect(established)", err_out);
	if (rc != 0) {
		goto cleanup;
	}

	rc = go_rdma_ensure_ready(conn, timeout_ms, err_out);
	if (rc != 0) {
		goto cleanup;
	}

	*out = conn;
	conn = NULL;
	rc = 0;

cleanup:
	if (res != NULL) {
		rdma_freeaddrinfo(res);
	}
	if (conn != NULL) {
		go_rdma_free_conn(conn);
	}
	return rc;
}

static int go_rdma_listen(
	const char *host,
	const char *port,
	uint32_t frame_cap,
	uint32_t send_wr,
	uint32_t recv_wr,
	uint32_t inline_threshold,
	uint32_t send_signal_interval,
	int backlog,
	go_rdma_listener **out,
	char **err_out
) {
	if (out == NULL) {
		go_rdma_set_err(err_out, "go_rdma_listen: out is nil");
		return EINVAL;
	}
	*out = NULL;

	if (frame_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_listen: invalid frame_cap=%u", frame_cap);
		return EINVAL;
	}
	if (send_wr == 0 || recv_wr == 0) {
		go_rdma_set_err(err_out, "go_rdma_listen: queue depth must be > 0");
		return EINVAL;
	}
	if (send_signal_interval == 0) {
		send_signal_interval = 1;
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

	struct rdma_event_channel *cm_channel = rdma_create_event_channel();
	if (cm_channel == NULL) {
		rdma_freeaddrinfo(res);
		go_rdma_set_errno(err_out, "rdma_create_event_channel(listen)");
		return errno != 0 ? errno : EIO;
	}

	struct rdma_cm_id *listen_id = NULL;
	rc = rdma_create_id(cm_channel, &listen_id, NULL, RDMA_PS_TCP);
	if (rc != 0) {
		rdma_freeaddrinfo(res);
		go_rdma_set_errno(err_out, "rdma_create_id(listen)");
		rdma_destroy_event_channel(cm_channel);
		return errno != 0 ? errno : EIO;
	}

	struct sockaddr *bind_addr = NULL;
	if (res->ai_src_addr != NULL) {
		bind_addr = (struct sockaddr *)res->ai_src_addr;
	} else if (res->ai_dst_addr != NULL) {
		bind_addr = (struct sockaddr *)res->ai_dst_addr;
	}

	if (bind_addr == NULL) {
		rdma_freeaddrinfo(res);
		go_rdma_set_err(err_out, "rdma bind address is nil");
		rdma_destroy_id(listen_id);
		rdma_destroy_event_channel(cm_channel);
		return EINVAL;
	}

	rc = rdma_bind_addr(listen_id, bind_addr);
	rdma_freeaddrinfo(res);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_bind_addr(listen)");
		rdma_destroy_id(listen_id);
		rdma_destroy_event_channel(cm_channel);
		return errno != 0 ? errno : EIO;
	}

	rc = rdma_listen(listen_id, backlog);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_listen");
		rdma_destroy_id(listen_id);
		rdma_destroy_event_channel(cm_channel);
		return errno != 0 ? errno : EIO;
	}

	go_rdma_listener *listener = (go_rdma_listener *)calloc(1, sizeof(go_rdma_listener));
	if (listener == NULL) {
		go_rdma_set_errno(err_out, "calloc go_rdma_listener");
		rdma_destroy_id(listen_id);
		rdma_destroy_event_channel(cm_channel);
		return errno != 0 ? errno : ENOMEM;
	}

	listener->listen_id = listen_id;
	listener->cm_channel = cm_channel;
	listener->frame_cap = frame_cap;
	listener->send_wr = send_wr;
	listener->recv_wr = recv_wr;
	listener->inline_threshold = inline_threshold;
	listener->send_signal_interval = send_signal_interval;

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

	if (listener == NULL || listener->listen_id == NULL || listener->cm_channel == NULL) {
		go_rdma_set_err(err_out, "go_rdma_accept: listener closed");
		return EBADF;
	}

	struct rdma_cm_id *id = NULL;
	struct rdma_cm_event *event = NULL;
	for (;;) {
		struct pollfd pfd;
		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = listener->cm_channel->fd;
		pfd.events = POLLIN;

		int poll_rc = poll(&pfd, 1, GO_RDMA_ACCEPT_POLL_TIMEOUT_MS);
		if (poll_rc == 0) {
			return EAGAIN;
		}
		if (poll_rc < 0) {
			if (errno == EINTR) {
				continue;
			}
			go_rdma_set_errno(err_out, "poll(accept)");
			return errno != 0 ? errno : EIO;
		}

		int rc = rdma_get_cm_event(listener->cm_channel, &event);
		if (rc != 0) {
			go_rdma_set_errno(err_out, "rdma_get_cm_event(accept)");
			return errno != 0 ? errno : EIO;
		}

		enum rdma_cm_event_type ev_type = event->event;
		int ev_status = event->status;
		struct rdma_cm_id *ev_id = event->id;
		rdma_ack_cm_event(event);
		event = NULL;

		if (ev_type == RDMA_CM_EVENT_CONNECT_REQUEST) {
			if (ev_status != 0 || ev_id == NULL) {
				if (ev_id != NULL) {
					rdma_reject(ev_id, NULL, 0);
					rdma_destroy_id(ev_id);
				}
				go_rdma_set_err(err_out, "connect request event status=%d", ev_status);
				return EIO;
			}
			id = ev_id;
			break;
		}

		// Keep listener CM queue healthy by draining all other events here.
		// Connection lifecycle events on child IDs are not needed by this data path.
		if (ev_id != NULL && ev_id != listener->listen_id) {
			switch (ev_type) {
			case RDMA_CM_EVENT_DISCONNECTED:
			case RDMA_CM_EVENT_REJECTED:
			case RDMA_CM_EVENT_CONNECT_ERROR:
			case RDMA_CM_EVENT_UNREACHABLE:
			case RDMA_CM_EVENT_ADDR_CHANGE:
			case RDMA_CM_EVENT_TIMEWAIT_EXIT:
				rdma_destroy_id(ev_id);
				break;
			default:
				break;
			}
		}
	}

	go_rdma_conn *conn = (go_rdma_conn *)calloc(1, sizeof(go_rdma_conn));
	if (conn == NULL) {
		go_rdma_set_errno(err_out, "calloc go_rdma_conn(accept)");
		rdma_reject(id, NULL, 0);
		rdma_destroy_id(id);
		return errno != 0 ? errno : ENOMEM;
	}

	struct rdma_event_channel *conn_channel = rdma_create_event_channel();
	if (conn_channel == NULL) {
		go_rdma_set_errno(err_out, "rdma_create_event_channel(accept)");
		go_rdma_free_conn(conn);
		rdma_reject(id, NULL, 0);
		rdma_destroy_id(id);
		return errno != 0 ? errno : EIO;
	}
	conn->cm_channel = conn_channel;
	int rc = rdma_migrate_id(id, conn_channel);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_migrate_id(accept)");
		go_rdma_free_conn(conn);
		rdma_reject(id, NULL, 0);
		return errno != 0 ? errno : EIO;
	}

	conn->id = id;
	conn->frame_cap = listener->frame_cap;
	conn->send_depth = listener->send_wr;
	conn->recv_depth = listener->recv_wr;
	conn->inline_threshold = listener->inline_threshold;
	conn->send_signal_interval = listener->send_signal_interval;
	conn->send_completed = 0;
	conn->has_last_notify_slot = 0;
	conn->manual_resources = 1;

	conn->pd = ibv_alloc_pd(conn->id->verbs);
	if (conn->pd == NULL) {
		go_rdma_set_errno(err_out, "ibv_alloc_pd(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	int cq_capacity = (int)(listener->send_wr + listener->recv_wr + 16);
	if (cq_capacity < 64) {
		cq_capacity = 64;
	}
	conn->send_comp_ch = ibv_create_comp_channel(conn->id->verbs);
	if (conn->send_comp_ch == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_comp_channel(send,accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}
	conn->recv_comp_ch = ibv_create_comp_channel(conn->id->verbs);
	if (conn->recv_comp_ch == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_comp_channel(recv,accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	conn->send_cq = ibv_create_cq(conn->id->verbs, cq_capacity, NULL, conn->send_comp_ch, 0);
	if (conn->send_cq == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_cq(send,accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}
	conn->recv_cq = ibv_create_cq(conn->id->verbs, cq_capacity, NULL, conn->recv_comp_ch, 0);
	if (conn->recv_cq == NULL) {
		go_rdma_set_errno(err_out, "ibv_create_cq(recv,accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	struct ibv_qp_init_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.send_cq = conn->send_cq;
	qp_attr.recv_cq = conn->recv_cq;
	qp_attr.cap.max_send_wr = listener->send_wr;
	qp_attr.cap.max_recv_wr = listener->recv_wr;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;
	qp_attr.cap.max_inline_data = listener->inline_threshold;
	rc = rdma_create_qp(conn->id, conn->pd, &qp_attr);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "rdma_create_qp(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	size_t send_region_size = (size_t)listener->frame_cap * (size_t)listener->send_wr;
	if (listener->send_wr > 0 && send_region_size / listener->send_wr != listener->frame_cap) {
		go_rdma_set_err(err_out, "send ring size overflow(accept)");
		go_rdma_free_conn(conn);
		return EOVERFLOW;
	}
	conn->send_buf = (uint8_t *)malloc(send_region_size);
	if (conn->send_buf == NULL) {
		go_rdma_set_errno(err_out, "malloc send buffer(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : ENOMEM;
	}

	conn->send_mr = rdma_reg_msgs(conn->id, conn->send_buf, send_region_size);
	if (conn->send_mr == NULL) {
		go_rdma_set_errno(err_out, "rdma_reg_msgs send(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}

	size_t rw_recv_cap = (size_t)go_rdma_choose_rw_recv_cap(listener->frame_cap);
	size_t rw_recv_slots = (size_t)GO_RDMA_RW_RECV_SLOTS;
	size_t rw_recv_size = rw_recv_cap * rw_recv_slots;
	if (rw_recv_slots > 0 && rw_recv_size / rw_recv_slots != rw_recv_cap) {
		go_rdma_set_err(err_out, "rw recv region size overflow(accept)");
		go_rdma_free_conn(conn);
		return EOVERFLOW;
	}
	conn->rw_recv_buf = (uint8_t *)malloc(rw_recv_size);
	if (conn->rw_recv_buf == NULL) {
		go_rdma_set_errno(err_out, "malloc rw recv buffer(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : ENOMEM;
	}
	conn->rw_recv_mr = ibv_reg_mr(
		conn->pd,
		conn->rw_recv_buf,
		rw_recv_size,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
	);
	if (conn->rw_recv_mr == NULL) {
		go_rdma_set_errno(err_out, "ibv_reg_mr rw recv(accept)");
		go_rdma_free_conn(conn);
		return errno != 0 ? errno : EIO;
	}
	conn->rw_recv_cap = (uint32_t)rw_recv_cap;
	conn->rw_recv_slots = (uint32_t)rw_recv_slots;

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
	rc = go_rdma_wait_cm_event(conn->cm_channel, GO_RDMA_ACCEPT_HANDSHAKE_TIMEOUT_MS, RDMA_CM_EVENT_ESTABLISHED, "rdma_accept(established)", err_out);
	if (rc != 0) {
		go_rdma_free_conn(conn);
		return rc;
	}

	rc = go_rdma_ensure_ready(conn, GO_RDMA_ACCEPT_HANDSHAKE_TIMEOUT_MS, err_out);
	if (rc != 0) {
		go_rdma_free_conn(conn);
		return rc;
	}

	*out = conn;
	return 0;
}

static int go_rdma_send_frame(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t payload_len,
	int force_signal,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_frame: conn closed");
		return EBADF;
	}

	if ((uint64_t)payload_len > conn->frame_cap) {
		go_rdma_set_err(err_out, "go_rdma_send_frame: payload_len=%u exceeds frame capacity", payload_len);
		return EMSGSIZE;
	}
	if (conn->send_depth == 0) {
		go_rdma_set_err(err_out, "go_rdma_send_frame: local send depth not initialized");
		return EINVAL;
	}

	int rc = go_rdma_wait_local_send_slot(conn, timeout_ms, err_out);
	if (rc != 0) {
		return rc;
	}

	uint32_t local_slot = conn->tx_prod % conn->send_depth;
	uint8_t *local_buf = conn->send_buf + ((size_t)local_slot * conn->frame_cap);
	uint32_t send_len = payload_len;

	int send_flags = 0;
	uint32_t outstanding = conn->tx_prod - conn->send_completed;
	int need_signal = force_signal ? 1 : 0;
	if (!need_signal) {
		if (conn->send_signal_interval <= 1) {
			need_signal = 1;
		} else if (((conn->tx_prod + 1) % conn->send_signal_interval) == 0) {
			need_signal = 1;
		} else if ((outstanding + 1) >= conn->send_depth) {
			// Always keep at least one signaled WQE to guarantee progress.
			need_signal = 1;
		}
	}
	if (need_signal) {
		send_flags |= IBV_SEND_SIGNALED;
	}
	if (conn->inline_threshold > 0 && send_len <= conn->inline_threshold) {
		send_flags |= IBV_SEND_INLINE;
	}

	uintptr_t send_addr = (uintptr_t)local_buf;
	uint32_t send_lkey = conn->send_mr->lkey;

	if ((send_flags & IBV_SEND_INLINE) != 0 && send_len > 0 && payload != NULL) {
		// Inline SEND copies payload into the WQE during ibv_post_send,
		// so we can directly use caller payload without staging memcpy.
		send_addr = (uintptr_t)payload;
		send_lkey = 0; // ignored for inline payload
	} else if (send_len > 0 && payload != NULL) {
		memcpy(local_buf, payload, send_len);
	}

	struct ibv_sge sge;
	memset(&sge, 0, sizeof(sge));
	sge.addr = send_addr;
	sge.length = send_len;
	sge.lkey = send_lkey;

	struct ibv_send_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uintptr_t)conn->tx_prod;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = send_flags;

	struct ibv_send_wr *bad_wr = NULL;
	rc = ibv_post_send(conn->id->qp, &wr, &bad_wr);
	if (rc != 0) {
		go_rdma_set_errno(err_out, "ibv_post_send(send_data)");
		return errno != 0 ? errno : EIO;
	}

	conn->tx_prod++;

	return 0;
}

static int go_rdma_send_message(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t total_len,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_message: conn closed");
		return EBADF;
	}

	if (total_len == 0) {
		return 0;
	}
	if (payload == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_message: payload is nil");
		return EINVAL;
	}

	uint32_t payload_cap = conn->frame_cap;
	if (payload_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_send_message: invalid payload capacity");
		return EINVAL;
	}

	uint32_t offset = 0;
	while (offset < total_len) {
		uint32_t remaining = total_len - offset;
		uint32_t chunk_len = payload_cap;
		if (chunk_len > remaining) {
			chunk_len = remaining;
		}
		int force_signal = (offset + chunk_len >= total_len) ? 1 : 0;
			int rc = go_rdma_send_frame(
				conn,
				payload + offset,
				chunk_len,
				force_signal,
				timeout_ms,
				err_out
		);
		if (rc != 0) {
			return rc;
		}
		offset += chunk_len;
	}

	// Do not flush on every message. Keep send path asynchronous and rely on
	// queue-depth pressure to reap completions when needed.
	return 0;
}

static int go_rdma_send_message_rw(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t total_len,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw: conn closed");
		return EBADF;
	}
	if (total_len == 0) {
		return 0;
	}
	if (payload == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw: payload is nil");
		return EINVAL;
	}
	if (!conn->rw_ready || conn->peer_rw_addr == 0 || conn->peer_rw_rkey == 0 || conn->peer_rw_cap == 0 || conn->peer_rw_slots == 0) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw: peer rw descriptor unavailable");
		return ENOTSUP;
	}
	if (total_len > conn->peer_rw_cap) {
		go_rdma_set_err(
			err_out,
			"go_rdma_send_message_rw: payload too large payload=%u peer_cap=%u",
			total_len,
			conn->peer_rw_cap
		);
		return EMSGSIZE;
	}
	if (conn->frame_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw: invalid frame capacity");
		return EINVAL;
	}

	// Try true zero-copy source for RDMA_WRITE by registering caller payload
	// directly as an MR. If registration fails, fall back to staged send_buf.
	struct ibv_mr *payload_mr = ibv_reg_mr(
		conn->pd,
		(void *)payload,
		(size_t)total_len,
		IBV_ACCESS_LOCAL_WRITE
	);
	int use_payload_mr = (payload_mr != NULL) ? 1 : 0;
	uint32_t writes_start = conn->tx_prod;
	int final_rc = 0;

	uint32_t offset = 0;
	uint32_t slot = conn->rw_send_seq % conn->peer_rw_slots;
	uint64_t remote_base = conn->peer_rw_addr + ((uint64_t)slot * (uint64_t)conn->peer_rw_cap);
	while (offset < total_len) {
		uint32_t remaining = total_len - offset;
		uint32_t chunk_len = conn->frame_cap;
		if (chunk_len > remaining) {
			chunk_len = remaining;
		}

		int rc = go_rdma_wait_local_send_slot(conn, timeout_ms, err_out);
		if (rc != 0) {
			final_rc = rc;
			goto cleanup;
		}

		uintptr_t sge_addr = 0;
		uint32_t sge_lkey = 0;
		if (use_payload_mr) {
			sge_addr = (uintptr_t)(payload + offset);
			sge_lkey = payload_mr->lkey;
		} else {
			int use_rw_recv_mr = 0;
			if (conn->rw_recv_buf != NULL && conn->rw_recv_mr != NULL && conn->rw_recv_cap > 0 && conn->rw_recv_slots > 0) {
				size_t rw_total = (size_t)conn->rw_recv_cap * (size_t)conn->rw_recv_slots;
				if (rw_total / conn->rw_recv_slots == conn->rw_recv_cap) {
					uintptr_t rw_begin = (uintptr_t)conn->rw_recv_buf;
					uintptr_t payload_addr = (uintptr_t)(payload + offset);
					if (payload_addr >= rw_begin) {
						size_t rel = (size_t)(payload_addr - rw_begin);
						if (rel <= rw_total && (size_t)chunk_len <= (rw_total - rel)) {
							use_rw_recv_mr = 1;
						}
					}
				}
			}

			if (use_rw_recv_mr) {
				sge_addr = (uintptr_t)(payload + offset);
				sge_lkey = conn->rw_recv_mr->lkey;
			} else {
				uint32_t local_slot = conn->tx_prod % conn->send_depth;
				uint8_t *local_buf = conn->send_buf + ((size_t)local_slot * conn->frame_cap);
				memcpy(local_buf, payload + offset, chunk_len);
				sge_addr = (uintptr_t)local_buf;
				sge_lkey = conn->send_mr->lkey;
			}
		}

		struct ibv_sge sge;
		memset(&sge, 0, sizeof(sge));
		sge.addr = sge_addr;
		sge.length = chunk_len;
		sge.lkey = sge_lkey;

		struct ibv_send_wr wr;
		memset(&wr, 0, sizeof(wr));
		wr.wr_id = (uintptr_t)conn->tx_prod;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.opcode = IBV_WR_RDMA_WRITE;
		wr.send_flags = IBV_SEND_SIGNALED;
		wr.wr.rdma.remote_addr = remote_base + (uint64_t)offset;
		wr.wr.rdma.rkey = conn->peer_rw_rkey;

		struct ibv_send_wr *bad_wr = NULL;
		rc = ibv_post_send(conn->id->qp, &wr, &bad_wr);
		if (rc != 0) {
			go_rdma_set_errno(err_out, "ibv_post_send(write_data)");
			final_rc = errno != 0 ? errno : EIO;
			goto cleanup;
		}

		conn->tx_prod++;
		offset += chunk_len;
	}

	uint32_t writes_done_target = conn->tx_prod;
	int rc = go_rdma_wait_send_completed_until(conn, writes_done_target, timeout_ms, err_out);
	if (rc != 0) {
		final_rc = rc;
		goto cleanup;
	}

	go_rdma_rw_notify notify;
	memset(&notify, 0, sizeof(notify));
	notify.magic = GO_RDMA_RW_NOTIFY_MAGIC;
	notify.kind = GO_RDMA_RW_NOTIFY_KIND_WRITE;
	notify.length = total_len;
	notify.slot = slot;

	rc = go_rdma_send_message(
		conn,
		(const uint8_t *)&notify,
		(uint32_t)sizeof(notify),
		timeout_ms,
		err_out
	);
	if (rc != 0) {
		final_rc = rc;
		goto cleanup;
	}
	conn->rw_send_seq++;

cleanup:
	if (payload_mr != NULL) {
		// If we posted WRs referencing payload_mr, ensure they are completed
		// before dereg and function return.
		if ((int32_t)(conn->tx_prod - writes_start) > 0) {
			int wait_rc = go_rdma_wait_send_completed_until(conn, conn->tx_prod, -1, NULL);
			if (wait_rc != 0 && final_rc == 0) {
				go_rdma_set_err(err_out, "go_rdma_send_message_rw: wait payload send completion failed rc=%d", wait_rc);
				final_rc = wait_rc;
			}
		}
		if (ibv_dereg_mr(payload_mr) != 0 && final_rc == 0) {
			go_rdma_set_errno(err_out, "ibv_dereg_mr(payload)");
			final_rc = errno != 0 ? errno : EIO;
		}
	}

	return final_rc;
}

static int go_rdma_send_message_rw_at(
	go_rdma_conn *conn,
	const uint8_t *payload,
	uint32_t total_len,
	uint32_t remote_offset,
	int timeout_ms,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw_at: conn closed");
		return EBADF;
	}
	if (total_len == 0) {
		return 0;
	}
	if (payload == NULL) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw_at: payload is nil");
		return EINVAL;
	}
	if (!conn->rw_ready || conn->peer_rw_addr == 0 || conn->peer_rw_rkey == 0 || conn->peer_rw_cap == 0 || conn->peer_rw_slots == 0) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw_at: peer rw descriptor unavailable");
		return ENOTSUP;
	}
	uint64_t peer_total = (uint64_t)conn->peer_rw_cap * (uint64_t)conn->peer_rw_slots;
	if (peer_total == 0 || peer_total > UINT32_MAX) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw_at: invalid peer rw window total=%llu", (unsigned long long)peer_total);
		return EINVAL;
	}
	uint64_t end_off = (uint64_t)remote_offset + (uint64_t)total_len;
	if (end_off > peer_total) {
		go_rdma_set_err(
			err_out,
			"go_rdma_send_message_rw_at: payload out of range offset=%u payload=%u total=%llu",
			remote_offset,
			total_len,
			(unsigned long long)peer_total
		);
		return EMSGSIZE;
	}
	if (conn->frame_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_send_message_rw_at: invalid frame capacity");
		return EINVAL;
	}

	struct ibv_mr *payload_mr = ibv_reg_mr(
		conn->pd,
		(void *)payload,
		(size_t)total_len,
		IBV_ACCESS_LOCAL_WRITE
	);
	int use_payload_mr = (payload_mr != NULL) ? 1 : 0;
	uint32_t writes_start = conn->tx_prod;
	int final_rc = 0;

	uint32_t offset = 0;
	uint64_t remote_base = conn->peer_rw_addr + (uint64_t)remote_offset;
	while (offset < total_len) {
		uint32_t remaining = total_len - offset;
		uint32_t chunk_len = conn->frame_cap;
		if (chunk_len > remaining) {
			chunk_len = remaining;
		}

		int rc = go_rdma_wait_local_send_slot(conn, timeout_ms, err_out);
		if (rc != 0) {
			final_rc = rc;
			goto cleanup;
		}

		uintptr_t sge_addr = 0;
		uint32_t sge_lkey = 0;
		if (use_payload_mr) {
			sge_addr = (uintptr_t)(payload + offset);
			sge_lkey = payload_mr->lkey;
		} else {
			int use_rw_recv_mr = 0;
			if (conn->rw_recv_buf != NULL && conn->rw_recv_mr != NULL && conn->rw_recv_cap > 0 && conn->rw_recv_slots > 0) {
				size_t rw_total = (size_t)conn->rw_recv_cap * (size_t)conn->rw_recv_slots;
				if (rw_total / conn->rw_recv_slots == conn->rw_recv_cap) {
					uintptr_t rw_begin = (uintptr_t)conn->rw_recv_buf;
					uintptr_t payload_addr = (uintptr_t)(payload + offset);
					if (payload_addr >= rw_begin) {
						size_t rel = (size_t)(payload_addr - rw_begin);
						if (rel <= rw_total && (size_t)chunk_len <= (rw_total - rel)) {
							use_rw_recv_mr = 1;
						}
					}
				}
			}

			if (use_rw_recv_mr) {
				sge_addr = (uintptr_t)(payload + offset);
				sge_lkey = conn->rw_recv_mr->lkey;
			} else {
				uint32_t local_slot = conn->tx_prod % conn->send_depth;
				uint8_t *local_buf = conn->send_buf + ((size_t)local_slot * conn->frame_cap);
				memcpy(local_buf, payload + offset, chunk_len);
				sge_addr = (uintptr_t)local_buf;
				sge_lkey = conn->send_mr->lkey;
			}
		}

		struct ibv_sge sge;
		memset(&sge, 0, sizeof(sge));
		sge.addr = sge_addr;
		sge.length = chunk_len;
		sge.lkey = sge_lkey;

		struct ibv_send_wr wr;
		memset(&wr, 0, sizeof(wr));
		wr.wr_id = (uintptr_t)conn->tx_prod;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.opcode = IBV_WR_RDMA_WRITE;
		wr.send_flags = IBV_SEND_SIGNALED;
		wr.wr.rdma.remote_addr = remote_base + (uint64_t)offset;
		wr.wr.rdma.rkey = conn->peer_rw_rkey;

		struct ibv_send_wr *bad_wr = NULL;
		rc = ibv_post_send(conn->id->qp, &wr, &bad_wr);
		if (rc != 0) {
			go_rdma_set_errno(err_out, "ibv_post_send(write_data_at)");
			final_rc = errno != 0 ? errno : EIO;
			goto cleanup;
		}

		conn->tx_prod++;
		offset += chunk_len;
	}

	uint32_t writes_done_target = conn->tx_prod;
	int rc = go_rdma_wait_send_completed_until(conn, writes_done_target, timeout_ms, err_out);
	if (rc != 0) {
		final_rc = rc;
		goto cleanup;
	}

	go_rdma_rw_notify notify;
	memset(&notify, 0, sizeof(notify));
	notify.magic = GO_RDMA_RW_NOTIFY_MAGIC;
	notify.kind = GO_RDMA_RW_NOTIFY_KIND_WRITE_AT;
	notify.length = total_len;
	notify.slot = remote_offset;

	rc = go_rdma_send_message(
		conn,
		(const uint8_t *)&notify,
		(uint32_t)sizeof(notify),
		timeout_ms,
		err_out
	);
	if (rc != 0) {
		final_rc = rc;
		goto cleanup;
	}

cleanup:
	if (payload_mr != NULL) {
		if ((int32_t)(conn->tx_prod - writes_start) > 0) {
			int wait_rc = go_rdma_wait_send_completed_until(conn, conn->tx_prod, -1, NULL);
			if (wait_rc != 0 && final_rc == 0) {
				go_rdma_set_err(err_out, "go_rdma_send_message_rw_at: wait payload send completion failed rc=%d", wait_rc);
				final_rc = wait_rc;
			}
		}
		if (ibv_dereg_mr(payload_mr) != 0 && final_rc == 0) {
			go_rdma_set_errno(err_out, "ibv_dereg_mr(payload)");
			final_rc = errno != 0 ? errno : EIO;
		}
	}

	return final_rc;
}

static int go_rdma_get_rw_recv_payload(
	go_rdma_conn *conn,
	uint32_t slot,
	uint32_t payload_len,
	uint8_t **payload_ptr,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_get_rw_recv_payload: conn closed");
		return EBADF;
	}
	if (payload_ptr == NULL) {
		go_rdma_set_err(err_out, "go_rdma_get_rw_recv_payload: nil payload_ptr");
		return EINVAL;
	}
	if (!conn->rw_ready || conn->rw_recv_buf == NULL || conn->rw_recv_mr == NULL || conn->rw_recv_cap == 0) {
		go_rdma_set_err(err_out, "go_rdma_get_rw_recv_payload: rw recv buffer unavailable");
		return ENOTSUP;
	}
	if (conn->rw_recv_slots == 0 || slot >= conn->rw_recv_slots) {
		go_rdma_set_err(
			err_out,
			"go_rdma_get_rw_recv_payload: invalid slot slot=%u slots=%u",
			slot,
			conn->rw_recv_slots
		);
		return EINVAL;
	}
	if (payload_len > conn->rw_recv_cap) {
		go_rdma_set_err(
			err_out,
			"go_rdma_get_rw_recv_payload: payload too large payload=%u cap=%u",
			payload_len,
			conn->rw_recv_cap
		);
		return EMSGSIZE;
	}
	*payload_ptr = conn->rw_recv_buf + ((size_t)slot * (size_t)conn->rw_recv_cap);
	return 0;
}

static int go_rdma_get_rw_recv_payload_at(
	go_rdma_conn *conn,
	uint32_t remote_offset,
	uint32_t payload_len,
	uint8_t **payload_ptr,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_get_rw_recv_payload_at: conn closed");
		return EBADF;
	}
	if (payload_ptr == NULL) {
		go_rdma_set_err(err_out, "go_rdma_get_rw_recv_payload_at: nil payload_ptr");
		return EINVAL;
	}
	if (!conn->rw_ready || conn->rw_recv_buf == NULL || conn->rw_recv_mr == NULL || conn->rw_recv_cap == 0 || conn->rw_recv_slots == 0) {
		go_rdma_set_err(err_out, "go_rdma_get_rw_recv_payload_at: rw recv buffer unavailable");
		return ENOTSUP;
	}
	uint64_t rw_total = (uint64_t)conn->rw_recv_cap * (uint64_t)conn->rw_recv_slots;
	uint64_t end_off = (uint64_t)remote_offset + (uint64_t)payload_len;
	if (end_off > rw_total) {
		go_rdma_set_err(
			err_out,
			"go_rdma_get_rw_recv_payload_at: payload out of range offset=%u payload=%u total=%llu",
			remote_offset,
			payload_len,
			(unsigned long long)rw_total
		);
		return EMSGSIZE;
	}
	*payload_ptr = conn->rw_recv_buf + (size_t)remote_offset;
	return 0;
}

static int go_rdma_release_last_recv_slot(
	go_rdma_conn *conn,
	char **err_out
) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_release_last_recv_slot: conn closed");
		return EBADF;
	}
	if (!conn->has_last_notify_slot) {
		return 0;
	}

	uint32_t idx = conn->last_notify_slot;
	int rc = go_rdma_post_notify_recv_slot(conn, idx, err_out);
	if (rc != 0) {
		return rc;
	}

	conn->has_last_notify_slot = 0;
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
	if (conn->notify_slots == NULL) {
		go_rdma_set_err(err_out, "go_rdma_recv_frame: conn receive resources not initialized");
		return EINVAL;
	}

	int rc = go_rdma_release_last_recv_slot(conn, err_out);
	if (rc != 0) {
		return rc;
	}

	struct ibv_wc wc;
	rc = go_rdma_wait_recv_completion(conn, timeout_ms, "ibv_poll_cq(recv_data)", &wc, err_out);
	if (rc != 0) {
		return rc;
	}
	if (wc.opcode != IBV_WC_RECV) {
		go_rdma_set_err(err_out, "go_rdma_recv_frame: unexpected recv opcode=%u", wc.opcode);
		return EPROTO;
	}

	uintptr_t wr = (uintptr_t)wc.wr_id;
	if (wr == 0 || wr > conn->notify_depth) {
		go_rdma_set_err(err_out, "invalid notify recv wr_id=%llu", (unsigned long long)wc.wr_id);
		return EIO;
	}
	uint32_t notify_idx = (uint32_t)(wr - 1);
	uint8_t *slot_buf = conn->notify_slots[notify_idx].buf;

	*payload_len = wc.byte_len;
	*total_len = wc.byte_len;
	*offset = 0;

	if (*payload_len > conn->frame_cap) {
		go_rdma_set_err(err_out, "frame payload length invalid payload=%u frame_cap=%u", *payload_len, conn->frame_cap);
		return EPROTO;
	}

	*payload_ptr = slot_buf;
	conn->last_notify_slot = notify_idx;
	conn->has_last_notify_slot = 1;
	return 0;
}

static int go_rdma_repost_recv(go_rdma_conn *conn, char **err_out) {
	if (conn == NULL || conn->id == NULL) {
		go_rdma_set_err(err_out, "go_rdma_repost_recv: conn closed");
		return EBADF;
	}
	if (!conn->has_last_notify_slot) {
		go_rdma_set_err(err_out, "go_rdma_repost_recv: no recv slot to repost");
		return EINVAL;
	}
	return go_rdma_release_last_recv_slot(conn, err_out);
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
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

const (
	verbsBackendEnabled = true
	// Allow RDMA Open to survive connection bursts where CM/QP setup may
	// temporarily queue for multiple seconds.
	verbsOpenTimeoutMills = 3000
	// Match per-attempt context timeout with underlying C open timeout so
	// higher-concurrency dials don't get canceled too early.
	verbsOpenAttemptTimeout = time.Duration(verbsOpenTimeoutMills) * time.Millisecond
	verbsOpenRetryBackoff   = 250 * time.Millisecond
	verbsOpenAttemptLimit   = 8

	rdmaWriteNotifyMagic  = uint64(0x5244574e4f544659) // "RDWNOTFY"
	rdmaWriteNotifyKind   = uint32(1)
	rdmaWriteNotifyKindAt = uint32(2)
	rdmaWriteNotifySize   = 20
	rdmaWriteDiagEnv      = "AWS_RDMA_RW_DIAG"
)

func rdmaWriteDiagEnabled() bool {
	v, ok := os.LookupEnv(rdmaWriteDiagEnv)
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
}

type verbsMessageConn struct {
	mu sync.RWMutex
	cc *C.go_rdma_conn

	framePayloadSize int
	inlineThreshold  int
	localAddr        net.Addr
	remoteAddr       net.Addr

	readyMu sync.Mutex
	ready   bool

	recvFrameMu sync.Mutex

	// Keep caller-provided external shared memory alive for the lifetime of
	// this connection when configured through VerbsOptions.SharedRWMemory.
	sharedRWMemory []byte
}

var _ MessageConn = (*verbsMessageConn)(nil)
var _ BorrowingMessageConn = (*verbsMessageConn)(nil)

type verbsListener struct {
	mu sync.RWMutex
	cl *C.go_rdma_listener

	framePayloadSize int
	inlineThreshold  int
	localAddr        net.Addr

	acceptWorkers int
	acceptOut     chan *C.go_rdma_conn
	done          chan struct{}

	workerWg  sync.WaitGroup
	closeOnce sync.Once
}

var _ MessageListener = (*verbsListener)(nil)

var errRdmaAcceptTimeout = errors.New("rdma accept timeout")

func newVerbsMessageListener(network, address string, opts VerbsListenerOptions) (MessageListener, error) {
	cfg, err := opts.VerbsOptions.normalize()
	if err != nil {
		return nil, err
	}

	host, port, err := splitHostPortListenAddress(network, address)
	if err != nil {
		return nil, err
	}

	frameCap := cfg.framePayloadSize
	if frameCap <= 0 {
		return nil, fmt.Errorf("rdma verbs: invalid frame capacity")
	}

	backlog := opts.Backlog
	if backlog == 0 {
		backlog = DefaultVerbsListenBacklog
	}
	if backlog < 0 {
		return nil, fmt.Errorf("rdma verbs: listen backlog must be >= 0")
	}
	acceptWorkers := opts.AcceptWorkers
	if acceptWorkers <= 0 {
		acceptWorkers = DefaultVerbsAcceptWorkers
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
		C.uint32_t(cfg.sendSignalIntvl),
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

	l := &verbsListener{
		cl:               cListener,
		framePayloadSize: cfg.framePayloadSize,
		inlineThreshold:  cfg.inlineThreshold,
		localAddr:        rdmaAddr{network: "rdma", address: net.JoinHostPort(localHost, port)},
		acceptWorkers:    acceptWorkers,
		acceptOut:        make(chan *C.go_rdma_conn, acceptWorkers*4),
		done:             make(chan struct{}),
	}
	l.startAcceptWorkers()
	return l, nil
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

func (l *verbsListener) AcceptMessage() (MessageConn, error) {
	l.mu.RLock()
	framePayloadSize := l.framePayloadSize
	inlineThreshold := l.inlineThreshold
	localAddr := l.localAddr
	acceptOut := l.acceptOut
	done := l.done
	l.mu.RUnlock()

	select {
	case <-done:
		return nil, net.ErrClosed
	case cConn, ok := <-acceptOut:
		if !ok || cConn == nil {
			return nil, net.ErrClosed
		}
		msgConn := &verbsMessageConn{
			cc:               cConn,
			framePayloadSize: framePayloadSize,
			inlineThreshold:  inlineThreshold,
			localAddr:        localAddr,
			remoteAddr:       rdmaAddr{network: "rdma", address: "remote"},
			ready:            true,
		}
		return msgConn, nil
	}
}

func (l *verbsListener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		cListener := l.cl
		l.cl = nil
		close(l.done)
		l.mu.Unlock()

		l.workerWg.Wait()
		if cListener != nil {
			C.go_rdma_listener_close(cListener)
		}
		close(l.acceptOut)
	})
	return nil
}

func (l *verbsListener) Addr() net.Addr {
	return l.localAddr
}

func (l *verbsListener) startAcceptWorkers() {
	for i := 0; i < l.acceptWorkers; i++ {
		l.workerWg.Add(1)
		go l.acceptWorker()
	}
}

func (l *verbsListener) acceptWorker() {
	defer l.workerWg.Done()

	var (
		lastErrLog     time.Time
		suppressedErrs int
		errLogInterval = 1 * time.Second
	)

	for {
		l.mu.RLock()
		cListener := l.cl
		done := l.done
		l.mu.RUnlock()

		if cListener == nil {
			return
		}

		cConn, err := l.acceptOnce(cListener)
		if err != nil {
			if errors.Is(err, errRdmaAcceptTimeout) {
				l.mu.RLock()
				closed := l.cl == nil
				l.mu.RUnlock()
				if closed {
					return
				}
				continue
			}

			now := time.Now()
			if now.Sub(lastErrLog) >= errLogInterval {
				if suppressedErrs > 0 {
					log.Printf("rdma accept worker error: %v (suppressed=%d)", err, suppressedErrs)
					suppressedErrs = 0
				} else {
					log.Printf("rdma accept worker error: %v", err)
				}
				lastErrLog = now
			} else {
				suppressedErrs++
			}
			l.mu.RLock()
			closed := l.cl == nil
			l.mu.RUnlock()
			if closed {
				return
			}
			// Per-connection handshake/CM errors should not stop draining accepts.
			time.Sleep(5 * time.Millisecond)
			continue
		}

		if cConn == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		select {
		case <-done:
			C.go_rdma_close(cConn)
			return
		case l.acceptOut <- cConn:
		}
	}
}

func (l *verbsListener) acceptOnce(cListener *C.go_rdma_listener) (*C.go_rdma_conn, error) {
	var cConn *C.go_rdma_conn
	var cErr *C.char
	rc := C.go_rdma_accept(cListener, &cConn, &cErr)
	if rc != 0 {
		if rc == C.int(C.EAGAIN) {
			return nil, errRdmaAcceptTimeout
		}
		return nil, rdmaCError("accept", rc, cErr)
	}
	return cConn, nil
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

	frameCap := cfg.framePayloadSize
	if frameCap <= 0 {
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

		attemptTimeout := verbsOpenAttemptTimeout
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
				inlineThreshold:  cfg.inlineThreshold,
				localAddr:        rdmaAddr{network: "rdma", address: "local"},
				remoteAddr:       rdmaAddr{network: "rdma", address: net.JoinHostPort(host, port)},
				ready:            true,
				sharedRWMemory:   cfg.sharedRWMemory,
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

		var (
			cSharedRWPtr *C.uint8_t
			cSharedRWLen C.uint32_t
		)
		if len(cfg.sharedRWMemory) > 0 {
			cSharedRWPtr = (*C.uint8_t)(unsafe.Pointer(&cfg.sharedRWMemory[0]))
			cSharedRWLen = C.uint32_t(len(cfg.sharedRWMemory))
		}

		var result openResult
		result.rc = C.go_rdma_open(
			cHost,
			cPort,
			C.uint32_t(frameCap),
			C.uint32_t(cfg.sendQueueDepth),
			C.uint32_t(cfg.recvQueueDepth),
			C.uint32_t(cfg.inlineThreshold),
			C.uint32_t(cfg.sendSignalIntvl),
			cSharedRWPtr,
			cSharedRWLen,
			openTimeoutMS,
			&result.cConn,
			&result.cErr,
		)
		resultCh <- result
	}()

	var result openResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		// C open cannot be canceled in-flight. Drain completion asynchronously and
		// close any late-success connection so canceled dials do not leak resources
		// or occupy server-side CM slots.
		go func() {
			late := <-resultCh
			discardOpenResult(late)
		}()
		return nil, ctx.Err()
	}

	if result.rc != 0 {
		return nil, rdmaCError("open", result.rc, result.cErr)
	}
	if result.cConn == nil {
		return nil, errors.New("rdma verbs open returned nil connection")
	}
	return result.cConn, nil
}

func discardOpenResult(result openResult) {
	if result.cErr != nil {
		C.free(unsafe.Pointer(result.cErr))
	}
	if result.rc == 0 && result.cConn != nil {
		C.go_rdma_close(result.cConn)
	}
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
	if err := c.ensureReady(ctx); err != nil {
		return err
	}
	timeoutMS, err := rdmaContextTimeoutMillis(ctx)
	if err != nil {
		return err
	}

	var payloadPtr *C.uint8_t
	if len(payload) > 0 {
		payloadPtr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}

	var cErr *C.char
	rc := C.int(0)
	usedRW := false
	rwFallbackToSend := false

	// Keep control-plane sized frames on SEND to avoid clobbering RW shared
	// memory windows that may be targeted by offset-based SendMessageAt writes.
	// RW dataplane is still used for larger payloads.
	rwCutover := c.inlineThreshold
	if rwCutover < c.framePayloadSize {
		rwCutover = c.framePayloadSize
	}
	useRWDataplane := len(payload) > rwCutover
	if useRWDataplane {
		rc = C.go_rdma_send_message_rw(
			c.cc,
			payloadPtr,
			C.uint32_t(len(payload)),
			timeoutMS,
			&cErr,
		)
		if rc == 0 {
			usedRW = true
		} else if rc == C.int(C.EMSGSIZE) || rc == C.int(C.ENOTSUP) {
			if cErr != nil {
				C.free(unsafe.Pointer(cErr))
				cErr = nil
			}
			rwFallbackToSend = true
			rc = C.go_rdma_send_message(
				c.cc,
				payloadPtr,
				C.uint32_t(len(payload)),
				timeoutMS,
				&cErr,
			)
		}
	} else {
		rc = C.go_rdma_send_message(
			c.cc,
			payloadPtr,
			C.uint32_t(len(payload)),
			timeoutMS,
			&cErr,
		)
	}
	if usedRW && rdmaWriteDiagEnabled() {
		log.Printf("rdma rw send used len=%d", len(payload))
	}
	if rwFallbackToSend && rdmaWriteDiagEnabled() {
		log.Printf("rdma rw fallback to send len=%d", len(payload))
	}
	runtime.KeepAlive(payload)
	if rc != 0 {
		if rc == C.int(C.EAGAIN) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
		if useRWDataplane && !rwFallbackToSend {
			return rdmaCError("rw_send", rc, cErr)
		}
		return rdmaCError("send", rc, cErr)
	}
	return nil
}

// SendMessageAt sends payload through the RW data plane and targets a specific
// offset in the peer shared memory region.
func (c *verbsMessageConn) SendMessageAt(ctx context.Context, payload []byte, sharedOffset int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sharedOffset < 0 {
		return fmt.Errorf("rdma verbs: invalid shared offset %d", sharedOffset)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cc == nil {
		return net.ErrClosed
	}
	if err := c.ensureReady(ctx); err != nil {
		return err
	}
	timeoutMS, err := rdmaContextTimeoutMillis(ctx)
	if err != nil {
		return err
	}

	var payloadPtr *C.uint8_t
	if len(payload) > 0 {
		payloadPtr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}

	var cErr *C.char
	rc := C.go_rdma_send_message_rw_at(
		c.cc,
		payloadPtr,
		C.uint32_t(len(payload)),
		C.uint32_t(sharedOffset),
		timeoutMS,
		&cErr,
	)
	runtime.KeepAlive(payload)
	if rc != 0 {
		if rc == C.int(C.EAGAIN) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
		return rdmaCError("rw_send_at", rc, cErr)
	}
	if rdmaWriteDiagEnabled() {
		log.Printf("rdma rw send_at used len=%d off=%d", len(payload), sharedOffset)
	}
	return nil
}

func (c *verbsMessageConn) RecvFrame(ctx context.Context) ([]byte, int, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, 0, err
	}

	c.recvFrameMu.Lock()
	c.mu.RLock()
	if c.cc == nil {
		c.mu.RUnlock()
		c.recvFrameMu.Unlock()
		return nil, 0, 0, net.ErrClosed
	}
	if err := c.ensureReady(ctx); err != nil {
		c.mu.RUnlock()
		c.recvFrameMu.Unlock()
		return nil, 0, 0, err
	}

	var (
		payloadPtr *C.uint8_t
		payloadLen C.uint32_t
		frameTotal C.uint32_t
		frameOff   C.uint32_t
		cErr       *C.char
	)
	timeoutMS, err := rdmaContextTimeoutMillisWithDefault(ctx, readPollInterval)
	if err != nil {
		c.mu.RUnlock()
		c.recvFrameMu.Unlock()
		return nil, 0, 0, err
	}

	rc := C.go_rdma_recv_frame(c.cc, &payloadPtr, &payloadLen, &frameTotal, &frameOff, timeoutMS, &cErr)
	c.mu.RUnlock()
	if rc != 0 {
		c.recvFrameMu.Unlock()
		if rc == C.int(C.EAGAIN) {
			if err := ctx.Err(); err != nil {
				return nil, 0, 0, err
			}
			return nil, 0, 0, context.DeadlineExceeded
		}
		return nil, 0, 0, rdmaCError("recv", rc, cErr)
	}

	total := int(frameTotal)
	offset := int(frameOff)
	payloadN := int(payloadLen)
	if total < 0 || offset < 0 || payloadN < 0 || offset+payloadN > total {
		c.recvFrameMu.Unlock()
		return nil, 0, 0, fmt.Errorf("rdma verbs: invalid frame range offset=%d payload=%d total=%d", offset, payloadN, total)
	}
	c.recvFrameMu.Unlock()

	payload := unsafe.Slice((*byte)(unsafe.Pointer(payloadPtr)), payloadN)
	return payload, total, offset, nil
}

func (c *verbsMessageConn) RepostFrame() error {
	c.recvFrameMu.Lock()
	defer c.recvFrameMu.Unlock()

	c.mu.RLock()
	if c.cc == nil {
		c.mu.RUnlock()
		return net.ErrClosed
	}
	err := c.repostRecvSlot()
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	return nil
}

func parseWriteNotifyFrame(frame []byte) (length int, token int, at bool, ok bool) {
	if len(frame) != rdmaWriteNotifySize {
		return 0, 0, false, false
	}
	magic := binary.LittleEndian.Uint64(frame[:8])
	if magic != rdmaWriteNotifyMagic {
		return 0, 0, false, false
	}
	kind := binary.LittleEndian.Uint32(frame[8:12])
	if kind != rdmaWriteNotifyKind && kind != rdmaWriteNotifyKindAt {
		return 0, 0, false, false
	}
	lengthU := binary.LittleEndian.Uint32(frame[12:16])
	tokenU := binary.LittleEndian.Uint32(frame[16:20])
	return int(lengthU), int(tokenU), kind == rdmaWriteNotifyKindAt, true
}

func (c *verbsMessageConn) sharedMemoryOffset(payload []byte) int {
	if len(payload) == 0 || len(c.sharedRWMemory) == 0 {
		return -1
	}

	sharedBase := uintptr(unsafe.Pointer(&c.sharedRWMemory[0]))
	payloadBase := uintptr(unsafe.Pointer(&payload[0]))
	if payloadBase < sharedBase {
		return -1
	}
	rel := payloadBase - sharedBase
	if rel > uintptr(len(c.sharedRWMemory)-len(payload)) {
		return -1
	}
	return int(rel)
}

func (c *verbsMessageConn) getRWRecvPayload(slot int, length int) ([]byte, error) {
	if slot < 0 {
		return nil, fmt.Errorf("rdma verbs: invalid rw slot %d", slot)
	}
	if length < 0 {
		return nil, fmt.Errorf("rdma verbs: invalid rw payload length %d", length)
	}
	if length == 0 {
		return []byte{}, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cc == nil {
		return nil, net.ErrClosed
	}

	var (
		payloadPtr *C.uint8_t
		cErr       *C.char
	)
	rc := C.go_rdma_get_rw_recv_payload(c.cc, C.uint32_t(slot), C.uint32_t(length), &payloadPtr, &cErr)
	if rc != 0 {
		return nil, rdmaCError("rw_recv_payload", rc, cErr)
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(payloadPtr)), length), nil
}

func (c *verbsMessageConn) getRWRecvPayloadAt(sharedOffset int, length int) ([]byte, error) {
	if sharedOffset < 0 {
		return nil, fmt.Errorf("rdma verbs: invalid shared offset %d", sharedOffset)
	}
	if length < 0 {
		return nil, fmt.Errorf("rdma verbs: invalid rw payload length %d", length)
	}
	if length == 0 {
		return []byte{}, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cc == nil {
		return nil, net.ErrClosed
	}

	var (
		payloadPtr *C.uint8_t
		cErr       *C.char
	)
	rc := C.go_rdma_get_rw_recv_payload_at(
		c.cc,
		C.uint32_t(sharedOffset),
		C.uint32_t(length),
		&payloadPtr,
		&cErr,
	)
	if rc != 0 {
		return nil, rdmaCError("rw_recv_payload_at", rc, cErr)
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(payloadPtr)), length), nil
}

func (c *verbsMessageConn) RecvBorrowedMessage(ctx context.Context) (*BorrowedMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		payload, frameTotalN, offsetN, err := c.RecvFrame(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		payloadLenN := len(payload)
		if frameTotalN != payloadLenN || offsetN != 0 {
			return nil, fmt.Errorf(
				"rdma verbs: unsupported multi-frame receive total=%d payload=%d offset=%d",
				frameTotalN, payloadLenN, offsetN,
			)
		}
		if rwLen, rwToken, rwAt, ok := parseWriteNotifyFrame(payload); ok {
			var rwPayload []byte
			var sharedOffset int
			if rwAt {
				rwPayload, err = c.getRWRecvPayloadAt(rwToken, rwLen)
				sharedOffset = rwToken
			} else {
				rwPayload, err = c.getRWRecvPayload(rwToken, rwLen)
				sharedOffset = c.sharedMemoryOffset(rwPayload)
			}
			if err != nil {
				return nil, err
			}
			if err := c.RepostFrame(); err != nil {
				return nil, err
			}
			if rdmaWriteDiagEnabled() {
				if rwAt {
					log.Printf("rdma rw recv_at used len=%d off=%d", rwLen, rwToken)
				} else {
					log.Printf("rdma rw recv used len=%d", rwLen)
				}
			}
			return &BorrowedMessage{
				Payload:      rwPayload,
				SharedOffset: sharedOffset,
			}, nil
		}
		return &BorrowedMessage{
			Payload:      payload,
			SharedOffset: c.sharedMemoryOffset(payload),
			release:      c.RepostFrame,
		}, nil
	}
}

func (c *verbsMessageConn) RecvMessage(ctx context.Context) ([]byte, error) {
	msg, err := c.RecvBorrowedMessage(ctx)
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), msg.Payload...)
	if err := msg.Release(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *verbsMessageConn) repostRecvSlot() error {
	var cErr *C.char
	rc := C.go_rdma_repost_recv(c.cc, &cErr)
	if rc != 0 {
		return rdmaCError("repost_recv", rc, cErr)
	}
	return nil
}

func (c *verbsMessageConn) ensureReady(ctx context.Context) error {
	if c.ready {
		return nil
	}

	c.readyMu.Lock()
	defer c.readyMu.Unlock()

	if c.ready {
		return nil
	}
	if c.cc == nil {
		return net.ErrClosed
	}

	timeoutMS, err := rdmaContextTimeoutMillis(ctx)
	if err != nil {
		return err
	}

	var cErr *C.char
	rc := C.go_rdma_ensure_ready(c.cc, timeoutMS, &cErr)
	if rc != 0 {
		if rc == C.int(C.EAGAIN) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
		return rdmaCError("ensure_ready", rc, cErr)
	}

	c.ready = true
	return nil
}

func (c *verbsMessageConn) Close() error {
	c.mu.Lock()
	if c.cc == nil {
		c.mu.Unlock()
		return net.ErrClosed
	}
	cc := c.cc
	c.cc = nil
	c.ready = false
	c.mu.Unlock()

	C.go_rdma_close(cc)
	return nil
}

func (c *verbsMessageConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *verbsMessageConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func rdmaContextTimeoutMillis(ctx context.Context) (C.int, error) {
	return rdmaContextTimeoutMillisWithDefault(ctx, 0)
}

func rdmaContextTimeoutMillisWithDefault(ctx context.Context, defaultTimeout time.Duration) (C.int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		if defaultTimeout <= 0 {
			return C.int(-1), nil
		}
		return rdmaDurationToTimeoutMillis(defaultTimeout), nil
	}

	remain := time.Until(deadline)
	if remain <= 0 {
		return 0, context.DeadlineExceeded
	}

	return rdmaDurationToTimeoutMillis(remain), nil
}

func rdmaDurationToTimeoutMillis(d time.Duration) C.int {
	if d <= 0 {
		return 0
	}

	ms := int64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	if ms < 1 {
		ms = 1
	}
	if ms > 2147483647 {
		ms = 2147483647
	}
	return C.int(ms)
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
