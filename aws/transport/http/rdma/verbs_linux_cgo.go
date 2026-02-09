//go:build linux && cgo && rdma

package rdma

/*
#cgo LDFLAGS: -lrdmacm -libverbs

#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
	GO_RDMA_FRAME_HEADER_SIZE = 12,
};

typedef struct {
	uint8_t *buf;
	struct ibv_mr *mr;
} go_rdma_recv_slot;

typedef struct {
	struct rdma_cm_id *id;
	uint8_t *send_buf;
	struct ibv_mr *send_mr;
	go_rdma_recv_slot *recv_slots;
	uint32_t recv_depth;
	uint32_t frame_cap;
	uint32_t inline_threshold;
	uint32_t last_recv_slot;
	int has_last_recv_slot;
} go_rdma_conn;

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
		rdma_destroy_ep(conn->id);
		conn->id = NULL;
	}

	free(conn);
}

static int go_rdma_open(
	const char *host,
	const char *port,
	uint32_t frame_cap,
	uint32_t send_wr,
	uint32_t recv_wr,
	uint32_t inline_threshold,
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
	hints.ai_flags = RAI_NUMERICSERV;

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

	if (conn->id->qp != NULL) {
		conn->inline_threshold = conn->id->qp->qp_cap.max_inline_data;
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

	struct ibv_wc wc;
	rc = rdma_get_send_comp(conn->id, &wc);
	if (rc <= 0) {
		if (rc == 0) {
			go_rdma_set_err(err_out, "rdma_get_send_comp: no completion");
			return EIO;
		}
		go_rdma_set_errno(err_out, "rdma_get_send_comp");
		return errno != 0 ? errno : EIO;
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

	struct ibv_wc wc;
	int rc = rdma_get_recv_comp(conn->id, &wc);
	if (rc <= 0) {
		if (rc == 0) {
			go_rdma_set_err(err_out, "rdma_get_recv_comp: no completion");
			return EIO;
		}
		go_rdma_set_errno(err_out, "rdma_get_recv_comp");
		return errno != 0 ? errno : EIO;
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
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"unsafe"
)

const (
	verbsBackendEnabled  = true
	verbsFrameHeaderSize = 12
)

type verbsMessageConn struct {
	mu sync.RWMutex
	cc *C.go_rdma_conn

	framePayloadSize int
	localAddr        net.Addr
	remoteAddr       net.Addr
}

var _ MessageConn = (*verbsMessageConn)(nil)

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

	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cPort := C.CString(port)
	defer C.free(unsafe.Pointer(cPort))

	var cConn *C.go_rdma_conn
	var cErr *C.char
	rc := C.go_rdma_open(
		cHost,
		cPort,
		C.uint32_t(frameCap),
		C.uint32_t(cfg.sendQueueDepth),
		C.uint32_t(cfg.recvQueueDepth),
		C.uint32_t(cfg.inlineThreshold),
		&cConn,
		&cErr,
	)
	if rc != 0 {
		return nil, rdmaCError("open", rc, cErr)
	}
	if cConn == nil {
		return nil, errors.New("rdma verbs open returned nil connection")
	}

	return &verbsMessageConn{
		cc:               cConn,
		framePayloadSize: cfg.framePayloadSize,
		localAddr:        rdmaAddr{network: "rdma", address: "local"},
		remoteAddr:       rdmaAddr{network: "rdma", address: net.JoinHostPort(host, port)},
	}, nil
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
		return c.sendFrame(nil, 0, 0)
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
		if err := c.sendFrame(chunk, total, offset); err != nil {
			return err
		}
	}

	return nil
}

func (c *verbsMessageConn) sendFrame(chunk []byte, totalLen int, offset int) error {
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
		&cErr,
	)
	runtime.KeepAlive(chunk)
	if rc != 0 {
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

		rc := C.go_rdma_recv_frame(c.cc, &payloadPtr, &payloadLen, &frameTotal, &frameOff, &cErr)
		if rc != 0 {
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
