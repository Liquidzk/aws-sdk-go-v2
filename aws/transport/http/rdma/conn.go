package rdma

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"time"
)

var (
	// readPollInterval bounds how long one RecvMessage call can block so
	// SetReadDeadline can interrupt in-flight reads. A larger slice reduces
	// idle wakeups/cgo churn at the cost of deadline reaction granularity.
	readPollInterval = 500 * time.Millisecond

	// defaultWriteTimeout bounds write-side stalls when net/http does not set
	// a write deadline for the connection.
	defaultWriteTimeout = 5 * time.Second
)

// MessageConn models an ordered, reliable message channel that can be adapted
// into a byte-stream net.Conn.
//
// RecvMessage should block until one message is available or the context is
// done. Returned message data should not be mutated after return.
//
// SendMessage implementations must not retain payload after returning.
type MessageConn interface {
	SendMessage(ctx context.Context, payload []byte) error
	RecvMessage(ctx context.Context) ([]byte, error)
	Close() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type frameReadableMessageConn interface {
	RecvFrame(ctx context.Context) (payload []byte, totalLen int, offset int, err error)
	RepostFrame() error
}

// Conn adapts a MessageConn into net.Conn semantics so it can be used by
// net/http transports.
type Conn struct {
	stream MessageConn

	readMu  sync.Mutex
	writeMu sync.Mutex

	closeOnce sync.Once
	closeErr  error

	readBuf []byte

	frameBuf         []byte
	frameBufPos      int
	frameMsgActive   bool
	frameMsgTotal    int
	frameMsgReceived int

	deadlineMu    sync.RWMutex
	readDeadline  time.Time
	writeDeadline time.Time
}

var _ net.Conn = (*Conn)(nil)

// NewConn wraps a MessageConn into a net.Conn.
func NewConn(stream MessageConn) *Conn {
	return &Conn{stream: stream}
}

func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.stream == nil {
		return 0, net.ErrClosed
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()

	if fr, ok := c.stream.(frameReadableMessageConn); ok {
		return c.readFromFrames(fr, p)
	}

	for len(c.readBuf) == 0 {
		msg, err := c.recvWithDeadline()
		if err != nil {
			return 0, err
		}
		if len(msg) == 0 {
			continue
		}
		c.readBuf = msg
	}

	n := copy(p, c.readBuf)
	c.readBuf = c.readBuf[n:]
	if len(c.readBuf) == 0 {
		c.readBuf = nil
	}

	return n, nil
}

func (c *Conn) readFromFrames(fr frameReadableMessageConn, p []byte) (int, error) {
	for {
		if len(c.frameBuf) == 0 {
			payload, total, offset, err := c.recvFrameWithDeadline(fr)
			if err != nil {
				return 0, err
			}

			if err := c.attachFrameState(total, offset, len(payload)); err != nil {
				_ = fr.RepostFrame()
				return 0, err
			}
			c.frameBuf = payload
			c.frameBufPos = 0

			if len(c.frameBuf) == 0 {
				if err := fr.RepostFrame(); err != nil {
					return 0, err
				}
				if c.frameMsgActive && c.frameMsgReceived == c.frameMsgTotal {
					c.frameMsgActive = false
					c.frameMsgTotal = 0
					c.frameMsgReceived = 0
				}
				continue
			}
		}

		n := copy(p, c.frameBuf[c.frameBufPos:])
		c.frameBufPos += n
		c.frameMsgReceived += n

		if c.frameBufPos == len(c.frameBuf) {
			c.frameBuf = nil
			c.frameBufPos = 0
			if err := fr.RepostFrame(); err != nil {
				return n, err
			}
			if c.frameMsgActive && c.frameMsgReceived == c.frameMsgTotal {
				c.frameMsgActive = false
				c.frameMsgTotal = 0
				c.frameMsgReceived = 0
			}
		}

		if n > 0 {
			return n, nil
		}
	}
}

func (c *Conn) recvFrameWithDeadline(fr frameReadableMessageConn) ([]byte, int, int, error) {
	for {
		deadline := c.getReadDeadline()
		ctx, cancel := c.contextWithReadSlice(deadline)
		payload, total, offset, err := fr.RecvFrame(ctx)
		cancel()
		if err == nil {
			return payload, total, offset, nil
		}

		if errors.Is(err, context.DeadlineExceeded) {
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return nil, 0, 0, os.ErrDeadlineExceeded
			}
			continue
		}
		return nil, 0, 0, normalizeContextErr(err)
	}
}

func (c *Conn) attachFrameState(total, offset, payloadLen int) error {
	if total < 0 || offset < 0 || payloadLen < 0 || offset+payloadLen > total {
		return net.InvalidAddrError("rdma frame range invalid")
	}

	if !c.frameMsgActive {
		if offset != 0 {
			return net.InvalidAddrError("rdma frame offset without active message")
		}
		c.frameMsgActive = true
		c.frameMsgTotal = total
		c.frameMsgReceived = 0
		return nil
	}

	if total != c.frameMsgTotal {
		return net.InvalidAddrError("rdma frame total mismatch")
	}
	if offset != c.frameMsgReceived {
		return net.InvalidAddrError("rdma frame offset mismatch")
	}
	return nil
}

func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.stream == nil {
		return 0, net.ErrClosed
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	writeDeadline := c.getWriteDeadline()
	if writeDeadline.IsZero() {
		writeDeadline = time.Now().Add(defaultWriteTimeout)
	}

	ctx, cancel := c.contextWithDeadline(writeDeadline)
	defer cancel()

	if err := c.stream.SendMessage(ctx, p); err != nil {
		return 0, normalizeContextErr(err)
	}

	return len(p), nil
}

func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		if c.stream == nil {
			c.closeErr = net.ErrClosed
			return
		}
		c.closeErr = c.stream.Close()
	})
	return c.closeErr
}

func (c *Conn) LocalAddr() net.Addr {
	if c.stream == nil {
		return nil
	}
	return c.stream.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	if c.stream == nil {
		return nil
	}
	return c.stream.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *Conn) recvWithDeadline() ([]byte, error) {
	if c.stream == nil {
		return nil, net.ErrClosed
	}

	for {
		deadline := c.getReadDeadline()
		ctx, cancel := c.contextWithReadSlice(deadline)
		msg, err := c.stream.RecvMessage(ctx)
		cancel()
		if err == nil {
			return msg, nil
		}

		if errors.Is(err, context.DeadlineExceeded) {
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return nil, os.ErrDeadlineExceeded
			}
			// Re-check deadline and keep waiting.
			continue
		}

		return nil, normalizeContextErr(err)
	}
}

func (c *Conn) getReadDeadline() time.Time {
	c.deadlineMu.RLock()
	defer c.deadlineMu.RUnlock()
	return c.readDeadline
}

func (c *Conn) getWriteDeadline() time.Time {
	c.deadlineMu.RLock()
	defer c.deadlineMu.RUnlock()
	return c.writeDeadline
}

func (c *Conn) contextWithDeadline(deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithDeadline(context.Background(), deadline)
}

func (c *Conn) contextWithReadSlice(deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithTimeout(context.Background(), readPollInterval)
	}

	remain := time.Until(deadline)
	if remain <= 0 {
		return context.WithDeadline(context.Background(), deadline)
	}
	if remain > readPollInterval {
		return context.WithTimeout(context.Background(), readPollInterval)
	}
	return context.WithDeadline(context.Background(), deadline)
}

func normalizeContextErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return os.ErrDeadlineExceeded
	}
	return err
}
