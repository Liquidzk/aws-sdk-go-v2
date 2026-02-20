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

// Conn adapts a MessageConn into net.Conn semantics so it can be used by
// net/http transports.
type Conn struct {
	stream MessageConn

	readMu  sync.Mutex
	writeMu sync.Mutex

	readCancelMu       sync.Mutex
	readInflightCancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error

	readBuf []byte

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
	ctx := context.Background()
	cancel := func() {}
	if !writeDeadline.IsZero() {
		ctx, cancel = c.contextWithDeadline(writeDeadline)
	}
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
	c.interruptInflightRead()
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.deadlineMu.Unlock()
	c.interruptInflightRead()
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

	for c.getReadDeadline().IsZero() {
		ctx, cancel := context.WithCancel(context.Background())
		c.setInflightReadCancel(cancel)
		msg, err := c.stream.RecvMessage(ctx)
		c.clearInflightReadCancel()
		cancel()
		if err == nil {
			return msg, nil
		}
		if errors.Is(err, context.Canceled) {
			continue
		}
		return nil, normalizeContextErr(err)
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

func (c *Conn) setInflightReadCancel(cancel context.CancelFunc) {
	c.readCancelMu.Lock()
	c.readInflightCancel = cancel
	c.readCancelMu.Unlock()
}

func (c *Conn) clearInflightReadCancel() {
	c.readCancelMu.Lock()
	c.readInflightCancel = nil
	c.readCancelMu.Unlock()
}

func (c *Conn) interruptInflightRead() {
	c.readCancelMu.Lock()
	cancel := c.readInflightCancel
	c.readCancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func normalizeContextErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return os.ErrDeadlineExceeded
	}
	return err
}
