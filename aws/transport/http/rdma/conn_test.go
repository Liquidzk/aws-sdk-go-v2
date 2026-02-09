package rdma

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type mockAddr string

func (a mockAddr) Network() string { return "rdma" }
func (a mockAddr) String() string  { return string(a) }

type mockMessageConn struct {
	sendFn  func(ctx context.Context, payload []byte) error
	recvFn  func(ctx context.Context) ([]byte, error)
	closeFn func() error
	local   net.Addr
	remote  net.Addr
}

func (m *mockMessageConn) SendMessage(ctx context.Context, payload []byte) error {
	if m.sendFn != nil {
		return m.sendFn(ctx, payload)
	}
	return nil
}

func (m *mockMessageConn) RecvMessage(ctx context.Context) ([]byte, error) {
	if m.recvFn != nil {
		return m.recvFn(ctx)
	}
	return nil, io.EOF
}

func (m *mockMessageConn) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func (m *mockMessageConn) LocalAddr() net.Addr {
	if m.local != nil {
		return m.local
	}
	return mockAddr("local")
}

func (m *mockMessageConn) RemoteAddr() net.Addr {
	if m.remote != nil {
		return m.remote
	}
	return mockAddr("remote")
}

func TestConnReadBuffersMessage(t *testing.T) {
	var recvCalls int32
	c := NewConn(&mockMessageConn{
		recvFn: func(ctx context.Context) ([]byte, error) {
			atomic.AddInt32(&recvCalls, 1)
			return []byte("hello"), nil
		},
	})

	buf := make([]byte, 2)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read 1 failed: %v", err)
	}
	if got := string(buf[:n]); got != "he" {
		t.Fatalf("read 1 got %q", got)
	}

	buf2 := make([]byte, 8)
	n, err = c.Read(buf2)
	if err != nil {
		t.Fatalf("read 2 failed: %v", err)
	}
	if got := string(buf2[:n]); got != "llo" {
		t.Fatalf("read 2 got %q", got)
	}

	if atomic.LoadInt32(&recvCalls) != 1 {
		t.Fatalf("expected 1 recv call, got %d", recvCalls)
	}
}

func TestConnWriteSendsWholePayload(t *testing.T) {
	var sent []byte
	c := NewConn(&mockMessageConn{
		sendFn: func(ctx context.Context, payload []byte) error {
			sent = append([]byte(nil), payload...)
			return nil
		},
	})

	n, err := c.Write([]byte("payload"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len("payload") {
		t.Fatalf("write n=%d", n)
	}
	if got := string(sent); got != "payload" {
		t.Fatalf("sent %q", got)
	}
}

func TestConnReadDeadline(t *testing.T) {
	c := NewConn(&mockMessageConn{
		recvFn: func(ctx context.Context) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	if err := c.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline failed: %v", err)
	}

	_, err := c.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestConnWriteDeadline(t *testing.T) {
	c := NewConn(&mockMessageConn{
		sendFn: func(ctx context.Context, payload []byte) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	if err := c.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("set write deadline failed: %v", err)
	}

	_, err := c.Write([]byte("x"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestConnCloseIdempotent(t *testing.T) {
	var closeCalls int32
	c := NewConn(&mockMessageConn{
		closeFn: func() error {
			atomic.AddInt32(&closeCalls, 1)
			return nil
		},
	})

	if err := c.Close(); err != nil {
		t.Fatalf("close 1 failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close 2 failed: %v", err)
	}
	if atomic.LoadInt32(&closeCalls) != 1 {
		t.Fatalf("close calls = %d", closeCalls)
	}
}

func TestDialerOpenSuccess(t *testing.T) {
	d := Dialer{
		Open: func(ctx context.Context, network, address string) (MessageConn, error) {
			return &mockMessageConn{}, nil
		},
	}

	conn, err := d.DialContext(context.Background(), "tcp", "example:1")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if _, ok := conn.(*Conn); !ok {
		t.Fatalf("expected *rdma.Conn, got %T", conn)
	}
}

func TestDialerFallbackOnOpenFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		conn, aerr := ln.Accept()
		if aerr == nil {
			_ = conn.Close()
		}
	}()

	d := Dialer{
		Open: func(ctx context.Context, network, address string) (MessageConn, error) {
			return nil, errors.New("rdma unavailable")
		},
	}

	conn, err := d.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial with fallback failed: %v", err)
	}
	_ = conn.Close()
	<-acceptDone
}

func TestDialerDisableFallback(t *testing.T) {
	d := Dialer{
		Open: func(ctx context.Context, network, address string) (MessageConn, error) {
			return nil, errors.New("rdma unavailable")
		},
		DisableFallback: true,
	}

	_, err := d.DialContext(context.Background(), "tcp", "127.0.0.1:1")
	if err == nil || err.Error() != "rdma unavailable" {
		t.Fatalf("expected rdma error, got %v", err)
	}
}

func TestDialerNilMessageConn(t *testing.T) {
	d := Dialer{
		Open: func(ctx context.Context, network, address string) (MessageConn, error) {
			return nil, nil
		},
		DisableFallback: true,
	}

	_, err := d.DialContext(context.Background(), "tcp", "127.0.0.1:1")
	if err == nil {
		t.Fatalf("expected error for nil message conn")
	}
}

func TestConnNilStreamReturnsErrClosed(t *testing.T) {
	c := NewConn(nil)

	_, err := c.Read(make([]byte, 1))
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read expected net.ErrClosed, got %v", err)
	}

	_, err = c.Write([]byte("x"))
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write expected net.ErrClosed, got %v", err)
	}

	err = c.Close()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close expected net.ErrClosed, got %v", err)
	}
}
