package rdma

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
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

func TestConnReadDeadlineInterruptsInFlightRead(t *testing.T) {
	var recvCalls int32
	c := NewConn(&mockMessageConn{
		recvFn: func(ctx context.Context) ([]byte, error) {
			atomic.AddInt32(&recvCalls, 1)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	if err := c.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline failed: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("read did not unblock after setting read deadline")
	}

	if atomic.LoadInt32(&recvCalls) == 0 {
		t.Fatalf("expected recv to be called at least once")
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

func TestConnWriteUsesDefaultTimeoutWhenNoDeadline(t *testing.T) {
	old := defaultWriteTimeout
	defaultWriteTimeout = 30 * time.Millisecond
	defer func() { defaultWriteTimeout = old }()

	c := NewConn(&mockMessageConn{
		sendFn: func(ctx context.Context, payload []byte) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	start := time.Now()
	_, err := c.Write([]byte("x"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("write took too long: %s", took)
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

func TestDialerOpenParallelismLimit(t *testing.T) {
	var active int32
	var maxActive int32

	d := NewVerbsDialer(VerbsOptions{})
	d.Open = func(ctx context.Context, network, address string) (MessageConn, error) {
		cur := atomic.AddInt32(&active, 1)
		for {
			prev := atomic.LoadInt32(&maxActive)
			if cur <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return &mockMessageConn{}, nil
	}
	d.OpenParallelism = 1
	d.OpenMinInterval = 0

	var wg sync.WaitGroup
	errCh := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := d.DialContext(context.Background(), "tcp", "example:1")
			if err != nil {
				errCh <- err
				return
			}
			_ = conn.Close()
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
	}

	if got := atomic.LoadInt32(&maxActive); got > 1 {
		t.Fatalf("expected max concurrent open <= 1, got %d", got)
	}
}

func TestDialerOpenMinInterval(t *testing.T) {
	var (
		mu    sync.Mutex
		times []time.Time
	)

	d := NewVerbsDialer(VerbsOptions{})
	d.Open = func(ctx context.Context, network, address string) (MessageConn, error) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		return &mockMessageConn{}, nil
	}
	d.OpenParallelism = 1
	d.OpenMinInterval = 40 * time.Millisecond

	for i := 0; i < 2; i++ {
		conn, err := d.DialContext(context.Background(), "tcp", "example:1")
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		_ = conn.Close()
	}

	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("expected 2 open calls, got %d", len(times))
	}

	gap := times[1].Sub(times[0])
	if gap < 30*time.Millisecond {
		t.Fatalf("expected open interval >= 30ms, got %s", gap)
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
