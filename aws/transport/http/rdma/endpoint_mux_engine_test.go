package rdma

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type loopbackMuxMessageConn struct {
	ch chan []byte

	closeOnce sync.Once
	closed    chan struct{}

	local  net.Addr
	remote net.Addr
}

func newLoopbackMuxMessageConn() *loopbackMuxMessageConn {
	return &loopbackMuxMessageConn{
		ch:     make(chan []byte, 256),
		closed: make(chan struct{}),
		local:  mockAddr("mux-local"),
		remote: mockAddr("mux-remote"),
	}
}

func (m *loopbackMuxMessageConn) SendMessage(ctx context.Context, payload []byte) error {
	msg := append([]byte(nil), payload...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closed:
		return net.ErrClosed
	case m.ch <- msg:
		return nil
	}
}

func (m *loopbackMuxMessageConn) RecvMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-m.ch:
		if !ok {
			return nil, net.ErrClosed
		}
		return append([]byte(nil), msg...), nil
	}
}

func (m *loopbackMuxMessageConn) Close() error {
	m.closeOnce.Do(func() {
		close(m.closed)
		close(m.ch)
	})
	return nil
}

func (m *loopbackMuxMessageConn) LocalAddr() net.Addr  { return m.local }
func (m *loopbackMuxMessageConn) RemoteAddr() net.Addr { return m.remote }

func closeEndpointMuxEngineForTest(e *endpointMuxEngine) {
	e.mu.Lock()
	e.closing = true
	links := append([]*endpointMuxPhysicalConn(nil), e.links...)
	e.mu.Unlock()
	for _, link := range links {
		link.closeWithError(net.ErrClosed)
	}
}

func TestMuxFrameCodecRoundTrip(t *testing.T) {
	payload := []byte("abc")
	frame := encodeMuxFrame(7, muxFlagData, 3, payload)

	streamID, flags, seq, out, err := decodeMuxFrame(frame)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if streamID != 7 {
		t.Fatalf("stream id = %d", streamID)
	}
	if flags != muxFlagData {
		t.Fatalf("flags = %#x", flags)
	}
	if seq != 3 {
		t.Fatalf("seq = %d", seq)
	}
	if !bytes.Equal(payload, out) {
		t.Fatalf("payload mismatch")
	}

	_, _, _, _, err = decodeMuxFrame([]byte{1, 2, 3})
	if err == nil {
		t.Fatalf("expected short frame decode error")
	}

	_, _, _, _, err = decodeMuxFrame(encodeMuxFrame(0, muxFlagData, 1, nil))
	if err == nil {
		t.Fatalf("expected stream id validation error")
	}

	_, _, _, _, err = decodeMuxFrame(encodeMuxFrame(7, muxFlagData, 0, nil))
	if err == nil {
		t.Fatalf("expected sequence validation error")
	}

	_, _, _, _, err = decodeMuxFrame(encodeMuxFrame(7, 0, 1, nil))
	if err == nil {
		t.Fatalf("expected flags validation error")
	}

	badLen := encodeMuxFrame(7, muxFlagData, 1, []byte("x"))
	badLen[7] = 7
	_, _, _, _, err = decodeMuxFrame(badLen)
	if err == nil {
		t.Fatalf("expected length mismatch validation error")
	}
}

func TestEndpointMuxEngineReusesPhysicalConnection(t *testing.T) {
	var openCalls int32
	engine, err := newEndpointMuxEngine(endpointMuxEngineConfig{
		key:      "k",
		network:  "rdma",
		address:  "127.0.0.1:1",
		poolSize: 1,
		open: func(ctx context.Context, network, address string) (MessageConn, error) {
			atomic.AddInt32(&openCalls, 1)
			return newLoopbackMuxMessageConn(), nil
		},
	})
	if err != nil {
		t.Fatalf("new endpoint mux engine: %v", err)
	}
	defer closeEndpointMuxEngineForTest(engine)

	s1, err := engine.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire stream1: %v", err)
	}
	s2, err := engine.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire stream2: %v", err)
	}
	defer s1.Close()
	defer s2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s1.SendMessage(ctx, []byte("one")); err != nil {
		t.Fatalf("stream1 send: %v", err)
	}
	if err := s2.SendMessage(ctx, []byte("two")); err != nil {
		t.Fatalf("stream2 send: %v", err)
	}

	got1, err := s1.RecvMessage(ctx)
	if err != nil {
		t.Fatalf("stream1 recv: %v", err)
	}
	got2, err := s2.RecvMessage(ctx)
	if err != nil {
		t.Fatalf("stream2 recv: %v", err)
	}
	if !bytes.Equal(got1, []byte("one")) {
		t.Fatalf("stream1 payload=%q", string(got1))
	}
	if !bytes.Equal(got2, []byte("two")) {
		t.Fatalf("stream2 payload=%q", string(got2))
	}

	if e, a := int32(1), atomic.LoadInt32(&openCalls); e != a {
		t.Fatalf("open calls=%d, want %d", a, e)
	}
}

func TestEndpointMuxEngineScalesToPoolSize(t *testing.T) {
	var openCalls int32
	engine, err := newEndpointMuxEngine(endpointMuxEngineConfig{
		key:      "k",
		network:  "rdma",
		address:  "127.0.0.1:1",
		poolSize: 2,
		open: func(ctx context.Context, network, address string) (MessageConn, error) {
			atomic.AddInt32(&openCalls, 1)
			return newLoopbackMuxMessageConn(), nil
		},
	})
	if err != nil {
		t.Fatalf("new endpoint mux engine: %v", err)
	}
	defer closeEndpointMuxEngineForTest(engine)

	s1, err := engine.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire stream1: %v", err)
	}
	defer s1.Close()

	s2, err := engine.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire stream2: %v", err)
	}
	defer s2.Close()

	if e, a := int32(2), atomic.LoadInt32(&openCalls); e != a {
		t.Fatalf("open calls=%d, want %d", a, e)
	}
}

func TestEndpointMuxEngineSharedMemoryBudget(t *testing.T) {
	var openCalls int32
	engine, err := newEndpointMuxEngine(endpointMuxEngineConfig{
		key:      "k",
		network:  "rdma",
		address:  "127.0.0.1:1",
		poolSize: 2,
		open: func(ctx context.Context, network, address string) (MessageConn, error) {
			atomic.AddInt32(&openCalls, 1)
			return newLoopbackMuxMessageConn(), nil
		},
		memoryBudget:  newSharedMemoryBudget(1024),
		memoryPerConn: 1024,
	})
	if err != nil {
		t.Fatalf("new endpoint mux engine: %v", err)
	}
	defer closeEndpointMuxEngineForTest(engine)

	s1, err := engine.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire stream1: %v", err)
	}
	defer s1.Close()

	s2, err := engine.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire stream2: %v", err)
	}
	defer s2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s2.SendMessage(ctx, []byte("ok")); err != nil {
		t.Fatalf("stream2 send: %v", err)
	}
	got, err := s2.RecvMessage(ctx)
	if err != nil {
		t.Fatalf("stream2 recv: %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("stream2 payload=%q", string(got))
	}

	if e, a := int32(1), atomic.LoadInt32(&openCalls); e != a {
		t.Fatalf("open calls=%d, want %d", a, e)
	}
}

func TestEndpointMuxEngineAcquireNoBudgetNoConn(t *testing.T) {
	engine, err := newEndpointMuxEngine(endpointMuxEngineConfig{
		key:      "k",
		network:  "rdma",
		address:  "127.0.0.1:1",
		poolSize: 1,
		open: func(ctx context.Context, network, address string) (MessageConn, error) {
			return newLoopbackMuxMessageConn(), nil
		},
		memoryBudget:  newSharedMemoryBudget(1),
		memoryPerConn: 1024,
	})
	if err != nil {
		t.Fatalf("new endpoint mux engine: %v", err)
	}
	defer closeEndpointMuxEngineForTest(engine)

	_, err = engine.Acquire(context.Background())
	if !errors.Is(err, ErrSharedMemoryBudgetExhausted) {
		t.Fatalf("expected shared memory budget exhausted, got %v", err)
	}
}

func TestEndpointMuxStreamSequenceValidation(t *testing.T) {
	s := &endpointMuxStream{id: 1}
	if err := s.acceptRecvSeq(1); err != nil {
		t.Fatalf("unexpected seq validation error: %v", err)
	}
	if err := s.acceptRecvSeq(3); err == nil {
		t.Fatalf("expected out-of-order sequence to fail")
	}
}
