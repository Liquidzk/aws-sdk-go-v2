package rdma

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type mockMessageListener struct {
	ch        chan MessageConn
	done      chan struct{}
	closeOnce sync.Once
}

func newMockMessageListener() *mockMessageListener {
	return &mockMessageListener{
		ch:   make(chan MessageConn, 16),
		done: make(chan struct{}),
	}
}

func (m *mockMessageListener) AcceptMessage() (MessageConn, error) {
	select {
	case <-m.done:
		return nil, net.ErrClosed
	case conn, ok := <-m.ch:
		if !ok || conn == nil {
			return nil, net.ErrClosed
		}
		return conn, nil
	}
}

func (m *mockMessageListener) Close() error {
	m.closeOnce.Do(func() {
		close(m.done)
		close(m.ch)
	})
	return nil
}

func (m *mockMessageListener) Addr() net.Addr {
	return mockAddr("mock-listener")
}

func (m *mockMessageListener) Push(conn MessageConn) {
	m.ch <- conn
}

type pipePairState struct {
	aToB chan []byte
	bToA chan []byte
	done chan struct{}
	once sync.Once
}

type pipeMessageConn struct {
	in    chan []byte
	out   chan []byte
	state *pipePairState
	local net.Addr
	peer  net.Addr
}

func newPipeMessageConnPair() (MessageConn, MessageConn) {
	state := &pipePairState{
		aToB: make(chan []byte, 256),
		bToA: make(chan []byte, 256),
		done: make(chan struct{}),
	}
	a := &pipeMessageConn{
		in:    state.bToA,
		out:   state.aToB,
		state: state,
		local: mockAddr("pipe-a"),
		peer:  mockAddr("pipe-b"),
	}
	b := &pipeMessageConn{
		in:    state.aToB,
		out:   state.bToA,
		state: state,
		local: mockAddr("pipe-b"),
		peer:  mockAddr("pipe-a"),
	}
	return a, b
}

func (p *pipeMessageConn) SendMessage(ctx context.Context, payload []byte) error {
	msg := append([]byte(nil), payload...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.state.done:
		return net.ErrClosed
	case p.out <- msg:
		return nil
	}
}

func (p *pipeMessageConn) RecvMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.state.done:
		return nil, net.ErrClosed
	case msg, ok := <-p.in:
		if !ok {
			return nil, net.ErrClosed
		}
		return append([]byte(nil), msg...), nil
	}
}

func (p *pipeMessageConn) Close() error {
	p.state.once.Do(func() {
		close(p.state.done)
		close(p.state.aToB)
		close(p.state.bToA)
	})
	return nil
}

func (p *pipeMessageConn) LocalAddr() net.Addr  { return p.local }
func (p *pipeMessageConn) RemoteAddr() net.Addr { return p.peer }

func TestEndpointMuxListenerAcceptAndReply(t *testing.T) {
	base := newMockMessageListener()
	ln := newEndpointMuxListener(base, 0)
	defer ln.Close()

	clientPhys, serverPhys := newPipeMessageConnPair()
	base.Push(serverPhys)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := clientPhys.SendMessage(ctx, encodeMuxFrame(11, muxFlagData, 1, []byte("ping"))); err != nil {
		t.Fatalf("client send: %v", err)
	}

	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("listener accept: %v", err)
	}
	defer conn.Close()

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("server read stream payload: %v", err)
	}
	if !bytes.Equal(buf, []byte("ping")) {
		t.Fatalf("unexpected payload=%q", string(buf))
	}

	if _, err := conn.Write([]byte("pong")); err != nil {
		t.Fatalf("server write stream payload: %v", err)
	}

	frame, err := clientPhys.RecvMessage(ctx)
	if err != nil {
		t.Fatalf("client recv response frame: %v", err)
	}
	streamID, flags, seq, payload, err := decodeMuxFrame(frame)
	if err != nil {
		t.Fatalf("decode response frame: %v", err)
	}
	if flags != muxFlagData || streamID != 11 || seq != 1 || !bytes.Equal(payload, []byte("pong")) {
		t.Fatalf("unexpected response frame flags=%#x stream=%d seq=%d payload=%q", flags, streamID, seq, string(payload))
	}
}

func TestEndpointMuxListenerMultipleLogicalStreams(t *testing.T) {
	base := newMockMessageListener()
	ln := newEndpointMuxListener(base, 0)
	defer ln.Close()

	clientPhys, serverPhys := newPipeMessageConnPair()
	base.Push(serverPhys)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := clientPhys.SendMessage(ctx, encodeMuxFrame(1, muxFlagData, 1, []byte("a"))); err != nil {
		t.Fatalf("send stream1: %v", err)
	}
	conn1, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept stream1: %v", err)
	}
	defer conn1.Close()

	if err := clientPhys.SendMessage(ctx, encodeMuxFrame(2, muxFlagData, 1, []byte("b"))); err != nil {
		t.Fatalf("send stream2: %v", err)
	}
	conn2, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept stream2: %v", err)
	}
	defer conn2.Close()

	buf1 := make([]byte, 1)
	if _, err := io.ReadFull(conn1, buf1); err != nil {
		t.Fatalf("read stream1: %v", err)
	}
	buf2 := make([]byte, 1)
	if _, err := io.ReadFull(conn2, buf2); err != nil {
		t.Fatalf("read stream2: %v", err)
	}

	if !bytes.Equal(buf1, []byte("a")) || !bytes.Equal(buf2, []byte("b")) {
		t.Fatalf("unexpected payloads stream1=%q stream2=%q", string(buf1), string(buf2))
	}
}

func TestEndpointMuxListenerRemoteCloseYieldsEOF(t *testing.T) {
	base := newMockMessageListener()
	ln := newEndpointMuxListener(base, 0)
	defer ln.Close()

	clientPhys, serverPhys := newPipeMessageConnPair()
	base.Push(serverPhys)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := clientPhys.SendMessage(ctx, encodeMuxFrame(7, muxFlagData, 1, []byte("x"))); err != nil {
		t.Fatalf("send stream data: %v", err)
	}
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept stream: %v", err)
	}
	defer conn.Close()

	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read stream payload: %v", err)
	}

	if err := clientPhys.SendMessage(ctx, encodeMuxFrame(7, muxFlagClose, 2, nil)); err != nil {
		t.Fatalf("send stream close: %v", err)
	}

	buf2 := make([]byte, 1)
	_, err = conn.Read(buf2)
	if err != io.EOF {
		t.Fatalf("expected io.EOF after remote close, got %v", err)
	}
}

func TestEndpointMuxServerStreamSequenceValidation(t *testing.T) {
	s := &endpointMuxServerStream{id: 9}
	if err := s.acceptRecvSeq(1); err != nil {
		t.Fatalf("unexpected seq validation error: %v", err)
	}
	if err := s.acceptRecvSeq(4); err == nil {
		t.Fatalf("expected out-of-order sequence to fail")
	}
}
