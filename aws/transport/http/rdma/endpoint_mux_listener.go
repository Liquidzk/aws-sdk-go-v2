package rdma

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type messageAcceptListener interface {
	AcceptMessage() (MessageConn, error)
	Close() error
	Addr() net.Addr
}

type endpointMuxListener struct {
	base           messageAcceptListener
	sendQueueDepth int

	acceptCh chan net.Conn
	done     chan struct{}

	shutdownOnce sync.Once
	acceptWg     sync.WaitGroup

	mu    sync.Mutex
	links map[*endpointMuxServerPhysicalConn]struct{}
}

type endpointMuxServerPhysicalConn struct {
	listener *endpointMuxListener
	inner    MessageConn

	sendQueue chan muxSendRequest
	done      chan struct{}

	recvCopyMode endpointMuxRecvCopyMode

	termOnce sync.Once
	termMu   sync.RWMutex
	termErr  error

	streamMu sync.RWMutex
	streams  map[uint32]*endpointMuxServerStream
}

type endpointMuxServerStream struct {
	link *endpointMuxServerPhysicalConn
	id   uint32

	recvQueueMu sync.Mutex
	recvQueue   []muxRecvEvent
	recvNotify  chan struct{}

	sendSeq atomic.Uint32
	recvSeq atomic.Uint32

	closeOnce sync.Once
	closed    atomic.Bool
	closedCh  chan struct{}

	termOnce sync.Once
	termMu   sync.RWMutex
	termErr  error
	termCh   chan struct{}

	remoteClosed atomic.Bool
}

var _ net.Listener = (*endpointMuxListener)(nil)
var _ MessageConn = (*endpointMuxServerStream)(nil)

func newEndpointMuxListener(base messageAcceptListener, sendQueueDepth int) net.Listener {
	l := &endpointMuxListener{
		base:           base,
		sendQueueDepth: sendQueueDepth,
		acceptCh:       make(chan net.Conn, 1024),
		done:           make(chan struct{}),
		links:          map[*endpointMuxServerPhysicalConn]struct{}{},
	}
	l.acceptWg.Add(1)
	go l.acceptLoop()
	return l
}

func (l *endpointMuxListener) acceptLoop() {
	defer l.acceptWg.Done()

	for {
		select {
		case <-l.done:
			return
		default:
		}

		msgConn, err := l.base.AcceptMessage()
		if err != nil {
			select {
			case <-l.done:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				l.shutdown()
				return
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if msgConn == nil {
			continue
		}

		link := newEndpointMuxServerPhysicalConn(l, msgConn, l.sendQueueDepth)
		l.mu.Lock()
		l.links[link] = struct{}{}
		l.mu.Unlock()
	}
}

func (l *endpointMuxListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	case c, ok := <-l.acceptCh:
		if !ok || c == nil {
			return nil, net.ErrClosed
		}
		return c, nil
	}
}

func (l *endpointMuxListener) Close() error {
	l.shutdown()
	l.acceptWg.Wait()
	return nil
}

func (l *endpointMuxListener) Addr() net.Addr {
	return l.base.Addr()
}

func (l *endpointMuxListener) onPhysicalClosed(link *endpointMuxServerPhysicalConn) {
	l.mu.Lock()
	delete(l.links, link)
	l.mu.Unlock()
}

func (l *endpointMuxListener) shutdown() {
	l.shutdownOnce.Do(func() {
		close(l.done)
		_ = l.base.Close()

		l.mu.Lock()
		links := make([]*endpointMuxServerPhysicalConn, 0, len(l.links))
		for link := range l.links {
			links = append(links, link)
		}
		l.links = map[*endpointMuxServerPhysicalConn]struct{}{}
		l.mu.Unlock()

		for _, link := range links {
			link.closeWithError(net.ErrClosed)
		}
	})
}

func newEndpointMuxServerPhysicalConn(listener *endpointMuxListener, inner MessageConn, sendQueueDepth int) *endpointMuxServerPhysicalConn {
	queueCap := sendQueueDepth
	if queueCap <= 0 {
		queueCap = endpointMuxDefaultSendQCap
	}

	recvCopyMode := endpointMuxRecvCopyModeFromEnv()
	if recvCopyMode != endpointMuxRecvCopyModeImmediate &&
		!isMessageConnRecvPayloadStable(inner) &&
		!(recvCopyMode == endpointMuxRecvCopyModeZeroCopy && endpointMuxRecvUnsafeZeroCopyEnabled()) {
		recvCopyMode = endpointMuxRecvCopyModeImmediate
	}

	p := &endpointMuxServerPhysicalConn{
		listener:     listener,
		inner:        inner,
		sendQueue:    make(chan muxSendRequest, queueCap),
		done:         make(chan struct{}),
		recvCopyMode: recvCopyMode,
		streams:      map[uint32]*endpointMuxServerStream{},
	}
	go p.sendLoop()
	go p.recvLoop()
	return p
}

func (p *endpointMuxServerPhysicalConn) sendLoop() {
	for {
		select {
		case <-p.done:
			return
		case req := <-p.sendQueue:
			sendCtx := req.ctx
			if sendCtx == nil {
				sendCtx = context.Background()
			}
			if err := sendCtx.Err(); err != nil {
				if req.done != nil {
					req.done <- err
				}
				continue
			}

			err := sendMuxFramedMessage(sendCtx, p.inner, req.streamID, req.flags, req.seq, req.payload)
			if req.done != nil {
				req.done <- err
			}
			if shouldMarkPooledConnBroken(err) {
				p.closeWithError(err)
				return
			}
		}
	}
}

func (p *endpointMuxServerPhysicalConn) recvLoop() {
	for {
		select {
		case <-p.done:
			return
		default:
		}

		frame, err := p.inner.RecvMessage(context.Background())
		if err != nil {
			p.closeWithError(err)
			return
		}

		streamID, flags, seq, payload, decodeErr := decodeMuxFrame(frame)
		if decodeErr != nil {
			p.closeWithError(decodeErr)
			return
		}

		if dispatchErr := p.dispatchFrame(streamID, flags, seq, payload); dispatchErr != nil {
			p.closeWithError(dispatchErr)
			return
		}
	}
}

func (p *endpointMuxServerPhysicalConn) dispatchFrame(streamID uint32, flags uint16, seq uint32, payload []byte) error {
	if flags&muxFlagData != 0 {
		if err := p.dispatchData(streamID, seq, payload); err != nil {
			return err
		}
	}
	if flags&muxFlagClose != 0 {
		if err := p.dispatchRemoteClose(streamID, seq, flags&muxFlagData != 0); err != nil {
			return err
		}
	}
	return nil
}

func (p *endpointMuxServerPhysicalConn) dispatchData(streamID uint32, seq uint32, payload []byte) error {
	stream, isNew := p.getOrCreateStream(streamID)
	if stream == nil {
		return nil
	}
	if err := stream.acceptRecvSeq(seq); err != nil {
		return err
	}
	if isNew {
		conn := NewConn(stream)
		select {
		case <-p.done:
			return net.ErrClosed
		case <-p.listener.done:
			return net.ErrClosed
		case p.listener.acceptCh <- conn:
		}
	}
	switch p.recvCopyMode {
	case endpointMuxRecvCopyModeZeroCopy:
		stream.enqueueEvent(muxRecvEvent{payload: payload})
	case endpointMuxRecvCopyModeDeferred:
		stream.enqueueEvent(muxRecvEvent{payload: payload, copyOnRead: true})
	default:
		stream.enqueueEvent(muxRecvEvent{payload: append([]byte(nil), payload...)})
	}
	return nil
}

func (p *endpointMuxServerPhysicalConn) dispatchRemoteClose(streamID uint32, seq uint32, seqAlreadyApplied bool) error {
	stream := p.detachStream(streamID)
	if stream == nil {
		return nil
	}
	if !seqAlreadyApplied {
		if err := stream.acceptRecvSeq(seq); err != nil {
			return err
		}
	}
	stream.remoteClosed.Store(true)
	stream.enqueueEvent(muxRecvEvent{err: io.EOF})
	return nil
}

func (p *endpointMuxServerPhysicalConn) getOrCreateStream(streamID uint32) (*endpointMuxServerStream, bool) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()

	if stream, ok := p.streams[streamID]; ok {
		return stream, false
	}

	stream := &endpointMuxServerStream{
		link:       p,
		id:         streamID,
		recvNotify: make(chan struct{}, 1),
		closedCh:   make(chan struct{}),
		termCh:     make(chan struct{}),
	}
	p.streams[streamID] = stream
	return stream, true
}

func (p *endpointMuxServerPhysicalConn) detachStream(streamID uint32) *endpointMuxServerStream {
	p.streamMu.Lock()
	stream := p.streams[streamID]
	if stream != nil {
		delete(p.streams, streamID)
	}
	p.streamMu.Unlock()
	return stream
}

func (p *endpointMuxServerPhysicalConn) sendFrame(ctx context.Context, streamID uint32, flags uint16, seq uint32, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}

	req := muxSendRequest{
		ctx:      ctx,
		streamID: streamID,
		flags:    flags,
		seq:      seq,
		payload:  payload,
		done:     make(chan error, 1),
	}

	select {
	case <-p.done:
		return p.getTermErr()
	case p.sendQueue <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.getTermErr()
	}
}

func (p *endpointMuxServerPhysicalConn) closeWithError(err error) {
	p.termOnce.Do(func() {
		if err == nil {
			err = net.ErrClosed
		}
		p.termMu.Lock()
		p.termErr = err
		p.termMu.Unlock()

		close(p.done)
		_ = p.inner.Close()

		p.streamMu.Lock()
		streams := make([]*endpointMuxServerStream, 0, len(p.streams))
		for _, stream := range p.streams {
			streams = append(streams, stream)
		}
		p.streams = map[uint32]*endpointMuxServerStream{}
		p.streamMu.Unlock()

		for _, stream := range streams {
			stream.setTerminal(err)
		}

		p.listener.onPhysicalClosed(p)
	})
}

func (p *endpointMuxServerPhysicalConn) getTermErr() error {
	p.termMu.RLock()
	defer p.termMu.RUnlock()
	if p.termErr == nil {
		return net.ErrClosed
	}
	return p.termErr
}

func (s *endpointMuxServerStream) SendMessage(ctx context.Context, payload []byte) error {
	if s.closed.Load() {
		return net.ErrClosed
	}
	if s.remoteClosed.Load() {
		return net.ErrClosed
	}
	select {
	case <-s.termCh:
		return s.getTermErr()
	default:
	}

	return s.link.sendFrame(ctx, s.id, muxFlagData, s.nextSendSeq(), payload)
}

func (s *endpointMuxServerStream) RecvMessage(ctx context.Context) ([]byte, error) {
	if s.closed.Load() {
		return nil, net.ErrClosed
	}

	for {
		if ev, ok := s.popEvent(); ok {
			if ev.err != nil {
				return nil, ev.err
			}
			if ev.copyOnRead {
				return append([]byte(nil), ev.payload...), nil
			}
			return ev.payload, nil
		}
		if s.remoteClosed.Load() {
			return nil, io.EOF
		}

		select {
		case <-s.recvNotify:
		case <-s.termCh:
			return nil, s.getTermErr()
		case <-s.closedCh:
			return nil, net.ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *endpointMuxServerStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.closedCh)
		_ = s.link.detachStream(s.id)

		controlCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_ = s.link.sendFrame(controlCtx, s.id, muxFlagClose, s.nextSendSeq(), nil)
		cancel()
	})
	return nil
}

func (s *endpointMuxServerStream) LocalAddr() net.Addr {
	return s.link.inner.LocalAddr()
}

func (s *endpointMuxServerStream) RemoteAddr() net.Addr {
	return s.link.inner.RemoteAddr()
}

func (s *endpointMuxServerStream) setTerminal(err error) {
	s.termOnce.Do(func() {
		if err == nil {
			err = net.ErrClosed
		}
		s.termMu.Lock()
		s.termErr = err
		s.termMu.Unlock()
		close(s.termCh)
	})
}

func (s *endpointMuxServerStream) getTermErr() error {
	s.termMu.RLock()
	defer s.termMu.RUnlock()
	if s.termErr == nil {
		return net.ErrClosed
	}
	return s.termErr
}

func (s *endpointMuxServerStream) enqueueEvent(ev muxRecvEvent) {
	select {
	case <-s.closedCh:
		return
	case <-s.termCh:
		return
	default:
	}

	s.recvQueueMu.Lock()
	s.recvQueue = append(s.recvQueue, ev)
	s.recvQueueMu.Unlock()
	select {
	case s.recvNotify <- struct{}{}:
	default:
	}
}

func (s *endpointMuxServerStream) hasPendingEvents() bool {
	s.recvQueueMu.Lock()
	n := len(s.recvQueue)
	s.recvQueueMu.Unlock()
	return n > 0
}

func (s *endpointMuxServerStream) popEvent() (muxRecvEvent, bool) {
	s.recvQueueMu.Lock()
	defer s.recvQueueMu.Unlock()
	if len(s.recvQueue) == 0 {
		return muxRecvEvent{}, false
	}
	ev := s.recvQueue[0]
	s.recvQueue[0] = muxRecvEvent{}
	s.recvQueue = s.recvQueue[1:]
	return ev, true
}

func (s *endpointMuxServerStream) nextSendSeq() uint32 {
	return s.sendSeq.Add(1)
}

func (s *endpointMuxServerStream) acceptRecvSeq(seq uint32) error {
	if seq == 0 {
		return errors.New("endpoint mux listener: sequence must be non-zero")
	}
	expect := s.recvSeq.Add(1)
	if seq != expect {
		return errors.New("endpoint mux listener: stream sequence out of order")
	}
	return nil
}
