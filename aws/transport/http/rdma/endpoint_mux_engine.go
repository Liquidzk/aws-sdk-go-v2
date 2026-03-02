package rdma

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	muxFlagData  uint16 = 1 << 0
	muxFlagClose uint16 = 1 << 1

	muxFrameHeaderSize         = 16
	endpointMuxDefaultSendQCap = 64
	endpointMuxDiagDefaultTick = 2 * time.Second

	endpointMuxRecvModeCopy      = "copy"
	endpointMuxRecvModeDeferred  = "deferred-copy"
	endpointMuxRecvModeZeroCopy  = "zero-copy"
	endpointMuxRecvModeEnv       = "AWS_RDMA_ENDPOINT_MUX_RECV_MODE"
	endpointMuxRecvUnsafeModeEnv = "AWS_RDMA_ENDPOINT_MUX_RECV_UNSAFE"
)

type endpointMuxEngineConfig struct {
	key            string
	network        string
	address        string
	poolSize       int
	warmup         bool
	acquireTimeout time.Duration
	sendQueueDepth int
	open           OpenFunc

	memoryBudget  *sharedMemoryBudget
	memoryPerConn int
}

type endpointMuxEngine struct {
	key            string
	network        string
	address        string
	poolSize       int
	warmup         bool
	acquireTimeout time.Duration
	sendQueueDepth int
	open           OpenFunc

	memoryBudget  *sharedMemoryBudget
	memoryPerConn int

	mu      sync.Mutex
	opened  int
	closing bool
	links   []*endpointMuxPhysicalConn

	diagEnabled  bool
	diagInterval time.Duration
	nextLinkID   atomic.Uint64

	recvCopyMode       endpointMuxRecvCopyMode
	recvUnsafeZeroCopy bool

	warmupRunning atomic.Bool
}

type muxRecvEvent struct {
	payload    []byte
	err        error
	copyOnRead bool
}

type endpointMuxRecvCopyMode int

const (
	endpointMuxRecvCopyModeImmediate endpointMuxRecvCopyMode = iota
	endpointMuxRecvCopyModeDeferred
	endpointMuxRecvCopyModeZeroCopy
)

type muxSendRequest struct {
	ctx      context.Context
	streamID uint32
	flags    uint16
	seq      uint32
	payload  []byte
	done     chan error

	enqueuedAt  time.Time
	payloadSize int
}

type endpointMuxPhysicalConn struct {
	engine *endpointMuxEngine
	inner  MessageConn

	id uint64

	sendQueue chan muxSendRequest
	done      chan struct{}

	termOnce sync.Once
	termMu   sync.RWMutex
	termErr  error

	streamMu sync.RWMutex
	streams  map[uint32]*endpointMuxStream
	nextID   atomic.Uint32

	activeStreams atomic.Int64

	streamsCreated atomic.Int64

	sendFramesTotal  atomic.Int64
	sendBytesTotal   atomic.Int64
	sendQueueWaitNs  atomic.Int64
	sendExecuteNs    atomic.Int64
	sendQueueMaxSeen atomic.Int64

	recvFramesTotal atomic.Int64
	recvBytesTotal  atomic.Int64
	recvDataTotal   atomic.Int64
	recvCloseTotal  atomic.Int64

	recvCopyMode endpointMuxRecvCopyMode
}

type endpointMuxStream struct {
	link *endpointMuxPhysicalConn
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

var _ MessageConn = (*endpointMuxStream)(nil)

func newEndpointMuxEngine(cfg endpointMuxEngineConfig) (*endpointMuxEngine, error) {
	if cfg.open == nil {
		return nil, errors.New("endpoint mux engine: open func is nil")
	}
	if cfg.poolSize <= 0 {
		return nil, fmt.Errorf("endpoint mux engine: pool size must be > 0")
	}
	if cfg.acquireTimeout < 0 {
		return nil, fmt.Errorf("endpoint mux engine: acquire timeout must be >= 0")
	}
	if cfg.sendQueueDepth < 0 {
		return nil, fmt.Errorf("endpoint mux engine: send queue depth must be >= 0")
	}
	if cfg.memoryPerConn < 0 {
		return nil, fmt.Errorf("endpoint mux engine: memory per conn must be >= 0")
	}

	e := &endpointMuxEngine{
		key:            cfg.key,
		network:        cfg.network,
		address:        cfg.address,
		poolSize:       cfg.poolSize,
		warmup:         cfg.warmup,
		acquireTimeout: cfg.acquireTimeout,
		sendQueueDepth: cfg.sendQueueDepth,
		open:           cfg.open,
		memoryBudget:   cfg.memoryBudget,
		memoryPerConn:  cfg.memoryPerConn,
		diagEnabled:    endpointMuxDiagEnabled(),
		diagInterval:   endpointMuxDiagInterval(),
		recvCopyMode:   endpointMuxRecvCopyModeFromEnv(),
	}
	e.recvUnsafeZeroCopy = endpointMuxRecvUnsafeZeroCopyEnabled()

	if e.diagEnabled {
		e.startDiagLoop()
	}
	if e.warmup {
		e.startWarmup()
	}
	return e, nil
}

func (e *endpointMuxEngine) Acquire(ctx context.Context) (MessageConn, error) {
	if e.acquireTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.acquireTimeout)
		defer cancel()
	}

	for {
		link, minActive, total := e.pickLink()
		if link != nil {
			if total < e.poolSize && minActive > 0 {
				if newLink, opened, err := e.tryOpen(ctx); err == nil && opened {
					stream, acqErr := newLink.acquireStream(ctx)
					if acqErr == nil {
						return stream, nil
					}
					if !errors.Is(acqErr, net.ErrClosed) {
						return nil, acqErr
					}
				}
			}
			stream, acqErr := link.acquireStream(ctx)
			if acqErr == nil {
				return stream, nil
			}
			if !errors.Is(acqErr, net.ErrClosed) {
				return nil, acqErr
			}
			continue
		}

		newLink, opened, err := e.tryOpen(ctx)
		if err == nil && opened {
			stream, acqErr := newLink.acquireStream(ctx)
			if acqErr == nil {
				return stream, nil
			}
			if !errors.Is(acqErr, net.ErrClosed) {
				return nil, acqErr
			}
			continue
		}
		if err != nil {
			if errors.Is(err, ErrSharedMemoryBudgetExhausted) {
				if e.currentOpened() == 0 {
					return nil, err
				}
			} else if e.currentOpened() == 0 {
				return nil, err
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (e *endpointMuxEngine) pickLink() (*endpointMuxPhysicalConn, int64, int) {
	e.mu.Lock()
	links := append([]*endpointMuxPhysicalConn(nil), e.links...)
	e.mu.Unlock()

	if len(links) == 0 {
		return nil, 0, 0
	}

	best := links[0]
	bestActive := best.activeStreams.Load()
	for i := 1; i < len(links); i++ {
		n := links[i].activeStreams.Load()
		if n < bestActive {
			best = links[i]
			bestActive = n
		}
	}
	return best, bestActive, len(links)
}

func (e *endpointMuxEngine) tryOpen(ctx context.Context) (*endpointMuxPhysicalConn, bool, error) {
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		return nil, false, net.ErrClosed
	}
	if e.opened >= e.poolSize {
		e.mu.Unlock()
		return nil, false, nil
	}
	e.opened++
	e.mu.Unlock()

	reserved := false
	if e.memoryBudget != nil && e.memoryPerConn > 0 {
		if !e.memoryBudget.tryReserve(e.memoryPerConn) {
			e.mu.Lock()
			e.opened--
			e.mu.Unlock()
			return nil, false, ErrSharedMemoryBudgetExhausted
		}
		reserved = true
	}

	conn, err := e.open(ctx, e.network, e.address)
	if err != nil {
		if reserved {
			e.memoryBudget.release(e.memoryPerConn)
		}
		e.mu.Lock()
		e.opened--
		e.mu.Unlock()
		return nil, false, err
	}
	if conn == nil {
		if reserved {
			e.memoryBudget.release(e.memoryPerConn)
		}
		e.mu.Lock()
		e.opened--
		e.mu.Unlock()
		return nil, false, errors.New("endpoint mux engine: open returned nil connection")
	}

	link := newEndpointMuxPhysicalConn(e, conn, e.sendQueueDepth)
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		link.closeWithError(net.ErrClosed)
		return nil, false, net.ErrClosed
	}
	e.links = append(e.links, link)
	e.mu.Unlock()
	return link, true, nil
}

func (e *endpointMuxEngine) onPhysicalClosed(link *endpointMuxPhysicalConn) {
	if link == nil {
		return
	}

	e.mu.Lock()
	for i := range e.links {
		if e.links[i] == link {
			e.links = append(e.links[:i], e.links[i+1:]...)
			break
		}
	}
	if e.opened > 0 {
		e.opened--
	}
	e.mu.Unlock()

	if e.memoryBudget != nil && e.memoryPerConn > 0 {
		e.memoryBudget.release(e.memoryPerConn)
	}
	e.logPhysicalClose(link)
	if e.warmup {
		e.startWarmup()
	}
}

func (e *endpointMuxEngine) currentOpened() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.opened
}

func (e *endpointMuxEngine) startWarmup() {
	if !e.warmupRunning.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer e.warmupRunning.Store(false)

		for {
			if e.currentOpened() >= e.poolSize {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), endpointWarmupDialTimeout)
			_, opened, err := e.tryOpen(ctx)
			cancel()
			if err != nil {
				if errors.Is(err, ErrSharedMemoryBudgetExhausted) {
					return
				}
				time.Sleep(endpointWarmupRetryDelay)
				continue
			}
			if !opened {
				return
			}
		}
	}()
}

func (e *endpointMuxEngine) startDiagLoop() {
	interval := e.diagInterval
	if interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			e.logPeriodicSnapshot()
		}
	}()
}

func newEndpointMuxPhysicalConn(engine *endpointMuxEngine, inner MessageConn, sendQueueDepth int) *endpointMuxPhysicalConn {
	capacity := sendQueueDepth
	if capacity <= 0 {
		capacity = endpointMuxDefaultSendQCap
	}

	recvCopyMode := engine.recvCopyMode
	if recvCopyMode != endpointMuxRecvCopyModeImmediate &&
		!isMessageConnRecvPayloadStable(inner) &&
		!(recvCopyMode == endpointMuxRecvCopyModeZeroCopy && engine.recvUnsafeZeroCopy) {
		recvCopyMode = endpointMuxRecvCopyModeImmediate
		if engine.diagEnabled {
			log.Printf(
				"rdma endpoint mux diag: key=%s recv_mode_fallback=copy reason=unstable_payload",
				engine.key,
			)
		}
	}

	p := &endpointMuxPhysicalConn{
		engine:       engine,
		inner:        inner,
		id:           engine.nextLinkID.Add(1),
		sendQueue:    make(chan muxSendRequest, capacity),
		done:         make(chan struct{}),
		streams:      map[uint32]*endpointMuxStream{},
		recvCopyMode: recvCopyMode,
	}
	if engine.diagEnabled {
		log.Printf(
			"rdma endpoint mux diag: key=%s open_link id=%d send_queue_cap=%d recv_mode=%s",
			engine.key, p.id, capacity, p.recvCopyMode.String(),
		)
	}
	go p.sendLoop()
	go p.recvLoop()
	return p
}

func (p *endpointMuxPhysicalConn) acquireStream(ctx context.Context) (MessageConn, error) {
	_ = ctx
	select {
	case <-p.done:
		return nil, p.getTermErr()
	default:
	}

	for attempt := 0; attempt < 1<<20; attempt++ {
		streamID := p.nextID.Add(1)
		if streamID == 0 {
			continue
		}

		stream := &endpointMuxStream{
			link:       p,
			id:         streamID,
			recvNotify: make(chan struct{}, 1),
			closedCh:   make(chan struct{}),
			termCh:     make(chan struct{}),
		}

		p.streamMu.Lock()
		if _, exists := p.streams[streamID]; exists {
			p.streamMu.Unlock()
			continue
		}
		select {
		case <-p.done:
			p.streamMu.Unlock()
			return nil, p.getTermErr()
		default:
		}
		p.streams[streamID] = stream
		p.streamMu.Unlock()
		p.activeStreams.Add(1)
		if p.engine.diagEnabled {
			p.streamsCreated.Add(1)
		}
		return stream, nil
	}

	return nil, errors.New("endpoint mux engine: exhausted stream id space")
}

func (p *endpointMuxPhysicalConn) sendLoop() {
	for {
		select {
		case <-p.done:
			return
		case req := <-p.sendQueue:
			if p.engine.diagEnabled {
				if !req.enqueuedAt.IsZero() {
					p.sendQueueWaitNs.Add(time.Since(req.enqueuedAt).Nanoseconds())
				}
			}
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

			sendStarted := time.Time{}
			if p.engine.diagEnabled {
				sendStarted = time.Now()
			}
			err := sendMuxFramedMessage(sendCtx, p.inner, req.streamID, req.flags, req.seq, req.payload)
			if p.engine.diagEnabled {
				p.sendFramesTotal.Add(1)
				p.sendBytesTotal.Add(int64(req.payloadSize))
				if !sendStarted.IsZero() {
					p.sendExecuteNs.Add(time.Since(sendStarted).Nanoseconds())
				}
			}
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

func (p *endpointMuxPhysicalConn) recvLoop() {
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
		if p.engine.diagEnabled {
			p.recvFramesTotal.Add(1)
			p.recvBytesTotal.Add(int64(muxFrameHeaderSize + len(payload)))
			if flags&muxFlagData != 0 {
				p.recvDataTotal.Add(1)
			}
			if flags&muxFlagClose != 0 {
				p.recvCloseTotal.Add(1)
			}
		}

		if dispatchErr := p.dispatchFrame(streamID, flags, seq, payload); dispatchErr != nil {
			p.closeWithError(dispatchErr)
			return
		}
	}
}

func (p *endpointMuxPhysicalConn) dispatchFrame(streamID uint32, flags uint16, seq uint32, payload []byte) error {
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

func (p *endpointMuxPhysicalConn) dispatchData(streamID uint32, seq uint32, payload []byte) error {
	stream := p.getStream(streamID)
	if stream == nil {
		return nil
	}
	if err := stream.acceptRecvSeq(seq); err != nil {
		return err
	}

	switch p.recvCopyMode {
	case endpointMuxRecvCopyModeZeroCopy:
		stream.enqueueEvent(muxRecvEvent{payload: payload})
	case endpointMuxRecvCopyModeDeferred:
		stream.enqueueEvent(muxRecvEvent{payload: payload, copyOnRead: true})
	default:
		msg := append([]byte(nil), payload...)
		stream.enqueueEvent(muxRecvEvent{payload: msg})
	}
	return nil
}

func (p *endpointMuxPhysicalConn) dispatchRemoteClose(streamID uint32, seq uint32, seqAlreadyApplied bool) error {
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
	p.activeStreams.Add(-1)
	return nil
}

func (p *endpointMuxPhysicalConn) getStream(streamID uint32) *endpointMuxStream {
	p.streamMu.RLock()
	stream := p.streams[streamID]
	p.streamMu.RUnlock()
	return stream
}

func (p *endpointMuxPhysicalConn) detachStream(streamID uint32) *endpointMuxStream {
	p.streamMu.Lock()
	stream := p.streams[streamID]
	if stream != nil {
		delete(p.streams, streamID)
	}
	p.streamMu.Unlock()
	return stream
}

func (p *endpointMuxPhysicalConn) removeStream(streamID uint32) {
	p.streamMu.Lock()
	if _, ok := p.streams[streamID]; ok {
		delete(p.streams, streamID)
		p.activeStreams.Add(-1)
	}
	p.streamMu.Unlock()
}

func (p *endpointMuxPhysicalConn) sendFrame(ctx context.Context, streamID uint32, flags uint16, seq uint32, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}

	req := muxSendRequest{
		ctx:         ctx,
		streamID:    streamID,
		flags:       flags,
		seq:         seq,
		payload:     payload,
		done:        make(chan error, 1),
		payloadSize: muxFrameHeaderSize + len(payload),
	}
	if p.engine.diagEnabled {
		req.enqueuedAt = time.Now()
	}

	select {
	case <-p.done:
		return p.getTermErr()
	case p.sendQueue <- req:
		if p.engine.diagEnabled {
			updateAtomicMax(&p.sendQueueMaxSeen, int64(len(p.sendQueue)))
		}
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

func (p *endpointMuxPhysicalConn) sendControlFrame(ctx context.Context, streamID uint32, flags uint16, seq uint32) error {
	if ctx == nil {
		ctx = context.Background()
	}

	req := muxSendRequest{
		ctx:         ctx,
		streamID:    streamID,
		flags:       flags,
		seq:         seq,
		done:        make(chan error, 1),
		payloadSize: muxFrameHeaderSize,
	}
	if p.engine.diagEnabled {
		req.enqueuedAt = time.Now()
	}

	select {
	case <-p.done:
		return p.getTermErr()
	case p.sendQueue <- req:
		if p.engine.diagEnabled {
			updateAtomicMax(&p.sendQueueMaxSeen, int64(len(p.sendQueue)))
		}
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

func (p *endpointMuxPhysicalConn) getTermErr() error {
	p.termMu.RLock()
	defer p.termMu.RUnlock()
	if p.termErr == nil {
		return net.ErrClosed
	}
	return p.termErr
}

func (p *endpointMuxPhysicalConn) closeWithError(err error) {
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
		streams := make([]*endpointMuxStream, 0, len(p.streams))
		for _, stream := range p.streams {
			streams = append(streams, stream)
		}
		p.streams = map[uint32]*endpointMuxStream{}
		p.streamMu.Unlock()

		for _, stream := range streams {
			stream.setTerminal(err)
		}
		p.activeStreams.Store(0)
		p.engine.onPhysicalClosed(p)
	})
}

func (c *endpointMuxStream) SendMessage(ctx context.Context, payload []byte) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.remoteClosed.Load() {
		return net.ErrClosed
	}
	select {
	case <-c.termCh:
		return c.getTermErr()
	default:
	}

	return c.link.sendFrame(ctx, c.id, muxFlagData, c.nextSendSeq(), payload)
}

func (c *endpointMuxStream) RecvMessage(ctx context.Context) ([]byte, error) {
	if c.closed.Load() {
		return nil, net.ErrClosed
	}

	for {
		if ev, ok := c.popEvent(); ok {
			if ev.err != nil {
				return nil, ev.err
			}
			if ev.copyOnRead {
				return append([]byte(nil), ev.payload...), nil
			}
			return ev.payload, nil
		}
		if c.remoteClosed.Load() {
			return nil, io.EOF
		}

		select {
		case <-c.recvNotify:
		case <-c.termCh:
			return nil, c.getTermErr()
		case <-c.closedCh:
			return nil, net.ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *endpointMuxStream) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closedCh)
		c.link.removeStream(c.id)

		controlCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_ = c.link.sendControlFrame(controlCtx, c.id, muxFlagClose, c.nextSendSeq())
		cancel()
	})
	return nil
}

func (c *endpointMuxStream) LocalAddr() net.Addr {
	return c.link.inner.LocalAddr()
}

func (c *endpointMuxStream) RemoteAddr() net.Addr {
	return c.link.inner.RemoteAddr()
}

func (c *endpointMuxStream) setTerminal(err error) {
	c.termOnce.Do(func() {
		if err == nil {
			err = net.ErrClosed
		}
		c.termMu.Lock()
		c.termErr = err
		c.termMu.Unlock()
		close(c.termCh)
	})
}

func (c *endpointMuxStream) getTermErr() error {
	c.termMu.RLock()
	defer c.termMu.RUnlock()
	if c.termErr == nil {
		return net.ErrClosed
	}
	return c.termErr
}

func (c *endpointMuxStream) enqueueEvent(ev muxRecvEvent) {
	select {
	case <-c.closedCh:
		return
	case <-c.termCh:
		return
	default:
	}

	c.recvQueueMu.Lock()
	c.recvQueue = append(c.recvQueue, ev)
	c.recvQueueMu.Unlock()
	select {
	case c.recvNotify <- struct{}{}:
	default:
	}
}

func (c *endpointMuxStream) hasPendingEvents() bool {
	c.recvQueueMu.Lock()
	n := len(c.recvQueue)
	c.recvQueueMu.Unlock()
	return n > 0
}

func (c *endpointMuxStream) popEvent() (muxRecvEvent, bool) {
	c.recvQueueMu.Lock()
	defer c.recvQueueMu.Unlock()
	if len(c.recvQueue) == 0 {
		return muxRecvEvent{}, false
	}
	ev := c.recvQueue[0]
	c.recvQueue[0] = muxRecvEvent{}
	c.recvQueue = c.recvQueue[1:]
	return ev, true
}

func (c *endpointMuxStream) nextSendSeq() uint32 {
	return c.sendSeq.Add(1)
}

func (c *endpointMuxStream) acceptRecvSeq(seq uint32) error {
	if seq == 0 {
		return errors.New("endpoint mux engine: sequence must be non-zero")
	}
	expect := c.recvSeq.Add(1)
	if seq != expect {
		return fmt.Errorf("endpoint mux engine: stream %d sequence out of order got=%d want=%d", c.id, seq, expect)
	}
	return nil
}

func (e *endpointMuxEngine) logPeriodicSnapshot() {
	if !e.diagEnabled {
		return
	}

	e.mu.Lock()
	links := append([]*endpointMuxPhysicalConn(nil), e.links...)
	opened := e.opened
	e.mu.Unlock()
	if len(links) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintf(
		&b,
		"rdma endpoint mux diag: key=%s opened=%d links=%d",
		e.key, opened, len(links),
	)
	for _, link := range links {
		fmt.Fprintf(
			&b,
			" | id=%d active=%d streams=%d send_frames=%d recv_frames=%d send_qlen=%d send_qmax=%d",
			link.id,
			link.activeStreams.Load(),
			link.streamsCreated.Load(),
			link.sendFramesTotal.Load(),
			link.recvFramesTotal.Load(),
			len(link.sendQueue),
			link.sendQueueMaxSeen.Load(),
		)
	}
	log.Print(b.String())
}

func (e *endpointMuxEngine) logPhysicalClose(link *endpointMuxPhysicalConn) {
	if !e.diagEnabled || link == nil {
		return
	}

	sendFrames := link.sendFramesTotal.Load()
	recvFrames := link.recvFramesTotal.Load()
	sendWaitNs := link.sendQueueWaitNs.Load()
	sendExecNs := link.sendExecuteNs.Load()

	log.Printf(
		"rdma endpoint mux diag: key=%s close_link id=%d err=%v streams=%d send_frames=%d send_bytes=%d send_q_wait_total=%s send_q_wait_avg=%s send_exec_total=%s send_exec_avg=%s send_qmax=%d recv_frames=%d recv_bytes=%d recv_data=%d recv_close=%d",
		e.key,
		link.id,
		link.getTermErr(),
		link.streamsCreated.Load(),
		sendFrames,
		link.sendBytesTotal.Load(),
		time.Duration(sendWaitNs),
		avgDuration(sendWaitNs, sendFrames),
		time.Duration(sendExecNs),
		avgDuration(sendExecNs, sendFrames),
		link.sendQueueMaxSeen.Load(),
		recvFrames,
		link.recvBytesTotal.Load(),
		link.recvDataTotal.Load(),
		link.recvCloseTotal.Load(),
	)
}

func avgDuration(totalNs int64, count int64) time.Duration {
	if totalNs <= 0 || count <= 0 {
		return 0
	}
	return time.Duration(totalNs / count)
}

func updateAtomicMax(dst *atomic.Int64, next int64) {
	for {
		cur := dst.Load()
		if next <= cur {
			return
		}
		if dst.CompareAndSwap(cur, next) {
			return
		}
	}
}

func endpointMuxDiagEnabled() bool {
	v := strings.TrimSpace(os.Getenv("AWS_RDMA_ENDPOINT_MUX_DIAG"))
	if v == "" {
		return false
	}
	enabled, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return enabled
}

func endpointMuxDiagInterval() time.Duration {
	v := strings.TrimSpace(os.Getenv("AWS_RDMA_ENDPOINT_MUX_DIAG_INTERVAL"))
	if v == "" {
		return endpointMuxDiagDefaultTick
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return endpointMuxDiagDefaultTick
	}
	return d
}

func (m endpointMuxRecvCopyMode) String() string {
	switch m {
	case endpointMuxRecvCopyModeDeferred:
		return endpointMuxRecvModeDeferred
	case endpointMuxRecvCopyModeZeroCopy:
		return endpointMuxRecvModeZeroCopy
	default:
		return endpointMuxRecvModeCopy
	}
}

func endpointMuxRecvCopyModeFromEnv() endpointMuxRecvCopyMode {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(endpointMuxRecvModeEnv)))
	switch v {
	case "", endpointMuxRecvModeCopy, "eager-copy":
		return endpointMuxRecvCopyModeImmediate
	case endpointMuxRecvModeDeferred, "deferred", "defer":
		return endpointMuxRecvCopyModeDeferred
	case endpointMuxRecvModeZeroCopy, "zerocopy":
		return endpointMuxRecvCopyModeZeroCopy
	default:
		return endpointMuxRecvCopyModeImmediate
	}
}

func endpointMuxRecvUnsafeZeroCopyEnabled() bool {
	v := strings.TrimSpace(os.Getenv(endpointMuxRecvUnsafeModeEnv))
	if v == "" {
		return false
	}
	enabled, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return enabled
}

func isMessageConnRecvPayloadStable(conn MessageConn) bool {
	if conn == nil {
		return true
	}
	if v, ok := conn.(messageConnRecvPayloadStability); ok {
		return v.RecvPayloadStable()
	}
	return true
}

func buildMuxFrameHeader(streamID uint32, flags uint16, seq uint32, payloadLen int) [muxFrameHeaderSize]byte {
	var header [muxFrameHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], streamID)
	binary.BigEndian.PutUint32(header[4:8], uint32(payloadLen))
	binary.BigEndian.PutUint16(header[8:10], flags)
	binary.BigEndian.PutUint32(header[12:16], seq)
	return header
}

func sendMuxFramedMessage(ctx context.Context, conn MessageConn, streamID uint32, flags uint16, seq uint32, payload []byte) error {
	header := buildMuxFrameHeader(streamID, flags, seq, len(payload))
	if sender, ok := conn.(messageConnPartSender); ok {
		return sender.SendMessageParts(ctx, header[:], payload)
	}

	frame := make([]byte, muxFrameHeaderSize+len(payload))
	copy(frame[:muxFrameHeaderSize], header[:])
	copy(frame[muxFrameHeaderSize:], payload)
	return conn.SendMessage(ctx, frame)
}

func encodeMuxFrame(streamID uint32, flags uint16, seq uint32, payload []byte) []byte {
	header := buildMuxFrameHeader(streamID, flags, seq, len(payload))
	frame := make([]byte, muxFrameHeaderSize+len(payload))
	copy(frame[:muxFrameHeaderSize], header[:])
	copy(frame[muxFrameHeaderSize:], payload)
	return frame
}

func decodeMuxFrame(frame []byte) (streamID uint32, flags uint16, seq uint32, payload []byte, err error) {
	if len(frame) < muxFrameHeaderSize {
		return 0, 0, 0, nil, errors.New("endpoint mux engine: frame too short")
	}
	streamID = binary.BigEndian.Uint32(frame[0:4])
	if streamID == 0 {
		return 0, 0, 0, nil, errors.New("endpoint mux engine: stream id must be non-zero")
	}
	payloadLen := binary.BigEndian.Uint32(frame[4:8])
	flags = binary.BigEndian.Uint16(frame[8:10])
	if flags == 0 {
		return 0, 0, 0, nil, errors.New("endpoint mux engine: frame flags must be non-zero")
	}
	if flags&^(muxFlagData|muxFlagClose) != 0 {
		return 0, 0, 0, nil, fmt.Errorf("endpoint mux engine: unsupported frame flags=%#x", flags)
	}
	seq = binary.BigEndian.Uint32(frame[12:16])
	if seq == 0 {
		return 0, 0, 0, nil, errors.New("endpoint mux engine: sequence must be non-zero")
	}
	if uint32(len(frame)-muxFrameHeaderSize) != payloadLen {
		return 0, 0, 0, nil, fmt.Errorf(
			"endpoint mux engine: frame length mismatch header=%d actual=%d",
			payloadLen, len(frame)-muxFrameHeaderSize,
		)
	}
	payload = frame[muxFrameHeaderSize:]
	return streamID, flags, seq, payload, nil
}
