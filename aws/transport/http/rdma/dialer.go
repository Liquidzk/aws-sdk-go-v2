package rdma

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	// DefaultOpenParallelism keeps a small amount of open concurrency by
	// default, avoiding open storms while still allowing steady connection
	// creation under load.
	DefaultOpenParallelism = 1

	// DefaultOpenMinInterval keeps no extra spacing between RDMA open attempts.
	DefaultOpenMinInterval = 0 * time.Millisecond
)

// OpenFunc opens a RDMA-like message connection to a remote address.
type OpenFunc func(ctx context.Context, network, address string) (MessageConn, error)

type dialerState struct {
	mu       sync.Mutex
	lastOpen time.Time
	sem      chan struct{}

	endpointEngines     map[string]*endpointEngine
	endpointMuxEngines  map[string]*endpointMuxEngine
	sharedMemoryBudget  *sharedMemoryBudget
	sharedMemoryMaxByte int
}

// EndpointEngineOptions configures per-endpoint physical connection pooling.
type EndpointEngineOptions struct {
	// PoolSize limits number of physical RDMA connections opened per endpoint.
	// Values <= 0 disable endpoint engine pooling.
	PoolSize int

	// Warmup pre-opens physical connections in the background up to PoolSize.
	Warmup bool

	// AcquireTimeout bounds waiting for an endpoint engine connection when
	// PoolSize is reached.
	AcquireTimeout time.Duration

	// EnableMultiplex enables logical stream multiplexing over fixed physical
	// endpoint connection pool.
	EnableMultiplex bool

	// SendQueueDepth sets buffered frame queue depth per physical connection for
	// multiplexed sending. Values <= 0 use defaults.
	SendQueueDepth int
}

// SharedMemoryBudgetOptions defines a dialer-local memory budget used by
// endpoint engine pooled connections.
//
// This is a phase-1 budget model for controlling pooled RDMA footprint. It
// does not yet implement cross-connection shared MR regions.
type SharedMemoryBudgetOptions struct {
	// TotalBytes is the maximum total bytes allowed for pooled physical
	// connections. Values <= 0 disable budget enforcement.
	TotalBytes int

	// EstimatedConnBytes is the per-connection estimated bytes consumed by RDMA
	// send/recv buffers for budget accounting. Values <= 0 disable accounting.
	EstimatedConnBytes int
}

// Dialer bridges a message-oriented RDMA backend to net/http by exposing a
// DialContext compatible function.
type Dialer struct {
	// Open opens the RDMA message connection. If nil, TCP fallback is used.
	Open OpenFunc

	// FallbackDialContext is used when Open is nil or Open returns an error and
	// DisableFallback is false. If nil, net.Dialer{}.DialContext is used.
	FallbackDialContext func(ctx context.Context, network, address string) (net.Conn, error)

	// DisableFallback forces failures from Open to be returned directly.
	DisableFallback bool

	// OpenParallelism limits concurrent Open calls. Values <= 0 disable the
	// limit. NewVerbsDialer sets this to DefaultOpenParallelism.
	OpenParallelism int

	// OpenMinInterval enforces minimum spacing between Open start times.
	// A value <= 0 disables spacing. NewVerbsDialer sets this to
	// DefaultOpenMinInterval.
	OpenMinInterval time.Duration

	// EndpointEngine controls fixed-size physical connection pooling per
	// endpoint.
	EndpointEngine EndpointEngineOptions

	// SharedMemoryBudget controls pooled-connection memory budgeting.
	SharedMemoryBudget SharedMemoryBudgetOptions

	state *dialerState
}

// DialContext dials using RDMA Open first, with optional TCP fallback.
func (d Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.Open == nil {
		return d.fallbackDialContext()(ctx, network, address)
	}

	msgConn, openErr := d.openMessageConnForDial(ctx, network, address)
	if openErr == nil {
		return NewConn(msgConn), nil
	}

	if d.DisableFallback {
		return nil, openErr
	}

	conn, fallbackErr := d.fallbackDialContext()(ctx, network, address)
	if fallbackErr == nil {
		return conn, nil
	}

	return nil, fmt.Errorf("rdma open failed: %w; fallback failed: %v", openErr, fallbackErr)
}

func (d Dialer) openMessageConnForDial(ctx context.Context, network, address string) (MessageConn, error) {
	if d.EndpointEngine.PoolSize > 0 {
		if d.EndpointEngine.EnableMultiplex {
			engine, err := d.getEndpointMuxEngine(network, address)
			if err != nil {
				return nil, err
			}
			return engine.Acquire(ctx)
		}

		engine, err := d.getEndpointEngine(network, address)
		if err != nil {
			return nil, err
		}
		return engine.Acquire(ctx)
	}

	return d.openMessageConn(ctx, network, address)
}

func (d Dialer) openMessageConn(ctx context.Context, network, address string) (MessageConn, error) {
	release, err := d.beforeOpen(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	msgConn, openErr := d.Open(ctx, network, address)
	if openErr != nil {
		return nil, openErr
	}
	if msgConn == nil {
		return nil, errors.New("rdma dialer open returned nil MessageConn")
	}
	return msgConn, nil
}

func (d Dialer) fallbackDialContext() func(ctx context.Context, network, address string) (net.Conn, error) {
	if d.FallbackDialContext != nil {
		return d.FallbackDialContext
	}
	return (&net.Dialer{}).DialContext
}

func (d Dialer) beforeOpen(ctx context.Context) (func(), error) {
	state := d.getState()
	releasePermit, err := state.acquirePermit(ctx, d.OpenParallelism)
	if err != nil {
		return func() {}, err
	}

	if err := state.waitOpenInterval(ctx, d.OpenMinInterval); err != nil {
		releasePermit()
		return func() {}, err
	}

	return releasePermit, nil
}

func (d Dialer) getState() *dialerState {
	if d.state != nil {
		return d.state
	}
	return &dialerState{}
}

func (d Dialer) getEndpointEngine(network, address string) (*endpointEngine, error) {
	state := d.getState()
	key := endpointEngineKey(network, address)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.endpointEngines == nil {
		state.endpointEngines = map[string]*endpointEngine{}
	}
	if eng, ok := state.endpointEngines[key]; ok {
		return eng, nil
	}

	cfg := endpointEngineConfig{
		key:            key,
		network:        network,
		address:        address,
		poolSize:       d.EndpointEngine.PoolSize,
		warmup:         d.EndpointEngine.Warmup,
		acquireTimeout: d.EndpointEngine.AcquireTimeout,
		open:           d.openMessageConn,
	}

	if b := state.getSharedMemoryBudgetLocked(d.SharedMemoryBudget.TotalBytes); b != nil {
		cfg.memoryBudget = b
		cfg.memoryPerConn = d.SharedMemoryBudget.EstimatedConnBytes
	}

	engine, err := newEndpointEngine(cfg)
	if err != nil {
		return nil, err
	}
	state.endpointEngines[key] = engine
	return engine, nil
}

func (d Dialer) getEndpointMuxEngine(network, address string) (*endpointMuxEngine, error) {
	state := d.getState()
	key := endpointEngineKey(network, address)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.endpointMuxEngines == nil {
		state.endpointMuxEngines = map[string]*endpointMuxEngine{}
	}
	if eng, ok := state.endpointMuxEngines[key]; ok {
		return eng, nil
	}

	cfg := endpointMuxEngineConfig{
		key:            key,
		network:        network,
		address:        address,
		poolSize:       d.EndpointEngine.PoolSize,
		warmup:         d.EndpointEngine.Warmup,
		acquireTimeout: d.EndpointEngine.AcquireTimeout,
		sendQueueDepth: d.EndpointEngine.SendQueueDepth,
		open:           d.openMessageConn,
	}

	if b := state.getSharedMemoryBudgetLocked(d.SharedMemoryBudget.TotalBytes); b != nil {
		cfg.memoryBudget = b
		cfg.memoryPerConn = d.SharedMemoryBudget.EstimatedConnBytes
	}

	engine, err := newEndpointMuxEngine(cfg)
	if err != nil {
		return nil, err
	}
	state.endpointMuxEngines[key] = engine
	return engine, nil
}

func (s *dialerState) getSharedMemoryBudgetLocked(totalBytes int) *sharedMemoryBudget {
	if totalBytes <= 0 {
		return nil
	}

	if s.sharedMemoryBudget != nil && s.sharedMemoryMaxByte == totalBytes {
		return s.sharedMemoryBudget
	}

	s.sharedMemoryBudget = newSharedMemoryBudget(totalBytes)
	s.sharedMemoryMaxByte = totalBytes
	return s.sharedMemoryBudget
}

func (s *dialerState) acquirePermit(ctx context.Context, parallelism int) (func(), error) {
	if parallelism <= 0 {
		return func() {}, nil
	}

	sem := s.getSemaphore(parallelism)
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}

func (s *dialerState) getSemaphore(parallelism int) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sem == nil || cap(s.sem) != parallelism {
		s.sem = make(chan struct{}, parallelism)
	}
	return s.sem
}

func (s *dialerState) waitOpenInterval(ctx context.Context, minInterval time.Duration) error {
	if minInterval <= 0 {
		return nil
	}

	for {
		wait := s.nextWait(minInterval)
		if wait <= 0 {
			s.markOpenNow()
			return nil
		}

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (s *dialerState) nextWait(minInterval time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastOpen.IsZero() {
		return 0
	}
	remain := minInterval - time.Since(s.lastOpen)
	if remain < 0 {
		return 0
	}
	return remain
}

func (s *dialerState) markOpenNow() {
	s.mu.Lock()
	s.lastOpen = time.Now()
	s.mu.Unlock()
}
