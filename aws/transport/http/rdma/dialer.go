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
	// DefaultOpenParallelism serializes RDMA open attempts by default.
	// This reduces CM churn when callers issue bursts of new connections.
	DefaultOpenParallelism = 1

	// DefaultOpenMinInterval spaces RDMA open attempts to avoid reconnect storms.
	DefaultOpenMinInterval = 200 * time.Millisecond
)

// OpenFunc opens a RDMA-like message connection to a remote address.
type OpenFunc func(ctx context.Context, network, address string) (MessageConn, error)

type dialerState struct {
	mu       sync.Mutex
	lastOpen time.Time
	sem      chan struct{}
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

	state *dialerState
}

// DialContext dials using RDMA Open first, with optional TCP fallback.
func (d Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.Open == nil {
		return d.fallbackDialContext()(ctx, network, address)
	}

	release, err := d.beforeOpen(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	msgConn, openErr := d.Open(ctx, network, address)
	if openErr == nil {
		if msgConn == nil {
			return nil, errors.New("rdma dialer open returned nil MessageConn")
		}
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
