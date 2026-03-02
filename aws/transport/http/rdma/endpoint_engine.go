package rdma

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	endpointWarmupDialTimeout = 3 * time.Second
	endpointWarmupRetryDelay  = 50 * time.Millisecond
)

var ErrSharedMemoryBudgetExhausted = errors.New("rdma dialer shared memory budget exhausted")

type endpointEngineConfig struct {
	key            string
	network        string
	address        string
	poolSize       int
	warmup         bool
	acquireTimeout time.Duration
	open           OpenFunc

	memoryBudget  *sharedMemoryBudget
	memoryPerConn int
}

type endpointEngine struct {
	key            string
	network        string
	address        string
	poolSize       int
	warmup         bool
	acquireTimeout time.Duration
	open           OpenFunc

	memoryBudget  *sharedMemoryBudget
	memoryPerConn int

	mu      sync.Mutex
	opened  int
	closing bool
	idle    chan MessageConn

	warmupRunning atomic.Bool
}

func newEndpointEngine(cfg endpointEngineConfig) (*endpointEngine, error) {
	if cfg.open == nil {
		return nil, errors.New("endpoint engine: open func is nil")
	}
	if cfg.poolSize <= 0 {
		return nil, fmt.Errorf("endpoint engine: pool size must be > 0")
	}
	if cfg.acquireTimeout < 0 {
		return nil, fmt.Errorf("endpoint engine: acquire timeout must be >= 0")
	}
	if cfg.memoryPerConn < 0 {
		return nil, fmt.Errorf("endpoint engine: memory per conn must be >= 0")
	}

	e := &endpointEngine{
		key:            cfg.key,
		network:        cfg.network,
		address:        cfg.address,
		poolSize:       cfg.poolSize,
		warmup:         cfg.warmup,
		acquireTimeout: cfg.acquireTimeout,
		open:           cfg.open,
		memoryBudget:   cfg.memoryBudget,
		memoryPerConn:  cfg.memoryPerConn,
		idle:           make(chan MessageConn, cfg.poolSize),
	}
	if e.warmup {
		e.startWarmup()
	}
	return e, nil
}

func endpointEngineKey(network, address string) string {
	return network + "|" + address
}

func (e *endpointEngine) Acquire(ctx context.Context) (MessageConn, error) {
	if e.acquireTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.acquireTimeout)
		defer cancel()
	}

	for {
		if conn := e.tryGetIdle(); conn != nil {
			return newPooledMessageConn(e, conn), nil
		}

		conn, opened, err := e.tryOpen(ctx)
		if err == nil && opened {
			return newPooledMessageConn(e, conn), nil
		}
		if err != nil && !errors.Is(err, ErrSharedMemoryBudgetExhausted) {
			return nil, err
		}
		if err != nil && errors.Is(err, ErrSharedMemoryBudgetExhausted) && e.currentOpened() == 0 {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case conn := <-e.idle:
			if conn != nil {
				return newPooledMessageConn(e, conn), nil
			}
		}
	}
}

func (e *endpointEngine) tryGetIdle() MessageConn {
	select {
	case conn := <-e.idle:
		return conn
	default:
		return nil
	}
}

func (e *endpointEngine) tryOpen(ctx context.Context) (MessageConn, bool, error) {
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
		return nil, false, errors.New("endpoint engine: open returned nil connection")
	}

	return conn, true, nil
}

func (e *endpointEngine) release(conn MessageConn, broken bool) {
	if conn == nil {
		return
	}

	if broken {
		_ = conn.Close()
		e.mu.Lock()
		if e.opened > 0 {
			e.opened--
		}
		e.mu.Unlock()
		if e.memoryBudget != nil && e.memoryPerConn > 0 {
			e.memoryBudget.release(e.memoryPerConn)
		}
		if e.warmup {
			e.startWarmup()
		}
		return
	}

	select {
	case e.idle <- conn:
	default:
		_ = conn.Close()
		e.mu.Lock()
		if e.opened > 0 {
			e.opened--
		}
		e.mu.Unlock()
		if e.memoryBudget != nil && e.memoryPerConn > 0 {
			e.memoryBudget.release(e.memoryPerConn)
		}
	}
}

func (e *endpointEngine) currentOpened() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.opened
}

func (e *endpointEngine) startWarmup() {
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
			conn, opened, err := e.tryOpen(ctx)
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

			select {
			case e.idle <- conn:
			default:
				e.release(conn, true)
				return
			}
		}
	}()
}

type pooledMessageConn struct {
	engine *endpointEngine
	inner  MessageConn

	releaseOnce sync.Once
	broken      atomic.Bool
}

func newPooledMessageConn(engine *endpointEngine, inner MessageConn) *pooledMessageConn {
	return &pooledMessageConn{
		engine: engine,
		inner:  inner,
	}
}

func (c *pooledMessageConn) SendMessage(ctx context.Context, payload []byte) error {
	err := c.inner.SendMessage(ctx, payload)
	if shouldMarkPooledConnBroken(err) {
		c.broken.Store(true)
	}
	return err
}

func (c *pooledMessageConn) RecvMessage(ctx context.Context) ([]byte, error) {
	msg, err := c.inner.RecvMessage(ctx)
	if shouldMarkPooledConnBroken(err) {
		c.broken.Store(true)
	}
	return msg, err
}

func (c *pooledMessageConn) Close() error {
	c.releaseOnce.Do(func() {
		c.engine.release(c.inner, c.broken.Load())
	})
	return nil
}

func (c *pooledMessageConn) LocalAddr() net.Addr {
	return c.inner.LocalAddr()
}

func (c *pooledMessageConn) RemoteAddr() net.Addr {
	return c.inner.RemoteAddr()
}

func shouldMarkPooledConnBroken(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	return true
}

type sharedMemoryBudget struct {
	total int64
	used  atomic.Int64
}

func newSharedMemoryBudget(totalBytes int) *sharedMemoryBudget {
	return &sharedMemoryBudget{total: int64(totalBytes)}
}

func (b *sharedMemoryBudget) tryReserve(n int) bool {
	if b == nil || n <= 0 {
		return true
	}

	delta := int64(n)
	for {
		cur := b.used.Load()
		next := cur + delta
		if next > b.total {
			return false
		}
		if b.used.CompareAndSwap(cur, next) {
			return true
		}
	}
}

func (b *sharedMemoryBudget) release(n int) {
	if b == nil || n <= 0 {
		return
	}
	b.used.Add(-int64(n))
}
