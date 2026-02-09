package rdma

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// OpenFunc opens a RDMA-like message connection to a remote address.
type OpenFunc func(ctx context.Context, network, address string) (MessageConn, error)

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
}

// DialContext dials using RDMA Open first, with optional TCP fallback.
func (d Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.Open == nil {
		return d.fallbackDialContext()(ctx, network, address)
	}

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
