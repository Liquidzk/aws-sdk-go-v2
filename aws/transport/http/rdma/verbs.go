package rdma

import (
	"fmt"
	"net"
)

const (
	// DefaultVerbsFramePayloadSize is the payload size of one RDMA frame.
	// A larger value reduces framing overhead but increases per-frame memory.
	DefaultVerbsFramePayloadSize = 64 * 1024

	// DefaultVerbsSendQueueDepth is the default send queue depth for RC QP.
	DefaultVerbsSendQueueDepth = 64

	// DefaultVerbsRecvQueueDepth is the default receive queue depth for RC QP.
	DefaultVerbsRecvQueueDepth = 64

	// DefaultVerbsInlineThreshold is the default inline send threshold in bytes.
	DefaultVerbsInlineThreshold = 0
)

// VerbsOptions controls the RDMA verbs backend used by Dialer.Open.
type VerbsOptions struct {
	// FramePayloadSize controls max payload bytes sent in one RDMA SEND work
	// request (excluding internal frame header).
	FramePayloadSize int

	// SendQueueDepth controls max outstanding SEND work requests on QP.
	SendQueueDepth int

	// RecvQueueDepth controls max outstanding RECV work requests on QP.
	RecvQueueDepth int

	// InlineThreshold enables IBV_SEND_INLINE when frame size is less than or
	// equal to this value. A value of 0 uses DefaultVerbsInlineThreshold.
	InlineThreshold int
}

// NewVerbsDialer creates a Dialer that opens connections through RDMA verbs.
func NewVerbsDialer(opts VerbsOptions) Dialer {
	return Dialer{
		Open:            opts.Open,
		OpenParallelism: DefaultOpenParallelism,
		OpenMinInterval: DefaultOpenMinInterval,
		state:           &dialerState{},
	}
}

type verbsConfig struct {
	framePayloadSize int
	sendQueueDepth   int
	recvQueueDepth   int
	inlineThreshold  int
}

func (o VerbsOptions) normalize() (verbsConfig, error) {
	cfg := verbsConfig{
		framePayloadSize: DefaultVerbsFramePayloadSize,
		sendQueueDepth:   DefaultVerbsSendQueueDepth,
		recvQueueDepth:   DefaultVerbsRecvQueueDepth,
		inlineThreshold:  DefaultVerbsInlineThreshold,
	}

	if o.FramePayloadSize < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: frame payload size must be >= 0")
	}
	if o.SendQueueDepth < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: send queue depth must be >= 0")
	}
	if o.RecvQueueDepth < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: recv queue depth must be >= 0")
	}
	if o.InlineThreshold < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: inline threshold must be >= 0")
	}

	if o.FramePayloadSize > 0 {
		cfg.framePayloadSize = o.FramePayloadSize
	}
	if o.SendQueueDepth > 0 {
		cfg.sendQueueDepth = o.SendQueueDepth
	}
	if o.RecvQueueDepth > 0 {
		cfg.recvQueueDepth = o.RecvQueueDepth
	}
	if o.InlineThreshold > 0 {
		cfg.inlineThreshold = o.InlineThreshold
	}

	return cfg, nil
}

func splitHostPortAddress(network, address string) (host string, port string, err error) {
	switch network {
	case "", "tcp", "tcp4", "tcp6", "rdma", "rdma4", "rdma6":
	default:
		return "", "", fmt.Errorf("rdma verbs: unsupported network %q", network)
	}

	host, port, err = net.SplitHostPort(address)
	if err != nil {
		return "", "", fmt.Errorf("rdma verbs: invalid address %q: %w", address, err)
	}
	if port == "" {
		return "", "", fmt.Errorf("rdma verbs: missing port in address %q", address)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port, nil
}

type rdmaAddr struct {
	network string
	address string
}

func (a rdmaAddr) Network() string { return a.network }
func (a rdmaAddr) String() string  { return a.address }
