package rdma

import (
	"fmt"
	"net"
	"time"
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

	// DefaultVerbsSendSignalInterval requests completion for every frame.
	// This minimizes latency but increases CQ/cpu overhead.
	DefaultVerbsSendSignalInterval = 1

	// DefaultVerbsLowCPUSendSignalInterval requests completion every N frames
	// in low-cpu mode. This reduces completion handling overhead at the cost of
	// higher completion latency.
	DefaultVerbsLowCPUSendSignalInterval = 16
)

// VerbsOptions controls the RDMA verbs backend used by Dialer.Open.
type VerbsOptions struct {
	// FramePayloadSize controls max payload bytes sent in one RDMA SEND work
	// request.
	FramePayloadSize int

	// SendQueueDepth controls max outstanding SEND work requests on QP.
	SendQueueDepth int

	// RecvQueueDepth controls max outstanding RECV work requests on QP.
	RecvQueueDepth int

	// InlineThreshold enables IBV_SEND_INLINE when frame size is less than or
	// equal to this value. A value of 0 uses DefaultVerbsInlineThreshold.
	InlineThreshold int

	// LowCPU prioritizes lower CPU usage over latency/throughput.
	// When true and SendSignalInterval is 0, a larger completion interval is used.
	LowCPU bool

	// SendSignalInterval controls how often sends are signaled for CQ completion.
	// 1 means every frame; larger values reduce completion overhead.
	// A value of 0 uses defaults (depends on LowCPU).
	SendSignalInterval int

	// EndpointPoolSize limits number of physical connections opened per endpoint.
	// Values <= 0 disable endpoint engine pooling.
	EndpointPoolSize int

	// EndpointPoolWarmup pre-opens EndpointPoolSize physical connections in
	// background when endpoint engine pooling is enabled.
	EndpointPoolWarmup bool

	// EndpointAcquireTimeout bounds waiting for pooled connections when
	// EndpointPoolSize is reached.
	EndpointAcquireTimeout time.Duration

	// SharedMemoryBudgetBytes enables memory-budget accounting for pooled
	// physical RDMA connections. Values <= 0 disable budget enforcement.
	SharedMemoryBudgetBytes int

	// EndpointEnableMultiplex enables logical stream multiplexing over pooled
	// physical RDMA endpoint connections.
	EndpointEnableMultiplex bool

	// EndpointSendQueueDepth controls buffered frame queue depth per physical
	// multiplexed connection. Values <= 0 use defaults.
	EndpointSendQueueDepth int
}

// NewVerbsDialer creates a Dialer that opens connections through RDMA verbs.
func NewVerbsDialer(opts VerbsOptions) Dialer {
	estimatedConnBytes := estimateVerbsConnBytes(opts)
	return Dialer{
		Open:            opts.Open,
		OpenParallelism: DefaultOpenParallelism,
		OpenMinInterval: DefaultOpenMinInterval,
		EndpointEngine: EndpointEngineOptions{
			PoolSize:        opts.EndpointPoolSize,
			Warmup:          opts.EndpointPoolWarmup,
			AcquireTimeout:  opts.EndpointAcquireTimeout,
			EnableMultiplex: opts.EndpointEnableMultiplex,
			SendQueueDepth:  opts.EndpointSendQueueDepth,
		},
		SharedMemoryBudget: SharedMemoryBudgetOptions{
			TotalBytes:         opts.SharedMemoryBudgetBytes,
			EstimatedConnBytes: estimatedConnBytes,
		},
		state: &dialerState{},
	}
}

type verbsConfig struct {
	framePayloadSize int
	sendQueueDepth   int
	recvQueueDepth   int
	inlineThreshold  int
	sendSignalIntvl  int
	lowCPU           bool
}

func (o VerbsOptions) normalize() (verbsConfig, error) {
	cfg := verbsConfig{
		framePayloadSize: DefaultVerbsFramePayloadSize,
		sendQueueDepth:   DefaultVerbsSendQueueDepth,
		recvQueueDepth:   DefaultVerbsRecvQueueDepth,
		inlineThreshold:  DefaultVerbsInlineThreshold,
		sendSignalIntvl:  DefaultVerbsSendSignalInterval,
		lowCPU:           o.LowCPU,
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
	if o.SendSignalInterval < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: send signal interval must be >= 0")
	}
	if o.EndpointPoolSize < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: endpoint pool size must be >= 0")
	}
	if o.EndpointAcquireTimeout < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: endpoint acquire timeout must be >= 0")
	}
	if o.SharedMemoryBudgetBytes < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: shared memory budget bytes must be >= 0")
	}
	if o.EndpointSendQueueDepth < 0 {
		return verbsConfig{}, fmt.Errorf("rdma verbs: endpoint send queue depth must be >= 0")
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
	if o.LowCPU {
		cfg.sendSignalIntvl = DefaultVerbsLowCPUSendSignalInterval
	}
	if o.SendSignalInterval > 0 {
		cfg.sendSignalIntvl = o.SendSignalInterval
	}

	return cfg, nil
}

func estimateVerbsConnBytes(o VerbsOptions) int {
	framePayloadSize := DefaultVerbsFramePayloadSize
	sendQueueDepth := DefaultVerbsSendQueueDepth
	recvQueueDepth := DefaultVerbsRecvQueueDepth

	if o.FramePayloadSize > 0 {
		framePayloadSize = o.FramePayloadSize
	}
	if o.SendQueueDepth > 0 {
		sendQueueDepth = o.SendQueueDepth
	}
	if o.RecvQueueDepth > 0 {
		recvQueueDepth = o.RecvQueueDepth
	}
	if framePayloadSize <= 0 || sendQueueDepth < 0 || recvQueueDepth < 0 {
		return 0
	}

	totalDepth := sendQueueDepth + recvQueueDepth
	if totalDepth <= 0 {
		return 0
	}
	return framePayloadSize * totalDepth
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
