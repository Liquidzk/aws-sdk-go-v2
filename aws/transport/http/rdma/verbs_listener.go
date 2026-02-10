package rdma

import "net"

const (
	// DefaultVerbsListenBacklog is the default RDMA CM listen backlog.
	DefaultVerbsListenBacklog = 512
)

// VerbsListenerOptions controls RDMA verbs listener behavior.
type VerbsListenerOptions struct {
	// VerbsOptions controls queue and framing for accepted connections.
	VerbsOptions VerbsOptions

	// Backlog controls the RDMA CM listen backlog.
	// A value of 0 uses DefaultVerbsListenBacklog.
	Backlog int
}

// NewVerbsListener creates a net.Listener backed by RDMA verbs.
func NewVerbsListener(network, address string, opts VerbsListenerOptions) (net.Listener, error) {
	return newVerbsListener(network, address, opts)
}
