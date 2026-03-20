package rdma

const (
	// DefaultVerbsListenBacklog is the default RDMA CM listen backlog.
	DefaultVerbsListenBacklog = 512

	// DefaultVerbsAcceptWorkers is the default number of concurrent accept
	// workers. Keep this conservative because rdma_get_request on a single
	// listen CM ID is not guaranteed to scale with concurrent callers.
	DefaultVerbsAcceptWorkers = 1
)

// VerbsListenerOptions controls RDMA verbs listener behavior.
type VerbsListenerOptions struct {
	// VerbsOptions controls queue and framing for accepted connections.
	VerbsOptions VerbsOptions

	// Backlog controls the RDMA CM listen backlog.
	// A value of 0 uses DefaultVerbsListenBacklog.
	Backlog int

	// AcceptWorkers controls concurrent RDMA accept workers.
	// A value <= 0 uses DefaultVerbsAcceptWorkers.
	AcceptWorkers int
}

// NewVerbsMessageListener creates the supported server-side RDMA listener for
// the zero-copy message protocol used by s3rdmaclient.
func NewVerbsMessageListener(network, address string, opts VerbsListenerOptions) (MessageListener, error) {
	return newVerbsMessageListener(network, address, opts)
}
