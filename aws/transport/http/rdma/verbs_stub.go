//go:build !linux || !cgo || !rdma

package rdma

import (
	"context"
	"fmt"
)

const verbsBackendEnabled = false

// Open opens a MessageConn backed by RDMA verbs.
//
// This stub is used unless built with: linux + cgo + rdma build tag.
func (o VerbsOptions) Open(ctx context.Context, network, address string) (MessageConn, error) {
	_, _, _ = o, network, address
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("rdma verbs backend unavailable; rebuild with -tags rdma and CGO_ENABLED=1")
}

func newVerbsMessageListener(network, address string, opts VerbsListenerOptions) (MessageListener, error) {
	_, _, _ = network, address, opts
	return nil, fmt.Errorf("rdma verbs backend unavailable; rebuild with -tags rdma and CGO_ENABLED=1")
}
