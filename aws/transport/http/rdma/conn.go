package rdma

import (
	"context"
	"net"
	"time"
)

var (
	// readPollInterval bounds how long one RecvMessage call can block so
	// SetReadDeadline can interrupt in-flight reads. A larger slice reduces
	// idle wakeups/cgo churn at the cost of deadline reaction granularity.
	readPollInterval = 500 * time.Millisecond
)

// MessageConn models an ordered, reliable message channel for the zero-copy
// RDMA transport.
//
// RecvMessage should block until one message is available or the context is
// done. Returned message data should not be mutated after return.
//
// SendMessage implementations must not retain payload after returning.
type MessageConn interface {
	SendMessage(ctx context.Context, payload []byte) error
	RecvMessage(ctx context.Context) ([]byte, error)
	Close() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// BorrowedMessage is a message view that may alias transport-managed memory.
// The caller must call Release once the payload is no longer needed.
type BorrowedMessage struct {
	Payload      []byte
	SharedOffset int
	release      func() error
}

// Release returns borrowed transport resources.
func (m *BorrowedMessage) Release() error {
	if m == nil || m.release == nil {
		return nil
	}
	release := m.release
	m.release = nil
	return release()
}

// BorrowingMessageConn exposes a borrowed receive API for zero-copy consumers.
type BorrowingMessageConn interface {
	RecvBorrowedMessage(ctx context.Context) (*BorrowedMessage, error)
}

// MessageListener accepts message-oriented connections directly for the
// zero-copy RDMA server path.
type MessageListener interface {
	AcceptMessage() (MessageConn, error)
	Close() error
	Addr() net.Addr
}
