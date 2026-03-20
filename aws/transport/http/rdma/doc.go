// Package rdma exposes the message-oriented RDMA transport primitives used by
// the zero-copy S3 benchmark path in this repository.
//
// The supported public surface is:
//   - VerbsOptions.Open for client-side message connections
//   - NewVerbsMessageListener for server-side accepts
//   - MessageConn, BorrowingMessageConn, and BorrowedMessage for zero-copy payload flow
//
// The older net/http and net.Conn bridge has been removed. In this repository,
// the standard AWS service clients remain TCP/HTTP clients, and RDMA clients
// should go through s3rdmaclient.
package rdma
