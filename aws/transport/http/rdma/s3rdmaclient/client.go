package s3rdmaclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	awsrdmahttp "github.com/aws/aws-sdk-go-v2/aws/transport/http/rdma"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http/rdma/zcopyproto"
)

const (
	defaultMaxCredits   = 64
	defaultSendQueueLen = 4096
)

// BufferRef describes a payload slice inside the shared memory region.
type BufferRef struct {
	Offset int
	Size   int
	token  uint64
}

func (r BufferRef) end() int {
	return r.Offset + r.Size
}

// Config configures the specialized S3 RDMA zero-copy client.
type Config struct {
	Endpoint       string
	RequestTimeout time.Duration
	VerbsOptions   awsrdmahttp.VerbsOptions
	MaxCredits     int
	SendQueueLen   int
}

type inboundResult struct {
	msg      zcopyproto.Message
	borrowed *awsrdmahttp.BorrowedMessage
	err      error
}

type sendRequest struct {
	ctx context.Context
	op  func(context.Context) error
	out chan error
}

type offsetSender interface {
	SendMessageAt(ctx context.Context, payload []byte, sharedOffset int) error
}

// Client is a specialized S3 client that operates on caller-provided shared
// memory by offset/size.
//
// This client uses one RDMA connection with request multiplexing and
// credit/ack flow control for borrowed get payloads.
type Client struct {
	conn       awsrdmahttp.MessageConn
	borrowConn awsrdmahttp.BorrowingMessageConn
	sendAtConn offsetSender

	sharedMemory   []byte
	requestTimeout time.Duration
	framePayload   int
	maxCredits     int
	sendReqCh      chan sendRequest

	pendingMu sync.Mutex
	pending   map[uint64]chan inboundResult
	nextReqID atomic.Uint64

	releaseMu  sync.Mutex
	releaseFns map[uint64]func() error
	nextToken  atomic.Uint64

	closeOnce sync.Once
	closedCh  chan struct{}
	wg        sync.WaitGroup

	recvErrMu sync.RWMutex
	recvErr   error
}

// New initializes the client and configures RDMA transport to register and use
// the provided shared memory region.
func New(cfg Config, sharedMemory []byte) (*Client, error) {
	if len(sharedMemory) == 0 {
		return nil, fmt.Errorf("s3rdmaclient: sharedMemory must not be empty")
	}

	address, err := normalizeAddress(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	verbsOpts := cfg.VerbsOptions
	verbsOpts.SharedRWMemory = sharedMemory

	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	maxCredits := cfg.MaxCredits
	if maxCredits <= 0 {
		maxCredits = defaultMaxCredits
	}
	sendQueueLen := cfg.SendQueueLen
	if sendQueueLen <= 0 {
		sendQueueLen = defaultSendQueueLen
	}

	openCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgConn, err := verbsOpts.Open(openCtx, "rdma", address)
	if err != nil {
		return nil, fmt.Errorf("s3rdmaclient: open rdma connection: %w", err)
	}
	borrowConn, ok := msgConn.(awsrdmahttp.BorrowingMessageConn)
	if !ok {
		_ = msgConn.Close()
		return nil, fmt.Errorf("s3rdmaclient: transport does not support borrowed receive")
	}
	sendAtConn, ok := msgConn.(offsetSender)
	if !ok {
		_ = msgConn.Close()
		return nil, fmt.Errorf("s3rdmaclient: transport does not support offset send")
	}

	c := &Client{
		conn:           msgConn,
		borrowConn:     borrowConn,
		sendAtConn:     sendAtConn,
		sharedMemory:   sharedMemory,
		requestTimeout: timeout,
		framePayload:   verbsOpts.FramePayloadSize,
		maxCredits:     maxCredits,
		sendReqCh:      make(chan sendRequest, sendQueueLen),
		pending:        make(map[uint64]chan inboundResult),
		releaseFns:     make(map[uint64]func() error),
		closedCh:       make(chan struct{}),
	}

	c.wg.Add(2)
	go c.sendLoop()
	go c.recvLoop()

	if err := c.hello(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func normalizeAddress(in string) (string, error) {
	v := strings.TrimSpace(in)
	if v == "" {
		return "", fmt.Errorf("s3rdmaclient: endpoint must not be empty")
	}
	if strings.Contains(v, "://") {
		u, err := url.Parse(v)
		if err != nil {
			return "", fmt.Errorf("s3rdmaclient: parse endpoint %q: %w", in, err)
		}
		if u.Host == "" {
			return "", fmt.Errorf("s3rdmaclient: endpoint missing host:port")
		}
		return u.Host, nil
	}
	if _, _, err := net.SplitHostPort(v); err != nil {
		return "", fmt.Errorf("s3rdmaclient: invalid endpoint %q: %w", in, err)
	}
	return v, nil
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.requestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.requestTimeout)
}

func (c *Client) validateRef(ref BufferRef) error {
	if ref.Offset < 0 || ref.Size < 0 {
		return fmt.Errorf("s3rdmaclient: invalid BufferRef offset=%d size=%d", ref.Offset, ref.Size)
	}
	if ref.end() < ref.Offset || ref.end() > len(c.sharedMemory) {
		return fmt.Errorf(
			"s3rdmaclient: BufferRef out of range offset=%d size=%d region=%d",
			ref.Offset,
			ref.Size,
			len(c.sharedMemory),
		)
	}
	return nil
}

func (c *Client) setRecvErr(err error) {
	if err == nil {
		return
	}
	c.recvErrMu.Lock()
	if c.recvErr == nil {
		c.recvErr = err
	}
	c.recvErrMu.Unlock()
}

func (c *Client) getRecvErr() error {
	c.recvErrMu.RLock()
	defer c.recvErrMu.RUnlock()
	return c.recvErr
}

func (c *Client) failAllPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for reqID, ch := range c.pending {
		delete(c.pending, reqID)
		ch <- inboundResult{err: err}
		close(ch)
	}
}

func (c *Client) nextRequestID() uint64 {
	return c.nextReqID.Add(1)
}

func (c *Client) registerPending(reqID uint64) chan inboundResult {
	ch := make(chan inboundResult, 1)
	c.pendingMu.Lock()
	c.pending[reqID] = ch
	c.pendingMu.Unlock()
	return ch
}

func (c *Client) removePending(reqID uint64) {
	c.pendingMu.Lock()
	delete(c.pending, reqID)
	c.pendingMu.Unlock()
}

func (c *Client) deliver(reqID uint64, res inboundResult) bool {
	c.pendingMu.Lock()
	ch, ok := c.pending[reqID]
	if ok {
		delete(c.pending, reqID)
	}
	c.pendingMu.Unlock()
	if !ok {
		return false
	}
	ch <- res
	close(ch)
	return true
}

func (c *Client) sendControl(ctx context.Context, msg zcopyproto.Message) error {
	payload, err := zcopyproto.Encode(msg)
	if err != nil {
		return err
	}
	return c.conn.SendMessage(ctx, payload)
}

func (c *Client) sendLoop() {
	defer c.wg.Done()
	for {
		select {
		case req := <-c.sendReqCh:
			if req.op == nil {
				req.out <- errors.New("s3rdmaclient: nil send op")
				continue
			}
			req.out <- req.op(req.ctx)
		case <-c.closedCh:
			for {
				select {
				case req := <-c.sendReqCh:
					req.out <- net.ErrClosed
				default:
					return
				}
			}
		}
	}
}

func (c *Client) sendSerialized(ctx context.Context, op func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	req := sendRequest{
		ctx: ctx,
		op:  op,
		out: make(chan error, 1),
	}
	select {
	case c.sendReqCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closedCh:
		return net.ErrClosed
	}
	select {
	case err := <-req.out:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closedCh:
		select {
		case err := <-req.out:
			return err
		default:
			return net.ErrClosed
		}
	}
}

func (c *Client) sendControlSerialized(ctx context.Context, msg zcopyproto.Message) error {
	return c.sendSerialized(ctx, func(sendCtx context.Context) error {
		return c.sendControl(sendCtx, msg)
	})
}

func (c *Client) sendAck(n int) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()
	return c.sendControlSerialized(ctx, zcopyproto.Message{
		Op:  zcopyproto.OpAck,
		Ack: n,
	})
}

func (c *Client) waitResult(ctx context.Context, reqID uint64, ch chan inboundResult) (inboundResult, error) {
	select {
	case res, ok := <-ch:
		if !ok {
			if err := c.getRecvErr(); err != nil {
				return inboundResult{}, err
			}
			return inboundResult{}, errors.New("s3rdmaclient: response channel closed")
		}
		if res.err != nil {
			return inboundResult{}, res.err
		}
		return res, nil
	case <-ctx.Done():
		c.removePending(reqID)
		return inboundResult{}, ctx.Err()
	case <-c.closedCh:
		if err := c.getRecvErr(); err != nil {
			return inboundResult{}, err
		}
		return inboundResult{}, net.ErrClosed
	}
}

func (c *Client) hello() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	reqID := c.nextRequestID()
	ch := c.registerPending(reqID)
	credits := computeSafeCredits(len(c.sharedMemory), c.connFramePayloadHint(), c.maxCredits)
	if err := c.sendControlSerialized(ctx, zcopyproto.Message{
		Op:      zcopyproto.OpHelloReq,
		ReqID:   reqID,
		Credits: credits,
	}); err != nil {
		c.removePending(reqID)
		return err
	}
	res, err := c.waitResult(ctx, reqID, ch)
	if err != nil {
		return err
	}
	if res.msg.Op != zcopyproto.OpHelloResp {
		return fmt.Errorf("s3rdmaclient: unexpected hello response op=%q", res.msg.Op)
	}
	if res.msg.Err != "" {
		return fmt.Errorf("s3rdmaclient: hello failed: %s", res.msg.Err)
	}
	return nil
}

func (c *Client) connFramePayloadHint() int {
	if c.framePayload > 0 {
		return c.framePayload
	}
	return 64 * 1024
}

func computeSafeCredits(sharedLen int, frameCap int, maxCredits int) int {
	if frameCap <= 0 {
		frameCap = 64 * 1024
	}
	if maxCredits <= 0 {
		maxCredits = defaultMaxCredits
	}
	capBytes := frameCap * 8
	if capBytes < 256*1024 {
		capBytes = 256 * 1024
	}
	if capBytes > 1024*1024 {
		capBytes = 1024 * 1024
	}
	if capBytes > sharedLen {
		capBytes = sharedLen
	}
	if capBytes < frameCap {
		capBytes = frameCap
	}
	slots := sharedLen / capBytes
	if slots < 1 {
		slots = 1
	}
	if slots > maxCredits {
		slots = maxCredits
	}
	return slots
}

func (c *Client) recvLoop() {
	defer c.wg.Done()

	for {
		payload, err := c.conn.RecvMessage(context.Background())
		if err != nil {
			c.setRecvErr(err)
			c.failAllPending(err)
			return
		}
		msg, err := zcopyproto.Decode(payload)
		if err != nil {
			c.setRecvErr(err)
			c.failAllPending(err)
			return
		}

		switch msg.Op {
		case zcopyproto.OpHelloResp, zcopyproto.OpRespOK, zcopyproto.OpRespErr:
			_ = c.deliver(msg.ReqID, inboundResult{msg: msg})
		case zcopyproto.OpGetMeta:
			borrowed, err := c.borrowConn.RecvBorrowedMessage(context.Background())
			if err != nil {
				c.setRecvErr(err)
				c.failAllPending(err)
				return
			}
			if !c.deliver(msg.ReqID, inboundResult{msg: msg, borrowed: borrowed}) {
				_ = borrowed.Release()
				_ = c.sendAck(1)
			}
		default:
			// Ignore unknown control messages to keep connection alive.
		}
	}
}

// EnsureBucket creates bucket when needed and tolerates already-exists cases.
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("s3rdmaclient: bucket must not be empty")
	}
	opCtx, cancel := c.withTimeout(ctx)
	defer cancel()

	reqID := c.nextRequestID()
	ch := c.registerPending(reqID)
	if err := c.sendControlSerialized(opCtx, zcopyproto.Message{
		Op:     zcopyproto.OpEnsureBucketReq,
		ReqID:  reqID,
		Bucket: bucket,
	}); err != nil {
		c.removePending(reqID)
		return err
	}
	res, err := c.waitResult(opCtx, reqID, ch)
	if err != nil {
		return err
	}
	switch res.msg.Op {
	case zcopyproto.OpRespOK:
		return nil
	case zcopyproto.OpRespErr:
		if res.msg.Err == "" {
			return fmt.Errorf("zcopy: remote error")
		}
		return fmt.Errorf("zcopy: %s", res.msg.Err)
	default:
		return fmt.Errorf("zcopy: unexpected response op=%q", res.msg.Op)
	}
}

// PutZeroCopy uploads object data from shared memory [offset, offset+size).
func (c *Client) PutZeroCopy(ctx context.Context, bucket, key string, ref BufferRef) error {
	if err := c.validateRef(ref); err != nil {
		return err
	}
	if strings.TrimSpace(bucket) == "" || strings.TrimSpace(key) == "" {
		return fmt.Errorf("s3rdmaclient: bucket/key must not be empty")
	}
	opCtx, cancel := c.withTimeout(ctx)
	defer cancel()

	reqID := c.nextRequestID()
	ch := c.registerPending(reqID)

	payload := c.sharedMemory[ref.Offset:ref.end()]
	reqOffset := ref.Offset
	dataOffset := &reqOffset
	err := c.sendSerialized(opCtx, func(sendCtx context.Context) error {
		if err := c.sendControl(sendCtx, zcopyproto.Message{
			Op:         zcopyproto.OpPutReq,
			ReqID:      reqID,
			Bucket:     bucket,
			Key:        key,
			Size:       ref.Size,
			DataOffset: dataOffset,
		}); err != nil {
			return err
		}
		return c.sendAtConn.SendMessageAt(sendCtx, payload, ref.Offset)
	})
	if err != nil {
		c.removePending(reqID)
		return err
	}

	res, err := c.waitResult(opCtx, reqID, ch)
	if err != nil {
		return err
	}
	switch res.msg.Op {
	case zcopyproto.OpRespOK:
		return nil
	case zcopyproto.OpRespErr:
		if res.msg.Err == "" {
			return fmt.Errorf("zcopy: remote error")
		}
		return fmt.Errorf("zcopy: %s", res.msg.Err)
	default:
		return fmt.Errorf("zcopy: unexpected response op=%q", res.msg.Op)
	}
}

func (c *Client) registerRelease(fn func() error) uint64 {
	if fn == nil {
		return 0
	}
	token := c.nextToken.Add(1)
	c.releaseMu.Lock()
	c.releaseFns[token] = fn
	c.releaseMu.Unlock()
	return token
}

// Release informs the client that the borrowed buffer ref is no longer needed.
func (c *Client) Release(ref BufferRef) error {
	if ref.token == 0 {
		return nil
	}
	c.releaseMu.Lock()
	fn := c.releaseFns[ref.token]
	delete(c.releaseFns, ref.token)
	c.releaseMu.Unlock()
	if fn == nil {
		return nil
	}
	return fn()
}

// GetZeroCopy downloads object data and returns its shared-memory location.
func (c *Client) GetZeroCopy(ctx context.Context, bucket, key string, targetOffset, maxSize int) (BufferRef, error) {
	ref := BufferRef{Offset: targetOffset, Size: maxSize}
	if err := c.validateRef(ref); err != nil {
		return BufferRef{}, err
	}
	if strings.TrimSpace(bucket) == "" || strings.TrimSpace(key) == "" {
		return BufferRef{}, fmt.Errorf("s3rdmaclient: bucket/key must not be empty")
	}

	opCtx, cancel := c.withTimeout(ctx)
	defer cancel()

	reqID := c.nextRequestID()
	ch := c.registerPending(reqID)
	reqOffset := targetOffset
	if err := c.sendControlSerialized(opCtx, zcopyproto.Message{
		Op:         zcopyproto.OpGetReq,
		ReqID:      reqID,
		Bucket:     bucket,
		Key:        key,
		Max:        maxSize,
		DataOffset: &reqOffset,
	}); err != nil {
		c.removePending(reqID)
		return BufferRef{}, err
	}

	res, err := c.waitResult(opCtx, reqID, ch)
	if err != nil {
		return BufferRef{}, err
	}

	switch res.msg.Op {
	case zcopyproto.OpRespErr:
		if res.msg.Err == "" {
			return BufferRef{}, fmt.Errorf("zcopy: remote error")
		}
		return BufferRef{}, fmt.Errorf("zcopy: %s", res.msg.Err)
	case zcopyproto.OpGetMeta:
	default:
		return BufferRef{}, fmt.Errorf("zcopy: unexpected response op=%q", res.msg.Op)
	}

	if res.msg.Size < 0 || res.msg.Size > maxSize {
		_ = res.borrowed.Release()
		_ = c.sendAck(1)
		return BufferRef{}, fmt.Errorf("zcopy: invalid get size=%d max=%d", res.msg.Size, maxSize)
	}
	if len(res.borrowed.Payload) != res.msg.Size {
		_ = res.borrowed.Release()
		_ = c.sendAck(1)
		return BufferRef{}, fmt.Errorf("zcopy: payload size mismatch got=%d want=%d", len(res.borrowed.Payload), res.msg.Size)
	}
	if res.msg.DataOffset == nil {
		_ = res.borrowed.Release()
		_ = c.sendAck(1)
		return BufferRef{}, fmt.Errorf("zcopy: missing data_offset in get meta")
	}
	expectedOffset := *res.msg.DataOffset
	if expectedOffset < 0 || expectedOffset+res.msg.Size > len(c.sharedMemory) {
		_ = res.borrowed.Release()
		_ = c.sendAck(1)
		return BufferRef{}, fmt.Errorf("zcopy: invalid returned data_offset=%d size=%d", expectedOffset, res.msg.Size)
	}
	if res.borrowed.SharedOffset != expectedOffset {
		_ = res.borrowed.Release()
		_ = c.sendAck(1)
		return BufferRef{}, fmt.Errorf("zcopy: transport did not honor requested data_offset=%d got=%d", expectedOffset, res.borrowed.SharedOffset)
	}
	token := c.registerRelease(func() error {
		err1 := res.borrowed.Release()
		err2 := c.sendAck(1)
		if err1 != nil {
			return err1
		}
		return err2
	})
	return BufferRef{Offset: expectedOffset, Size: res.msg.Size, token: token}, nil
}

func (c *Client) SharedMemory() []byte {
	return c.sharedMemory
}

func (c *Client) Close() error {
	var firstErr error
	c.closeOnce.Do(func() {
		c.releaseMu.Lock()
		for token, fn := range c.releaseFns {
			if fn != nil {
				if err := fn(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			delete(c.releaseFns, token)
		}
		c.releaseMu.Unlock()

		close(c.closedCh)

		if err := c.conn.Close(); err != nil && firstErr == nil && !errors.Is(err, net.ErrClosed) {
			firstErr = err
		}
		c.wg.Wait()
		if err := c.getRecvErr(); err != nil && firstErr == nil && !errors.Is(err, net.ErrClosed) {
			firstErr = err
		}
	})
	return firstErr
}
