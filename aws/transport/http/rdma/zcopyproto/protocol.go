package zcopyproto

import (
	"encoding/json"
	"fmt"
)

const (
	OpHelloReq        = "hello_req"
	OpHelloResp       = "hello_resp"
	OpEnsureBucketReq = "ensure_bucket_req"
	OpPutReq          = "put_req"
	OpGetReq          = "get_req"
	OpAck             = "ack"
	OpRespOK          = "resp_ok"
	OpRespErr         = "resp_err"
	OpGetMeta         = "get_meta"
)

// Message is one control message in the zcopy protocol.
// Large object payload is transferred as a separate data message.
type Message struct {
	Op         string `json:"op"`
	ReqID      uint64 `json:"req_id,omitempty"`
	Credits    int    `json:"credits,omitempty"`
	Ack        int    `json:"ack,omitempty"`
	Bucket     string `json:"bucket,omitempty"`
	Key        string `json:"key,omitempty"`
	Size       int    `json:"size,omitempty"`
	Max        int    `json:"max,omitempty"`
	DataOffset *int   `json:"data_offset,omitempty"`
	Err        string `json:"err,omitempty"`
}

func Encode(msg Message) ([]byte, error) {
	if msg.Op == "" {
		return nil, fmt.Errorf("zcopyproto: empty op")
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("zcopyproto: marshal control: %w", err)
	}
	return b, nil
}

func Decode(payload []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return Message{}, fmt.Errorf("zcopyproto: decode control: %w", err)
	}
	if msg.Op == "" {
		return Message{}, fmt.Errorf("zcopyproto: control missing op")
	}
	return msg, nil
}
