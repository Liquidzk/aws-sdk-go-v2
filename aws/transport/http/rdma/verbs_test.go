package rdma

import (
	"context"
	"strings"
	"testing"
)

func TestVerbsOptionsNormalizeDefaults(t *testing.T) {
	cfg, err := (VerbsOptions{}).normalize()
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}

	if cfg.framePayloadSize != DefaultVerbsFramePayloadSize {
		t.Fatalf("frame payload default = %d", cfg.framePayloadSize)
	}
	if cfg.sendQueueDepth != DefaultVerbsSendQueueDepth {
		t.Fatalf("send queue depth default = %d", cfg.sendQueueDepth)
	}
	if cfg.recvQueueDepth != DefaultVerbsRecvQueueDepth {
		t.Fatalf("recv queue depth default = %d", cfg.recvQueueDepth)
	}
	if cfg.inlineThreshold != DefaultVerbsInlineThreshold {
		t.Fatalf("inline threshold default = %d", cfg.inlineThreshold)
	}
	if cfg.sendSignalIntvl != DefaultVerbsSendSignalInterval {
		t.Fatalf("send signal interval default = %d", cfg.sendSignalIntvl)
	}
}

func TestVerbsOptionsNormalizeValidation(t *testing.T) {
	cases := []VerbsOptions{
		{FramePayloadSize: -1},
		{SendQueueDepth: -1},
		{RecvQueueDepth: -1},
		{InlineThreshold: -2},
		{SendSignalInterval: -1},
	}

	for i, tc := range cases {
		if _, err := tc.normalize(); err == nil {
			t.Fatalf("case %d expected validation error", i)
		}
	}
}

func TestVerbsOptionsNormalizeLowCPU(t *testing.T) {
	cfg, err := (VerbsOptions{LowCPU: true}).normalize()
	if err != nil {
		t.Fatalf("normalize lowcpu failed: %v", err)
	}
	if cfg.sendSignalIntvl != DefaultVerbsLowCPUSendSignalInterval {
		t.Fatalf("lowcpu send signal interval = %d", cfg.sendSignalIntvl)
	}

	cfg, err = (VerbsOptions{
		LowCPU:             true,
		SendSignalInterval: 7,
	}).normalize()
	if err != nil {
		t.Fatalf("normalize lowcpu override failed: %v", err)
	}
	if cfg.sendSignalIntvl != 7 {
		t.Fatalf("send signal interval override = %d", cfg.sendSignalIntvl)
	}
}

func TestVerbsOptionsNormalizeSharedRWMemory(t *testing.T) {
	_, err := (VerbsOptions{
		FramePayloadSize: 1024,
		SharedRWMemory:   make([]byte, 512),
	}).normalize()
	if err == nil {
		t.Fatalf("expected shared memory validation error")
	}

	cfg, err := (VerbsOptions{
		FramePayloadSize: 1024,
		SharedRWMemory:   make([]byte, 4096),
	}).normalize()
	if err != nil {
		t.Fatalf("unexpected normalize error: %v", err)
	}
	if got := len(cfg.sharedRWMemory); got != 4096 {
		t.Fatalf("sharedRWMemory length=%d", got)
	}
}

func TestSplitHostPortAddress(t *testing.T) {
	host, port, err := splitHostPortAddress("tcp", "10.0.1.1:7471")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "10.0.1.1" || port != "7471" {
		t.Fatalf("unexpected split host=%q port=%q", host, port)
	}

	_, _, err = splitHostPortAddress("udp", "10.0.1.1:7471")
	if err == nil {
		t.Fatalf("expected unsupported network error")
	}

	_, _, err = splitHostPortAddress("tcp", "invalid")
	if err == nil {
		t.Fatalf("expected invalid address error")
	}
}

func TestVerbsOpenUnavailableByDefaultBuild(t *testing.T) {
	if verbsBackendEnabled {
		t.Skip("verbs backend enabled for this build")
	}

	_, err := (VerbsOptions{}).Open(context.Background(), "tcp", "10.0.1.1:7471")
	if err == nil {
		t.Fatalf("expected unavailable error in non-rdma build")
	}
	if !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("unexpected error: %v", err)
	}
}
