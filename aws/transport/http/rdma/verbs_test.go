package rdma

import (
	"context"
	"strings"
	"testing"
	"time"
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
		{EndpointPoolSize: -1},
		{EndpointAcquireTimeout: -1 * time.Millisecond},
		{SharedMemoryBudgetBytes: -1},
		{EndpointSendQueueDepth: -1},
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

func TestNewVerbsDialer(t *testing.T) {
	d := NewVerbsDialer(VerbsOptions{})
	if d.Open == nil {
		t.Fatalf("expected Open to be configured")
	}

	d = NewVerbsDialer(VerbsOptions{
		FramePayloadSize:        4096,
		SendQueueDepth:          8,
		RecvQueueDepth:          16,
		EndpointPoolSize:        4,
		EndpointPoolWarmup:      true,
		EndpointAcquireTimeout:  25 * time.Millisecond,
		SharedMemoryBudgetBytes: 1 << 20,
		EndpointEnableMultiplex: true,
		EndpointSendQueueDepth:  32,
	})
	if e, a := 4, d.EndpointEngine.PoolSize; e != a {
		t.Fatalf("endpoint pool size = %d, want %d", a, e)
	}
	if !d.EndpointEngine.Warmup {
		t.Fatalf("expected endpoint warmup to be enabled")
	}
	if e, a := 25*time.Millisecond, d.EndpointEngine.AcquireTimeout; e != a {
		t.Fatalf("endpoint acquire timeout = %v, want %v", a, e)
	}
	if !d.EndpointEngine.EnableMultiplex {
		t.Fatalf("expected endpoint multiplex to be enabled")
	}
	if e, a := 32, d.EndpointEngine.SendQueueDepth; e != a {
		t.Fatalf("endpoint send queue depth = %d, want %d", a, e)
	}
	if e, a := 1<<20, d.SharedMemoryBudget.TotalBytes; e != a {
		t.Fatalf("shared budget bytes = %d, want %d", a, e)
	}
	if e, a := 4096*(8+16), d.SharedMemoryBudget.EstimatedConnBytes; e != a {
		t.Fatalf("estimated conn bytes = %d, want %d", a, e)
	}
}
