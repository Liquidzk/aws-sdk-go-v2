//go:build linux && cgo && rdma

package rdma

import (
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestVerbsMessageListenerClosePrompt(t *testing.T) {
	listener := newTestVerbsMessageListener(t)

	done := make(chan error, 1)
	go func() {
		done <- listener.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung longer than 2s with no active accepts")
	}
}

func newTestVerbsMessageListener(t *testing.T) MessageListener {
	t.Helper()

	var errs []string
	for _, addr := range rdmaTestListenAddrs(t) {
		listener, err := NewVerbsMessageListener("rdma", addr, VerbsListenerOptions{})
		if err == nil {
			t.Cleanup(func() {
				_ = listener.Close()
			})
			return listener
		}
		errs = append(errs, fmt.Sprintf("%s: %v", addr, err))
	}

	t.Skipf("no RDMA listen address available for close test: %s", strings.Join(errs, "; "))
	return nil
}

func rdmaTestListenAddrs(t *testing.T) []string {
	t.Helper()

	addrs := []string{":0"}
	seen := map[string]struct{}{
		":0": {},
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("list interfaces: %v", err)
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		ifaceAddrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, ifaceAddr := range ifaceAddrs {
			ipNet, ok := ifaceAddr.(*net.IPNet)
			if !ok || ipNet.IP == nil {
				continue
			}
			ip := ipNet.IP.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}

			addr := net.JoinHostPort(ip.String(), "0")
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			addrs = append(addrs, addr)
		}
	}

	slices.Sort(addrs)
	return addrs
}
