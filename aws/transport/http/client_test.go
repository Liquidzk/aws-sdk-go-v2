package http

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestBuildableClient_NoFollowRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Moved Permanently", http.StatusMovedPermanently)
		}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)

	client := NewBuildableClient()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	if e, a := http.StatusMovedPermanently, resp.StatusCode; e != a {
		t.Errorf("expect %v code, got %v", e, a)
	}
}

func TestBuildableClient_WithTimeout(t *testing.T) {
	client := &BuildableClient{}

	expect := 10 * time.Millisecond
	client2 := client.WithTimeout(expect)

	if e, a := time.Duration(0), client.GetTimeout(); e != a {
		t.Errorf("expect %v initial timeout, got %v", e, a)
	}

	if e, a := expect, client2.GetTimeout(); e != a {
		t.Errorf("expect %v timeout, got %v", e, a)
	}
}

func TestBuildableClient_WithDialContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	defer server.Close()

	var called int32
	client := NewBuildableClient().
		WithTransportOptions(func(tr *http.Transport) {
			tr.Proxy = nil
		}).
		WithDialContext(func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&called, 1)
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		})

	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	defer resp.Body.Close()

	if e, a := http.StatusOK, resp.StatusCode; e != a {
		t.Fatalf("expect %v code, got %v", e, a)
	}

	if atomic.LoadInt32(&called) == 0 {
		t.Fatalf("expect custom dial context to be called")
	}
}

func TestBuildableClient_WithDialContext_PreservedWhenDialerOptionsApplied(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	defer server.Close()

	var called int32
	client := NewBuildableClient().
		WithTransportOptions(func(tr *http.Transport) {
			tr.Proxy = nil
		}).
		WithDialContext(func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&called, 1)
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}).
		WithDialerOptions(func(d *net.Dialer) {
			d.Timeout = 2 * time.Second
		})

	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	defer resp.Body.Close()

	if atomic.LoadInt32(&called) == 0 {
		t.Fatalf("expect custom dial context to remain active after WithDialerOptions")
	}
}

func TestBuildableClient_WithDialContextNil_ClearsOverride(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	defer server.Close()

	var called int32
	client := NewBuildableClient().
		WithTransportOptions(func(tr *http.Transport) {
			tr.Proxy = nil
		}).
		WithDialContext(func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&called, 1)
			return nil, errors.New("unexpected custom dial context call")
		}).
		WithDialContext(nil)

	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	defer resp.Body.Close()

	if atomic.LoadInt32(&called) != 0 {
		t.Fatalf("expect custom dial context to be cleared")
	}
}

func TestBuildableClient_concurrent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
		}))
	defer server.Close()

	var client aws.HTTPClient = NewBuildableClient()

	atOnce := 100
	var wg sync.WaitGroup
	wg.Add(atOnce)
	for i := 0; i < atOnce; i++ {
		go func(i int, client aws.HTTPClient) {
			defer wg.Done()

			if v, ok := client.(interface{ GetTimeout() time.Duration }); ok {
				v.GetTimeout()
			}

			if i%3 == 0 {
				if v, ok := client.(interface {
					WithTransportOptions(opts ...func(*http.Transport)) aws.HTTPClient
				}); ok {
					client = v.WithTransportOptions()
				}
			}

			req, _ := http.NewRequest("GET", server.URL, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Errorf("expect no error, got %v", err)
			}
			resp.Body.Close()
		}(i, client)
	}

	wg.Wait()
}
