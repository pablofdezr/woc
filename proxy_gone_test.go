package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// flakyBackend is an app on a fixed address that can go away and come back, like a container
// stopped from outside and started again.
type flakyBackend struct {
	addr string
	mu   sync.Mutex
	srv  *http.Server
}

func (b *flakyBackend) up(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp", b.addr)
	if err != nil {
		t.Fatal(err)
	}
	b.addr = l.Addr().String()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello "+r.Method)
	})}
	go srv.Serve(l)
	b.mu.Lock()
	b.srv = srv
	b.mu.Unlock()
}

func (b *flakyBackend) down() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.srv.Close()
}

// The container dies (or is stopped by hand) while woc thinks it's awake: the next request must
// wake it and get through, not answer 502.
func TestAppGoneIsWokenAndRetried(t *testing.T) {
	b := &flakyBackend{addr: "127.0.0.1:0"}
	b.up(t)
	t.Cleanup(b.down)
	_, port, _ := net.SplitHostPort(b.addr)

	labels := map[string]string{labelEnable: "true", labelHost: "app.test", labelPort: port, labelIdle: "30m"}
	fd := &fakeDocker{cs: map[string]*fakeContainer{"web": {labels: labels, running: true}}}
	fd.onStart = func(string) { b.up(t) }
	ds := httptest.NewServer(fd)
	t.Cleanup(ds.Close)
	d, err := newDocker(ds.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	e := newEngine(d, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Minute)
	e.addr = func(m member) string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(m.port)) }
	if err := e.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := loadConfig()
	cfg.pageAfter = 5 * time.Second
	h := e.proxyHandler(cfg)
	do := func(method, body string) *httptest.ResponseRecorder {
		var req *http.Request
		if body == "" {
			req = httptest.NewRequest(method, "http://app.test/", nil)
		} else {
			req = httptest.NewRequest(method, "http://app.test/", strings.NewReader(body))
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := do(http.MethodGet, ""); rec.Code != 200 {
		t.Fatalf("awake: %d", rec.Code)
	}

	// Stopped from outside, woc not told.
	fd.mu.Lock()
	fd.cs["web"].running = false
	fd.mu.Unlock()
	b.down()

	rec := do(http.MethodGet, "")
	if rec.Code != 200 || rec.Body.String() != "hello GET" {
		t.Fatalf("after the app went away: %d %q, want 200 (woken and retried)", rec.Code, rec.Body.String())
	}
	if s, _ := fd.counts(); s != 1 {
		t.Fatalf("starts=%d, want 1", s)
	}

	// A request with a body can't be replayed: 503 + Retry-After, and the app is woken for the retry.
	fd.mu.Lock()
	fd.cs["web"].running = false
	fd.mu.Unlock()
	b.down()
	rec = do(http.MethodPost, "payload")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("POST to a gone app: %d, want 503 with Retry-After", rec.Code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if s, _ := fd.counts(); s == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the app was not woken after the failed POST")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// wakeAsync finishes marking the group ready; then the retry goes straight through.
	for time.Now().Before(deadline) {
		if rec = do(http.MethodPost, "payload"); rec.Code == 200 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rec.Code != 200 || rec.Body.String() != "hello POST" {
		t.Fatalf("POST retry: %d %q", rec.Code, rec.Body.String())
	}
}
