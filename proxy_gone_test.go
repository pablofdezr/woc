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
	"sync/atomic"
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
		b, _ := io.ReadAll(r.Body)
		io.WriteString(w, "hello "+r.Method+" "+strconv.Itoa(len(b)))
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

	gone := func() {
		fd.mu.Lock()
		fd.cs["web"].running = false
		fd.mu.Unlock()
		b.down()
	}

	gone()
	rec := do(http.MethodGet, "")
	if rec.Code != 200 || rec.Body.String() != "hello GET 0" {
		t.Fatalf("GET after the app went away: %d %q, want 200 (woken and retried)", rec.Code, rec.Body.String())
	}
	if s, _ := fd.counts(); s != 1 {
		t.Fatalf("starts=%d, want 1", s)
	}

	// A form POST, and a 5 MB upload: replayed whole, the client never sees an error to retry.
	for i, size := range []int{len("name=ada&email=ada@example.com"), 5 << 20} {
		gone()
		rec = do(http.MethodPost, strings.Repeat("x", size))
		want := "hello POST " + strconv.Itoa(size)
		if rec.Code != 200 || rec.Body.String() != want {
			t.Fatalf("POST of %d bytes after the app went away: %d %q, want 200 %q", size, rec.Code, rec.Body.String(), want)
		}
		if s, _ := fd.counts(); s != 2+i {
			t.Fatalf("starts=%d, want %d", s, 2+i)
		}
	}
}

// newGoneEngine: an engine over fakeDocker routing app.test to addr, with the proxy handler.
func newGoneEngine(t *testing.T, fd *fakeDocker, port string) http.Handler {
	t.Helper()
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
	return e.proxyHandler(cfg)
}

// A live app that refuses connections (overloaded, restarting its listener) is not "gone":
// Docker says it runs, so requests are answered at once instead of being held, and a flood of
// them costs a single inspect, not one each.
func TestRefusingButRunningAppIsNotHeld(t *testing.T) {
	b := &flakyBackend{addr: "127.0.0.1:0"}
	b.up(t)
	_, port, _ := net.SplitHostPort(b.addr)
	labels := map[string]string{labelEnable: "true", labelHost: "app.test", labelPort: port, labelIdle: "30m", labelStartTimeout: "3s"}
	fd := &fakeDocker{cs: map[string]*fakeContainer{"web": {labels: labels, running: true}}}
	h := newGoneEngine(t, fd, port)
	if rec := httptest.NewRecorder(); true {
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://app.test/", nil))
		if rec.Code != 200 {
			t.Fatalf("warm-up: %d", rec.Code)
		}
	}
	b.down() // still running for Docker, but every dial is now refused
	fd.mu.Lock()
	base := fd.inspects
	fd.mu.Unlock()

	var wg sync.WaitGroup
	codes := make([]int, 50)
	t0 := time.Now()
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "http://app.test/", strings.NewReader("x"))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			codes[i] = rec.Code
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusBadGateway {
			t.Fatalf("request %d: %d, want 502", i, c)
		}
	}
	// Answered at once: nobody waited for a wake (the start timeout is 3 s).
	if el := time.Since(t0); el > time.Second {
		t.Fatalf("took %s", el)
	}
	fd.mu.Lock()
	defer fd.mu.Unlock()
	if fd.starts != 0 {
		t.Fatalf("starts=%d, want 0 (it was running)", fd.starts)
	}
	if n := fd.inspects - base; n > 5 {
		t.Fatalf("%d Docker inspects for 50 failing requests, want a handful", n)
	}
}

// The app read the request line and headers and then died: it may have acted on it, so the
// request is NOT sent again (a POST must never run twice), even though the app is now gone.
func TestRequestTheAppSawIsNotReplayed(t *testing.T) {
	var hits atomic.Int32
	var fd *fakeDocker
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(l.Addr().String())
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fd.mu.Lock()
		fd.cs["web"].running = false // dies while handling it, without reading the body
		fd.mu.Unlock()
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	labels := map[string]string{labelEnable: "true", labelHost: "app.test", labelPort: port, labelIdle: "30m"}
	fd = &fakeDocker{cs: map[string]*fakeContainer{"web": {labels: labels, running: true}}}
	h := newGoneEngine(t, fd, port)

	req := httptest.NewRequest(http.MethodPost, "http://app.test/pay", strings.NewReader("amount=10"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", rec.Code)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("the app received the POST %d times, want exactly 1", n)
	}
}

// An app that starts but never gets ready: requests queued behind the failed wake share its error
// instead of each waiting out another start timeout in turn (10 requests, 1 s timeout: ~1 s, not 10).
func TestFailedWakeIsSharedByQueuedRequests(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(l.Addr().String())
	l.Close() // the app never listens
	labels := map[string]string{labelEnable: "true", labelHost: "app.test", labelPort: port, labelIdle: "30m", labelStartTimeout: "1s"}
	fd := &fakeDocker{cs: map[string]*fakeContainer{"web": {labels: labels, running: false}}}
	h := newGoneEngine(t, fd, port)

	var wg sync.WaitGroup
	t0 := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://app.test/", nil))
			if rec.Code != http.StatusBadGateway {
				t.Errorf("got %d, want 502", rec.Code)
			}
		}()
	}
	wg.Wait()
	if el := time.Since(t0); el > 3*time.Second {
		t.Fatalf("10 queued requests took %s: each waited out its own start timeout", el)
	}
}
