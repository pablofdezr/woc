package main

import (
	"context"
	"encoding/json"
	"fmt"
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

// fakeDocker answers the four Engine API calls woc makes, over real HTTP.
type fakeDocker struct {
	mu         sync.Mutex
	cs         map[string]*fakeContainer
	starts     int
	stops      int
	healthyLag int // inspections a started container reports "starting" before "healthy"
}

type fakeContainer struct {
	labels    map[string]string
	running   bool
	hasHealth bool
	untilOK   int
}

func (f *fakeDocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/containers/json":
		var out []containerSummary
		for n, c := range f.cs {
			st := "exited"
			if c.running {
				st = "running"
			}
			out = append(out, containerSummary{ID: n, Names: []string{"/" + n}, Labels: c.labels, State: st})
		}
		json.NewEncoder(w).Encode(out)
	case strings.HasPrefix(path, "/containers/"):
		parts := strings.Split(strings.TrimPrefix(path, "/containers/"), "/")
		c := f.cs[parts[0]]
		if c == nil {
			http.NotFound(w, r)
			return
		}
		switch parts[1] {
		case "json":
			st := containerState{Running: c.running}
			if c.hasHealth {
				st.Health = &struct {
					Status string `json:"Status"`
				}{Status: "healthy"}
				if c.untilOK > 0 {
					c.untilOK--
					st.Health.Status = "starting"
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"State": st})
		case "start":
			if c.running {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			c.running, c.untilOK = true, f.healthyLag
			f.starts++
			w.WriteHeader(http.StatusNoContent)
		case "stop":
			c.running = false
			f.stops++
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeDocker) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts, f.stops
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type harness struct {
	t       *testing.T
	fd      *fakeDocker
	e       *engine
	clk     *clock
	h       http.Handler
	release chan struct{} // closes to let /slow answer
}

// newHarness: one app container "web" serving app.test from a real local backend.
func newHarness(t *testing.T, running bool, labels map[string]string) *harness {
	t.Helper()
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-release
		}
		io.WriteString(w, "hello from "+r.Host+" proto="+r.Header.Get("X-Forwarded-Proto"))
	}))
	t.Cleanup(backend.Close)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(backend.URL, "http://"))

	l := map[string]string{labelEnable: "true", labelHost: "app.test", labelPort: port, labelIdle: "30m"}
	for k, v := range labels {
		l[k] = v
	}
	fd := &fakeDocker{cs: map[string]*fakeContainer{"web": {labels: l, running: running, hasHealth: true}}}
	ds := httptest.NewServer(fd)
	t.Cleanup(ds.Close)
	d, err := newDocker(ds.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	clk := &clock{t: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	e := newEngine(d, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Minute)
	e.now = clk.now
	e.addr = func(m member) string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(m.port)) }
	if err := e.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := loadConfig()
	cfg.pageAfter = 50 * time.Millisecond
	return &harness{t: t, fd: fd, e: e, clk: clk, h: e.proxyHandler(cfg), release: release}
}

func (h *harness) get(path, accept string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "http://app.test"+path, nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	rec := httptest.NewRecorder()
	h.h.ServeHTTP(rec, req)
	return rec
}

func TestWakeOnFirstRequest(t *testing.T) {
	h := newHarness(t, false, nil)
	rec := h.get("/", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "hello from app.test proto=https") {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if s, _ := h.fd.counts(); s != 1 {
		t.Fatalf("starts=%d, want 1", s)
	}
	// Second request: straight through, no second start.
	h.get("/", "")
	if s, _ := h.fd.counts(); s != 1 {
		t.Fatalf("starts=%d after 2nd request, want 1", s)
	}
}

func TestSleepsAfterIdleAndWakesAgain(t *testing.T) {
	h := newHarness(t, false, nil)
	h.get("/", "")
	h.clk.add(29 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped before the idle window")
	}
	h.clk.add(2 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("not stopped after 31 min idle")
	}
	if rec := h.get("/", ""); rec.Code != 200 {
		t.Fatalf("after sleeping: %d", rec.Code)
	}
	if s, _ := h.fd.counts(); s != 2 {
		t.Fatalf("starts=%d, want 2 (woken again)", s)
	}
}

func TestRequestInFlightKeepsItAwake(t *testing.T) {
	h := newHarness(t, true, nil)
	h.get("/", "") // warm
	done := make(chan int)
	go func() { done <- h.get("/slow", "").Code }()
	time.Sleep(50 * time.Millisecond)
	h.clk.add(time.Hour)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped with a request in flight")
	}
	close(h.release)
	if c := <-done; c != 200 {
		t.Fatalf("slow request: %d", c)
	}
	h.e.reap(context.Background()) // the request just ended: its clock restarts
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped right after the long request ended")
	}
	h.clk.add(31 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("not stopped 31 min after the long request")
	}
}

func TestStartedFromOutsideStillSleeps(t *testing.T) {
	// Running when woc sees it and never requested: it must not stay up forever, and the
	// periodic sync must not keep resetting its clock.
	h := newHarness(t, true, nil)
	for range 3 {
		h.clk.add(11 * time.Minute)
		if err := h.e.sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("a never-requested running app was not put to sleep")
	}
}

func TestBrowserGetsStartingPage(t *testing.T) {
	h := newHarness(t, false, map[string]string{labelReady: "healthy"})
	h.fd.healthyLag = 1 << 30 // never healthy while we look
	rec := h.get("/", "text/html,application/xhtml+xml")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "We’re waking up the server") ||
		!strings.Contains(rec.Body.String(), "Wake On Call<span") {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("starting page must not be cached")
	}
	h.fd.mu.Lock()
	h.fd.cs["web"].untilOK = 0 // now it turns healthy
	h.fd.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rec := h.get("/", "text/html"); rec.Code == 200 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("never served the app after it became healthy")
}

func TestUnknownHost(t *testing.T) {
	h := newHarness(t, true, nil)
	req := httptest.NewRequest(http.MethodGet, "http://other.test/", nil)
	rec := httptest.NewRecorder()
	h.h.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("got %d", rec.Code)
	}
	if s, _ := h.fd.counts(); s != 0 {
		t.Fatal("an unknown host must not wake anything")
	}
}

func TestTraefikConfig(t *testing.T) {
	h := newHarness(t, false, map[string]string{labelHost: "b.test, A.test", labelMiddlewares: "cloudflare-only@file"})
	cfg := loadConfig()
	tc := h.e.traefikConfig(cfg)
	r, ok := tc.HTTP.Routers["woc-web"]
	if !ok {
		t.Fatalf("routers: %+v", tc.HTTP.Routers)
	}
	if r.Rule != "Host(`a.test`) || Host(`b.test`)" || r.Service != "woc" || r.TLS["certResolver"] != "le" ||
		len(r.Middlewares) != 1 || r.Middlewares[0] != "cloudflare-only@file" || r.EntryPoints[0] != "websecure" {
		t.Fatalf("router: %+v", r)
	}
	if tc.HTTP.Services["woc"].LoadBalancer.Servers[0]["url"] != cfg.selfURL {
		t.Fatalf("service: %+v", tc.HTTP.Services)
	}
}

func TestPickLang(t *testing.T) {
	for accept, want := range map[string]string{
		"":                        "en",
		"es-ES,es;q=0.9,en;q=0.8": "es",
		"en-US;q=0.5, pl;q=0.9":   "pl",
		"pt-PT":                   "pt-pt",
		"pt-BR,pt;q=0.9":          "pt",
		"zh-TW":                   "zh-hant",
		"zh-Hant-HK":              "zh-hant",
		"zh-CN":                   "zh",
		"nn-NO":                   "nb",
		"xx-YY, *;q=0.1":          "en",
		"sw, de-CH;q=0.7":         "de",
		"es-419":                  "es",
		"fr;q=0, it;q=0.2":        "it",
	} {
		if got := pickLang(accept); got != want {
			t.Errorf("%q → %s, want %s", accept, got, want)
		}
	}
}

func TestUUIDv7(t *testing.T) {
	now := time.UnixMilli(1791579578081)
	a, b := uuidv7(now), uuidv7(now.Add(time.Millisecond))
	if len(a) != 36 || a[14] != '7' || !strings.ContainsRune("89ab", rune(a[19])) {
		t.Fatalf("not a v7 uuid: %s", a)
	}
	if !(a < b) {
		t.Fatalf("not time-ordered: %s !< %s", a, b)
	}
	ms := fmt.Sprintf("%012x", now.UnixMilli())
	if strings.ReplaceAll(a, "-", "")[:12] != ms {
		t.Fatalf("timestamp prefix: %s, want %s…", a, ms)
	}
}

func TestReadyOnPortDoesNotWaitForHealthcheck(t *testing.T) {
	h := newHarness(t, false, nil)
	h.fd.healthyLag = 1 << 30 // its healthcheck would never pass while we look
	t0 := time.Now()
	if rec := h.get("/", "text/html"); rec.Code != 200 {
		t.Fatalf("got %d: port readiness should not wait for the healthcheck", rec.Code)
	}
	if d := time.Since(t0); d > 500*time.Millisecond {
		t.Fatalf("wake took %s", d)
	}
}

func TestPreviewIsOK(t *testing.T) {
	h := newHarness(t, true, nil)
	rec := h.get("/.well-known/woc/preview", "text/html")
	if rec.Code != 200 || rec.Header().Get("Retry-After") != "" || !strings.Contains(rec.Body.String(), "waking up") {
		t.Fatalf("preview: %d %q", rec.Code, rec.Header())
	}
}

func TestBusyURLPostponesSleep(t *testing.T) {
	var busy atomic.Bool
	busy.Store(true)
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if busy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(bs.Close)
	h := newHarness(t, true, map[string]string{labelBusyURL: bs.URL + "/busy"})
	h.clk.add(time.Hour)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped a member that said it was busy")
	}
	busy.Store(false)
	h.e.reap(context.Background()) // no extra idle window once the work is done
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("not stopped on the first reap after the work ended")
	}
}

func TestWakeEndpoint(t *testing.T) {
	h := newHarness(t, false, nil)
	cfg := loadConfig()
	cfg.adminToken = "s3cret"
	admin := h.e.adminHandler(cfg)
	post := func(path, auth string) int {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		return rec.Code
	}
	if c := post("/wake/web?wait=1", ""); c != http.StatusUnauthorized {
		t.Fatalf("no token: %d", c)
	}
	if c := post("/wake/nope", "Bearer s3cret"); c != http.StatusNotFound {
		t.Fatalf("unknown group: %d", c)
	}
	if c := post("/wake/web?wait=1", "Bearer s3cret"); c != http.StatusOK {
		t.Fatalf("wake: %d", c)
	}
	if s, _ := h.fd.counts(); s != 1 {
		t.Fatalf("starts=%d", s)
	}
}

func TestWakeEvery(t *testing.T) {
	h := newHarness(t, false, map[string]string{labelWakeEvery: "1h"})
	h.clk.add(59 * time.Minute)
	h.e.reap(context.Background())
	time.Sleep(50 * time.Millisecond)
	if s, _ := h.fd.counts(); s != 0 {
		t.Fatal("woken before wake_every")
	}
	h.clk.add(2 * time.Minute)
	h.e.reap(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := h.fd.counts(); s == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("not woken after wake_every")
}

func TestResolveBusyURLAndChannels(t *testing.T) {
	for raw, want := range map[string]string{
		"http://:4100/busy":   "http://worker:4100/busy",
		"http://other:9000/x": "http://other:9000/x",
		"http:///busy":        "http://worker/busy",
	} {
		if got, err := resolveBusyURL(raw, "worker"); err != nil || got != want {
			t.Errorf("%s → %s %v, want %s", raw, got, err, want)
		}
	}
	if _, err := resolveBusyURL("ftp://x/busy", "worker"); err == nil {
		t.Error("ftp accepted")
	}
	for ch, ok := range map[string]bool{"jobs": true, "jobs_2": true, "2jobs": false, "jobs;drop": false, "": false, "Jobs": false} {
		if validChannel(ch) != ok {
			t.Errorf("validChannel(%q) != %v", ch, ok)
		}
	}
}
