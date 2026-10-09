// woc ("wake on call") keeps opted-in apps stopped while nobody uses them and starts them on
// the first request.
//
//	browser ─▶ Traefik ─▶ woc :8080 ─▶ app container (started on demand)
//	           ▲
//	           └─ routers from woc :8081/traefik (HTTP provider), built from container labels
//
// An app opts in with labels (see woc.go) and WITHOUT traefik.* labels: a stopped container
// disappears from Traefik's docker provider, so its router has to live somewhere that is always
// up — here. After woc.idle (default 30m) with no requests and none in flight, woc stops it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type config struct {
	listen, admin            string
	dockerHost, dockerWrite  string // reads; start/stop (empty = same as reads)
	selfURL                  string // how Traefik reaches the proxy port
	entryPoint, certResolver string
	node                     string // waiting page footer: which server answered
	brand                    string // waiting page footer: whose system this is
	defaultIdle              time.Duration
	syncEvery, reapEvery     time.Duration
	pageAfter                time.Duration // a browser waits this long before getting the "starting" page
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func loadConfig() config {
	return config{
		listen:       env("WOC_LISTEN", ":8080"),
		admin:        env("WOC_ADMIN_LISTEN", ":8081"),
		dockerHost:   env("DOCKER_HOST", "unix:///var/run/docker.sock"),
		dockerWrite:  env("DOCKER_WRITE_HOST", ""),
		selfURL:      env("WOC_SELF_URL", "http://woc:8080"),
		entryPoint:   env("WOC_ENTRYPOINT", "websecure"),
		certResolver: env("WOC_CERTRESOLVER", "le"),
		node:         env("WOC_NODE", hostname()),
		brand:        env("WOC_BRAND", "Wake On Call"),
		defaultIdle:  parseDur(os.Getenv("WOC_DEFAULT_IDLE"), 30*time.Minute),
		syncEvery:    parseDur(os.Getenv("WOC_SYNC_EVERY"), 5*time.Second),
		reapEvery:    parseDur(os.Getenv("WOC_REAP_EVERY"), 15*time.Second),
		pageAfter:    parseDur(os.Getenv("WOC_PAGE_AFTER"), 2*time.Second),
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" { // for the image's HEALTHCHECK (no curl in it)
		resp, err := http.Get("http://127.0.0.1" + env("WOC_ADMIN_LISTEN", ":8081") + "/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()
	d, err := newDocker(cfg.dockerHost, cfg.dockerWrite)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	e := newEngine(d, log, cfg.defaultIdle)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := e.sync(ctx); err != nil {
		log.Warn("first sync failed, will retry", "err", err)
	}
	go every(ctx, cfg.syncEvery, func() {
		if err := e.sync(ctx); err != nil {
			log.Warn("sync", "err", err)
		}
	})
	go every(ctx, cfg.reapEvery, func() { e.reap(ctx) })

	proxy := &http.Server{Addr: cfg.listen, Handler: e.proxyHandler(cfg), ReadHeaderTimeout: 10 * time.Second}
	admin := &http.Server{Addr: cfg.admin, Handler: e.adminHandler(cfg), ReadHeaderTimeout: 10 * time.Second}
	for _, s := range []*http.Server{proxy, admin} {
		go func() {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("listen", "addr", s.Addr, "err", err)
				os.Exit(1)
			}
		}()
	}
	log.Info("woc started", "proxy", cfg.listen, "admin", cfg.admin, "default_idle", cfg.defaultIdle.String())
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = proxy.Shutdown(sctx)
	_ = admin.Shutdown(sctx)
}

// hostname is the default node name. In a container that's its short id, so set WOC_NODE to the
// machine's name.
func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "woc"
}

func every(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

// wantsPage: a browser navigating to a page gets a "starting…" page that reloads itself
// instead of a blank tab spinning; APIs, assets and anything else just wait for the wake.
func wantsPage(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.Contains(r.Header.Get("Accept"), "text/html")
}

func (e *engine) proxyHandler(cfg config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rt, ok := e.lookup(r.Host)
		if !ok {
			http.NotFound(w, r)
			return
		}
		// The waiting page on demand, to see it without putting the app to sleep.
		if r.URL.Path == "/.well-known/woc/preview" {
			writeStartingPage(w, r, http.StatusOK, cfg.brand, cfg.node, uuidv7(e.now()))
			return
		}
		g := rt.group
		if !g.acquire(e.now()) {
			if err := e.wakeFor(w, r, g, cfg); err != nil {
				return // wakeFor already answered
			}
			if !g.acquire(e.now()) { // put back to sleep in between: rare, ask to retry
				w.Header().Set("Retry-After", "1")
				http.Error(w, "retry", http.StatusServiceUnavailable)
				return
			}
		}
		// A closure: the clock must be read when the request ENDS (a long download or a
		// websocket counts as activity until it closes), not when the defer is set up.
		defer func() { g.release(e.now()) }()
		e.forward(w, r, rt)
	})
}

// wakeFor wakes the group for this request. A browser navigation that is still waiting after
// cfg.pageAfter gets the "starting" page (the wake carries on in the background).
func (e *engine) wakeFor(w http.ResponseWriter, r *http.Request, g *group, cfg config) error {
	done := make(chan error, 1)
	go func() { done <- e.wake(context.WithoutCancel(r.Context()), g) }()
	var timer <-chan time.Time
	if wantsPage(r) {
		timer = time.After(cfg.pageAfter)
	}
	select {
	case err := <-done:
		if err != nil {
			e.log.Error("wake", "group", g.name, "err", err)
			http.Error(w, "the app could not start", http.StatusBadGateway)
		}
		return err
	case <-timer:
		id := uuidv7(e.now())
		e.log.Info("waiting page", "group", g.name, "request_id", id, "host", r.Host)
		writeStartingPage(w, r, http.StatusServiceUnavailable, cfg.brand, cfg.node, id)
		return errors.New("starting page served")
	case <-r.Context().Done():
		return r.Context().Err()
	}
}

func (e *engine) forward(w http.ResponseWriter, r *http.Request, rt route) {
	target := &url.URL{Scheme: "http", Host: rt.target}
	p := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host // the app sees its public hostname
			// ReverseProxy drops X-Forwarded-* before Rewrite. Traefik already set them with
			// the real client (https, Cloudflare's chain): pass them on as the app would see
			// them without woc in between, instead of woc's own view (http, Traefik's IP).
			for _, k := range []string{"X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host", "X-Forwarded-Port"} {
				if v := pr.In.Header.Values(k); len(v) > 0 {
					pr.Out.Header[k] = v
				}
			}
		},
		FlushInterval: -1, // SSE / streaming answers go out as they are written
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// The container went away under us (crash, someone stopped it): the next request
			// re-checks and wakes it again.
			rt.group.markDown()
			e.log.Warn("proxy", "group", rt.group.name, "target", rt.target, "err", err)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "the app is not responding", http.StatusBadGateway)
		},
	}
	p.ServeHTTP(w, r)
}

func (e *engine) adminHandler(cfg config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(e.status())
	})
	mux.HandleFunc("GET /traefik", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(e.traefikConfig(cfg))
	})
	return mux
}
