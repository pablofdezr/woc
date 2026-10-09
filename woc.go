package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Labels an app container opts in with. Containers without woc.enable=true are never touched.
const (
	labelEnable       = "woc.enable"        // "true"
	labelHost         = "woc.host"          // public hostnames, comma-separated
	labelPort         = "woc.port"          // port the app listens on inside the container
	labelIdle         = "woc.idle"          // stop after this long without requests (default 30m)
	labelGroup        = "woc.group"         // containers that wake and sleep together (default: its own name)
	labelMiddlewares  = "woc.middlewares"   // Traefik middlewares for its router, e.g. cloudflare-only@file
	labelStartTimeout = "woc.start_timeout" // how long a request waits for the wake (default 60s)
	labelReady        = "woc.ready"         // "port" (default): ready once it accepts TCP; "healthy": wait for its healthcheck
)

// member is one container of a group. Members without hosts (an internal API, a worker) start
// and stop with the group but receive no traffic from woc.
type member struct {
	name        string
	hosts       []string
	port        int
	waitHealthy bool
}

// group is the unit that wakes and sleeps.
type group struct {
	name         string
	members      []member
	idle         time.Duration
	startTimeout time.Duration
	middlewares  []string

	// op serializes start and stop, so a request that arrives while the group is being put to
	// sleep waits for the stop and then wakes it again, instead of racing it.
	op sync.Mutex

	mu       sync.Mutex
	running  bool      // at least one member running (what the reaper stops)
	ready    bool      // every member running and healthy: requests go straight through
	inflight int       // requests (and websockets) being proxied right now
	lastSeen time.Time // last request start or end
	wokeAt   time.Time
}

func (g *group) touch(now time.Time) {
	g.mu.Lock()
	g.lastSeen = now
	g.mu.Unlock()
}

type route struct {
	group  *group
	target string // host:port on the shared docker network
}

type engine struct {
	d       *docker
	log     *slog.Logger
	now     func() time.Time
	defIdle time.Duration
	grace   time.Duration // docker stop timeout
	// addr is where a member listens: container name on the shared docker network (tests
	// point it at localhost).
	addr func(m member) string

	mu     sync.RWMutex
	groups map[string]*group
	routes map[string]route // lowercase host → route
}

func newEngine(d *docker, log *slog.Logger, defIdle time.Duration) *engine {
	return &engine{d: d, log: log, now: time.Now, defIdle: defIdle, grace: 10 * time.Second,
		addr:   func(m member) string { return net.JoinHostPort(m.name, strconv.Itoa(m.port)) },
		groups: map[string]*group{}, routes: map[string]route{}}
}

func parseDur(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(s)); err == nil && d > 0 {
		return d
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// sync rebuilds groups and routes from the containers' labels. Existing groups keep their
// activity state, so a relabel or a woc restart never resets anybody's idle clock backwards.
func (e *engine) sync(ctx context.Context) error {
	cs, err := e.d.listManaged(ctx)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	type spec struct {
		members            []member
		idle, startTimeout time.Duration // 0 = not set by any member
		middlewares        []string
		running, total     int
	}
	specs := map[string]*spec{}
	for _, c := range cs {
		l := c.Labels
		gname := strings.TrimSpace(l[labelGroup])
		if gname == "" {
			gname = c.Name()
		}
		s := specs[gname]
		if s == nil {
			s = &spec{}
			specs[gname] = s
		}
		m := member{name: c.Name(), hosts: splitList(l[labelHost]), waitHealthy: strings.TrimSpace(l[labelReady]) == "healthy"}
		if len(m.hosts) > 0 {
			p, err := strconv.Atoi(l[labelPort])
			if err != nil || p <= 0 || p > 65535 {
				e.log.Warn("invalid woc.port, container ignored", "container", m.name, "port", l[labelPort])
				continue
			}
			m.port = p
		}
		// Group-wide settings: the longest idle and start timeout any member asks for wins.
		if v, ok := l[labelIdle]; ok {
			s.idle = max(s.idle, parseDur(v, e.defIdle))
		}
		if v, ok := l[labelStartTimeout]; ok {
			s.startTimeout = max(s.startTimeout, parseDur(v, 60*time.Second))
		}
		s.middlewares = append(s.middlewares, splitMiddlewares(l[labelMiddlewares])...)
		s.members = append(s.members, m)
		s.total++
		if c.State == "running" {
			s.running++
		}
	}

	now := e.now()
	groups := map[string]*group{}
	routes := map[string]route{}
	for name, s := range specs {
		g := e.groups[name]
		if g == nil {
			g = &group{name: name, lastSeen: now}
		}
		sort.Slice(s.members, func(i, j int) bool { return s.members[i].name < s.members[j].name })
		if s.idle == 0 {
			s.idle = e.defIdle
		}
		if s.startTimeout == 0 {
			s.startTimeout = 60 * time.Second
		}
		g.members, g.idle, g.startTimeout, g.middlewares = s.members, s.idle, s.startTimeout, dedupe(s.middlewares)
		g.mu.Lock()
		if s.running > 0 && !g.running {
			// Started from outside (deploy, CI, by hand): it gets a full idle window from now.
			g.lastSeen = now
		}
		g.running = s.running > 0
		if s.running < s.total {
			g.ready = false // the next request re-checks and starts what is missing
		}
		g.mu.Unlock()
		groups[name] = g
		for _, m := range s.members {
			for _, h := range m.hosts {
				if prev, dup := routes[h]; dup {
					e.log.Warn("duplicate host, keeping the first", "host", h, "group", prev.group.name, "ignored", name)
					continue
				}
				routes[h] = route{group: g, target: e.addr(m)}
			}
		}
	}
	e.groups, e.routes = groups, routes
	return nil
}

func splitMiddlewares(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func (e *engine) lookup(host string) (route, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.routes[strings.ToLower(host)]
	return r, ok
}

// acquire marks a request in flight if the group is up. The caller must release().
func (g *group) acquire(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.ready {
		return false
	}
	g.inflight++
	g.lastSeen = now
	return true
}

func (g *group) release(now time.Time) {
	g.mu.Lock()
	g.inflight--
	g.lastSeen = now
	g.mu.Unlock()
}

func (g *group) markDown() {
	g.mu.Lock()
	g.ready = false
	g.mu.Unlock()
}

var errWakeTimeout = errors.New("the app did not start in time")

// wake starts every stopped member and waits until all are ready (healthy if they declare a
// healthcheck, accepting TCP on their port if they serve traffic). Concurrent callers share
// one wake through g.op.
func (e *engine) wake(ctx context.Context, g *group) error {
	g.op.Lock()
	defer g.op.Unlock()
	g.mu.Lock()
	ready := g.ready
	g.mu.Unlock()
	if ready {
		return nil
	}
	t0 := e.now()
	ctx, cancel := context.WithTimeout(ctx, g.startTimeout)
	defer cancel()
	started := 0
	for _, m := range g.members {
		st, err := e.d.inspect(ctx, m.name)
		if err != nil {
			return err
		}
		if !st.Running {
			if err := e.d.start(ctx, m.name); err != nil {
				return err
			}
			started++
		}
	}
	for _, m := range g.members {
		if err := e.waitReady(ctx, m); err != nil {
			return err
		}
	}
	now := e.now()
	g.mu.Lock()
	g.ready, g.running, g.lastSeen = true, true, now
	if started > 0 {
		g.wokeAt = now
	}
	g.mu.Unlock()
	if started > 0 {
		e.log.Info("woke", "group", g.name, "started", started, "took", now.Sub(t0).Round(time.Millisecond).String())
	}
	return nil
}

// waitReady: a member that serves traffic is ready once its port accepts connections —
// Docker's healthcheck only re-runs every start_interval, which measured as +500 ms on a wake of
// ~400 ms. woc.ready=healthy waits for the healthcheck instead; members without a port (internal
// workers) are ready when running, or healthy if they declare a healthcheck.
func (e *engine) waitReady(ctx context.Context, m member) error {
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		var ok bool
		if m.port > 0 && !m.waitHealthy {
			ok = dialOK(ctx, e.addr(m))
		} else if st, err := e.d.inspect(ctx, m.name); err == nil {
			ok = st.ready() && (m.port == 0 || dialOK(ctx, e.addr(m)))
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", m.name, errWakeTimeout)
		case <-tick.C:
		}
	}
}

func dialOK(ctx context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// reap stops every group that has been idle longer than its window with nothing in flight.
func (e *engine) reap(ctx context.Context) {
	e.mu.RLock()
	gs := make([]*group, 0, len(e.groups))
	for _, g := range e.groups {
		gs = append(gs, g)
	}
	e.mu.RUnlock()
	for _, g := range gs {
		e.reapOne(ctx, g)
	}
}

func (e *engine) reapOne(ctx context.Context, g *group) {
	if !g.op.TryLock() { // a wake is in progress: by definition not idle
		return
	}
	defer g.op.Unlock()
	now := e.now()
	g.mu.Lock()
	idleFor := now.Sub(g.lastSeen)
	if !g.running || g.inflight > 0 || idleFor < g.idle {
		g.mu.Unlock()
		return
	}
	// From here new requests take the slow path and queue on g.op until the stop finishes.
	g.ready, g.running = false, false
	g.mu.Unlock()
	for _, m := range g.members {
		if err := e.d.stop(ctx, m.name, e.grace); err != nil {
			e.log.Error("stop failed", "group", g.name, "container", m.name, "err", err)
		}
	}
	e.log.Info("slept", "group", g.name, "idle", idleFor.Round(time.Second).String())
}

type groupStatus struct {
	Group    string   `json:"group"`
	Members  []string `json:"members"`
	Hosts    []string `json:"hosts"`
	Running  bool     `json:"running"`
	Ready    bool     `json:"ready"`
	Inflight int      `json:"inflight"`
	Idle     string   `json:"idle"`
	IdleFor  string   `json:"idle_for"`
	SleepsIn string   `json:"sleeps_in,omitempty"`
	WokeAt   string   `json:"woke_at,omitempty"`
}

func (e *engine) status() []groupStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := e.now()
	var out []groupStatus
	for _, g := range e.groups {
		s := groupStatus{Group: g.name, Idle: g.idle.String()}
		for _, m := range g.members {
			s.Members = append(s.Members, m.name)
			s.Hosts = append(s.Hosts, m.hosts...)
		}
		g.mu.Lock()
		s.Running, s.Ready, s.Inflight = g.running, g.ready, g.inflight
		s.IdleFor = now.Sub(g.lastSeen).Round(time.Second).String()
		if g.running && g.inflight == 0 {
			s.SleepsIn = max(0, g.idle-now.Sub(g.lastSeen)).Round(time.Second).String()
		}
		if !g.wokeAt.IsZero() {
			s.WokeAt = g.wokeAt.UTC().Format(time.RFC3339)
		}
		g.mu.Unlock()
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out
}
