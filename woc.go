package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
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

	// For apps that work without receiving HTTP (queue workers):
	labelBusyURL    = "woc.busy_url"       // checked before a stop; 200 = busy, don't stop yet. Host may be empty: http://:4100/busy
	labelWakeEvery  = "woc.wake_every"     // wake the group if it has been asleep this long (a safety net for missed work)
	labelWakeNotify = "woc.wake_on_notify" // Postgres NOTIFY channel(s) that wake the group (needs WOC_PG_URL)

	// Start order inside a group: compose's own depends_on label, or woc.after for containers
	// that don't come from compose (container names, comma-separated).
	labelAfter            = "woc.after"
	labelComposeDependsOn = "com.docker.compose.depends_on" // "db:service_healthy:false,api:service_started:false"
	labelComposeService   = "com.docker.compose.service"
	labelComposeProject   = "com.docker.compose.project"
)

// member is one container of a group. Members without hosts (an internal API, a worker) start
// and stop with the group but receive no traffic from woc.
type member struct {
	name        string
	hosts       []string
	port        int
	waitHealthy bool
	readyPG     bool     // woc.ready: postgres — probe the protocol instead of waiting for the healthcheck
	busyURL     string   // resolved: the container name filled in when the label leaves the host empty
	service     string   // compose project/service, to resolve depends_on
	after       []string // compose services (project/service) or container names it starts after
	tcp         []tcpMap // woc.tcp forwards (TCP mode)
}

// group is the unit that wakes and sleeps.
type group struct {
	name         string
	members      []member
	idle         time.Duration
	startTimeout time.Duration
	middlewares  []string
	wakeEvery    time.Duration // 0 = off
	channels     []string      // Postgres NOTIFY channels that wake it

	// op serializes start and stop, so a request that arrives while the group is being put to
	// sleep waits for the stop and then wakes it again, instead of racing it.
	op sync.Mutex

	mu         sync.Mutex
	running    bool      // at least one member running (what the reaper stops)
	ready      bool      // every member running and healthy: requests go straight through
	inflight   int       // requests (and websockets) being proxied right now
	lastSeen   time.Time // last request start or end
	wokeAt     time.Time
	sleptAt    time.Time // when it was last seen asleep after being up (or first seen asleep)
	holdUntil  time.Time // kept awake until then (woc hold), whatever the traffic
	probe      probeState
	probes     []probe       // woc.wake_when: checked while asleep, any reporting work wakes it
	checkEvery time.Duration // woc.wake_check_every
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

	mu        sync.RWMutex
	groups    map[string]*group
	routes    map[string]route // lowercase host → route
	tcpRoutes map[int]tcpRoute // WOC listen port → container port (TCP mode)

	tcp     tcpListeners
	tcpBind string                          // address the TCP listeners bind to ("" = all)
	tcpAddr func(m member, port int) string // where a member's TCP port is (tests use localhost)

	stateFile    string               // where holds are kept across restarts ("" = memory only)
	pendingHolds map[string]time.Time // restored holds waiting for their group's first sync

	pgURL  string          // Postgres for sql: work probes (WOC_PG_URL)
	warned map[string]bool // invalid labels already logged: sync runs every few seconds
}

func newEngine(d *docker, log *slog.Logger, defIdle time.Duration) *engine {
	return &engine{d: d, log: log, now: time.Now, defIdle: defIdle, grace: 10 * time.Second,
		addr:    func(m member) string { return net.JoinHostPort(m.name, strconv.Itoa(m.port)) },
		tcpAddr: func(m member, port int) string { return net.JoinHostPort(m.name, strconv.Itoa(port)) },
		groups:  map[string]*group{}, routes: map[string]route{}, tcpRoutes: map[int]tcpRoute{},
		warned: map[string]bool{}}
}

func parseDur(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(s)); err == nil && d > 0 {
		return d
	}
	return def
}

// warnOnce logs a label problem the first time sync sees it, not on every sync.
func (e *engine) warnOnce(key, msg string, args ...any) {
	if e.warned[key] {
		return
	}
	e.warned[key] = true
	e.log.Warn(msg, args...)
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
		wakeEvery          time.Duration
		channels           []string
		probes             []probe
		checkEvery         time.Duration // 0 = not set by any member
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
		ready := strings.TrimSpace(l[labelReady])
		m := member{name: c.Name(), hosts: splitList(l[labelHost]), waitHealthy: ready == "healthy", readyPG: ready == "postgres"}
		if svc := l[labelComposeService]; svc != "" {
			m.service = l[labelComposeProject] + "/" + svc
			for _, dep := range strings.Split(l[labelComposeDependsOn], ",") {
				if name, _, _ := strings.Cut(strings.TrimSpace(dep), ":"); name != "" {
					m.after = append(m.after, l[labelComposeProject]+"/"+name)
				}
			}
		}
		m.after = append(m.after, splitList(l[labelAfter])...)
		if v := strings.TrimSpace(l[labelTCP]); v != "" {
			tm, err := parseTCP(v)
			if err != nil {
				e.log.Warn("invalid woc.tcp, ignored", "container", m.name, "err", err)
			} else {
				m.tcp = tm
			}
		}
		if v := strings.TrimSpace(l[labelBusyURL]); v != "" {
			u, err := resolveBusyURL(v, m.name)
			if err != nil {
				e.log.Warn("invalid woc.busy_url, ignored", "container", m.name, "url", v, "err", err)
			} else {
				m.busyURL = u
			}
		}
		if v, ok := l[labelWakeEvery]; ok {
			s.wakeEvery = max(s.wakeEvery, parseDur(v, 0))
		}
		for _, ch := range splitList(l[labelWakeNotify]) {
			if validChannel(ch) {
				s.channels = append(s.channels, ch)
			} else {
				e.log.Warn("invalid woc.wake_on_notify channel, ignored", "container", m.name, "channel", ch)
			}
		}
		if v := strings.TrimSpace(l[labelWakeWhen]); v != "" {
			p, err := parseProbe(v)
			if err == nil && p.kind == probeSQL && e.pgURL == "" {
				err = errors.New("sql probes need WOC_PG_URL")
			}
			if err != nil {
				e.warnOnce(m.name+"\x00"+v, "invalid woc.wake_when, ignored", "container", m.name, "err", err)
			} else if !slices.ContainsFunc(s.probes, func(q probe) bool { return q.raw == p.raw }) {
				s.probes = append(s.probes, p)
			}
		}
		if v, ok := l[labelWakeCheckEvery]; ok {
			// The most frequent check any member asks for wins, but never below the floor: each
			// check is a connection to someone else's database or queue.
			d := max(parseDur(v, defaultCheckEvery), minCheckEvery)
			if s.checkEvery == 0 || d < s.checkEvery {
				s.checkEvery = d
			}
		}
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
	tcpRoutes := map[int]tcpRoute{}
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
		g.wakeEvery, g.channels = s.wakeEvery, dedupe(s.channels)
		if s.checkEvery == 0 {
			s.checkEvery = defaultCheckEvery
		}
		g.mu.Lock()
		g.probes, g.checkEvery = s.probes, s.checkEvery // under g.mu: the probe ticker reads them
		if s.running == 0 && g.sleptAt.IsZero() {
			g.sleptAt = now // asleep when woc first sees it: wake_every counts from here
		}
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
			for _, t := range m.tcp {
				if prev, dup := tcpRoutes[t.listen]; dup {
					e.log.Warn("duplicate woc.tcp port, keeping the first", "port", t.listen, "group", prev.group.name, "ignored", name)
					continue
				}
				tcpRoutes[t.listen] = tcpRoute{group: g, member: m, target: t.target}
			}
		}
	}
	e.groups, e.routes, e.tcpRoutes = groups, routes, tcpRoutes
	return nil
}

// refresh is one sync cycle: labels and states from Docker, restored holds, TCP listeners.
func (e *engine) refresh(ctx context.Context) error {
	err := e.sync(ctx)
	e.applyPendingHolds()
	e.syncListeners(ctx)
	return err
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
	// Tier by tier: a database before the API that needs it, the API before the web app. Each
	// tier is started together and must be ready before the next one starts.
	started := 0
	for _, tier := range startTiers(g.members) {
		for _, m := range tier {
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
		for _, m := range tier {
			if err := e.waitReady(ctx, m); err != nil {
				return err
			}
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
		switch {
		case len(m.tcp) > 0 && m.readyPG:
			// Postgres speaks for itself, every 25 ms, instead of the healthcheck's start_interval.
			if st, err := e.d.inspect(ctx, m.name); err == nil && st.Running {
				ok = true
				for _, t := range m.tcp {
					ok = ok && pgReady(ctx, e.tcpAddr(m, t.target))
				}
			}
		case len(m.tcp) > 0:
			// TCP mode: a database opens its port before it accepts queries ("the database system
			// is starting up"), so wait for its healthcheck too when it has one.
			if st, err := e.d.inspect(ctx, m.name); err == nil && st.ready() {
				ok = true
				for _, t := range m.tcp {
					ok = ok && dialOK(ctx, e.tcpAddr(m, t.target))
				}
			}
		case m.port > 0 && !m.waitHealthy:
			ok = dialOK(ctx, e.addr(m))
		default:
			if st, err := e.d.inspect(ctx, m.name); err == nil {
				ok = st.ready() && (m.port == 0 || dialOK(ctx, e.addr(m)))
			}
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
	for _, g := range e.snapshot() {
		e.reapOne(ctx, g)
	}
	e.wakePeriodic()
}

func (e *engine) reapOne(ctx context.Context, g *group) {
	if !g.op.TryLock() { // a wake is in progress: by definition not idle
		return
	}
	defer g.op.Unlock()
	if !e.idleNow(g) {
		return
	}
	// A worker that says it is busy (woc.busy_url answering 200) is not stopped. Its clock is
	// left alone, so it sleeps on the first reap after the work ends instead of a full idle
	// window later.
	for _, m := range g.members {
		if m.busyURL != "" && e.busy(ctx, m.busyURL) {
			e.log.Debug("busy, not stopping", "group", g.name, "container", m.name)
			return
		}
	}
	now := e.now()
	g.mu.Lock()
	idleFor := now.Sub(g.lastSeen)
	if !g.running || g.inflight > 0 || g.held(now) || idleFor < g.idle { // a request arrived during the checks
		g.mu.Unlock()
		return
	}
	// From here new requests take the slow path and queue on g.op until the stop finishes.
	g.ready, g.running, g.sleptAt = false, false, now
	g.mu.Unlock()
	for _, m := range g.members {
		if err := e.d.stop(ctx, m.name, e.grace); err != nil {
			e.log.Error("stop failed", "group", g.name, "container", m.name, "err", err)
		}
	}
	e.log.Info("slept", "group", g.name, "idle", idleFor.Round(time.Second).String())
}

func (e *engine) idleNow(g *group) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := e.now()
	return g.running && g.inflight == 0 && !g.held(now) && now.Sub(g.lastSeen) >= g.idle
}

// busy asks a member whether it is in the middle of work: 200 means yes. Anything else, a
// timeout or no answer at all means no: a worker that can't answer is not protected from sleep.
func (e *engine) busy(ctx context.Context, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// resolveBusyURL fills in the container name when the label leaves the host out
// (http://:4100/busy → http://worker:4100/busy).
func resolveBusyURL(raw, container string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("scheme must be http or https")
	}
	if u.Hostname() == "" {
		u.Host = net.JoinHostPort(container, u.Port())
		if u.Port() == "" {
			u.Host = container
		}
	}
	return u.String(), nil
}

// wakeAsync starts a group in the background (wake endpoint, NOTIFY, wake_every).
func (e *engine) wakeAsync(g *group, reason string) {
	go func() {
		if err := e.wake(context.Background(), g); err != nil {
			e.log.Error("wake", "group", g.name, "reason", reason, "err", err)
			return
		}
		e.log.Info("woken", "group", g.name, "reason", reason)
	}()
}

// wakePeriodic wakes groups with woc.wake_every that have been asleep that long, so work that
// arrived without anyone telling woc still gets picked up.
func (e *engine) wakePeriodic() {
	now := e.now()
	for _, g := range e.snapshot() {
		if g.wakeEvery == 0 {
			continue
		}
		g.mu.Lock()
		due := !g.running && !g.sleptAt.IsZero() && now.Sub(g.sleptAt) >= g.wakeEvery
		if due {
			g.sleptAt = now // one attempt per interval, even if the wake fails
		}
		g.mu.Unlock()
		if due {
			e.wakeAsync(g, "wake_every")
		}
	}
}

func (e *engine) snapshot() []*group {
	e.mu.RLock()
	defer e.mu.RUnlock()
	gs := make([]*group, 0, len(e.groups))
	for _, g := range e.groups {
		gs = append(gs, g)
	}
	return gs
}

func (e *engine) groupByName(name string) (*group, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	g, ok := e.groups[name]
	return g, ok
}

// validChannel accepts plain Postgres identifiers only, so a label can't smuggle SQL into LISTEN.
func validChannel(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		if !(r == '_' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

type groupStatus struct {
	Group     string   `json:"group"`
	Members   []string `json:"members"`
	Hosts     []string `json:"hosts"`
	Running   bool     `json:"running"`
	Ready     bool     `json:"ready"`
	Inflight  int      `json:"inflight"`
	Idle      string   `json:"idle"`
	IdleFor   string   `json:"idle_for"`
	SleepsIn  string   `json:"sleeps_in,omitempty"`
	HeldUntil string   `json:"held_until,omitempty"`
	WokeAt    string   `json:"woke_at,omitempty"`
	Probe     string   `json:"probe,omitempty"` // last woc.wake_when check: work, none or error: ...
	ProbeAt   string   `json:"probe_at,omitempty"`
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
		if g.held(now) {
			s.HeldUntil = g.holdUntil.UTC().Format(time.RFC3339)
		}
		if !g.wokeAt.IsZero() {
			s.WokeAt = g.wokeAt.UTC().Format(time.RFC3339)
		}
		if !g.probe.at.IsZero() {
			s.Probe, s.ProbeAt = g.probe.result, g.probe.at.UTC().Format(time.RFC3339)
		}
		g.mu.Unlock()
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out
}

// startTiers orders a group's members by their start dependencies (compose depends_on,
// woc.after): tier 0 depends on nothing in the group, tier 1 only on tier 0, and so on.
// Dependencies outside the group are ignored, and so is a cycle: whatever is left over starts in
// a last tier together.
func startTiers(members []member) [][]member {
	key := func(m member) []string {
		if m.service != "" {
			return []string{m.service, m.name}
		}
		return []string{m.name}
	}
	inGroup := map[string]int{}
	for i, m := range members {
		for _, k := range key(m) {
			inGroup[k] = i
		}
	}
	deps := make([]map[int]bool, len(members))
	for i, m := range members {
		deps[i] = map[int]bool{}
		for _, a := range m.after {
			if j, ok := inGroup[a]; ok && j != i {
				deps[i][j] = true
			}
		}
	}
	placed := map[int]bool{}
	var tiers [][]member
	for len(placed) < len(members) {
		var tier []int
		for i := range members {
			if placed[i] {
				continue
			}
			ready := true
			for j := range deps[i] {
				if !placed[j] {
					ready = false
					break
				}
			}
			if ready {
				tier = append(tier, i)
			}
		}
		if len(tier) == 0 { // a cycle: start the rest together
			for i := range members {
				if !placed[i] {
					tier = append(tier, i)
				}
			}
		}
		t := make([]member, 0, len(tier))
		for _, i := range tier {
			placed[i] = true
			t = append(t, members[i])
		}
		tiers = append(tiers, t)
	}
	return tiers
}
