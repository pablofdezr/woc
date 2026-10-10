package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Work probes wake a sleeping group when there is work waiting for it, for queues whose
// producer can't call /wake and doesn't NOTIFY. woc.wake_when names one probe; WOC runs it every
// woc.wake_check_every while the group is asleep, and never while it runs (the worker is
// draining the queue then, and the probe would only add load).
//
//	sql:SELECT count(*) FROM jobs WHERE state = 'queued'    → Postgres at WOC_PG_URL
//	redis://:pass@redis:6379/0#bull:emails:wait             → LLEN of that list
//	redis://:pass@redis:6379/0?due#app:wake_at              → members of that sorted set whose
//	                                                          score (a time, Unix ms) has come
//	http://api:3000/pending                                 → 200 = work
const (
	labelWakeWhen       = "woc.wake_when"
	labelWakeCheckEvery = "woc.wake_check_every"

	defaultCheckEvery = 30 * time.Second
	minCheckEvery     = 5 * time.Second
)

type probeKind int

const (
	probeSQL probeKind = iota + 1
	probeRedis
	probeHTTP
)

type probe struct {
	kind probeKind
	raw  string // the label as written: identifies the probe across syncs
	sql  string
	url  string // http(s)

	// redis
	addr, user, pass string
	db               int
	key              string
	due              bool // ?due: a sorted set of times (Unix ms), work once one has come
}

// String is the probe as it can be logged: no passwords.
func (p probe) String() string {
	switch p.kind {
	case probeSQL:
		return "sql"
	case probeRedis:
		u := url.URL{Scheme: "redis", Host: p.addr, Fragment: p.key}
		if p.due {
			u.RawQuery = "due"
		}
		if p.pass != "" {
			u.User = url.UserPassword(p.user, "xxxxx")
		}
		return u.String()
	}
	if u, err := url.Parse(p.url); err == nil {
		return u.Redacted()
	}
	return "http"
}

func parseProbe(raw string) (probe, error) {
	raw = strings.TrimSpace(raw)
	p := probe{raw: raw}
	if q, ok := strings.CutPrefix(raw, "sql:"); ok {
		p.kind, p.sql = probeSQL, strings.TrimSpace(q)
		if p.sql == "" {
			return p, errors.New("sql: needs a query")
		}
		return p, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return p, errors.New("not a valid URL") // url's own error repeats the URL, password included
	}
	switch u.Scheme {
	case "http", "https":
		if u.Hostname() == "" {
			return p, errors.New("http probe needs a host")
		}
		p.kind, p.url = probeHTTP, u.String()
	case "redis":
		if u.Hostname() == "" {
			return p, errors.New("redis probe needs a host")
		}
		p.kind, p.key = probeRedis, u.Fragment
		if p.key == "" {
			return p, errors.New("redis probe needs the list key after #, e.g. redis://redis:6379#bull:jobs:wait")
		}
		port := u.Port()
		if port == "" {
			port = "6379"
		}
		p.addr = net.JoinHostPort(u.Hostname(), port)
		if u.User != nil {
			p.user = u.User.Username()
			p.pass, _ = u.User.Password()
		}
		switch u.RawQuery {
		case "":
		case "due":
			p.due = true
		default:
			return p, fmt.Errorf("redis probe: unknown option %q (the only one is ?due)", u.RawQuery)
		}
		if db := strings.Trim(u.Path, "/"); db != "" {
			n, err := strconv.Atoi(db)
			if err != nil || n < 0 {
				return p, fmt.Errorf("redis database must be a number, got %q", db)
			}
			p.db = n
		}
	default:
		return p, errors.New("want sql:<query>, redis://host:port#key or http(s)://...")
	}
	return p, nil
}

// probeState is a group's probe bookkeeping, guarded by group.mu.
type probeState struct {
	running bool      // a check is in flight: never two at once for the same group
	at      time.Time // last finished check
	result  string    // "work", "none" or "error: ..."
	failing bool      // the last check failed: logged once when it starts and once when it ends
}

// probeDue starts the checks that are due: groups asleep for at least woc.wake_check_every
// since they slept or since their last check. Each check runs in its own goroutine so a slow
// probe (up to its timeout) never holds up the others.
func (e *engine) probeDue(ctx context.Context) {
	now := e.now()
	for _, g := range e.snapshot() {
		g.mu.Lock()
		probes := g.probes
		last := g.sleptAt
		if g.probe.at.After(last) {
			last = g.probe.at
		}
		due := len(probes) > 0 && !g.running && !g.probe.running && now.Sub(last) >= g.checkEvery
		if due {
			g.probe.running = true
		}
		g.mu.Unlock()
		if !due {
			continue
		}
		// A wake (or stop) in progress holds g.op: the group is not stably asleep, skip this round.
		if !g.op.TryLock() {
			g.mu.Lock()
			g.probe.running = false
			g.mu.Unlock()
			continue
		}
		g.op.Unlock()
		go e.checkGroup(ctx, g, probes)
	}
}

// checkGroup runs the group's probes (one per member that declares one) until one reports work.
func (e *engine) checkGroup(ctx context.Context, g *group, probes []probe) {
	work, err := false, error(nil)
	var failed probe
	for _, p := range probes {
		w, perr := e.runProbe(ctx, p)
		if perr != nil {
			err, failed = perr, p
			continue
		}
		if w {
			work = true
			break
		}
	}
	result := "none"
	switch {
	case work:
		result = "work"
	case err != nil:
		result = "error: " + err.Error()
	}
	g.mu.Lock()
	wasFailing := g.probe.failing
	g.probe.running, g.probe.at, g.probe.result = false, e.now(), result
	failing := !work && err != nil
	g.probe.failing = failing
	asleep := !g.running
	g.mu.Unlock()

	switch {
	case failing && !wasFailing:
		e.log.Warn("work probe failing", "group", g.name, "probe", failed.String(), "err", err)
	case !failing && wasFailing:
		e.log.Info("work probe recovered", "group", g.name)
	}
	if work && asleep {
		e.wakeAsync(g, "wake_when")
	}
}

func (e *engine) runProbe(ctx context.Context, p probe) (bool, error) {
	switch p.kind {
	case probeSQL:
		return sqlProbe(ctx, e.pgURL, p.sql)
	case probeRedis:
		return redisProbe(ctx, p)
	case probeHTTP:
		return httpProbe(ctx, p.url)
	}
	return false, errors.New("unknown probe")
}

// sqlProbe runs the query in a read-only transaction, so a label can't write: an INSERT or
// DELETE fails with "cannot execute ... in a read-only transaction". pgx sends it through the
// extended protocol, which takes a single statement, so "SELECT 1; COMMIT; DELETE ..." can't
// step out of the transaction either. A connection per check: one every 30 s costs nothing,
// and there's no pool to go stale when Postgres restarts.
func sqlProbe(ctx context.Context, dsn, query string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	// Postgres errors (a write refused by the read-only transaction) only surface while reading,
	// so read first and judge the shape after. Two rows are enough to know it returned too many.
	var vals []any
	n := 0
	for n < 2 && rows.Next() {
		if n++; n == 1 {
			if vals, err = rows.Values(); err != nil {
				return false, err
			}
		}
	}
	cols := len(rows.FieldDescriptions())
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	switch {
	case cols != 1:
		return false, fmt.Errorf("query must return one column, got %d", cols)
	case n == 0:
		return false, errors.New("query returned no rows")
	case n > 1:
		return false, errors.New("query must return one row")
	}
	return sqlTruthy(vals[0])
}

// sqlTruthy: true, or a number above zero, means work. NULL means none (max() of nothing).
func sqlTruthy(v any) (bool, error) {
	switch x := v.(type) {
	case nil:
		return false, nil
	case bool:
		return x, nil
	case int16:
		return x > 0, nil
	case int32:
		return x > 0, nil
	case int64:
		return x > 0, nil
	case float32:
		return x > 0, nil
	case float64:
		return x > 0, nil
	case pgtype.Numeric: // numeric, e.g. sum()
		if !x.Valid || x.NaN {
			return false, nil
		}
		if x.InfinityModifier != pgtype.Finite {
			return x.InfinityModifier == pgtype.Infinity, nil
		}
		return x.Int != nil && x.Int.Cmp(big.NewInt(0)) > 0, nil
	}
	return false, fmt.Errorf("query must return a boolean or a number, got %T", v)
}

// redisProbe asks Redis for the length of a list (a BullMQ wait list, a plain LPUSH queue), or
// with ?due how many members of a sorted set have a score (a time in Unix ms) that has come: a
// sleeping worker's next cron, delayed jobs. A few lines of RESP instead of a client library:
// AUTH, SELECT and LLEN or ZCOUNT are all it needs.
func redisProbe(ctx context.Context, p probe) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return false, err
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		c.SetDeadline(dl)
	}
	r := bufio.NewReader(c)
	if p.pass != "" {
		args := []string{"AUTH", p.pass}
		if p.user != "" {
			args = []string{"AUTH", p.user, p.pass}
		}
		if _, err := respCall(c, r, args...); err != nil {
			return false, fmt.Errorf("AUTH: %w", err)
		}
	}
	if p.db != 0 {
		if _, err := respCall(c, r, "SELECT", strconv.Itoa(p.db)); err != nil {
			return false, fmt.Errorf("SELECT: %w", err)
		}
	}
	cmd := []string{"LLEN", p.key}
	if p.due {
		cmd = []string{"ZCOUNT", p.key, "-inf", strconv.FormatInt(time.Now().UnixMilli(), 10)}
	}
	v, err := respCall(c, r, cmd...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", cmd[0], err)
	}
	n, ok := v.(int64)
	if !ok {
		return false, fmt.Errorf("%s: unexpected reply %v", cmd[0], v)
	}
	return n > 0, nil
}

// respCall sends one command and reads its reply: a simple string, an integer or a bulk string
// (nil for a null bulk). An error reply comes back as an error.
func respCall(w io.Writer, r *bufio.Reader, args ...string) (any, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(w, b.String()); err != nil {
		return nil, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return nil, errors.New("empty reply")
	}
	switch body := line[1:]; line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, errors.New(body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		if n > 1<<20 {
			return nil, errors.New("reply too long")
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	}
	return nil, fmt.Errorf("unexpected reply %q", line)
}

// httpProbe: 200 means there is work. Anything else, or no answer, means none (and the error is
// reported in /status).
func httpProbe(ctx context.Context, rawURL string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // lets the connection be reused
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}
