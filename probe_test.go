package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseProbe(t *testing.T) {
	good := map[string]probe{
		"sql:SELECT count(*) FROM jobs":        {kind: probeSQL, sql: "SELECT count(*) FROM jobs"},
		"redis://redis:6379#bull:emails:wait":  {kind: probeRedis, addr: "redis:6379", key: "bull:emails:wait"},
		"redis://redis#q":                      {kind: probeRedis, addr: "redis:6379", key: "q"},
		"redis://:s3cret@redis:6380/2#q":       {kind: probeRedis, addr: "redis:6380", pass: "s3cret", db: 2, key: "q"},
		"redis://worker:s3cret@redis:6379/0#q": {kind: probeRedis, addr: "redis:6379", user: "worker", pass: "s3cret", key: "q"},
		"http://api:3000/pending":              {kind: probeHTTP, url: "http://api:3000/pending"},
		"https://api.example.com/pending?q=1":  {kind: probeHTTP, url: "https://api.example.com/pending?q=1"},
	}
	for raw, want := range good {
		got, err := parseProbe(raw)
		want.raw = raw
		if err != nil || got != want {
			t.Errorf("%s → %+v %v, want %+v", raw, got, err, want)
		}
	}
	for _, raw := range []string{
		"sql:", "sql:   ",
		"redis://redis:6379",     // no key
		"redis://redis:6379/x#q", // db not a number
		"redis://#q",             // no host
		"rediss://redis:6379#q",  // TLS: not supported
		"http:///pending",        // no host
		"ftp://x/y", "jobs", "", "postgres://db/x",
	} {
		if _, err := parseProbe(raw); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
	p, _ := parseProbe("redis://:s3cret@redis:6379#q")
	if strings.Contains(p.String(), "s3cret") {
		t.Errorf("password in %s", p)
	}
}

// fakeRedis speaks just enough RESP: AUTH, SELECT and LLEN over lists kept per database.
type fakeRedis struct {
	pass  string
	mu    sync.Mutex
	lists map[int]map[string]int // db → key → length
	zsets map[string][]int64     // key → scores (db 0)
	cmds  []string
}

func (f *fakeRedis) serve(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.handle(c)
		}
	}()
	return ln.Addr().String()
}

func (f *fakeRedis) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	db, authed := 0, f.pass == ""
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.cmds = append(f.cmds, args[0])
		f.mu.Unlock()
		switch strings.ToUpper(args[0]) {
		case "AUTH":
			if args[len(args)-1] != f.pass {
				fmt.Fprint(c, "-WRONGPASS invalid username-password pair\r\n")
				continue
			}
			authed = true
			fmt.Fprint(c, "+OK\r\n")
		case "SELECT":
			db, _ = strconv.Atoi(args[1])
			fmt.Fprint(c, "+OK\r\n")
		case "LLEN":
			if !authed {
				fmt.Fprint(c, "-NOAUTH Authentication required.\r\n")
				continue
			}
			f.mu.Lock()
			n := f.lists[db][args[1]]
			f.mu.Unlock()
			fmt.Fprintf(c, ":%d\r\n", n)
		case "ZCOUNT":
			if !authed {
				fmt.Fprint(c, "-NOAUTH Authentication required.\r\n")
				continue
			}
			max, _ := strconv.ParseInt(args[3], 10, 64)
			n := 0
			f.mu.Lock()
			for _, sc := range f.zsets[args[1]] {
				if sc <= max {
					n++
				}
			}
			f.mu.Unlock()
			fmt.Fprintf(c, ":%d\r\n", n)
		default:
			fmt.Fprint(c, "-ERR unknown command\r\n")
		}
	}
}

func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil || n < 1 {
		return nil, errors.New("bad command")
	}
	args := make([]string, n)
	for i := range args {
		if _, err := r.ReadString('\n'); err != nil { // $len
			return nil, err
		}
		v, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args[i] = strings.TrimSuffix(v, "\r\n")
	}
	return args, nil
}

func TestRedisProbe(t *testing.T) {
	f := &fakeRedis{pass: "s3cret", lists: map[int]map[string]int{0: {"empty": 0}, 2: {"bull:jobs:wait": 3}}}
	addr := f.serve(t)
	ctx := context.Background()
	check := func(raw string) (bool, error) {
		p, err := parseProbe(raw)
		if err != nil {
			t.Fatal(err)
		}
		return redisProbe(ctx, p)
	}
	if work, err := check("redis://:s3cret@" + addr + "/2#bull:jobs:wait"); err != nil || !work {
		t.Fatalf("db 2 list of 3: %v %v", work, err)
	}
	if work, err := check("redis://default:s3cret@" + addr + "#empty"); err != nil || work {
		t.Fatalf("empty list: %v %v", work, err)
	}
	if work, err := check("redis://" + addr + "/2#bull:jobs:wait"); err == nil || work {
		t.Fatalf("no password: %v %v", work, err)
	}
	if _, err := check("redis://:wrong@" + addr + "#empty"); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("wrong password: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Join(f.cmds[:3], " ") != "AUTH SELECT LLEN" {
		t.Fatalf("commands %v", f.cmds)
	}
}

// ?due: a sorted set of times. Work once one of them has come, none while all are ahead.
func TestRedisDueProbe(t *testing.T) {
	now := time.Now().UnixMilli()
	f := &fakeRedis{zsets: map[string][]int64{
		"app:wake_at":  {now + 3_600_000, now - 1000}, // one due a second ago
		"app:later":    {now + 60_000},
		"app:empty_at": nil,
	}}
	addr := f.serve(t)
	for key, want := range map[string]bool{"app:wake_at": true, "app:later": false, "app:empty_at": false} {
		p, err := parseProbe("redis://" + addr + "?due#" + key)
		if err != nil {
			t.Fatal(err)
		}
		if work, err := redisProbe(context.Background(), p); err != nil || work != want {
			t.Fatalf("%s: work=%v err=%v, want %v", key, work, err, want)
		}
	}
	if _, err := parseProbe("redis://" + addr + "?soon#k"); err == nil {
		t.Fatal("unknown option accepted")
	}
	p, _ := parseProbe("redis://:pw@" + addr + "?due#app:wake_at")
	if s := p.String(); strings.Contains(s, "pw") || !strings.Contains(s, "?due#app:wake_at") {
		t.Fatalf("String() = %q", s)
	}
}

func TestHTTPProbe(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusOK)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(code.Load()))
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()
	if work, err := httpProbe(ctx, srv.URL); err != nil || !work {
		t.Fatalf("200: %v %v", work, err)
	}
	code.Store(http.StatusNoContent)
	if work, err := httpProbe(ctx, srv.URL); err != nil || work {
		t.Fatalf("204: %v %v", work, err)
	}
	srv.Close()
	if work, err := httpProbe(ctx, srv.URL); err == nil || work {
		t.Fatalf("down: %v %v", work, err)
	}
}

// probeHarness: the harness app with an HTTP work probe answering status code.
func probeHarness(t *testing.T, running bool, code *atomic.Int32) (*harness, *atomic.Int32) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(int(code.Load()))
	}))
	t.Cleanup(srv.Close)
	h := newHarness(t, running, map[string]string{labelWakeWhen: srv.URL + "/pending", labelWakeCheckEvery: "10s"})
	return h, &calls
}

func waitFor(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestProbeWakesAsleepGroup(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusOK)
	h, calls := probeHarness(t, false, &code)
	ctx := context.Background()
	h.e.probeDue(ctx)
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("checked before wake_check_every")
	}
	h.clk.add(11 * time.Second)
	h.e.probeDue(ctx)
	if !waitFor(t, func() bool { s, _ := h.fd.counts(); return s == 1 }) {
		t.Fatal("work reported but the group was not woken")
	}
	if st := h.e.status()[0]; st.Probe != "work" || st.ProbeAt == "" {
		t.Fatalf("status %+v", st)
	}
}

func TestProbeNoWorkStaysAsleep(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusNoContent)
	h, calls := probeHarness(t, false, &code)
	ctx := context.Background()
	h.clk.add(11 * time.Second)
	h.e.probeDue(ctx)
	if !waitFor(t, func() bool { return h.e.status()[0].Probe == "none" }) {
		t.Fatal("probe never ran")
	}
	h.e.probeDue(ctx) // just checked: not due again until another interval passes
	time.Sleep(50 * time.Millisecond)
	if s, _ := h.fd.counts(); s != 0 || calls.Load() != 1 {
		t.Fatalf("starts=%d calls=%d, want 0 and 1", s, calls.Load())
	}
	h.clk.add(11 * time.Second)
	h.e.probeDue(ctx)
	if !waitFor(t, func() bool { return calls.Load() == 2 }) {
		t.Fatal("not checked again after another interval")
	}
}

func TestProbeNotCalledWhileRunning(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusOK)
	h, calls := probeHarness(t, true, &code)
	for range 3 {
		h.clk.add(time.Minute)
		h.e.probeDue(context.Background())
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("probe called %d times on a running group", calls.Load())
	}
}

func TestProbeLabelsValidated(t *testing.T) {
	// sql: without WOC_PG_URL, and a bad URL, are ignored; the check interval has a floor.
	h := newHarness(t, false, map[string]string{labelWakeWhen: "sql:SELECT 1", labelWakeCheckEvery: "1s"})
	g, _ := h.e.groupByName("web")
	if len(g.probes) != 0 || g.checkEvery != minCheckEvery {
		t.Fatalf("probes=%v every=%s", g.probes, g.checkEvery)
	}
	h.e.pgURL = "postgres://x"
	if err := h.e.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g, _ := h.e.groupByName("web"); len(g.probes) != 1 {
		t.Fatalf("sql probe with WOC_PG_URL: %v", g.probes)
	}
}

// Needs a real Postgres: WOC_TEST_PG_URL=postgres://... go test -run SQLProbe (CI sets it).
func TestSQLProbe(t *testing.T) {
	dsn := os.Getenv("WOC_TEST_PG_URL")
	if dsn == "" {
		t.Skip("WOC_TEST_PG_URL not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	// A real table, not a TEMP one: the probe opens its own connection.
	table := fmt.Sprintf("woc_probe_test_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (id int)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Exec(context.Background(), "DROP TABLE "+table) })

	probeIs := func(query string, want bool) {
		t.Helper()
		if work, err := sqlProbe(ctx, dsn, query); err != nil || work != want {
			t.Fatalf("%s: %v %v, want %v", query, work, err, want)
		}
	}
	probeIs("SELECT count(*) FROM "+table, false)
	probeIs("SELECT EXISTS (SELECT 1 FROM "+table+")", false)
	if _, err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	probeIs("SELECT count(*) FROM "+table, true)
	probeIs("SELECT EXISTS (SELECT 1 FROM "+table+")", true)
	probeIs("SELECT sum(id) FROM "+table, true) // numeric

	// Writes fail: the probe runs in a read-only transaction.
	for _, q := range []string{"INSERT INTO " + table + " VALUES (2) RETURNING 1", "DELETE FROM " + table + " RETURNING 1"} {
		_, err := sqlProbe(ctx, dsn, q)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25006" { // read_only_sql_transaction
			t.Fatalf("%s: %v, want a read-only transaction error", q, err)
		}
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 1 {
		t.Fatalf("table changed by a probe: %d rows, %v", n, err)
	}
	if _, err := sqlProbe(ctx, dsn, "SELECT 1, 2"); err == nil {
		t.Fatal("two columns accepted")
	}
	if _, err := sqlProbe(ctx, dsn, "SELECT 'yes'"); err == nil {
		t.Fatal("a string accepted")
	}
}
