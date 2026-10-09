package main

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestHoldKeepsAwakeAndSurvivesRestart(t *testing.T) {
	h := newHarness(t, false, nil)
	state := filepath.Join(t.TempDir(), "state.json")
	h.e.stateFile = state
	cfg := loadConfig()
	admin := h.e.adminHandler(cfg)
	call := func(method, path string) int {
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		return rec.Code
	}
	if c := call(http.MethodPost, "/hold/web?for=2h"); c != http.StatusOK {
		t.Fatalf("hold: %d", c)
	}
	if s, _ := h.fd.counts(); s != 1 {
		t.Fatal("hold did not wake the group")
	}
	h.clk.add(90 * time.Minute) // far past the 30m idle, inside the hold
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped while held")
	}

	// A new engine (WOC restarted) reads the hold back from the state file.
	e2 := newEngine(h.e.d, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Minute)
	e2.now, e2.addr, e2.stateFile = h.clk.now, h.e.addr, state
	e2.loadHolds()
	if err := e2.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	g, _ := e2.groupByName("web")
	if !g.held(h.clk.now()) {
		t.Fatal("hold lost across a restart")
	}

	if c := call(http.MethodDelete, "/hold/web"); c != http.StatusOK {
		t.Fatalf("release: %d", c)
	}
	h.e.reap(context.Background()) // released and idle for 90 min: sleeps now
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("not stopped after release")
	}
	if c := call(http.MethodPost, "/hold/web?for=48h"); c != http.StatusBadRequest {
		t.Fatalf("a 48h hold must be refused: %d", c)
	}
}

func TestHoldExpires(t *testing.T) {
	h := newHarness(t, true, nil)
	g, _ := h.e.groupByName("web")
	h.e.hold(g, time.Hour)
	h.clk.add(61 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("not stopped after the hold expired")
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestTCPModeWakesOnConnectAndHoldsWhileOpen(t *testing.T) {
	// The "database": a TCP echo server.
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backend.Close() })
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	target := backend.Addr().(*net.TCPAddr).Port
	listen := freePort(t)

	h := newHarness(t, true, nil) // the web container, irrelevant here
	h.fd.mu.Lock()
	h.fd.cs["db"] = &fakeContainer{hasHealth: true, labels: map[string]string{
		labelEnable: "true", labelTCP: strconv.Itoa(listen) + ":" + strconv.Itoa(target), labelIdle: "30m"}}
	h.fd.mu.Unlock()
	h.e.tcpBind = "127.0.0.1"
	h.e.tcpAddr = func(_ member, port int) string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); h.e.closeListeners() })
	if err := h.e.refresh(ctx); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(listen)))
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || line != "ping\n" {
		t.Fatalf("echo through woc: %q %v", line, err)
	}
	if s, _ := h.fd.counts(); s != 1 {
		t.Fatalf("starts=%d: the connection should have woken db", s)
	}

	h.clk.add(time.Hour)
	h.e.reapOne(context.Background(), mustGroup(t, h.e, "db"))
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped db with a connection open")
	}
	conn.Close()
	time.Sleep(100 * time.Millisecond) // the pipe notices the close
	h.clk.add(31 * time.Minute)
	h.e.reapOne(context.Background(), mustGroup(t, h.e, "db"))
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("db not stopped after its last connection closed and idle passed")
	}
}

func mustGroup(t *testing.T, e *engine, name string) *group {
	g, ok := e.groupByName(name)
	if !ok {
		t.Fatalf("no group %s", name)
	}
	return g
}

func TestParseTCP(t *testing.T) {
	if m, err := parseTCP("6432:5432, 7701:7700"); err != nil || len(m) != 2 || m[1] != (tcpMap{7701, 7700}) {
		t.Fatalf("%v %v", m, err)
	}
	for _, bad := range []string{"5432", "a:b", "0:5432", "70000:1"} {
		if _, err := parseTCP(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
