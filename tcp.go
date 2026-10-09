package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TCP mode, for services apps reach directly instead of through Traefik: Postgres, Meilisearch.
// A container with woc.tcp: "6432:5432" makes WOC listen on its own port 6432 and forward to the
// container's 5432. Apps connect to woc:6432; the first connection to a sleeping container wakes
// it and simply waits, then gets forwarded. An open connection counts as activity, like a
// websocket, and WOC never cuts one: the container sleeps once every connection has closed and
// woc.idle has passed, so connection pools must close idle connections before woc.idle.
//
// Clients can't keep using the container's own name: a stopped container drops out of Docker's
// DNS, and giving WOC the same alias would make DNS split connections between the two.

const labelTCP = "woc.tcp" // "listen:target", comma-separated, e.g. "6432:5432"

type tcpMap struct{ listen, target int }

type tcpRoute struct {
	group  *group
	member member
	target int
}

func parseTCP(s string) ([]tcpMap, error) {
	var out []tcpMap
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		l, t, ok := strings.Cut(part, ":")
		lp, err1 := strconv.Atoi(l)
		tp, err2 := strconv.Atoi(t)
		if !ok || err1 != nil || err2 != nil || lp <= 0 || lp > 65535 || tp <= 0 || tp > 65535 {
			return nil, fmt.Errorf("%q: want listen:target, e.g. 6432:5432", part)
		}
		out = append(out, tcpMap{lp, tp})
	}
	return out, nil
}

// tcpListeners keeps one listener per woc.tcp listen port, opening and closing them as labels
// come and go.
type tcpListeners struct {
	mu sync.Mutex
	ls map[int]net.Listener
}

// syncListeners opens listeners for new ports and closes the ones no label wants any more.
func (e *engine) syncListeners(ctx context.Context) {
	e.mu.RLock()
	want := map[int]bool{}
	for p := range e.tcpRoutes {
		want[p] = true
	}
	e.mu.RUnlock()

	e.tcp.mu.Lock()
	defer e.tcp.mu.Unlock()
	if e.tcp.ls == nil {
		e.tcp.ls = map[int]net.Listener{}
	}
	for p, l := range e.tcp.ls {
		if !want[p] {
			l.Close()
			delete(e.tcp.ls, p)
			e.log.Info("tcp listener closed", "port", p)
		}
	}
	for p := range want {
		if _, ok := e.tcp.ls[p]; ok {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort(e.tcpBind, strconv.Itoa(p)))
		if err != nil {
			e.log.Error("tcp listen", "port", p, "err", err)
			continue
		}
		e.tcp.ls[p] = l
		e.log.Info("tcp listener open", "port", p)
		go e.acceptTCP(ctx, l, p)
	}
}

func (e *engine) closeListeners() {
	e.tcp.mu.Lock()
	defer e.tcp.mu.Unlock()
	for p, l := range e.tcp.ls {
		l.Close()
		delete(e.tcp.ls, p)
	}
}

func (e *engine) acceptTCP(ctx context.Context, l net.Listener, port int) {
	for {
		c, err := l.Accept()
		if err != nil {
			return // closed
		}
		go e.serveTCP(ctx, c, port)
	}
}

func (e *engine) serveTCP(ctx context.Context, client net.Conn, port int) {
	defer client.Close()
	e.mu.RLock()
	rt, ok := e.tcpRoutes[port]
	e.mu.RUnlock()
	if !ok {
		return
	}
	g := rt.group
	if !g.acquire(e.now()) {
		// The client just waits while the container starts; its own connect timeout applies.
		if err := e.wake(ctx, g); err != nil {
			e.log.Error("wake", "group", g.name, "reason", "tcp "+strconv.Itoa(port), "err", err)
			return
		}
		if !g.acquire(e.now()) {
			return
		}
	}
	defer func() { g.release(e.now()) }()

	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	upstream, err := (&net.Dialer{}).DialContext(dctx, "tcp", e.tcpAddr(rt.member, rt.target))
	cancel()
	if err != nil {
		g.markDown()
		e.log.Warn("tcp dial", "group", g.name, "port", port, "err", err)
		return
	}
	defer upstream.Close()

	// Pipe both ways; when either side closes, close the other.
	done := make(chan struct{}, 2)
	go func() { io.Copy(upstream, client); closeWrite(upstream); done <- struct{}{} }()
	go func() { io.Copy(client, upstream); closeWrite(client); done <- struct{}{} }()
	<-done
	<-done
}

func closeWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.CloseWrite()
		return
	}
	c.Close()
}
