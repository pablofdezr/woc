package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

// A WOC restart resumes each running app's idle clock from the state file instead of giving it a
// fresh full window: an app idle for 20 of its 30 minutes sleeps 10 minutes after the restart.
func TestRestartKeepsIdleClock(t *testing.T) {
	h := newHarness(t, false, nil)
	state := filepath.Join(t.TempDir(), "state.json")
	h.e.stateFile = state
	h.get("/", "") // wakes it; last activity now
	h.clk.add(20 * time.Minute)
	h.e.reap(context.Background()) // not idle yet; saves the clocks

	e2 := newEngine(h.e.d, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Minute)
	e2.now, e2.addr, e2.stateFile = h.clk.now, h.e.addr, state
	e2.loadState()
	if err := e2.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.clk.add(11 * time.Minute) // 31 min since the last request, 11 since the restart
	e2.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("the restart reset the idle clock: not stopped 31 min after the last request")
	}
}

// Without a state file entry (first sight), an app started from outside still gets a full window.
func TestStartedFromOutsideWithoutStateGetsFullWindow(t *testing.T) {
	h := newHarness(t, true, nil)
	h.clk.add(29 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("stopped before its first idle window")
	}
}

// robots.txt and sitemap.xml are served but don't keep an app awake, and an app woken only to
// answer one goes back to sleep after a minute instead of a full idle window.
func TestQuietPathsDontKeepItAwake(t *testing.T) {
	h := newHarness(t, false, nil)
	h.get("/", "")
	h.clk.add(20 * time.Minute)
	if rec := h.get("/robots.txt", ""); rec.Code != 200 {
		t.Fatalf("robots.txt: %d", rec.Code)
	}
	h.clk.add(11 * time.Minute) // 31 min since the real request, 11 since robots.txt
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("robots.txt kept the app awake")
	}

	// Asleep: a crawler's sitemap.xml wakes it to answer, then it sleeps a minute later.
	if rec := h.get("/sitemap.xml", ""); rec.Code != 200 {
		t.Fatalf("sitemap.xml while asleep: %d", rec.Code)
	}
	if s, _ := h.fd.counts(); s != 2 {
		t.Fatalf("starts=%d, want 2 (woken to answer)", s)
	}
	h.clk.add(30 * time.Second)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 1 {
		t.Fatal("stopped before the quiet grace")
	}
	h.clk.add(31 * time.Second)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 2 {
		t.Fatal("an app woken only by sitemap.xml stayed up past the grace")
	}

	// A real visit still counts as usual.
	h.get("/", "")
	h.clk.add(2 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 2 {
		t.Fatal("a real visit was cut short")
	}
}

func TestQuietPathsLabel(t *testing.T) {
	h := newHarness(t, false, map[string]string{labelQuietPaths: "none"})
	h.get("/", "")
	h.clk.add(20 * time.Minute)
	h.get("/robots.txt", "") // counts: quiet paths turned off
	h.clk.add(11 * time.Minute)
	h.e.reap(context.Background())
	if _, st := h.fd.counts(); st != 0 {
		t.Fatal("woc.quiet_paths=none still ignored robots.txt")
	}
}
