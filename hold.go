package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Holds keep a group awake for a while without traffic: a backup that runs `docker exec … pg_dump`
// or pgBackRest against a database WOC would otherwise put to sleep. A hold wakes the group, and
// until it expires the group is never stopped. Every hold expires on its own, so a backup that
// dies without releasing it costs at most its duration. Holds are worth keeping across a WOC
// restart (a long backup must not lose its database), and so is each running group's last
// activity (a WOC deploy must not give every app a fresh idle window), so both are written to a
// small JSON file when WOC_STATE_FILE is set.

// hold sets (or extends, or with d == 0 releases) a group's hold, and persists the holds.
func (e *engine) hold(g *group, d time.Duration) {
	g.mu.Lock()
	if d <= 0 {
		// Releasing is not activity: the group sleeps on its last real traffic, so a database
		// woken only for a backup goes back to sleep right after it.
		g.holdUntil = time.Time{}
	} else {
		g.holdUntil = e.now().Add(d)
		g.lastSeen = e.now()
	}
	g.mu.Unlock()
	e.saveState()
}

func (g *group) held(now time.Time) bool {
	return !g.holdUntil.IsZero() && now.Before(g.holdUntil)
}

func (e *engine) saveState() {
	if e.stateFile == "" {
		return
	}
	holds, seen := map[string]time.Time{}, map[string]time.Time{}
	now := e.now()
	for _, g := range e.snapshot() {
		g.mu.Lock()
		if g.held(now) {
			holds[g.name] = g.holdUntil
		}
		if g.running {
			seen[g.name] = g.lastSeen
		}
		g.mu.Unlock()
	}
	b, _ := json.MarshalIndent(map[string]any{"holds": holds, "last_seen": seen}, "", "  ")
	tmp := e.stateFile + ".tmp"
	if err := os.MkdirAll(filepath.Dir(e.stateFile), 0o755); err == nil {
		if err := os.WriteFile(tmp, b, 0o600); err == nil {
			_ = os.Rename(tmp, e.stateFile)
			return
		}
	}
	e.log.Warn("could not save state", "file", e.stateFile)
}

// loadState restores holds after a restart; groups appear on the first sync, so they are kept
// aside until then.
func (e *engine) loadState() {
	if e.stateFile == "" {
		return
	}
	b, err := os.ReadFile(e.stateFile)
	if err != nil {
		return
	}
	var st struct {
		Holds    map[string]time.Time `json:"holds"`
		LastSeen map[string]time.Time `json:"last_seen"`
	}
	if json.Unmarshal(b, &st) == nil {
		e.pendingHolds, e.pendingSeen = st.Holds, st.LastSeen
	}
}

// applyPendingHolds gives restored holds to their groups once sync has created them.
func (e *engine) applyPendingHolds() {
	if len(e.pendingHolds) == 0 {
		return
	}
	now := e.now()
	for name, until := range e.pendingHolds {
		if g, ok := e.groupByName(name); ok {
			if now.Before(until) {
				g.mu.Lock()
				g.holdUntil = until
				g.mu.Unlock()
				e.log.Info("hold restored", "group", name, "until", until.UTC().Format(time.RFC3339))
			}
			delete(e.pendingHolds, name)
		}
	}
}

// runHoldCommand implements `woc hold <group> <duration>` and `woc release <group>`: they call
// the admin API of the woc running in the same container, so a host cron job can do
// `docker exec woc /woc hold db 3h` before a backup.
func runHoldCommand(args []string) int {
	if len(args) < 2 || (args[0] == "hold" && len(args) < 3) {
		fmt.Fprintln(os.Stderr, "usage: woc hold <group> <duration> | woc release <group>")
		return 2
	}
	path := "/hold/" + args[1]
	method := http.MethodPost
	if args[0] == "hold" {
		if _, err := time.ParseDuration(args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "invalid duration:", args[2])
			return 2
		}
		path += "?for=" + args[2]
	} else {
		method = http.MethodDelete
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // a hold waits for the wake
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, method, "http://127.0.0.1"+env("WOC_ADMIN_LISTEN", ":8081")+path, nil)
	if t := os.Getenv("WOC_ADMIN_TOKEN"); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "woc:", resp.Status)
		return 1
	}
	return 0
}
