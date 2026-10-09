package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Needs a real Postgres: WOC_TEST_PG_URL=postgres://... go test -run Notify (CI sets it).
func TestNotifyWakesGroup(t *testing.T) {
	dsn := os.Getenv("WOC_TEST_PG_URL")
	if dsn == "" {
		t.Skip("WOC_TEST_PG_URL not set")
	}
	h := newHarness(t, false, map[string]string{labelWakeNotify: "woc_test_jobs"})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.e.listenPostgres(ctx, dsn)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// Keep notifying until the listener (which connects asynchronously) has heard one.
		if _, err := conn.Exec(ctx, "NOTIFY woc_test_jobs"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		if s, _ := h.fd.counts(); s >= 1 {
			return
		}
	}
	t.Fatal("NOTIFY did not wake the group")
}
