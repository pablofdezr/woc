package main

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

var errChannelsChanged = errors.New("notify channels changed")

// listenPostgres wakes groups on Postgres NOTIFY: a queue that already notifies on enqueue
// (NOTIFY jobs) can wake its worker with no change to the producer. One connection LISTENs on
// every channel named in a woc.wake_on_notify label and reconnects with backoff if Postgres
// goes away. When the labels change the channel set, it reconnects at once with the new set:
// interrupting a wait can close a pgx connection, and the set rarely changes.
func (e *engine) listenPostgres(ctx context.Context, dsn string) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := e.listenOnce(ctx, dsn)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errChannelsChanged) {
			backoff = time.Second
			continue
		}
		e.log.Warn("postgres listener disconnected, retrying", "err", err, "in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
}

func (e *engine) listenOnce(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	e.log.Info("postgres listener connected")

	listening := e.notifyChannels()
	for _, ch := range listening {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
	}

	// The wait ends on a notification, on shutdown, or when the channel set changes.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-t.C:
				if !slices.Equal(e.notifyChannels(), listening) {
					cancel()
					return
				}
			}
		}
	}()
	for {
		n, err := conn.WaitForNotification(wctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if wctx.Err() != nil {
				return errChannelsChanged
			}
			return err
		}
		for _, g := range e.groupsOnChannel(n.Channel) {
			e.wakeAsync(g, "notify "+n.Channel)
		}
	}
}

// notifyChannels is every channel any group wants, sorted.
func (e *engine) notifyChannels() []string {
	seen := map[string]bool{}
	for _, g := range e.snapshot() {
		for _, ch := range g.channels {
			seen[ch] = true
		}
	}
	out := make([]string, 0, len(seen))
	for ch := range seen {
		out = append(out, ch)
	}
	sort.Strings(out)
	return out
}

func (e *engine) groupsOnChannel(ch string) []*group {
	var out []*group
	for _, g := range e.snapshot() {
		if slices.Contains(g.channels, ch) {
			out = append(out, g)
		}
	}
	return out
}
