package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strings"
	"testing"
)

// fakePG answers the first startup message with the given reply bytes.
func fakePG(t *testing.T, reply []byte) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			var n int32
			binary.Read(c, binary.BigEndian, &n)
			io.CopyN(io.Discard, c, int64(n-4))
			c.Write(reply)
			c.Close()
		}
	}()
	return l.Addr().String()
}

func pgError(sqlstate string) []byte {
	fields := "SFATAL\x00C" + sqlstate + "\x00Mthe database system is starting up\x00\x00"
	b := []byte{'E', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], uint32(4+len(fields)))
	return append(b, fields...)
}

func TestPGReady(t *testing.T) {
	ctx := context.Background()
	if !pgReady(ctx, fakePG(t, []byte{'R', 0, 0, 0, 8, 0, 0, 0, 10})) { // AuthenticationSASL
		t.Error("an auth request means ready")
	}
	if pgReady(ctx, fakePG(t, pgError("57P03"))) {
		t.Error("57P03 means still starting")
	}
	if !pgReady(ctx, fakePG(t, pgError("3D000"))) {
		t.Error("another refusal (no such database) still means it is up")
	}
	if pgReady(ctx, "127.0.0.1:1") {
		t.Error("nothing listening is not ready")
	}
}

// Against a real Postgres when WOC_TEST_PG_URL is set (CI).
func TestPGReadyReal(t *testing.T) {
	dsn := os.Getenv("WOC_TEST_PG_URL")
	if dsn == "" {
		t.Skip("WOC_TEST_PG_URL not set")
	}
	hostport := strings.Split(strings.Split(dsn, "@")[1], "/")[0]
	if !pgReady(context.Background(), hostport) {
		t.Fatal("a running Postgres reported not ready")
	}
}
