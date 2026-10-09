package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"
)

// pgReady reports whether Postgres at addr accepts connections, at the protocol level: it sends
// a startup message and reads the first reply. An authentication request (whatever the method,
// even for a user that doesn't exist) means the server is taking connections; error 57P03
// ("the database system is starting up") means not yet. It never authenticates: it hangs up
// right after the first message, so it needs no credentials.
//
// woc.ready: postgres uses it instead of the container's healthcheck, which only re-runs every
// start_interval: on srv-01 Postgres accepted queries ~130 ms after `docker start` and Docker
// reported it healthy ~550 ms after.
func pgReady(ctx context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		c.SetDeadline(dl)
	}

	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(196608)) // protocol 3.0
	for _, s := range []string{"user", "woc", "database", "postgres", "application_name", "woc-probe"} {
		body.WriteString(s)
		body.WriteByte(0)
	}
	body.WriteByte(0)
	msg := make([]byte, 4, 4+body.Len())
	binary.BigEndian.PutUint32(msg, uint32(4+body.Len()))
	if _, err := c.Write(append(msg, body.Bytes()...)); err != nil {
		return false
	}

	r := bufio.NewReader(c)
	typ, err := r.ReadByte()
	if err != nil {
		return false
	}
	switch typ {
	case 'R': // an authentication request: the server is accepting connections
		return true
	case 'E':
		var n int32
		if binary.Read(r, binary.BigEndian, &n) != nil || n < 4 || n > 1<<16 {
			return false
		}
		fields := make([]byte, n-4)
		if _, err := io.ReadFull(r, fields); err != nil {
			return false
		}
		// Error fields are a code byte and a NUL-terminated string; 'C' is the SQLSTATE.
		for len(fields) > 1 {
			code := fields[0]
			end := bytes.IndexByte(fields[1:], 0)
			if end < 0 {
				break
			}
			if code == 'C' {
				return string(fields[1:1+end]) != "57P03" // starting up, shutting down, in recovery
			}
			fields = fields[2+end:]
		}
		return true // some other refusal (bad database, too many clients): it is up
	}
	return false
}
