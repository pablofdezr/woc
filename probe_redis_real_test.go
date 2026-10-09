package main

import (
	"context"
	"os"
	"testing"
)

// Against a real Redis: WOC_TEST_REDIS_URL=redis://:pass@host:6379/0 and an empty list
// woc_test_jobs. The test checks an empty list reads as no work; the caller then LPUSHes a job
// and runs it again with WOC_TEST_REDIS_EXPECT=work.
func TestRedisProbeReal(t *testing.T) {
	u := os.Getenv("WOC_TEST_REDIS_URL")
	if u == "" {
		t.Skip("WOC_TEST_REDIS_URL not set")
	}
	p, err := parseProbe(u + "#woc_test_jobs")
	if err != nil {
		t.Fatal(err)
	}
	work, err := redisProbe(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if want := os.Getenv("WOC_TEST_REDIS_EXPECT") == "work"; work != want {
		t.Fatalf("work=%v, want %v", work, want)
	}
}
