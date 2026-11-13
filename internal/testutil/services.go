package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Integration tests run only when CFGCANARY_IT=1 and talk to the services in
// deploy/compose/test-deps.yaml (or the endpoints given in the environment).
func RequireIntegration(t testing.TB) {
	t.Helper()
	if os.Getenv("CFGCANARY_IT") != "1" {
		t.Skip("integration test: run `make test-integration` (sets CFGCANARY_IT=1 and starts PostgreSQL, S3, Prometheus, Toxiproxy)")
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// AdminDSN is the PostgreSQL URL of the test server's maintenance database.
func AdminDSN() string {
	return env("CFGCANARY_TEST_PG", "postgres://cfgcanary:cfgcanary-test@127.0.0.1:55432/cfgcanary?sslmode=disable")
}

// S3 endpoint and credentials of the test object store.
func S3() (endpoint, access, secret string) {
	return env("CFGCANARY_TEST_S3", "127.0.0.1:59000"), env("CFGCANARY_TEST_S3_ACCESS", "cfgcanary"), env("CFGCANARY_TEST_S3_SECRET", "cfgcanary-test-secret")
}

// PrometheusURL of the test Prometheus.
func PrometheusURL() string { return env("CFGCANARY_TEST_PROM", "http://127.0.0.1:59090") }

// ToxiproxyURL of the test Toxiproxy API.
func ToxiproxyURL() string { return env("CFGCANARY_TEST_TOXIPROXY", "http://127.0.0.1:58474") }

// RandomName returns a short random lowercase identifier.
func RandomName(prefix string) string {
	var b [5]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

// FreshDatabase creates an empty database for one test and returns its URL.
// It is dropped when the test ends.
func FreshDatabase(t testing.TB) string {
	t.Helper()
	RequireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, AdminDSN())
	if err != nil {
		t.Fatalf("connecting to test PostgreSQL (is `make test-deps-up` running?): %v", err)
	}
	defer admin.Close(ctx)
	name := RandomName("t_")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, AdminDSN())
		if err != nil {
			return
		}
		defer c.Close(ctx)
		_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(AdminDSN())
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// Eventually polls cond until it returns true or the timeout passes.
func Eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Must fails the test on error.
func Must(t testing.TB, err error, what ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprint(what...), err)
	}
}
