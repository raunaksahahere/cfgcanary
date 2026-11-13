package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/raunaksahahere/cfgcanary/examples/ratelimits"
	"github.com/raunaksahahere/cfgcanary/internal/agent"
	"github.com/raunaksahahere/cfgcanary/internal/cache"
	tu "github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// harness drives a real agent (Handle) writing the cache the SDK reads.
type harness struct {
	t     *testing.T
	keys  *tu.Keys
	root  string
	ag    *agent.Agent
	blobs map[string][]byte
	srv   *httptest.Server
}

func newHarness(t *testing.T) *harness {
	h := &harness{t: t, keys: tu.NewKeys(t), root: t.TempDir(), blobs: map[string][]byte{}}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(h.blobs[strings.TrimPrefix(r.URL.Path, "/")])
	}))
	t.Cleanup(h.srv.Close)
	h.newAgent()
	return h
}

func (h *harness) newAgent() {
	a, err := agent.New(agent.Config{InstanceID: "i-1", Environment: "prod", Root: h.root, Configs: []string{"ratelimits"},
		Trust: h.keys.Bundle, AllowedOrigins: []string{h.srv.URL}})
	if err != nil {
		h.t.Fatal(err)
	}
	h.ag = a
}

func (h *harness) push(version, gen int64, body string, opts ...func(*tu.UpdateSpec)) {
	sha := cache.Digest([]byte(body))
	h.blobs[sha] = []byte(body)
	s := tu.UpdateSpec{Config: "ratelimits", Version: version, Generation: gen, Payload: []byte(body), URL: h.srv.URL + "/" + sha, Release: "rel-1"}
	for _, o := range opts {
		o(&s)
	}
	h.ag.Handle(context.Background(), nil, h.keys.Update(s))
}

func cfgJSON(service string, timeout int) string {
	return fmt.Sprintf(`{"schema_version":1,"service":%q,"timeout_ms":%d,"features":[{"name":"a","enabled":true,"rollout_percent":5}],"limits":[]}`, service, timeout)
}

func (h *harness) opts() Options[ratelimits.Config] {
	return Options[ratelimits.Config]{Dir: h.root, Config: "ratelimits", Environment: "prod", TrustBundle: h.keys.BundleJSON,
		Parse: ratelimits.Parse, Clone: ratelimits.Config.Clone, PollInterval: 10 * time.Millisecond, HealthyAfter: time.Hour}
}

func (h *harness) open(o Options[ratelimits.Config]) *Client[ratelimits.Config] {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Open(ctx, o)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(c.Close)
	return c
}

func status(t *testing.T, root string) cache.SDKStatus {
	d, _ := cache.Open(root, "ratelimits")
	s, err := d.ReadSDKStatus()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestColdStartNoCache(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	c, err := Open(ctx, h.opts())
	var ie *InitError
	if c != nil || !errors.As(err, &ie) {
		t.Fatalf("cold start without cache or bootstrap: %v", err)
	}
}

func TestColdStartWaitsForFirstConfig(t *testing.T) {
	h := newHarness(t)
	go func() { time.Sleep(50 * time.Millisecond); h.push(1, 1, cfgJSON("checkout", 100)) }()
	c := h.open(h.opts())
	if c.Get().Service != "checkout" || c.Info().Version != 1 {
		t.Fatalf("%+v", c.Info())
	}
}

func TestBootstrap(t *testing.T) {
	h := newHarness(t)
	o := h.opts()
	o.Bootstrap = []byte(cfgJSON("bootstrap", 50))
	c := h.open(o)
	if c.Get().Service != "bootstrap" || c.Info().Source != "bootstrap" {
		t.Fatalf("%+v", c.Info())
	}
	h.push(1, 1, cfgJSON("real", 100))
	tu.Eventually(t, 2*time.Second, "real config", func() bool { return c.Get().Service == "real" })

	bad := h.opts()
	bad.Bootstrap = []byte(`{"schema_version":2}`)
	if _, err := Open(context.Background(), bad); err == nil {
		t.Fatal("unparseable bootstrap accepted")
	}
	fc := h.opts()
	fc.FailClosed, fc.Bootstrap = true, []byte(cfgJSON("x", 1))
	if _, err := Open(context.Background(), fc); err == nil {
		t.Fatal("fail-closed bootstrap accepted without BootstrapDenyAll")
	}
}

func TestRejectKeepsLKG(t *testing.T) {
	h := newHarness(t)
	h.push(1, 1, cfgJSON("checkout", 100))
	reg := prometheus.NewRegistry()
	o := h.opts()
	o.Metrics, o.InstanceID = NewMetrics(reg), "i-1"
	c := h.open(o)
	// Generic checks pass, the consumer's parser rejects (schema_version 2).
	h.push(2, 2, `{"schema_version":2,"service":"checkout","timeout_ms":100,"features":[],"limits":[]}`)
	tu.Eventually(t, 2*time.Second, "REJECTED status", func() bool { return status(t, h.root).State == "REJECTED" })
	s := status(t, h.root)
	if s.Generation != 2 || s.ServingVersion != 1 || c.Get().Service != "checkout" || c.Info().Version != 1 {
		t.Fatalf("status %+v info %+v", s, c.Info())
	}
	if n := testutil.ToFloat64(o.Metrics.failures.WithLabelValues("i-1", "ratelimits")); n != 1 {
		t.Fatalf("validation failures metric = %v", n)
	}
	// A restart keeps serving the LKG rather than retrying the rejected one.
	c.Close()
	c2 := h.open(o)
	if c2.Info().Version != 1 {
		t.Fatalf("after restart serving %d", c2.Info().Version)
	}
	// The next good generation is accepted.
	h.push(3, 3, cfgJSON("checkout-v3", 100))
	tu.Eventually(t, 2*time.Second, "v3", func() bool { return c2.Info().Version == 3 })
	if s := status(t, h.root); s.State != "ACCEPTED" || s.ServingVersion != 3 {
		t.Fatalf("%+v", s)
	}
	// Exactly one info series per instance.
	if n := testutil.CollectAndCount(o.Metrics.info); n != 1 {
		t.Fatalf("%d info series", n)
	}
}

func TestSameContentNewGeneration(t *testing.T) {
	h := newHarness(t)
	h.push(1, 1, cfgJSON("a", 10))
	parses := 0
	o := h.opts()
	o.Parse = func(b []byte) (ratelimits.Config, error) { parses++; return ratelimits.Parse(b) }
	c := h.open(o)
	h.push(1, 2, cfgJSON("a", 10)) // promotion to the next ring re-signs the same content
	tu.Eventually(t, 2*time.Second, "generation 2", func() bool { return c.Info().Generation == 2 })
	if parses != 1 {
		t.Fatalf("same content parsed %d times", parses)
	}
}

func TestCrashLoopQuarantine(t *testing.T) {
	h := newHarness(t)
	h.push(1, 1, cfgJSON("good", 100))
	o := h.opts()
	c := h.open(o)
	c.MarkHealthy()
	h.push(2, 2, cfgJSON("crashy", 100))
	tu.Eventually(t, 2*time.Second, "v2", func() bool { return c.Info().Version == 2 })
	c.Close()
	// The process now crashes repeatedly while serving v2.
	for start := 2; start <= 3; start++ {
		c := h.open(o)
		if c.Info().Version != 2 {
			t.Fatalf("start %d: served %d before the threshold", start, c.Info().Version)
		}
		c.Close()
	}
	c4 := h.open(o)
	if c4.Info().Version != 1 || c4.Get().Service != "good" {
		t.Fatalf("crash-looped config not quarantined: %+v", c4.Info())
	}
	if s := status(t, h.root); s.State != "CRASH_LOOP" || s.Generation != 2 || s.ServingVersion != 1 {
		t.Fatalf("%+v", s)
	}
	// A rollback (newer generation, old content) is served normally.
	h.push(1, 3, cfgJSON("good", 100))
	tu.Eventually(t, 2*time.Second, "rollback", func() bool { return c4.Info().Generation == 3 })
}

func TestCorruptCacheFallsBack(t *testing.T) {
	h := newHarness(t)
	h.push(1, 1, cfgJSON("one", 100))
	o := h.opts()
	c := h.open(o)
	c.MarkHealthy()
	h.push(2, 2, cfgJSON("two", 100))
	tu.Eventually(t, 2*time.Second, "v2", func() bool { return c.Info().Version == 2 })
	c.Close()
	// Corrupt the current object behind the agent's back.
	sha := cache.Digest([]byte(cfgJSON("two", 100)))
	os.WriteFile(filepath.Join(h.root, "ratelimits", "objects", sha), []byte("garbage"), 0o600)
	c2 := h.open(o)
	if c2.Info().Version != 1 {
		t.Fatalf("corrupt current not skipped: %+v", c2.Info())
	}
	// A cache whose signatures do not verify against the trust bundle is
	// never served, even if it parses.
	other := tu.NewKeys(t)
	o2 := o
	o2.TrustBundle = other.BundleJSON
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := Open(ctx, o2); err == nil {
		t.Fatal("served a cache signed by an untrusted key")
	}
}

func TestStalenessFailClosed(t *testing.T) {
	h := newHarness(t)
	h.push(1, 1, cfgJSON("acl", 100), func(s *tu.UpdateSpec) { s.FailureMode, s.MaxStaleness = "fail_closed", 1 })
	c := h.open(h.opts())
	if !c.Info().FailClosed || c.Stale() || !c.Usable() {
		t.Fatalf("fresh: %+v stale=%v", c.Info(), c.Stale())
	}
	time.Sleep(1200 * time.Millisecond)
	if !c.Stale() || c.Usable() {
		t.Fatal("not stale after max staleness without agent contact")
	}
	if _, err := c.GetFresh(); !errors.Is(err, ErrStale) {
		t.Fatalf("GetFresh: %v", err)
	}
	_ = c.Get() // Get still returns the snapshot
	// The agent reaching the control plane makes it fresh again.
	cache.BumpHeartbeat(h.root, 99)
	tu.Eventually(t, 2*time.Second, "fresh", func() bool { return c.Usable() })
}

func TestSnapshotImmutability(t *testing.T) {
	h := newHarness(t)
	h.push(1, 1, cfgJSON("svc", 100))
	o := h.opts()
	o.Clone = nil
	if _, err := Open(context.Background(), o); err == nil || !strings.Contains(err.Error(), "mutable") {
		t.Fatalf("mutable snapshot type accepted: %v", err)
	}
	c := h.open(h.opts())
	got := c.Get()
	got.Features[0].Name = "mutated"
	got.Service = "mutated"
	if again := c.Get(); again.Features[0].Name != "a" || again.Service != "svc" {
		t.Fatal("caller mutated the shared snapshot")
	}
	type flat struct {
		A int
		B [3]string
	}
	if _, ok := mutable(reflectType[flat]()); ok {
		t.Fatal("flat struct reported mutable")
	}
}
