package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/cache"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

type blobServer struct {
	*httptest.Server
	mu      sync.Mutex
	objects map[string][]byte
	// override serves these bytes instead (simulates a tampered store).
	override []byte
	hits     int
}

func newBlobServer(t *testing.T) *blobServer {
	b := &blobServer{objects: map[string][]byte{}}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.hits++
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "http://evil.example/x", http.StatusFound)
			return
		}
		if b.override != nil {
			_, _ = w.Write(b.override)
			return
		}
		data, ok := b.objects[strings.TrimPrefix(r.URL.Path, "/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *blobServer) put(data []byte) string {
	sha := cache.Digest(data)
	b.mu.Lock()
	b.objects[sha] = data
	b.mu.Unlock()
	return b.URL + "/" + sha
}

type env struct {
	t    *testing.T
	keys *testutil.Keys
	blob *blobServer
	root string
}

func newEnv(t *testing.T) *env {
	return &env{t: t, keys: testutil.NewKeys(t), blob: newBlobServer(t), root: t.TempDir()}
}

func (e *env) agent() *Agent {
	a, err := New(Config{InstanceID: "agent-1", Environment: "prod", Root: e.root, Configs: []string{"limits"},
		Trust: e.keys.Bundle, AllowedOrigins: []string{e.blob.URL}})
	if err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *env) update(version, gen int64, body string) *pb.ConfigUpdate {
	return e.keys.Update(testutil.UpdateSpec{Config: "limits", Version: version, Generation: gen, Payload: []byte(body), URL: e.blob.put([]byte(body))})
}

func current(t *testing.T, root string) cache.Pointer {
	d, _ := cache.Open(root, "limits")
	p, _, err := d.ReadPointer()
	if err != nil {
		t.Fatalf("pointer: %v", err)
	}
	return p
}

func pendingState(a *Agent) *pb.InstanceConfigStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pending["limits"]
}

func TestApplyRestartRecovery(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	a.Handle(context.Background(), nil, e.update(1, 5, `{"v":1}`))
	if g, v := a.Applied("limits"); g != 5 || v != 1 {
		t.Fatalf("applied %d/%d", g, v)
	}
	if s := pendingState(a); s.State != pb.ApplyState_APPLY_STATE_APPLIED || s.Generation != 5 {
		t.Fatalf("status %+v", s)
	}
	// A restarted agent recovers the applied generation from the verified
	// cache, without contacting anything.
	b := e.agent()
	if g, v := b.Applied("limits"); g != 5 || v != 1 {
		t.Fatalf("after restart: %d/%d", g, v)
	}
}

func TestStaleUpdateIgnored(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	a.Handle(context.Background(), nil, e.update(2, 10, `{"v":2}`))
	hits := e.blob.hits
	a.Handle(context.Background(), nil, e.update(3, 9, `{"v":3}`))  // older generation, newer version
	a.Handle(context.Background(), nil, e.update(2, 10, `{"v":2}`)) // duplicate
	if g, v := a.Applied("limits"); g != 10 || v != 2 {
		t.Fatalf("stale update applied: %d/%d", g, v)
	}
	if e.blob.hits != hits {
		t.Fatal("stale update was downloaded")
	}
}

func TestRollbackAppliesOlderVersion(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	a.Handle(context.Background(), nil, e.update(7, 1, `{"v":7}`))
	a.Handle(context.Background(), nil, e.update(8, 2, `{"v":8}`))
	hits := e.blob.hits
	a.Handle(context.Background(), nil, e.update(7, 3, `{"v":7}`)) // rollback: newer generation, older version
	p := current(t, e.root)
	if p.Current.Version != 7 || p.Current.Generation != 3 || p.Previous.Version != 8 {
		t.Fatalf("pointer after rollback: %+v", p)
	}
	if e.blob.hits != hits {
		t.Fatal("rollback re-downloaded a payload that was still cached")
	}
}

func TestVerificationFailuresKeepCurrent(t *testing.T) {
	e := newEnv(t)
	good := `{"v":1}`
	cases := []struct {
		name   string
		mutate func(u *pb.ConfigUpdate)
		setup  func()
		reason string
	}{
		{"bad artifact signature", func(u *pb.ConfigUpdate) { u.Artifact.Signature[0] ^= 1 }, nil, "signature"},
		{"bad delivery signature", func(u *pb.ConfigUpdate) { u.Delivery.Signature[5] ^= 1 }, nil, "signature"},
		{"unknown key", func(u *pb.ConfigUpdate) { u.Delivery.KeyId = "0123456789abcdef" }, nil, "signature"},
		{"generation not signed", func(u *pb.ConfigUpdate) { u.Generation = 99 }, nil, "signature"},
		{"ring outside delivery range", func(u *pb.ConfigUpdate) {
			*u = *e.keys.Update(testutil.UpdateSpec{Config: "limits", Version: 2, Generation: 20, Ring: 3, RingLo: 0, RingHi: 1, Payload: []byte(`{"v":2}`), URL: e.blob.put([]byte(`{"v":2}`))})
		}, nil, "scope"},
		{"other environment", func(u *pb.ConfigUpdate) {
			*u = *e.keys.Update(testutil.UpdateSpec{Config: "limits", Env: "staging", Version: 2, Generation: 20, Payload: []byte(`{"v":2}`), URL: e.blob.put([]byte(`{"v":2}`))})
		}, nil, "scope"},
		{"tampered object store", nil, func() { e.blob.mu.Lock(); e.blob.override = []byte(`{"v":666}`); e.blob.mu.Unlock() }, "download"},
		{"untrusted origin", func(u *pb.ConfigUpdate) { u.Url = "http://attacker.example/" + u.Sha256 }, nil, "untrusted_url"},
		{"credentials in url", func(u *pb.ConfigUpdate) { u.Url = strings.Replace(u.Url, "http://", "http://user:pw@", 1) }, nil, "untrusted_url"},
		{"redirect", func(u *pb.ConfigUpdate) { u.Url = e.blob.URL + "/redirect" }, nil, "download"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e.root = t.TempDir()
			e.blob.mu.Lock()
			e.blob.override = nil
			e.blob.mu.Unlock()
			a := e.agent()
			a.Handle(context.Background(), nil, e.update(1, 10, good))
			u := e.update(2, 20, `{"v":2}`)
			if c.mutate != nil {
				c.mutate(u)
			}
			if c.setup != nil {
				c.setup()
			}
			a.Handle(context.Background(), nil, u)
			if g, v := a.Applied("limits"); g != 10 || v != 1 {
				t.Fatalf("applied %d/%d after %s", g, v, c.name)
			}
			if p := current(t, e.root); p.Current.Version != 1 {
				t.Fatalf("pointer moved: %+v", p.Current)
			}
			s := pendingState(a)
			if s.State != pb.ApplyState_APPLY_STATE_FAILED || s.Reason != c.reason {
				t.Fatalf("status %+v, want FAILED/%s", s, c.reason)
			}
		})
	}
}

func TestDiskErrorsKeepCurrent(t *testing.T) {
	for _, step := range []string{"create", "write", "fsync", "rename"} {
		for _, target := range []string{"objects", "pointer.json"} {
			t.Run(step+"/"+target, func(t *testing.T) {
				e := newEnv(t)
				a := e.agent()
				a.Handle(context.Background(), nil, e.update(1, 1, `{"v":1}`))
				cache.SetFaultInjector(func(s, path string) error {
					if s == step && strings.Contains(path, target) {
						return errors.New("injected: no space left on device")
					}
					return nil
				})
				a.Handle(context.Background(), nil, e.update(2, 2, `{"v":2}`))
				cache.SetFaultInjector(nil)
				if g, _ := a.Applied("limits"); g != 1 {
					t.Fatalf("applied generation %d after disk error", g)
				}
				if p := current(t, e.root); p.Current.Version != 1 {
					t.Fatalf("pointer moved: %+v", p.Current)
				}
				if s := pendingState(a); s.State != pb.ApplyState_APPLY_STATE_FAILED || s.Reason != "disk" {
					t.Fatalf("status %+v", s)
				}
				// No temp files are left behind.
				filepath.Walk(e.root, func(p string, _ os.FileInfo, _ error) error {
					if strings.Contains(filepath.Base(p), ".tmp-") {
						t.Errorf("temp file left: %s", p)
					}
					return nil
				})
				// The same generation succeeds once the disk recovers.
				a.Handle(context.Background(), nil, e.update(2, 2, `{"v":2}`))
				if g, _ := a.Applied("limits"); g != 2 {
					t.Fatal("did not recover after the disk error cleared")
				}
			})
		}
	}
}

func TestPermissionError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	e := newEnv(t)
	a := e.agent()
	a.Handle(context.Background(), nil, e.update(1, 1, `{"v":1}`))
	dir := filepath.Join(e.root, "limits")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	a.Handle(context.Background(), nil, e.update(2, 2, `{"v":2}`))
	if g, _ := a.Applied("limits"); g != 1 {
		t.Fatal("applied without write permission")
	}
	os.Chmod(dir, 0o700)
	if p := current(t, e.root); p.Current.Version != 1 {
		t.Fatal("pointer changed")
	}
}

// A crash after the object is durable but before the pointer swap leaves the
// old pointer: the new object is simply unreferenced.
func TestCrashBetweenObjectAndPointer(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	a.Handle(context.Background(), nil, e.update(1, 1, `{"v":1}`))
	cache.SetFaultInjector(func(step, path string) error {
		if step == "rename" && strings.HasSuffix(path, "pointer.json") {
			return errors.New("injected crash")
		}
		return nil
	})
	a.Handle(context.Background(), nil, e.update(2, 2, `{"v":2}`))
	cache.SetFaultInjector(nil)
	b := e.agent() // "restart"
	if g, v := b.Applied("limits"); g != 1 || v != 1 {
		t.Fatalf("after crash: %d/%d", g, v)
	}
}

func TestCorruptPointerFallsBack(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	a.Handle(context.Background(), nil, e.update(1, 1, `{"v":1}`))
	a.Handle(context.Background(), nil, e.update(2, 2, `{"v":2}`))
	// Torn or corrupt pointer.json: the retained predecessor is used.
	if err := os.WriteFile(filepath.Join(e.root, "limits", "pointer.json"), []byte(`{"current":{"vers`), 0o600); err != nil {
		t.Fatal(err)
	}
	b := e.agent()
	if g, v := b.Applied("limits"); g != 1 || v != 1 {
		t.Fatalf("fallback pointer: %d/%d", g, v)
	}
	// Corrupt current object: not counted as applied; it is refetched.
	c := e.agent()
	c.Handle(context.Background(), nil, e.update(3, 3, `{"v":3}`))
	sha := cache.Digest([]byte(`{"v":3}`))
	os.WriteFile(filepath.Join(e.root, "limits", "objects", sha), []byte(`{"v":3`), 0o600)
	d := e.agent()
	if g, _ := d.Applied("limits"); g == 3 {
		t.Fatal("corrupt object counted as applied")
	}
	d.Handle(context.Background(), nil, e.update(3, 3, `{"v":3}`))
	if g, _ := d.Applied("limits"); g != 3 {
		t.Fatal("corrupt object was not refetched")
	}
}

func TestCacheBounded(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	for i := int64(1); i <= 20; i++ {
		a.Handle(context.Background(), nil, e.update(i, i, fmt.Sprintf(`{"v":%d}`, i)))
	}
	ents, _ := os.ReadDir(filepath.Join(e.root, "limits", "objects"))
	if len(ents) > 6 {
		t.Fatalf("%d objects retained", len(ents))
	}
	p := current(t, e.root)
	for _, sha := range []string{p.Current.SHA256, p.Previous.SHA256} {
		if _, err := os.Stat(filepath.Join(e.root, "limits", "objects", sha)); err != nil {
			t.Fatal("GC removed a referenced object")
		}
	}
}

func TestFileSource(t *testing.T) {
	e := newEnv(t)
	a := e.agent()
	dir := t.TempDir()
	body := []byte(`{"v":4}`)
	u := e.keys.Update(testutil.UpdateSpec{Config: "limits", Version: 4, Generation: 8, Ring: 1, RingLo: 0, RingHi: 1, Payload: body})
	env := fmt.Sprintf(`{"config":"limits","environment":"prod","version":4,"generation":8,"sha256":%q,"size":%d,"ring":1,
		"artifact":{"payload":%q,"signature":%q,"key_id":%q},"delivery":{"payload":%q,"signature":%q,"key_id":%q}}`,
		u.Sha256, len(body), b64(u.Artifact.Payload), b64(u.Artifact.Signature), u.Artifact.KeyId,
		b64(u.Delivery.Payload), b64(u.Delivery.Signature), u.Delivery.KeyId)
	os.WriteFile(filepath.Join(dir, "payload"), body, 0o600)
	os.WriteFile(filepath.Join(dir, "envelope.json"), []byte(env), 0o600)
	a.applyFromDir(context.Background(), "limits", dir)
	if g, v := a.Applied("limits"); g != 8 || v != 4 {
		t.Fatalf("file source: %d/%d", g, v)
	}
	// A payload that does not match the envelope is refused.
	os.WriteFile(filepath.Join(dir, "payload"), []byte(`{"v":5}`), 0o600)
	b, _ := New(Config{InstanceID: "agent-2", Environment: "prod", Root: t.TempDir(), Configs: []string{"limits"}, Trust: e.keys.Bundle})
	b.applyFromDir(context.Background(), "limits", dir)
	if g, _ := b.Applied("limits"); g != 0 {
		t.Fatal("mismatched payload applied")
	}
}
