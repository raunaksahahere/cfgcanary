package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

func cliBinary(t *testing.T) string {
	binOnce.Do(func() {
		dir, _ := os.MkdirTemp("", "cfgcanary-bin")
		binPath = filepath.Join(dir, "cfgcanary")
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/cfgcanary")
		cmd.Dir = testutil.RepoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			binErr = errors.New(string(out))
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

type cli struct {
	t   *testing.T
	bin string
	e   *testenv.Env
}

func (c cli) run(as string, args ...string) (string, int) {
	c.t.Helper()
	full := append([]string{"--server", c.e.GRPCAddr(), "--server-name", "localhost", "--pki-dir", c.e.PKI, "--as", as}, args...)
	cmd := exec.Command(c.bin, full...)
	cmd.Dir = testutil.RepoRoot()
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		c.t.Fatal(err)
	}
	return out.String(), code
}

func (c cli) ok(as string, args ...string) string {
	c.t.Helper()
	out, code := c.run(as, args...)
	if code != 0 {
		c.t.Fatalf("cfgcanary %v: exit %d\n%s", args, code, out)
	}
	return out
}

func TestCLI(t *testing.T) {
	e := testenv.Start(t)
	c := cli{t: t, bin: cliBinary(t), e: e}
	// The spec references validator.wasm; build it where the spec expects it.
	wasm := testutil.BuildWasm(t, "examples/ratelimits/validator")
	wasmPath := filepath.Join(testutil.RepoRoot(), "examples", "ratelimits", "validator.wasm")
	if _, err := os.Stat(wasmPath); err != nil {
		testutil.Must(t, os.WriteFile(wasmPath, wasm, 0o644), "write wasm")
	}

	if out := c.ok("viewer", "info"); !strings.Contains(out, "roles [viewer]") {
		t.Fatalf("info: %s", out)
	}
	c.ok("admin", "configs", "register", "-f", "examples/ratelimits/spec-seed.yaml")
	e.Fleet(e.FleetOptions(5, "ratelimits"))
	waitInstances(t, e.Control("users/viewer"), 5)
	if out := c.ok("viewer", "configs", "list"); !strings.Contains(out, "ratelimits") {
		t.Fatal(out)
	}
	// Rejected payloads exit 3 and explain why.
	out, code := c.run("operator", "validate", "ratelimits", "-f", "examples/ratelimits/payloads/bad-parser-rule.json")
	if code != 3 || !strings.Contains(out, "[wasm/rejected]") {
		t.Fatalf("validate bad payload: exit %d\n%s", code, out)
	}
	if out, code := c.run("viewer", "versions", "submit", "ratelimits", "-f", "examples/ratelimits/payloads/v1-baseline.json"); code != 5 {
		t.Fatalf("viewer submit: exit %d\n%s", code, out)
	}
	out = c.ok("operator", "-o", "json", "versions", "submit", "ratelimits", "-f", "examples/ratelimits/payloads/v1-baseline.json")
	var env struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Data       struct {
			Version struct {
				Version string `json:"version"`
			} `json:"version"`
		} `json:"data"`
	}
	testutil.Must(t, json.Unmarshal([]byte(out), &env), out)
	if env.APIVersion != "cfgcanary/v1" || env.Kind != "SubmitVersionResult" || env.Data.Version.Version != "1" {
		t.Fatalf("json output: %s", out)
	}
	if out := c.ok("viewer", "releases", "plan", "ratelimits", "1"); !strings.Contains(out, "dry run: no release was created") {
		t.Fatal(out)
	}
	out = c.ok("operator", "releases", "create", "ratelimits", "1")
	id := strings.Fields(strings.TrimPrefix(out, "created release "))[0]
	// Permission boundaries hold through the CLI.
	for _, as := range []string{"viewer"} {
		if _, code := c.run(as, "releases", "promote", id); code != 5 {
			t.Fatalf("%s promote: exit %d", as, code)
		}
		if _, code := c.run(as, "releases", "rollback", id); code != 5 {
			t.Fatalf("%s rollback: exit %d", as, code)
		}
	}
	if out, code := c.run("operator", "kill-switch", "ratelimits", "--reason", "test"); code != 5 {
		t.Fatalf("operator without break-glass used the kill switch: exit %d\n%s", code, out)
	}
	if out, code := c.run("operator", "releases", "promote", id, "--override", "--reason", "x"); code != 5 {
		t.Fatalf("override without break-glass: exit %d\n%s", code, out)
	}
	if out := c.ok("viewer", "releases", "status", id); !strings.Contains(out, "release "+id) {
		t.Fatal(out)
	}
	if out := c.ok("admin", "audit", "--action", "target.kill_switch"); !strings.Contains(out, "denied") {
		t.Fatalf("denied kill switch not audited:\n%s", out)
	}
	// A dry run plan against a missing config is a clean not-found.
	if _, code := c.run("viewer", "releases", "plan", "nope", "1"); code != 6 {
		t.Fatalf("missing config: exit %d", code)
	}
	if out, code := c.run("viewer", "releases", "promote"); code != 2 {
		t.Fatalf("usage error: exit %d\n%s", code, out)
	}
}
