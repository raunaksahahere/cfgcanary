// Package testutil holds helpers shared by tests.
package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

var (
	wasmMu    sync.Mutex
	wasmCache = map[string][]byte{}
)

// RepoRoot returns the repository root.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

// BuildWasm compiles a Go main package (path relative to the repository root)
// to a WASI module and returns its bytes. Results are cached per process.
func BuildWasm(t testing.TB, pkg string) []byte {
	t.Helper()
	wasmMu.Lock()
	defer wasmMu.Unlock()
	if b, ok := wasmCache[pkg]; ok {
		return b
	}
	out := filepath.Join(t.TempDir(), "m.wasm")
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, "./"+pkg)
	cmd.Dir = RepoRoot()
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building %s for wasip1: %v\n%s", pkg, err, msg)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	wasmCache[pkg] = b
	return b
}

// ReadRepoFile reads a file relative to the repository root.
func ReadRepoFile(t testing.TB, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(RepoRoot(), rel))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
