// Package bench holds reproducible measurements. They run against the
// integration-test services (make test-deps-up) and write raw results to
// bench/results:
//
//	CFGCANARY_BENCH=1 go test ./bench -run TestScale -timeout 60m -v
//
// The control plane runs as a separate `cfgcanary server` process so its CPU
// and memory are measured apart from the simulated fleet, which runs real
// agent and SDK code in goroutines of the test process.
package bench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/client"
	"github.com/raunaksahahere/cfgcanary/internal/devpki"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func requireBench(t *testing.T) {
	if os.Getenv("CFGCANARY_BENCH") != "1" {
		t.Skip("benchmark: set CFGCANARY_BENCH=1 (needs make test-deps-up)")
	}
	os.Setenv("CFGCANARY_IT", "1")
}

var (
	binOnce sync.Once
	binPath string
)

func binary(t *testing.T) string {
	binOnce.Do(func() {
		binPath = filepath.Join(os.TempDir(), "cfgcanary-bench-bin")
		cmd := exec.Command("go", "build", "-trimpath", "-o", binPath, "./cmd/cfgcanary")
		cmd.Dir = testutil.RepoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			panic(string(out))
		}
	})
	return binPath
}

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// harness is a control plane in its own process.
type harness struct {
	t                     *testing.T
	dir, pki              string
	dbURL                 string
	bucket                string
	grpcAddr, metricsAddr string
	srv                   *exec.Cmd
	extraArgs             []string
	ca                    *devpki.CA
	trust                 []byte
	admin                 pb.ControlServiceClient
}

func startHarness(t *testing.T, extra ...string) *harness {
	t.Helper()
	h := &harness{t: t, dir: t.TempDir(), extraArgs: extra}
	h.pki = filepath.Join(h.dir, "pki")
	if _, err := devpki.Bootstrap(h.pki, devpki.DefaultOptions()); err != nil {
		t.Fatal(err)
	}
	h.ca, _ = devpki.LoadCA(h.pki)
	h.trust, _ = os.ReadFile(filepath.Join(h.pki, "trust.json"))
	h.dbURL = testutil.FreshDatabase(t)
	h.bucket = testutil.RandomName("bench-")
	h.grpcAddr, h.metricsAddr = freePort(t), freePort(t)
	h.start()
	t.Cleanup(h.stop)
	cc, err := client.Dial(client.Options{Server: h.grpcAddr, ServerName: "localhost", Cert: filepath.Join(h.pki, "users", "admin.pem"),
		Key: filepath.Join(h.pki, "users", "admin-key.pem"), CA: filepath.Join(h.pki, "ca.pem")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	h.admin = pb.NewControlServiceClient(cc)
	return h
}

func (h *harness) start() {
	ep, ak, sk := testutil.S3()
	args := append([]string{"server", "--listen", h.grpcAddr, "--rest-listen", "", "--metrics-listen", h.metricsAddr,
		"--s3-endpoint", ep, "--s3-bucket", h.bucket, "--tls-cert", filepath.Join(h.pki, "server.pem"), "--tls-key", filepath.Join(h.pki, "server-key.pem"),
		"--tls-ca", filepath.Join(h.pki, "ca.pem"), "--artifact-key", filepath.Join(h.pki, "artifact.key"), "--delivery-key", filepath.Join(h.pki, "delivery.key"),
		"--rbac", filepath.Join(h.pki, "rbac.yaml"), "--controller-tick", "100ms", "--membership-window", "10m", "--log-level", "warn"}, h.extraArgs...)
	cmd := exec.Command(binary(h.t), args...)
	cmd.Env = append(os.Environ(), "CFGCANARY_DB_URL="+h.dbURL, "CFGCANARY_S3_ACCESS_KEY="+ak, "CFGCANARY_S3_SECRET_KEY="+sk, "CFGCANARY_DB_MAX_CONNS=40")
	logf, _ := os.Create(filepath.Join(h.dir, fmt.Sprintf("server-%d.log", time.Now().UnixNano())))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.srv = cmd
	testutil.Eventually(h.t, time.Minute, "server ready", func() bool {
		r, err := http.Get("http://" + h.metricsAddr + "/readyz")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})
}

// kill stops the server abruptly (SIGKILL) or gracefully.
func (h *harness) kill(abrupt bool) {
	if h.srv == nil {
		return
	}
	if abrupt {
		_ = h.srv.Process.Kill()
	} else {
		_ = h.srv.Process.Signal(os.Interrupt)
	}
	_ = h.srv.Wait()
	h.srv = nil
}

func (h *harness) stop() { h.kill(false) }

// metrics scrapes the server's /metrics and sums each family.
func (h *harness) metrics() map[string]float64 {
	out := map[string]float64{}
	r, err := http.Get("http://" + h.metricsAddr + "/metrics")
	if err != nil {
		return out
	}
	defer r.Body.Close()
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(bufio.NewReader(r.Body))
	if err != nil {
		return out
	}
	for name, f := range fams {
		for _, m := range f.Metric {
			switch {
			case m.Counter != nil:
				out[name] += m.Counter.GetValue()
			case m.Gauge != nil:
				out[name] += m.Gauge.GetValue()
			}
		}
	}
	return out
}

// --- statistics and output --------------------------------------------------------

type dist struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50_ms"`
	P90 float64 `json:"p90_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func summarize(ds []time.Duration) dist {
	v := make([]float64, len(ds))
	for i, d := range ds {
		v[i] = float64(d.Microseconds()) / 1000
	}
	sort.Float64s(v)
	r := dist{N: len(v), P50: quantile(v, 0.5), P90: quantile(v, 0.9), P99: quantile(v, 0.99)}
	if len(v) > 0 {
		r.Max = v[len(v)-1]
	}
	return r
}

// Environment describes where a measurement ran.
type Environment struct {
	Host      string `json:"host"`
	CPU       string `json:"cpu"`
	CPUs      int    `json:"cpus"`
	MemoryMB  int    `json:"memory_mb"`
	OS        string `json:"os"`
	Go        string `json:"go"`
	Commit    string `json:"commit"`
	Dirty     bool   `json:"dirty"`
	Postgres  string `json:"postgres"`
	S3        string `json:"s3"`
	Timestamp string `json:"timestamp"`
	Command   string `json:"command"`
}

func environment(command string) Environment {
	e := Environment{CPUs: runtime.NumCPU(), OS: runtime.GOOS + "/" + runtime.GOARCH, Go: runtime.Version(),
		Postgres: "postgres:18.6 (fsync=off, tmpfs; see deploy/compose/test-deps.yaml)", S3: "pgsty/minio:RELEASE.2026-08-04T00-00-00Z",
		Timestamp: time.Now().UTC().Format(time.RFC3339), Command: command}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "model name") {
				e.CPU = strings.TrimSpace(strings.SplitN(l, ":", 2)[1])
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		f := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])
		if len(f) >= 2 {
			kb, _ := strconv.Atoi(f[1])
			e.MemoryMB = kb / 1024
		}
	}
	if out, err := exec.Command("git", "-C", testutil.RepoRoot(), "rev-parse", "--short=12", "HEAD").Output(); err == nil {
		e.Commit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "-C", testutil.RepoRoot(), "status", "--porcelain", "--untracked-files=no").Output(); err == nil {
		e.Dirty = len(strings.TrimSpace(string(out))) > 0
	}
	e.Host = "local workstation (" + e.CPU + ")"
	return e
}

func writeJSON(t *testing.T, name string, v any) {
	dir := filepath.Join(testutil.RepoRoot(), "bench", "results")
	_ = os.MkdirAll(dir, 0o755)
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote bench/results/%s", name)
}

func writeCSV(t *testing.T, name string, header string, rows []string) {
	dir := filepath.Join(testutil.RepoRoot(), "bench", "results")
	_ = os.MkdirAll(dir, 0o755)
	content := header + "\n" + strings.Join(rows, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// recorder observes agent connection attempts and applies.
type recorder struct {
	mu         sync.Mutex
	connecting []time.Time
	connected  map[string]time.Time
	applied    map[string]map[int64]time.Time // instance -> version -> first apply
}

func newRecorder() *recorder {
	return &recorder{connected: map[string]time.Time{}, applied: map[string]map[int64]time.Time{}}
}

func (r *recorder) Connecting(string) {
	r.mu.Lock()
	r.connecting = append(r.connecting, time.Now())
	r.mu.Unlock()
}

func (r *recorder) Connected(id string) {
	r.mu.Lock()
	r.connected[id] = time.Now()
	r.mu.Unlock()
}

func (r *recorder) Applied(id, _ string, _, version int64) {
	r.mu.Lock()
	m := r.applied[id]
	if m == nil {
		m = map[int64]time.Time{}
		r.applied[id] = m
	}
	if _, ok := m[version]; !ok {
		m[version] = time.Now()
	}
	r.mu.Unlock()
}

func (r *recorder) appliedAt(id string, version int64) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.applied[id][version]
	return t, ok
}

func (r *recorder) attemptsSince(t0 time.Time) []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []time.Time
	for _, t := range r.connecting {
		if !t.Before(t0) {
			out = append(out, t)
		}
	}
	return out
}

func (r *recorder) connectedSince(t0 time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, t := range r.connected {
		if !t.Before(t0) {
			n++
		}
	}
	return n
}
