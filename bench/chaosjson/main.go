// Command chaosjson summarises `go test -json ./test/chaos` output (stdin)
// into bench/results/chaos.json.
//
//	go test -json -count=1 ./test/chaos | go run ./bench/chaosjson -coverage 93.6%
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"
)

type event struct {
	Action  string  `json:"Action"`
	Test    string  `json:"Test"`
	Elapsed float64 `json:"Elapsed"`
	Output  string  `json:"Output"`
}

func main() {
	coverage := flag.String("coverage", "", "state-machine statement coverage")
	out := flag.String("out", "bench/results/chaos.json", "output file")
	flag.Parse()
	type scenario struct {
		Name    string  `json:"name"`
		Result  string  `json:"result"`
		Seconds float64 `json:"seconds"`
	}
	results := map[string]*scenario{}
	panics := 0
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var e event
		if json.Unmarshal(sc.Bytes(), &e) != nil {
			continue
		}
		if strings.Contains(e.Output, "panic:") || strings.Contains(e.Output, "fatal error:") {
			panics++
		}
		if e.Test == "" || (e.Action != "pass" && e.Action != "fail" && e.Action != "skip") {
			continue
		}
		results[e.Test] = &scenario{Name: e.Test, Result: strings.ToUpper(e.Action), Seconds: e.Elapsed}
	}
	var list []scenario
	failed := 0
	for _, s := range results {
		list = append(list, *s)
		if s.Result != "PASS" {
			failed++
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	commit, _ := exec.Command("git", "rev-parse", "--short=12", "HEAD").Output()
	cpu := ""
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "model name") {
				cpu = strings.TrimSpace(strings.SplitN(l, ":", 2)[1])
				break
			}
		}
	}
	memMB := 0
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		var kb int
		fmt.Sscanf(strings.SplitN(string(b), "\n", 2)[0], "MemTotal: %d kB", &kb)
		memMB = kb / 1024
	}
	doc := map[string]any{
		"environment": map[string]any{"cpu": cpu, "cpus": runtime.NumCPU(), "memory_mb": memMB, "os": runtime.GOOS + "/" + runtime.GOARCH, "go": runtime.Version(),
			"commit": strings.TrimSpace(string(commit)), "timestamp": time.Now().UTC().Format(time.RFC3339),
			"command": "go test -json -count=1 ./test/chaos | go run ./bench/chaosjson"},
		"scenarios": list,
		// Agents and SDKs run inside the test binary: any panic would print
		// "panic:" and abort the run. Failing scenarios are reported too.
		"client_crashes":                   panics,
		"failed_scenarios":                 failed,
		"expected_initialization_failures": 1,
		"state_machine_statement_coverage": *coverage,
		"notes":                            "Client crashes counts panics or fatal errors from any agent, SDK or test goroutine in the chaos run. The one expected initialization failure is the typed sdk.InitError on a cold start with neither cache nor bootstrap (sdk TestColdStartNoCache), which is the documented behaviour rather than a crash.",
	}
	b, _ := json.MarshalIndent(doc, "", "  ")
	if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s: %d scenarios, %d failed, %d crashes\n", *out, len(list), failed, panics)
	if failed > 0 || panics > 0 {
		os.Exit(1)
	}
}
