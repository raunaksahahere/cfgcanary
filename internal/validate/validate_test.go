package validate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/examples/ratelimits"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func examplePolicy(t testing.TB) *pb.ValidationPolicy {
	return &pb.ValidationPolicy{
		Limits:     &pb.Limits{MaxBytes: 64 << 10, MaxItems: map[string]int64{"/features": 200, "/limits": 100}},
		JsonSchema: string(testutil.ReadRepoFile(t, "examples/ratelimits/schema.json")),
		Cue:        string(testutil.ReadRepoFile(t, "examples/ratelimits/policy.cue")),
		Relative:   &pb.RelativeGuards{MaxSizeRatio: 2, MaxItemsRatio: map[string]float64{"/features": 2}, MinItemsRatio: map[string]float64{"/features": 0.5}},
	}
}

func payload(features int, mutate func(m map[string]any)) []byte {
	fs := make([]any, features)
	for i := range fs {
		fs[i] = map[string]any{"name": fmt.Sprint("feature-", i), "enabled": true, "rollout_percent": 10, "rule": "region == eu"}
	}
	m := map[string]any{
		"schema_version": 1, "service": "checkout", "timeout_ms": 250, "features": fs,
		"limits": []any{map[string]any{"route": "/api/pay", "rps": 100, "burst": 200}},
	}
	if mutate != nil {
		mutate(m)
	}
	b, _ := json.Marshal(m)
	return b
}

func codes(r *pb.ValidationReport) string {
	var out []string
	for _, i := range r.Issues {
		out = append(out, i.Stage+"/"+i.Code)
	}
	return strings.Join(out, ",")
}

func TestPipelineRejections(t *testing.T) {
	wasm := testutil.BuildWasm(t, "examples/ratelimits/validator")
	sum := sha256.Sum256(wasm)
	pol := examplePolicy(t)
	pol.Wasm = &pb.WasmValidator{Sha256: hex.EncodeToString(sum[:]), Required: true}
	v := New(1<<20, NewSandbox(2))
	base := payload(20, nil)

	cases := []struct {
		name    string
		payload []byte
		base    []byte
		want    string // stage/code prefix of the first issue; "" = accepted
	}{
		{"valid", payload(20, nil), base, ""},
		{"valid first version", payload(150, nil), nil, ""},
		{"oversized", append(payload(20, nil), make([]byte, 70<<10)...), base, "limits/too_large"},
		{"malformed", []byte(`{"schema_version": 1,`), base, "json/syntax"},
		{"trailing data", append(payload(5, nil), []byte(` {}`)...), base, "json/trailing_data"},
		{"duplicate key", []byte(`{"schema_version":1,"schema_version":2}`), base, "json/duplicate_key"},
		{"invalid utf8", []byte("{\"service\":\"\xff\"}"), base, "json/invalid_utf8"},
		{"too deep", []byte(strings.Repeat("[", 100) + strings.Repeat("]", 100)), base, "json/too_deep"},
		{"over cardinality", payload(201, nil), nil, "limits/too_many_items"},
		{"blank required field", payload(20, func(m map[string]any) { m["service"] = "" }), base, "schema/"},
		{"whitespace-only field", payload(20, func(m map[string]any) { m["service"] = "   " }), base, "schema/"},
		{"missing required field", payload(20, func(m map[string]any) { delete(m, "timeout_ms") }), base, "schema/"},
		{"wrong type", payload(20, func(m map[string]any) { m["timeout_ms"] = "250" }), base, "schema/"},
		{"cue cross-field", payload(20, func(m map[string]any) {
			m["limits"] = []any{map[string]any{"route": "/a", "rps": 1, "burst": 500}}
		}), base, "cue/constraint"},
		{"doubled in size (Cloudflare 2025-11-18 shape)", payload(45, nil), base, "relative/size_grew"},
		{"items shrank", payload(5, nil), base, "relative/items_shrank"},
		// Schema says rule is any string; only the consumer's parser knows
		// the expression grammar.
		{"schema-valid, consumer-parser-invalid", payload(20, func(m map[string]any) {
			m["features"].([]any)[3].(map[string]any)["rule"] = "region =~ eu"
		}), base, "wasm/rejected"},
		{"schema-valid, unsupported schema_version", payload(20, func(m map[string]any) { m["schema_version"] = 2 }), base, "wasm/rejected"},
	}
	for _, c := range cases {
		rep := v.Validate(context.Background(), Input{Policy: pol, Payload: c.payload, Base: c.base, BaseVersion: 1, WasmModule: wasm})
		got := codes(rep)
		if c.want == "" {
			if !rep.Ok {
				t.Errorf("%s: rejected: %s %v", c.name, got, rep.Issues)
			}
			continue
		}
		if rep.Ok || !strings.HasPrefix(got, c.want) {
			t.Errorf("%s: want %s, got ok=%v %q", c.name, c.want, rep.Ok, got)
		}
	}
}

// The loose schema accepts what the consumer rejects. Without the WASM stage
// the pipeline passes it; with it, the pipeline stops it. This is the
// validator/consumer mismatch the sandbox exists for.
func TestConsumerParserClosesSchemaGap(t *testing.T) {
	bad := payload(3, func(m map[string]any) { m["features"].([]any)[0].(map[string]any)["rule"] = "a b c d" })
	if _, err := ratelimits.Parse(bad); err == nil {
		t.Fatal("fixture is not consumer-invalid")
	}
	pol := examplePolicy(t)
	v := New(1<<20, NewSandbox(1))
	if rep := v.Validate(context.Background(), Input{Policy: pol, Payload: bad}); !rep.Ok {
		t.Fatalf("generic checks were expected to pass this payload: %s", codes(rep))
	}
	wasm := testutil.BuildWasm(t, "examples/ratelimits/validator")
	pol.Wasm = &pb.WasmValidator{Sha256: "set"}
	if rep := v.Validate(context.Background(), Input{Policy: pol, Payload: bad, WasmModule: wasm}); rep.Ok {
		t.Fatal("consumer-invalid payload passed the consumer-parser stage")
	}
	// A required validator that is unavailable rejects.
	if rep := v.Validate(context.Background(), Input{Policy: pol, Payload: payload(3, nil)}); rep.Ok || codes(rep) != "wasm/unavailable" {
		t.Fatalf("missing module: %s", codes(rep))
	}
}

func TestSandboxFailuresReject(t *testing.T) {
	sb := NewSandbox(2)
	defer sb.Close(context.Background())
	short := &pb.WasmValidator{Timeout: durationpb.New(300 * time.Millisecond), MemoryLimitMib: 32}
	cases := []struct {
		pkg  string
		want error
	}{
		{"internal/validate/testdata/loop", ErrWasmTimeout},
		{"internal/validate/testdata/trap", ErrWasmTrap},
		{"internal/validate/testdata/bigout", ErrWasmOutput},
		{"internal/validate/testdata/garbage", ErrWasmOutput},
	}
	for _, c := range cases {
		start := time.Now()
		_, err := sb.Run(context.Background(), testutil.BuildWasm(t, c.pkg), short, []byte(`{}`))
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.pkg, err, c.want)
		}
		if d := time.Since(start); d > 20*time.Second {
			t.Errorf("%s: took %s; the sandbox did not bound it", c.pkg, d)
		}
	}
	// The memory ceiling, not the deadline, must stop an allocation bomb: give
	// it plenty of time and a small ceiling.
	hog := &pb.WasmValidator{Timeout: durationpb.New(25 * time.Second), MemoryLimitMib: 24}
	if _, err := sb.Run(context.Background(), testutil.BuildWasm(t, "internal/validate/testdata/memhog"), hog, []byte(`{}`)); !errors.Is(err, ErrWasmTrap) {
		t.Errorf("memhog: got %v, want a trap at the memory ceiling", err)
	}
	if _, err := sb.Run(context.Background(), []byte("not wasm"), short, nil); !errors.Is(err, ErrWasmModule) {
		t.Errorf("garbage module: %v", err)
	}
}

func TestSandboxEscapeAttempts(t *testing.T) {
	t.Setenv("CFGCANARY_SECRET", "must-not-leak")
	sb := NewSandbox(1)
	defer sb.Close(context.Background())
	res, err := sb.Run(context.Background(), testutil.BuildWasm(t, "internal/validate/testdata/escape"), nil, []byte(`{}`))
	if err != nil {
		t.Fatalf("escape module did not run: %v", err)
	}
	if !res.OK {
		t.Fatalf("validator reached outside the sandbox: %v", res.Errors)
	}
}

func TestRelativeGuardBaseHandling(t *testing.T) {
	v := New(0, nil)
	pol := &pb.ValidationPolicy{Limits: &pb.Limits{MaxBytes: 1 << 20}, Relative: &pb.RelativeGuards{MaxSizeRatio: 2, MaxItemsRatio: map[string]float64{"/features": 2}}}
	// Empty baseline list: no ratio is defined, so absolute limits decide.
	empty := payload(0, nil)
	if rep := v.Validate(context.Background(), Input{Policy: pol, Payload: payload(1, nil), Base: empty, BaseVersion: 1}); !rep.Ok {
		t.Fatalf("growth from an empty list rejected: %s", codes(rep))
	}
	rep := v.Validate(context.Background(), Input{Policy: pol, Payload: payload(3, nil)})
	if !rep.Ok || rep.BaseVersion != 0 {
		t.Fatalf("first version: ok=%v base=%d", rep.Ok, rep.BaseVersion)
	}
	if rep := v.Validate(context.Background(), Input{Policy: pol, Payload: payload(9, nil), Base: payload(4, nil), BaseVersion: 2}); rep.Ok {
		t.Fatal("2.25x item growth accepted")
	}
}

func TestCheckPolicy(t *testing.T) {
	if err := CheckPolicy(examplePolicy(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	bad := []*pb.ValidationPolicy{
		nil,
		{Limits: &pb.Limits{}},
		{Limits: &pb.Limits{MaxBytes: 2 << 20}},
		{Limits: &pb.Limits{MaxBytes: 10}, JsonSchema: `{"type": 12}`},
		{Limits: &pb.Limits{MaxBytes: 10}, JsonSchema: `{"$ref": "https://example.com/x.json"}`},
		{Limits: &pb.Limits{MaxBytes: 10}, Cue: `a: {`},
		{Limits: &pb.Limits{MaxBytes: 10, MaxItems: map[string]int64{"features": 1}}},
		{Limits: &pb.Limits{MaxBytes: 10}, Relative: &pb.RelativeGuards{MaxSizeRatio: 0.5}},
	}
	for i, p := range bad {
		if CheckPolicy(p, 1<<20) == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

func TestSelectAndDiff(t *testing.T) {
	doc, _ := ParseStrict([]byte(`{"t":{"a":{"r":[1,2]},"b":{"r":[3]}},"x/y":[0]}`), 0)
	ms, _ := Select(doc, "/t/*/r")
	if len(ms) != 2 || Items(ms[0].Value)+Items(ms[1].Value) != 3 {
		t.Fatalf("wildcard: %+v", ms)
	}
	if ms, _ := Select(doc, "/x~1y"); len(ms) != 1 {
		t.Fatal("escaped pointer")
	}
	a, _ := ParseStrict([]byte(`{"a":1,"b":[1,2],"c":{"d":true}}`), 0)
	b, _ := ParseStrict([]byte(`{"a":2,"b":[1],"c":{"d":true,"e":null}}`), 0)
	ch, total := Diff(a, b, 10)
	if total != 3 || ch[0].Op != "changed" || ch[1].Op != "removed" || ch[2].Op != "added" {
		t.Fatalf("diff: %+v", ch)
	}
}

func FuzzParseStrict(f *testing.F) {
	for _, s := range []string{`{}`, `[1,2,{"a":null}]`, `{"a":1,"a":2}`, `"x"`, `[[[[[[[[`, "\xff", `1 2`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := ParseStrict(data, 16)
		if err != nil {
			return
		}
		// Anything accepted must be accepted by encoding/json and survive a
		// round trip.
		var std any
		if json.Unmarshal(data, &std) != nil {
			t.Fatalf("ParseStrict accepted what encoding/json rejects: %q", data)
		}
		if _, err := json.Marshal(v); err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzValidate(f *testing.F) {
	f.Add(payload(2, nil))
	f.Add([]byte(`{"features": [], "service": ""}`))
	pol := &pb.ValidationPolicy{
		Limits:     &pb.Limits{MaxBytes: 4096, MaxItems: map[string]int64{"/features": 3, "/*": 5}},
		JsonSchema: `{"type":"object","properties":{"service":{"type":"string","minLength":1}}}`,
		Cue:        `timeout_ms?: int & >0`,
		Relative:   &pb.RelativeGuards{MaxSizeRatio: 2, MaxItemsRatio: map[string]float64{"/features": 2}},
	}
	v := New(0, nil)
	base := payload(1, nil)
	f.Fuzz(func(t *testing.T, data []byte) {
		rep := v.Validate(context.Background(), Input{Policy: pol, Payload: data, Base: base, BaseVersion: 1})
		if rep.Ok && (len(data) > 4096 || !json.Valid(data)) {
			t.Fatalf("accepted an oversized or invalid payload")
		}
		if !rep.Ok && len(rep.Issues) == 0 {
			t.Fatal("rejected without an issue")
		}
	})
}
