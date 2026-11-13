// Package validate is the submission pipeline: everything that can be known
// about a payload before any agent sees it.
//
// Stages run cheapest first and each stage only runs if the earlier ones
// passed: limits (bytes, well-formed strict JSON, depth, cardinality), JSON
// Schema, CUE, relative-change guards against the correct base, and finally
// the consumer's own parser inside a WASM sandbox.
package validate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	cueerrors "cuelang.org/go/cue/errors"
	"github.com/santhosh-tekuri/jsonschema/v6"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

// MaxIssues caps the number of issues in a report.
const MaxIssues = 50

// Validator runs the pipeline. It caches compiled schemas and WASM modules.
type Validator struct {
	// HardMaxBytes is the server-wide ceiling regardless of policy.
	HardMaxBytes int64
	Sandbox      *Sandbox

	mu      sync.Mutex
	schemas map[string]*jsonschema.Schema
}

// New returns a Validator with the given server-wide size ceiling.
func New(hardMaxBytes int64, sb *Sandbox) *Validator {
	return &Validator{HardMaxBytes: hardMaxBytes, Sandbox: sb, schemas: map[string]*jsonschema.Schema{}}
}

// Input is one validation request.
type Input struct {
	Policy  *pb.ValidationPolicy
	Payload []byte
	// Base is the payload of the version relative guards compare against:
	// the target's baseline, or the latest version when nothing is released.
	// Nil means this is the first version; relative guards are then skipped
	// and the report says so.
	Base        []byte
	BaseVersion int64
	// WasmModule is the validator module named by Policy.Wasm.
	WasmModule []byte
}

type report struct{ r *pb.ValidationReport }

func (r report) add(stage, code, path, format string, a ...any) {
	if len(r.r.Issues) < MaxIssues {
		r.r.Issues = append(r.r.Issues, &pb.ValidationIssue{Stage: stage, Code: code, Path: path, Message: fmt.Sprintf(format, a...)})
	}
}

// Validate runs the pipeline. It never returns a Go error for a bad payload:
// problems are in the report. Messages describe structure, never values.
func (v *Validator) Validate(ctx context.Context, in Input) *pb.ValidationReport {
	rep := report{&pb.ValidationReport{BaseVersion: in.BaseVersion}}
	done := func() *pb.ValidationReport {
		rep.r.Ok = len(rep.r.Issues) == 0
		return rep.r
	}
	pol := in.Policy
	if pol == nil || pol.Limits == nil || pol.Limits.MaxBytes <= 0 {
		rep.add("limits", "no_policy", "", "the config has no size limit; refusing to validate")
		return done()
	}

	// Stage 1: limits. Size is checked before the bytes are parsed.
	rep.r.StagesRun = append(rep.r.StagesRun, "limits")
	max := pol.Limits.MaxBytes
	if v.HardMaxBytes > 0 && max > v.HardMaxBytes {
		max = v.HardMaxBytes
	}
	if int64(len(in.Payload)) > max {
		rep.add("limits", "too_large", "", "payload is %d bytes; the limit is %d", len(in.Payload), max)
		return done()
	}
	doc, err := ParseStrict(in.Payload, int(pol.Limits.MaxDepth))
	if err != nil {
		var pe *ParseError
		errors.As(err, &pe)
		rep.add("json", pe.Code, pe.Path, "%s", pe.Msg)
		return done()
	}
	for _, ptr := range sortedKeys(pol.Limits.MaxItems) {
		limit := pol.Limits.MaxItems[ptr]
		ms, err := Select(doc, ptr)
		if err != nil {
			rep.add("limits", "bad_pointer", ptr, "%v", err)
			continue
		}
		for _, m := range ms {
			if n := Items(m.Value); n > limit {
				rep.add("limits", "too_many_items", m.Path, "%d items; the limit is %d", n, limit)
			}
		}
	}
	if len(rep.r.Issues) > 0 {
		return done()
	}

	// Stage 2: JSON Schema.
	if pol.JsonSchema != "" {
		rep.r.StagesRun = append(rep.r.StagesRun, "schema")
		sch, err := v.schema(pol.JsonSchema)
		if err != nil {
			rep.add("schema", "bad_schema", "", "the config's JSON Schema does not compile: %v", err)
			return done()
		}
		if err := sch.Validate(doc); err != nil {
			var ve *jsonschema.ValidationError
			if errors.As(err, &ve) {
				flattenSchema(rep, ve.BasicOutput())
			} else {
				rep.add("schema", "error", "", "%v", err)
			}
			return done()
		}
	}

	// Stage 3: CUE.
	if pol.Cue != "" {
		rep.r.StagesRun = append(rep.r.StagesRun, "cue")
		if !checkCUE(rep, pol.Cue, in.Payload) {
			return done()
		}
	}

	// Stage 4: relative change against the base.
	if g := pol.Relative; g != nil {
		rep.r.StagesRun = append(rep.r.StagesRun, "relative")
		if in.Base == nil {
			// First version: there is nothing to be relative to. Absolute
			// limits above are the only bound, and the report records that
			// by leaving base_version at 0.
		} else if !checkRelative(rep, g, doc, in.Payload, in.Base, int(pol.Limits.MaxDepth)) {
			return done()
		}
	}

	// Stage 5: the consumer's parser, sandboxed.
	if w := pol.Wasm; w != nil && w.Sha256 != "" {
		rep.r.StagesRun = append(rep.r.StagesRun, "wasm")
		if v.Sandbox == nil || len(in.WasmModule) == 0 {
			rep.add("wasm", "unavailable", "", "the required consumer-parser validator is not available")
			return done()
		}
		res, err := v.Sandbox.Run(ctx, in.WasmModule, w, in.Payload)
		if err != nil {
			rep.add("wasm", sandboxCode(err), "", "consumer-parser validator did not complete: %v", err)
			return done()
		}
		if !res.OK {
			if len(res.Errors) == 0 {
				rep.add("wasm", "rejected", "", "the consumer parser rejected the payload")
			}
			for _, e := range res.Errors {
				rep.add("wasm", "rejected", "", "%s", e)
			}
		}
	}
	return done()
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type denyLoader struct{}

func (denyLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external schema references are not allowed (%s)", url)
}

// CompileSchema compiles a JSON Schema document without network or file
// access for $ref.
func CompileSchema(src string) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(src))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	c.UseLoader(denyLoader{})
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource("urn:cfgcanary:schema", doc); err != nil {
		return nil, err
	}
	return c.Compile("urn:cfgcanary:schema")
}

func (v *Validator) schema(src string) (*jsonschema.Schema, error) {
	sum := sha256.Sum256([]byte(src))
	key := hex.EncodeToString(sum[:])
	v.mu.Lock()
	s, ok := v.schemas[key]
	v.mu.Unlock()
	if ok {
		return s, nil
	}
	s, err := CompileSchema(src)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	if len(v.schemas) > 256 {
		v.schemas = map[string]*jsonschema.Schema{}
	}
	v.schemas[key] = s
	v.mu.Unlock()
	return s, nil
}

func flattenSchema(rep report, u *jsonschema.OutputUnit) {
	if u == nil {
		return
	}
	if u.Error != nil && len(u.Errors) == 0 {
		msg := u.Error.String()
		// The library quotes offending values for some keywords; keep the
		// keyword and location, drop anything after the first value quote.
		rep.add("schema", lastSegment(u.KeywordLocation), u.InstanceLocation, "%s", scrub(msg))
	}
	for i := range u.Errors {
		flattenSchema(rep, &u.Errors[i])
	}
}

func lastSegment(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

func scrub(s string) string {
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// CheckCUE verifies that src compiles as CUE constraints.
func CheckCUE(src string) error {
	return cuecontext.New().CompileString(src).Err()
}

func checkCUE(rep report, src string, payload []byte) bool {
	cctx := cuecontext.New()
	schema := cctx.CompileString(src, cue.Filename("policy.cue"))
	if err := schema.Err(); err != nil {
		rep.add("cue", "bad_policy", "", "the config's CUE policy does not compile: %v", err)
		return false
	}
	// Strict JSON is a subset of CUE, and the payload already passed
	// ParseStrict.
	val := cctx.CompileBytes(payload, cue.Filename("payload.json"))
	if err := val.Err(); err != nil {
		rep.add("cue", "error", "", "payload could not be loaded: %v", err)
		return false
	}
	if err := schema.Unify(val).Validate(cue.Concrete(true)); err != nil {
		for _, e := range cueerrors.Errors(err) {
			path := "/" + strings.Join(e.Path(), "/")
			if len(e.Path()) == 0 {
				path = ""
			}
			format, args := e.Msg()
			rep.add("cue", "constraint", path, "%s", scrub(fmt.Sprintf(format, args...)))
		}
		return false
	}
	return true
}

func checkRelative(rep report, g *pb.RelativeGuards, doc any, payload, base []byte, maxDepth int) bool {
	before := len(rep.r.Issues)
	bs, ns := float64(len(base)), float64(len(payload))
	if g.MaxSizeRatio > 0 && bs > 0 && ns > g.MaxSizeRatio*bs {
		rep.add("relative", "size_grew", "", "size grew from %d to %d bytes (x%.2f); the limit is x%.2f", len(base), len(payload), ns/bs, g.MaxSizeRatio)
	}
	if g.MinSizeRatio > 0 && ns < g.MinSizeRatio*bs {
		rep.add("relative", "size_shrank", "", "size shrank from %d to %d bytes; the floor is x%.2f", len(base), len(payload), g.MinSizeRatio)
	}
	if len(g.MaxItemsRatio)+len(g.MinItemsRatio) > 0 {
		baseDoc, err := ParseStrict(base, maxDepth)
		if err != nil {
			// The base was valid when it was stored; if it no longer parses
			// under the current limits we cannot compare, and must not pass.
			rep.add("relative", "base_unreadable", "", "the base version cannot be parsed under the current limits")
			return false
		}
		count := func(root any, ptr string) (int64, bool) {
			ms, err := Select(root, ptr)
			if err != nil || len(ms) == 0 {
				return 0, false
			}
			var n int64
			for _, m := range ms {
				if c := Items(m.Value); c > 0 {
					n += c
				}
			}
			return n, true
		}
		for _, ptr := range sortedKeys(g.MaxItemsRatio) {
			b, bok := count(baseDoc, ptr)
			n, _ := count(doc, ptr)
			// An empty or absent base has no ratio; absolute limits apply.
			if bok && b > 0 && float64(n) > g.MaxItemsRatio[ptr]*float64(b) {
				rep.add("relative", "items_grew", ptr, "items grew from %d to %d; the limit is x%.2f", b, n, g.MaxItemsRatio[ptr])
			}
		}
		for _, ptr := range sortedKeys(g.MinItemsRatio) {
			b, bok := count(baseDoc, ptr)
			n, _ := count(doc, ptr)
			if bok && b > 0 && float64(n) < g.MinItemsRatio[ptr]*float64(b) {
				rep.add("relative", "items_shrank", ptr, "items shrank from %d to %d; the floor is x%.2f", b, n, g.MinItemsRatio[ptr])
			}
		}
	}
	return len(rep.r.Issues) == before
}

// CheckPolicy validates a policy at registration time so that a broken policy
// is rejected when it is written, not when a payload is submitted.
func CheckPolicy(p *pb.ValidationPolicy, hardMaxBytes int64) error {
	if p == nil || p.Limits == nil || p.Limits.MaxBytes <= 0 {
		return errors.New("validation.limits.max_bytes is required")
	}
	if hardMaxBytes > 0 && p.Limits.MaxBytes > hardMaxBytes {
		return fmt.Errorf("validation.limits.max_bytes %d exceeds the server limit %d", p.Limits.MaxBytes, hardMaxBytes)
	}
	if p.Limits.MaxDepth < 0 || p.Limits.MaxDepth > 512 {
		return errors.New("validation.limits.max_depth must be between 0 and 512")
	}
	for ptr, n := range p.Limits.MaxItems {
		if _, err := Select(map[string]any{}, ptr); err != nil || n < 0 {
			return fmt.Errorf("validation.limits.max_items[%q] is invalid", ptr)
		}
	}
	if p.JsonSchema != "" {
		if _, err := CompileSchema(p.JsonSchema); err != nil {
			return fmt.Errorf("validation.json_schema: %w", err)
		}
	}
	if p.Cue != "" {
		if err := CheckCUE(p.Cue); err != nil {
			return fmt.Errorf("validation.cue: %w", err)
		}
	}
	if g := p.Relative; g != nil {
		if g.MaxSizeRatio < 0 || g.MinSizeRatio < 0 || (g.MaxSizeRatio > 0 && g.MaxSizeRatio < 1) || g.MinSizeRatio > 1 {
			return errors.New("validation.relative: max_size_ratio must be >= 1 and min_size_ratio <= 1")
		}
		for ptr, r := range g.MaxItemsRatio {
			if _, err := Select(map[string]any{}, ptr); err != nil || r < 1 {
				return fmt.Errorf("validation.relative.max_items_ratio[%q] must be >= 1", ptr)
			}
		}
		for ptr, r := range g.MinItemsRatio {
			if _, err := Select(map[string]any{}, ptr); err != nil || r < 0 || r > 1 {
				return fmt.Errorf("validation.relative.min_items_ratio[%q] must be between 0 and 1", ptr)
			}
		}
	}
	return nil
}
