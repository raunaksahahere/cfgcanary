// Package ratelimits is the example consumer of a cfgcanary config: a service
// that reads feature rules, per-route rate limits and a dependency timeout.
//
// Parse is the single source of truth for what this consumer accepts. The
// sample application calls it through the SDK, and the WASM validator in
// ./validator calls the same function, so the control plane rejects exactly
// what the consumer would reject (the CrowdStrike Channel File 291 lesson).
//
// The package uses only the standard library so it compiles to WASI.
package ratelimits

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// MaxFeatures is a hard limit of this consumer: it preallocates a table of
// this size, like the 200-feature limit in Cloudflare's 2025-11-18 incident.
const MaxFeatures = 200

// Config is the parsed configuration.
type Config struct {
	SchemaVersion int       `json:"schema_version"`
	Service       string    `json:"service"`
	TimeoutMS     int       `json:"timeout_ms"`
	Features      []Feature `json:"features"`
	Limits        []Limit   `json:"limits"`
}

// Feature is one feature rule.
type Feature struct {
	Name           string `json:"name"`
	Enabled        bool   `json:"enabled"`
	RolloutPercent int    `json:"rollout_percent"`
	// Rule is a small expression: `key == value` or `key != value` terms
	// joined by `&&`. A JSON Schema can only say "string"; only this parser
	// knows whether it is well formed.
	Rule string `json:"rule,omitempty"`

	terms []term
}

type term struct {
	key, value string
	negate     bool
}

// Limit is a per-route rate limit.
type Limit struct {
	Route string `json:"route"`
	RPS   int    `json:"rps"`
	Burst int    `json:"burst"`
}

// Clone returns a deep copy, so SDK callers cannot mutate a shared snapshot.
func (c Config) Clone() Config {
	out := c
	out.Features = append([]Feature(nil), c.Features...)
	for i := range out.Features {
		out.Features[i].terms = append([]term(nil), c.Features[i].terms...)
	}
	out.Limits = append([]Limit(nil), c.Limits...)
	return out
}

// Parse decodes and checks a payload exactly as the running service does.
func Parse(data []byte) (Config, error) {
	var c Config
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return Config{}, errors.New("decode: trailing data")
	}
	if c.SchemaVersion != 1 {
		return Config{}, fmt.Errorf("schema_version %d is not supported by this consumer (supports 1)", c.SchemaVersion)
	}
	if strings.TrimSpace(c.Service) == "" {
		return Config{}, errors.New("service is blank")
	}
	if c.TimeoutMS < 1 || c.TimeoutMS > 60000 {
		return Config{}, fmt.Errorf("timeout_ms %d is outside 1..60000", c.TimeoutMS)
	}
	if len(c.Features) > MaxFeatures {
		return Config{}, fmt.Errorf("%d features; this consumer holds at most %d", len(c.Features), MaxFeatures)
	}
	seen := make(map[string]struct{}, len(c.Features))
	for i := range c.Features {
		f := &c.Features[i]
		if strings.TrimSpace(f.Name) == "" {
			return Config{}, fmt.Errorf("features[%d].name is blank", i)
		}
		if _, dup := seen[f.Name]; dup {
			return Config{}, fmt.Errorf("features[%d]: duplicate feature name", i)
		}
		seen[f.Name] = struct{}{}
		if f.RolloutPercent < 0 || f.RolloutPercent > 100 {
			return Config{}, fmt.Errorf("features[%d].rollout_percent is outside 0..100", i)
		}
		terms, err := parseRule(f.Rule)
		if err != nil {
			return Config{}, fmt.Errorf("features[%d].rule: %w", i, err)
		}
		f.terms = terms
	}
	routes := make(map[string]struct{}, len(c.Limits))
	for i, l := range c.Limits {
		if !strings.HasPrefix(l.Route, "/") {
			return Config{}, fmt.Errorf("limits[%d].route must start with /", i)
		}
		if _, dup := routes[l.Route]; dup {
			return Config{}, fmt.Errorf("limits[%d]: duplicate route", i)
		}
		routes[l.Route] = struct{}{}
		if l.RPS < 1 || l.Burst < 1 || l.Burst > l.RPS*10 {
			return Config{}, fmt.Errorf("limits[%d]: rps and burst must be positive and burst at most 10x rps", i)
		}
	}
	return c, nil
}

func parseRule(rule string) ([]term, error) {
	if rule == "" {
		return nil, nil
	}
	var out []term
	for _, part := range strings.Split(rule, "&&") {
		fields := strings.Fields(part)
		if len(fields) != 3 || (fields[1] != "==" && fields[1] != "!=") {
			return nil, errors.New("expected `key == value` or `key != value` terms joined by &&")
		}
		for _, r := range fields[0] + fields[2] {
			if !(r == '_' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
				return nil, errors.New("keys and values may contain only letters, digits, _ and -")
			}
		}
		out = append(out, term{key: fields[0], value: fields[2], negate: fields[1] == "!="})
	}
	return out, nil
}

// Enabled evaluates a feature for request attributes.
func (c Config) Enabled(name string, attrs map[string]string) bool {
	for _, f := range c.Features {
		if f.Name != name {
			continue
		}
		if !f.Enabled {
			return false
		}
		for _, t := range f.terms {
			if (attrs[t.key] == t.value) == t.negate {
				return false
			}
		}
		return true
	}
	return false
}
