// Package ring assigns instances to rollout rings.
//
// Assignment is a pure function of the instance ID and a policy that is frozen
// into each release, so cohorts do not reshuffle on reconnect, restart or
// leader change. See docs/invariants.md (I2).
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// Buckets is the size of the hash space, in basis points.
const Buckets = 10000

// Holdout is the ring index reported for instances beyond the last ring.
// They receive the candidate only when the release completes.
const Holdout = int32(1 << 20)

// Policy is the frozen assignment policy of a release.
type Policy struct {
	Salt string
	// Cumulative thresholds in basis points, strictly increasing, <= Buckets.
	Thresholds []int32
	// Authenticated group -> ring index.
	Pinned map[string]int32
}

// Bucket returns the instance's position in the hash space.
//
// Version 1: big-endian uint64 of the first 8 bytes of
// SHA-256("cfgcanary/ring/v1" 0x00 salt 0x00 instance) modulo 10000.
func Bucket(salt, instance string) int32 {
	h := sha256.New()
	h.Write([]byte("cfgcanary/ring/v1"))
	h.Write([]byte{0})
	h.Write([]byte(salt))
	h.Write([]byte{0})
	h.Write([]byte(instance))
	return int32(binary.BigEndian.Uint64(h.Sum(nil)[:8]) % Buckets)
}

// Group returns the authenticated group of an instance ID ("" when none).
// Group instances are named "<group>/<suffix>"; the server only allows that
// form for certificates issued to the group.
func Group(instance string) string {
	if i := strings.IndexByte(instance, '/'); i > 0 {
		return instance[:i]
	}
	return ""
}

// Assign returns the ring index of an instance, or Holdout.
func (p Policy) Assign(instance string) int32 {
	if g := Group(instance); g != "" {
		if r, ok := p.Pinned[g]; ok {
			return r
		}
	}
	b := Bucket(p.Salt, instance)
	for i, t := range p.Thresholds {
		if b < t {
			return int32(i)
		}
	}
	return Holdout
}

// HasHoldout reports whether the last ring stops short of the whole fleet.
func (p Policy) HasHoldout() bool {
	return len(p.Thresholds) > 0 && p.Thresholds[len(p.Thresholds)-1] < Buckets
}

// Validate checks structural rules.
func (p Policy) Validate() error {
	if len(p.Thresholds) == 0 {
		return errors.New("at least one ring is required")
	}
	if len(p.Thresholds) > 16 {
		return errors.New("at most 16 rings")
	}
	prev := int32(0)
	for i, t := range p.Thresholds {
		if t <= prev || t > Buckets {
			return fmt.Errorf("ring %d: cumulative_bps %d must be greater than the previous ring (%d) and at most %d", i, t, prev, Buckets)
		}
		prev = t
	}
	for g, r := range p.Pinned {
		if g == "" || strings.ContainsRune(g, '/') {
			return fmt.Errorf("pinned group %q: invalid name", g)
		}
		if r < 0 || int(r) >= len(p.Thresholds) {
			return fmt.Errorf("pinned group %q: ring %d does not exist", g, r)
		}
	}
	return nil
}

// Sizes counts members per ring; the last element is the holdout.
func (p Policy) Sizes(instances []string) []int {
	out := make([]int, len(p.Thresholds)+1)
	for _, id := range instances {
		r := p.Assign(id)
		if r == Holdout {
			out[len(p.Thresholds)]++
		} else {
			out[r]++
		}
	}
	return out
}
