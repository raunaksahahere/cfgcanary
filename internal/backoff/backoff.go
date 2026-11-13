// Package backoff implements capped exponential backoff with full jitter
// (delay drawn uniformly from [0, min(cap, base*2^attempt))), as recommended
// in the AWS Architecture Blog post "Exponential Backoff And Jitter" and as
// called out as missing in Google Cloud's 2025-06-12 incident report.
//
// Randomness is injected so tests and benchmarks are reproducible.
package backoff

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Policy configures a backoff.
type Policy struct {
	Base time.Duration
	Cap  time.Duration
	// NoJitter disables jitter. Only the isolated reconnect-storm baseline
	// uses it; production code never sets it.
	NoJitter bool
}

// Backoff produces delays. It is safe for concurrent use.
type Backoff struct {
	p   Policy
	mu  sync.Mutex
	rng *rand.Rand
}

// New returns a Backoff drawing from rng (a fresh seeded source when nil).
func New(p Policy, rng *rand.Rand) *Backoff {
	if p.Base <= 0 {
		p.Base = 250 * time.Millisecond
	}
	if p.Cap <= 0 {
		p.Cap = 30 * time.Second
	}
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &Backoff{p: p, rng: rng}
}

// Ceiling returns the upper bound of the delay for an attempt (0-based).
func (b *Backoff) Ceiling(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	c := float64(b.p.Base) * math.Pow(2, float64(min(attempt, 40)))
	if c > float64(b.p.Cap) {
		c = float64(b.p.Cap)
	}
	return time.Duration(c)
}

// Delay returns the delay before retry number attempt (0-based).
func (b *Backoff) Delay(attempt int) time.Duration {
	c := b.Ceiling(attempt)
	if b.p.NoJitter {
		return c
	}
	b.mu.Lock()
	f := b.rng.Float64()
	b.mu.Unlock()
	return time.Duration(f * float64(c))
}

// Jitter returns d scaled by a uniform factor in [1-frac, 1+frac).
func (b *Backoff) Jitter(d time.Duration, frac float64) time.Duration {
	b.mu.Lock()
	f := b.rng.Float64()
	b.mu.Unlock()
	return time.Duration(float64(d) * (1 - frac + 2*frac*f))
}
