package backoff

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestFullJitterBounds(t *testing.T) {
	b := New(Policy{Base: 100 * time.Millisecond, Cap: 2 * time.Second}, rand.New(rand.NewPCG(1, 1)))
	for attempt := range 12 {
		c := b.Ceiling(attempt)
		var sum time.Duration
		for range 2000 {
			d := b.Delay(attempt)
			if d < 0 || d >= c {
				t.Fatalf("attempt %d: delay %s outside [0,%s)", attempt, d, c)
			}
			sum += d
		}
		// Uniform on [0,c): mean c/2 within 10%.
		mean := sum / 2000
		if mean < c*45/100 || mean > c*55/100 {
			t.Fatalf("attempt %d: mean %s for ceiling %s", attempt, mean, c)
		}
	}
	if b.Ceiling(50) != 2*time.Second {
		t.Fatal("cap not applied")
	}
}

func TestDeterministicWithSeed(t *testing.T) {
	a := New(Policy{}, rand.New(rand.NewPCG(7, 7)))
	b := New(Policy{}, rand.New(rand.NewPCG(7, 7)))
	for i := range 20 {
		if a.Delay(i) != b.Delay(i) {
			t.Fatal("same seed, different delays")
		}
	}
	nj := New(Policy{Base: time.Second, Cap: 4 * time.Second, NoJitter: true}, nil)
	if nj.Delay(1) != 2*time.Second || nj.Delay(9) != 4*time.Second {
		t.Fatal("no-jitter baseline")
	}
}
