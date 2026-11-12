package ring

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestBucketIsPinnedToV1(t *testing.T) {
	// Golden values: changing the hash would silently reshuffle every fleet.
	for id, want := range map[string]int32{"agent-001": Bucket("limits/prod", "agent-001")} {
		for range 3 {
			if got := Bucket("limits/prod", id); got != want {
				t.Fatalf("unstable bucket for %s", id)
			}
		}
	}
	if a, b := Bucket("s", "ab"), Bucket("sa", "b"); a == b && Bucket("s\x00a", "b") == a {
		t.Fatal("salt and instance are not domain separated")
	}
	if got := Bucket("limits/prod", "agent-001"); got != 7411 {
		t.Fatalf("ring hash v1 changed: agent-001 -> %d", got)
	}
}

func TestProperty_DisjointCumulative(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for trial := range 200 {
		n := 1 + r.IntN(5)
		th := make([]int32, 0, n)
		cur := int32(0)
		for i := 0; i < n && cur < Buckets; i++ {
			cur += 1 + int32(r.IntN(int(Buckets-cur)))
			th = append(th, cur)
		}
		p := Policy{Salt: fmt.Sprint("salt", trial), Thresholds: th}
		if err := p.Validate(); err != nil {
			t.Fatal(err)
		}
		for i := range 300 {
			id := fmt.Sprint("i-", i)
			got := p.Assign(id)
			b := Bucket(p.Salt, id)
			// Monotone exposure: an instance in ring k is exposed at every
			// active ring >= k and never below it.
			for k, t2 := range th {
				exposed := got != Holdout && got <= int32(k)
				if exposed != (b < t2) {
					t.Fatalf("instance %s bucket %d ring %d: exposure at ring %d inconsistent", id, b, got, k)
				}
			}
			if got != p.Assign(id) {
				t.Fatal("assignment not stable")
			}
		}
	}
}

func TestPinnedGroupOverridesHash(t *testing.T) {
	p := Policy{Salt: "s", Thresholds: []int32{1000, 5000, 10000}, Pinned: map[string]int32{"ring2": 2}}
	for i := range 50 {
		if got := p.Assign(fmt.Sprint("ring2/pod-", i)); got != 2 {
			t.Fatalf("pinned instance landed in ring %d", got)
		}
	}
	if Group("plain-agent") != "" {
		t.Fatal("plain instance has a group")
	}
}

func TestSmallFleetCanHaveEmptyCanary(t *testing.T) {
	// A 1% ring over 20 instances is usually empty; Sizes makes that visible.
	p := Policy{Salt: "s", Thresholds: []int32{100, 10000}}
	ids := make([]string, 20)
	for i := range ids {
		ids[i] = fmt.Sprint("a", i)
	}
	sz := p.Sizes(ids)
	if sz[0]+sz[1]+sz[2] != 20 || sz[2] != 0 {
		t.Fatalf("sizes do not add up: %v", sz)
	}
}

func TestValidate(t *testing.T) {
	for _, bad := range []Policy{{}, {Thresholds: []int32{0}}, {Thresholds: []int32{500, 500}}, {Thresholds: []int32{10001}},
		{Thresholds: []int32{100}, Pinned: map[string]int32{"g": 1}}, {Thresholds: []int32{100}, Pinned: map[string]int32{"a/b": 0}}} {
		if bad.Validate() == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}
