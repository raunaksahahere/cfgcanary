package auth

import (
	"crypto/x509"
	"net/url"
	"testing"
)

func cert(uris ...string) *x509.Certificate {
	c := &x509.Certificate{}
	for _, u := range uris {
		p, _ := url.Parse(u)
		c.URIs = append(c.URIs, p)
	}
	return c
}

func TestParse(t *testing.T) {
	td := "cfgcanary.local"
	ok := map[string]Identity{
		"spiffe://cfgcanary.local/env/prod/agent/agent-007": {Kind: KindAgent, Environment: "prod", Name: "agent-007"},
		"spiffe://cfgcanary.local/env/prod/group/ring1":     {Kind: KindGroup, Environment: "prod", Name: "ring1"},
		"spiffe://cfgcanary.local/user/alice":               {Kind: KindUser, Name: "alice"},
		"spiffe://cfgcanary.local/service/k8s":              {Kind: KindService, Name: "k8s"},
	}
	for u, want := range ok {
		got, err := Parse(cert(u), td)
		if err != nil || got.Kind != want.Kind || got.Name != want.Name || got.Environment != want.Environment {
			t.Errorf("%s: %+v %v", u, got, err)
		}
	}
	bad := [][]string{
		{},
		{"spiffe://other.domain/user/alice"},
		{"spiffe://cfgcanary.local/user/alice", "spiffe://cfgcanary.local/user/bob"},
		{"spiffe://cfgcanary.local/admin"},
		{"spiffe://cfgcanary.local/env/PROD/agent/a"},
		{"spiffe://cfgcanary.local/env/prod/agent/../x"},
		{"spiffe://cfgcanary.local/env/prod/pod/a"},
		{"spiffe://cfgcanary.local:8443/user/alice"},
		{"https://cfgcanary.local/user/alice"},
	}
	for _, uris := range bad {
		if id, err := Parse(cert(uris...), td); err == nil {
			t.Errorf("%v accepted as %+v", uris, id)
		}
	}
}

func TestClaims(t *testing.T) {
	a := Identity{Kind: KindAgent, Name: "agent-1"}
	if !a.MayClaim("agent-1") || a.MayClaim("agent-2") || a.MayClaim("agent-1/x") {
		t.Fatal("agent claims")
	}
	g := Identity{Kind: KindGroup, Name: "ring2"}
	if !g.MayClaim("ring2/pod-abc") || g.MayClaim("ring2") || g.MayClaim("ring0/pod-abc") || g.MayClaim("ring2/a/b") || g.MayClaim("ring2/") {
		t.Fatal("group claims")
	}
}

func TestRBAC(t *testing.T) {
	p := &Policy{Subjects: map[string][]string{
		"spiffe://td/user/viewer": {"viewer"},
		"spiffe://td/user/op":     {"operator"},
		"spiffe://td/user/admin":  {"admin"},
		"spiffe://td/user/oncall": {"operator", "breakglass"},
		// Policy cannot grant roles to workloads.
		"spiffe://td/env/prod/agent/a": {"admin"},
	}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	az := NewAuthorizer(p)
	user := func(n string) Identity { return Identity{URI: "spiffe://td/user/" + n, Kind: KindUser, Name: n} }
	cases := []struct {
		id   Identity
		perm Permission
		want bool
	}{
		{user("viewer"), PermRead, true},
		{user("viewer"), PermOperate, false},
		{user("viewer"), PermBreakGlass, false},
		{user("op"), PermOperate, true},
		{user("op"), PermSubmit, false},
		{user("op"), PermBreakGlass, false},
		{user("admin"), PermAdmin, true},
		{user("admin"), PermBreakGlass, false},
		{user("oncall"), PermBreakGlass, true},
		{user("stranger"), PermRead, false},
		{Identity{URI: "spiffe://td/env/prod/agent/a", Kind: KindAgent, Name: "a"}, PermRead, false},
		{Identity{URI: "spiffe://td/env/prod/agent/a", Kind: KindAgent, Name: "a"}, PermBreakGlass, false},
	}
	for _, c := range cases {
		if got := az.Allowed(c.id, c.perm); got != c.want {
			t.Errorf("%s %s: got %v", c.id.URI, c.perm, got)
		}
	}
	if (&Policy{Subjects: map[string][]string{"spiffe://td/user/x": {"root"}}}).Validate() == nil {
		t.Fatal("unknown role accepted")
	}
}
