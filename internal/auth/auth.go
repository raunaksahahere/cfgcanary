// Package auth turns verified client certificates into identities and
// enforces role-based permissions on the server.
//
// Identity convention (SPIFFE-style URI SANs; full SPIRE is not required):
//
//	spiffe://<trust-domain>/env/<env>/agent/<instance>  one workload instance
//	spiffe://<trust-domain>/env/<env>/group/<group>     a group of instances; may
//	                                                    only claim "<group>/<suffix>"
//	spiffe://<trust-domain>/user/<name>                 a human operator
//	spiffe://<trust-domain>/service/<name>              an automation client
//
// A certificate must carry exactly one URI SAN in the configured trust domain.
// The TLS layer has already verified the chain, validity period and key usage.
package auth

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"
)

// Kind of identity.
type Kind string

const (
	KindAgent   Kind = "agent"
	KindGroup   Kind = "group"
	KindUser    Kind = "user"
	KindService Kind = "service"
)

// Identity is an authenticated caller.
type Identity struct {
	URI         string
	Kind        Kind
	Environment string // agents and groups
	Name        string // instance, group, user or service name
}

// Workload reports whether the identity may use the agent API.
func (i Identity) Workload() bool { return i.Kind == KindAgent || i.Kind == KindGroup }

// MayClaim reports whether an agent identity may use an instance ID.
func (i Identity) MayClaim(instance string) bool {
	switch i.Kind {
	case KindAgent:
		return instance == i.Name
	case KindGroup:
		rest, ok := strings.CutPrefix(instance, i.Name+"/")
		return ok && segment.MatchString(rest)
	}
	return false
}

// Group returns the group for group identities.
func (i Identity) Group() string {
	if i.Kind == KindGroup {
		return i.Name
	}
	return ""
}

var segment = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
var envName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,30}$`)

// ErrNoIdentity is returned when a certificate carries no usable identity.
var ErrNoIdentity = errors.New("client certificate has no cfgcanary identity")

// Parse extracts the identity from a verified leaf certificate.
func Parse(cert *x509.Certificate, trustDomain string) (Identity, error) {
	var found []*url.URL
	for _, u := range cert.URIs {
		if u.Scheme == "spiffe" && u.Host == trustDomain {
			found = append(found, u)
		}
	}
	if len(found) != 1 {
		return Identity{}, fmt.Errorf("%w: want exactly one spiffe://%s/... URI SAN, found %d", ErrNoIdentity, trustDomain, len(found))
	}
	u := found[0]
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Port() != "" {
		return Identity{}, fmt.Errorf("%w: malformed SPIFFE ID", ErrNoIdentity)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	id := Identity{URI: u.String()}
	switch {
	case len(parts) == 4 && parts[0] == "env" && (parts[2] == "agent" || parts[2] == "group"):
		if !envName.MatchString(parts[1]) || !segment.MatchString(parts[3]) || (parts[2] == "group" && strings.Contains(parts[3], "/")) {
			return Identity{}, fmt.Errorf("%w: invalid workload SPIFFE ID", ErrNoIdentity)
		}
		id.Kind, id.Environment, id.Name = Kind(parts[2]), parts[1], parts[3]
	case len(parts) == 2 && (parts[0] == "user" || parts[0] == "service"):
		if !segment.MatchString(parts[1]) {
			return Identity{}, fmt.Errorf("%w: invalid SPIFFE ID", ErrNoIdentity)
		}
		id.Kind, id.Name = Kind(parts[0]), parts[1]
	default:
		return Identity{}, fmt.Errorf("%w: unrecognized SPIFFE ID path %q", ErrNoIdentity, u.Path)
	}
	return id, nil
}

// Permission is something a caller may do on the control API.
type Permission string

const (
	PermRead       Permission = "read"
	PermSubmit     Permission = "submit"
	PermOperate    Permission = "operate"
	PermAdmin      Permission = "admin"
	PermBreakGlass Permission = "breakglass"
)

// Roles and the permissions they grant.
var Roles = map[string][]Permission{
	"viewer":     {PermRead},
	"submitter":  {PermRead, PermSubmit},
	"operator":   {PermRead, PermOperate},
	"releaser":   {PermRead, PermSubmit, PermOperate},
	"admin":      {PermRead, PermSubmit, PermOperate, PermAdmin},
	"breakglass": {PermRead, PermBreakGlass},
}

// Policy maps identities to roles.
type Policy struct {
	// Subjects maps a SPIFFE ID to roles.
	Subjects map[string][]string `json:"subjects"`
}

// Validate checks role names.
func (p *Policy) Validate() error {
	for s, roles := range p.Subjects {
		if !strings.HasPrefix(s, "spiffe://") {
			return fmt.Errorf("rbac: subject %q is not a SPIFFE ID", s)
		}
		for _, r := range roles {
			if _, ok := Roles[r]; !ok {
				return fmt.Errorf("rbac: subject %q has unknown role %q", s, r)
			}
		}
	}
	return nil
}

// LoadPolicy reads an RBAC policy (YAML or JSON).
func LoadPolicy(path string) (*Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Policy
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("rbac: %w", err)
	}
	return &p, p.Validate()
}

// Authorizer answers permission questions. The policy can be swapped at
// runtime (on SIGHUP).
type Authorizer struct {
	mu     sync.RWMutex
	policy *Policy
}

// NewAuthorizer wraps a policy.
func NewAuthorizer(p *Policy) *Authorizer { return &Authorizer{policy: p} }

// Set replaces the policy.
func (a *Authorizer) Set(p *Policy) { a.mu.Lock(); a.policy = p; a.mu.Unlock() }

// RolesOf returns the roles of an identity. Workload identities have none:
// they can only use the agent API.
func (a *Authorizer) RolesOf(id Identity) []string {
	if id.Workload() {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return slices.Clone(a.policy.Subjects[id.URI])
}

// Allowed reports whether id has permission p.
func (a *Authorizer) Allowed(id Identity, p Permission) bool {
	for _, r := range a.RolesOf(id) {
		if slices.Contains(Roles[r], p) {
			return true
		}
	}
	return false
}

// MarshalJSON keeps the policy printable for `cfgcanary doctor`.
func (p *Policy) String() string { b, _ := json.Marshal(p); return string(b) }

type ctxKey struct{}

// WithIdentity stores an identity in a context.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the identity stored by the transport layer.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}
