// Package sign implements cfgcanary's ed25519 statements and trust bundle.
//
// A statement is a small JSON document. The signature covers a
// domain-separation prefix plus the exact statement bytes, so verifiers check
// the bytes they received and only then decode them. The package depends on
// the standard library only, because the public SDK links it.
package sign

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
)

// Purpose separates what a key may sign.
type Purpose string

const (
	PurposeArtifact Purpose = "artifact"
	PurposeDelivery Purpose = "delivery"
)

func prefix(p Purpose) []byte { return []byte("cfgcanary/" + string(p) + "/v1\n") }

// Artifact binds payload bytes to a config, an immutable version and the
// environments it may be delivered to.
type Artifact struct {
	Config       string   `json:"config"`
	Version      int64    `json:"version"`
	SHA256       string   `json:"sha256"`
	Size         int64    `json:"size"`
	Environments []string `json:"environments"`
}

// Delivery authorizes one generation of a target for a range of rings.
type Delivery struct {
	Config      string `json:"config"`
	Environment string `json:"environment"`
	Generation  int64  `json:"generation"`
	Version     int64  `json:"version"`
	SHA256      string `json:"sha256"`
	ReleaseID   string `json:"release_id,omitempty"`
	RingLo      int32  `json:"ring_lo"`
	RingHi      int32  `json:"ring_hi"`
	// FailureMode and MaxStalenessSeconds tell the consumer how to behave
	// when its source goes away (see docs/invariants.md).
	FailureMode         string `json:"failure_mode"`
	MaxStalenessSeconds int64  `json:"max_staleness_seconds,omitempty"`
}

// Signed is a statement with its signature.
type Signed struct {
	Payload   []byte `json:"payload"`
	Signature []byte `json:"signature"`
	KeyID     string `json:"key_id"`
}

// KeyID is the first 16 hex characters of SHA-256 of the public key.
func KeyID(pub ed25519.PublicKey) string {
	h := sha256.Sum256(pub)
	return hex.EncodeToString(h[:])[:16]
}

// Signer signs statements for one purpose.
type Signer struct {
	purpose Purpose
	key     ed25519.PrivateKey
	id      string
}

// NewSigner wraps a private key.
func NewSigner(p Purpose, key ed25519.PrivateKey) *Signer {
	return &Signer{purpose: p, key: key, id: KeyID(key.Public().(ed25519.PublicKey))}
}

// Sign2 signs an artifact statement; it has the shape store.InsertVersion
// expects.
func (s *Signer) Sign2(a Artifact) (Signed, error) { return s.Sign(a) }

// KeyID returns the signer's key ID.
func (s *Signer) KeyID() string { return s.id }

// Public returns the public key.
func (s *Signer) Public() ed25519.PublicKey { return s.key.Public().(ed25519.PublicKey) }

// Sign marshals v and signs it.
func (s *Signer) Sign(v any) (Signed, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Signed{}, err
	}
	msg := append(prefix(s.purpose), b...)
	return Signed{Payload: b, Signature: ed25519.Sign(s.key, msg), KeyID: s.id}, nil
}

// TrustedKey is one entry of a trust bundle.
type TrustedKey struct {
	KeyID     string    `json:"key_id"`
	PublicKey string    `json:"public_key"` // base64 (std) ed25519 public key
	Purposes  []Purpose `json:"purposes"`
}

// Bundle is the set of keys a verifier trusts.
type Bundle struct {
	Keys []TrustedKey `json:"keys"`
	keys map[string]trusted
}

type trusted struct {
	pub      ed25519.PublicKey
	purposes []Purpose
}

// Errors returned by verification. They are distinguishable so callers can
// report a precise, payload-free reason.
var (
	ErrUnknownKey   = errors.New("sign: key not in trust bundle")
	ErrWrongPurpose = errors.New("sign: key not trusted for this purpose")
	ErrBadSignature = errors.New("sign: signature does not verify")
	ErrMalformed    = errors.New("sign: malformed statement")
)

// NewBundle builds a bundle from keys and validates it.
func NewBundle(keys ...TrustedKey) (*Bundle, error) {
	b := &Bundle{Keys: keys}
	return b, b.init()
}

func (b *Bundle) init() error {
	b.keys = make(map[string]trusted, len(b.Keys))
	for _, k := range b.Keys {
		raw, err := base64.StdEncoding.DecodeString(k.PublicKey)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return fmt.Errorf("trust bundle: key %q is not a base64 ed25519 public key", k.KeyID)
		}
		if KeyID(raw) != k.KeyID {
			return fmt.Errorf("trust bundle: key_id %q does not match its public key", k.KeyID)
		}
		if len(k.Purposes) == 0 {
			return fmt.Errorf("trust bundle: key %q has no purposes", k.KeyID)
		}
		b.keys[k.KeyID] = trusted{pub: raw, purposes: k.Purposes}
	}
	return nil
}

// LoadBundle reads a trust bundle file.
func LoadBundle(path string) (*Bundle, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseBundle(raw)
}

// ParseBundle decodes a trust bundle.
func ParseBundle(raw []byte) (*Bundle, error) {
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("trust bundle: %w", err)
	}
	if len(b.Keys) == 0 {
		return nil, errors.New("trust bundle: no keys")
	}
	return &b, b.init()
}

// Trusted describes a signer's public key for inclusion in a bundle.
func Trusted(pub ed25519.PublicKey, purposes ...Purpose) TrustedKey {
	return TrustedKey{KeyID: KeyID(pub), PublicKey: base64.StdEncoding.EncodeToString(pub), Purposes: purposes}
}

func (b *Bundle) verify(p Purpose, s Signed) error {
	k, ok := b.keys[s.KeyID]
	if !ok {
		return ErrUnknownKey
	}
	if !slices.Contains(k.purposes, p) {
		return ErrWrongPurpose
	}
	if len(s.Signature) != ed25519.SignatureSize || !ed25519.Verify(k.pub, append(prefix(p), s.Payload...), s.Signature) {
		return ErrBadSignature
	}
	return nil
}

// VerifyArtifact checks the signature and decodes the statement.
func (b *Bundle) VerifyArtifact(s Signed) (Artifact, error) {
	var a Artifact
	if err := b.verify(PurposeArtifact, s); err != nil {
		return a, err
	}
	if err := json.Unmarshal(s.Payload, &a); err != nil || a.Config == "" || a.Version <= 0 || len(a.SHA256) != 64 || a.Size < 0 {
		return a, ErrMalformed
	}
	return a, nil
}

// VerifyDelivery checks the signature and decodes the statement.
func (b *Bundle) VerifyDelivery(s Signed) (Delivery, error) {
	var d Delivery
	if err := b.verify(PurposeDelivery, s); err != nil {
		return d, err
	}
	if err := json.Unmarshal(s.Payload, &d); err != nil || d.Config == "" || d.Environment == "" || d.Generation <= 0 || d.Version <= 0 || d.RingLo > d.RingHi {
		return d, ErrMalformed
	}
	return d, nil
}

// Scope is what a verifier expects an update to be for.
type Scope struct {
	Config      string
	Environment string
	// Ring is the verifier's ring, or -1 when it does not know it.
	Ring int32
}

// ErrScope is returned when verified statements do not apply to the verifier.
var ErrScope = errors.New("sign: statements are valid but not for this config, environment or ring")

// VerifyUpdate verifies both statements and that together they authorize this
// content for this scope. It returns the decoded statements.
func (b *Bundle) VerifyUpdate(artifact, delivery Signed, sc Scope) (Artifact, Delivery, error) {
	a, err := b.VerifyArtifact(artifact)
	if err != nil {
		return a, Delivery{}, fmt.Errorf("artifact: %w", err)
	}
	d, err := b.VerifyDelivery(delivery)
	if err != nil {
		return a, d, fmt.Errorf("delivery: %w", err)
	}
	switch {
	case a.Config != sc.Config, d.Config != sc.Config, d.Environment != sc.Environment,
		!slices.Contains(a.Environments, sc.Environment),
		a.Version != d.Version, a.SHA256 != d.SHA256,
		sc.Ring >= 0 && (sc.Ring < d.RingLo || sc.Ring > d.RingHi):
		return a, d, ErrScope
	}
	return a, d, nil
}

// GenerateKey creates a new ed25519 private key.
func GenerateKey() (ed25519.PrivateKey, error) {
	_, k, err := ed25519.GenerateKey(rand.Reader)
	return k, err
}

// WriteKey stores a private key seed, base64-encoded, with mode 0600.
func WriteKey(path string, k ed25519.PrivateKey) error {
	return os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(k.Seed())+"\n"), 0o600)
}

// LoadKey reads a key written by WriteKey.
func LoadKey(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	seed, err := base64.StdEncoding.DecodeString(string(trimSpace(raw)))
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("%s: not a base64 ed25519 seed", path)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}
