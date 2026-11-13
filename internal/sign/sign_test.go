package sign

import (
	"errors"
	"strings"
	"testing"
)

func fixture(t *testing.T) (*Signer, *Signer, *Bundle, Signed, Signed) {
	t.Helper()
	ak, _ := GenerateKey()
	dk, _ := GenerateKey()
	as, ds := NewSigner(PurposeArtifact, ak), NewSigner(PurposeDelivery, dk)
	b, err := NewBundle(Trusted(as.Public(), PurposeArtifact), Trusted(ds.Public(), PurposeDelivery))
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("ab", 32)
	a, _ := as.Sign(Artifact{Config: "limits", Version: 3, SHA256: sha, Size: 10, Environments: []string{"prod"}})
	d, _ := ds.Sign(Delivery{Config: "limits", Environment: "prod", Generation: 7, Version: 3, SHA256: sha, RingLo: 0, RingHi: 1, FailureMode: "fail_static"})
	return as, ds, b, a, d
}

func TestVerifyUpdate(t *testing.T) {
	as, ds, b, a, d := fixture(t)
	if _, _, err := b.VerifyUpdate(a, d, Scope{"limits", "prod", 1}); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}
	sha := strings.Repeat("ab", 32)
	cases := []struct {
		name string
		a, d Signed
		sc   Scope
		want error
	}{
		{"other config", a, d, Scope{"other", "prod", 0}, ErrScope},
		{"other environment", a, d, Scope{"limits", "staging", 0}, ErrScope},
		{"ring outside range", a, d, Scope{"limits", "prod", 2}, ErrScope},
		{"tampered payload", Signed{append([]byte{}, append(a.Payload, ' ')...), a.Signature, a.KeyID}, d, Scope{"limits", "prod", 0}, ErrBadSignature},
		{"unknown key", Signed{a.Payload, a.Signature, "0000000000000000"}, d, Scope{"limits", "prod", 0}, ErrUnknownKey},
		// A delivery key must not be able to vouch for content, and vice versa.
		{"purpose confusion", func() Signed {
			s, _ := NewSigner(PurposeArtifact, ds.key).Sign(Artifact{Config: "limits", Version: 3, SHA256: sha, Environments: []string{"prod"}})
			return s
		}(), d, Scope{"limits", "prod", 0}, ErrWrongPurpose},
		{"version mismatch", a, func() Signed {
			s, _ := ds.Sign(Delivery{Config: "limits", Environment: "prod", Generation: 8, Version: 4, SHA256: sha, RingHi: 1})
			return s
		}(), Scope{"limits", "prod", 0}, ErrScope},
		{"digest mismatch", a, func() Signed {
			s, _ := ds.Sign(Delivery{Config: "limits", Environment: "prod", Generation: 8, Version: 3, SHA256: strings.Repeat("cd", 32), RingHi: 1})
			return s
		}(), Scope{"limits", "prod", 0}, ErrScope},
		{"env not allowed by artifact", func() Signed {
			s, _ := as.Sign(Artifact{Config: "limits", Version: 3, SHA256: sha, Environments: []string{"staging"}})
			return s
		}(), d, Scope{"limits", "prod", 0}, ErrScope},
	}
	for _, c := range cases {
		if _, _, err := b.VerifyUpdate(c.a, c.d, c.sc); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestBundleRejectsMismatchedKeyID(t *testing.T) {
	k, _ := GenerateKey()
	tk := Trusted(NewSigner(PurposeArtifact, k).Public(), PurposeArtifact)
	tk.KeyID = "ffffffffffffffff"
	if _, err := NewBundle(tk); err == nil {
		t.Fatal("bundle accepted a key_id that does not match its key")
	}
}

func FuzzVerifyArtifact(f *testing.F) {
	k, _ := GenerateKey()
	s := NewSigner(PurposeArtifact, k)
	b, _ := NewBundle(Trusted(s.Public(), PurposeArtifact))
	good, _ := s.Sign(Artifact{Config: "c", Version: 1, SHA256: strings.Repeat("a", 64)})
	f.Add(good.Payload, good.Signature)
	f.Fuzz(func(t *testing.T, payload, sig []byte) {
		_, err := b.VerifyArtifact(Signed{Payload: payload, Signature: sig, KeyID: s.KeyID()})
		if err == nil && (string(payload) != string(good.Payload) || string(sig) != string(good.Signature)) {
			t.Fatalf("accepted a statement that was not signed")
		}
	})
}
