package testutil

import (
	"encoding/json"
	"testing"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/cache"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// Keys is a test signing setup: artifact and delivery signers and the
// matching trust bundle.
type Keys struct {
	Art, Del   *sign.Signer
	Bundle     *sign.Bundle
	BundleJSON []byte
}

// NewKeys generates fresh keys.
func NewKeys(t testing.TB) *Keys {
	ak, _ := sign.GenerateKey()
	dk, _ := sign.GenerateKey()
	k := &Keys{Art: sign.NewSigner(sign.PurposeArtifact, ak), Del: sign.NewSigner(sign.PurposeDelivery, dk)}
	b, err := sign.NewBundle(sign.Trusted(k.Art.Public(), sign.PurposeArtifact), sign.Trusted(k.Del.Public(), sign.PurposeDelivery))
	if err != nil {
		t.Fatal(err)
	}
	k.Bundle = b
	k.BundleJSON, _ = json.Marshal(b)
	return k
}

// UpdateSpec describes a test update.
type UpdateSpec struct {
	Config, Env         string
	Version, Generation int64
	Ring, RingLo        int32
	RingHi              int32
	Payload             []byte
	URL                 string
	FailureMode         string
	MaxStaleness        int64
	Release             string
}

func pbSigned(s sign.Signed) *pb.SignedStatement {
	return &pb.SignedStatement{Payload: s.Payload, Signature: s.Signature, KeyId: s.KeyID}
}

// Update returns a correctly signed ConfigUpdate.
func (k *Keys) Update(s UpdateSpec) *pb.ConfigUpdate {
	if s.Env == "" {
		s.Env = "prod"
	}
	if s.RingHi == 0 {
		s.RingHi = 1 << 20
	}
	if s.FailureMode == "" {
		s.FailureMode = "fail_static"
	}
	sha := cache.Digest(s.Payload)
	a, _ := k.Art.Sign(sign.Artifact{Config: s.Config, Version: s.Version, SHA256: sha, Size: int64(len(s.Payload)), Environments: []string{s.Env}})
	d, _ := k.Del.Sign(sign.Delivery{Config: s.Config, Environment: s.Env, Generation: s.Generation, Version: s.Version, SHA256: sha,
		ReleaseID: s.Release, RingLo: s.RingLo, RingHi: s.RingHi, FailureMode: s.FailureMode, MaxStalenessSeconds: s.MaxStaleness})
	return &pb.ConfigUpdate{Kind: pb.ConfigUpdate_KIND_DESIRED, Config: s.Config, Environment: s.Env, Generation: s.Generation,
		Version: s.Version, Sha256: sha, Size: int64(len(s.Payload)), Url: s.URL, Artifact: pbSigned(a), Delivery: pbSigned(d),
		Ring: s.Ring, ReleaseId: s.Release}
}
