package devpki

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/raunaksahahere/cfgcanary/internal/auth"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

func TestBootstrapIdempotent(t *testing.T) {
	dir := t.TempDir()
	created, err := Bootstrap(dir, DefaultOptions())
	if err != nil || len(created) == 0 {
		t.Fatal(created, err)
	}
	ca1, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	trust1, _ := os.ReadFile(filepath.Join(dir, "trust.json"))
	again, err := Bootstrap(dir, DefaultOptions())
	if err != nil || len(again) != 0 {
		t.Fatalf("second run created %v (%v)", again, err)
	}
	ca2, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	trust2, _ := os.ReadFile(filepath.Join(dir, "trust.json"))
	if string(ca1) != string(ca2) || string(trust1) != string(trust2) {
		t.Fatal("bootstrap rotated existing material")
	}
	if _, err := sign.LoadBundle(filepath.Join(dir, "trust.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.LoadPolicy(filepath.Join(dir, "rbac.yaml")); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(filepath.Join(dir, "ca-key.pem"))
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("CA key mode %v", st.Mode())
	}
	if _, err := auth.NewReloader(filepath.Join(dir, "users", "admin.pem"), filepath.Join(dir, "users", "admin-key.pem"), filepath.Join(dir, "ca.pem")); err != nil {
		t.Fatal(err)
	}
}
