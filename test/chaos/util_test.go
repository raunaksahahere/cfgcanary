package chaos

import (
	"errors"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"

	"github.com/raunaksahahere/cfgcanary/internal/client"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
)

type rolloutEvent = rollout.Event

func startEvent(r *store.ReleaseRow) rollout.Event {
	return rollout.Event{Kind: rollout.EvStart, ExpectStateVersion: r.R.StateVersion}
}

func isFenced(err error) bool { return errors.Is(err, store.ErrFenced) }

func dialAddr(t *testing.T, e *testenv.Env, addr, who string) *grpc.ClientConn {
	cc, err := client.Dial(client.Options{Server: addr, ServerName: "localhost",
		Cert: filepath.Join(e.PKI, who+".pem"), Key: filepath.Join(e.PKI, who+"-key.pem"), CA: filepath.Join(e.PKI, "ca.pem")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}
