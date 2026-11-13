package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/raunaksahahere/cfgcanary/sdk"
)

// Limits is an application's config type. It has no maps, slices or
// pointers, so snapshots are immutable without a Clone function.
type Limits struct {
	RPS       int `json:"rps"`
	TimeoutMS int `json:"timeout_ms"`
}

func parseLimits(b []byte) (Limits, error) {
	var l Limits
	if err := json.Unmarshal(b, &l); err != nil {
		return l, err
	}
	if l.RPS <= 0 || l.TimeoutMS <= 0 {
		return l, errors.New("rps and timeout_ms must be positive")
	}
	return l, nil
}

func ExampleOpen() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := sdk.Open(ctx, sdk.Options[Limits]{
		Dir:             "/var/lib/cfgcanary", // the agent's --cache-dir
		Config:          "limits",
		Environment:     "prod",
		TrustBundleFile: "/etc/cfgcanary/trust.json",
		Parse:           parseLimits,
		Bootstrap:       []byte(`{"rps": 100, "timeout_ms": 250}`),
	})
	var initErr *sdk.InitError
	if errors.As(err, &initErr) {
		fmt.Println("no usable config yet:", initErr.Reason)
		return
	}
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()
	cfg := client.Get() // never fails; the last config the parser accepted
	fmt.Println(cfg.RPS, client.Info().Version)
}
