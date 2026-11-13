package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/raunaksahahere/cfgcanary/examples/ratelimits"
	"github.com/raunaksahahere/cfgcanary/internal/sim"
	"github.com/raunaksahahere/cfgcanary/sdk"
)

func newDemoAppCmd() *cobra.Command {
	var cacheDir, config, environment, instance, trustFile, listen string
	var rps float64
	cmd := &cobra.Command{
		Use:   "demo-app",
		Short: "Run the sample consumer: the ratelimits parser behind the Go SDK, next to an agent sidecar",
		Long: `Run the example application. It reads the cache written by an agent (sidecar mode) through
the Go SDK with the same parser as the WASM validator, serves its current config on /config,
reports readiness on /readyz only once a config is published, and exposes request metrics
from a synthetic workload whose error rate depends on timeout_ms.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if instance == "" {
				instance, _ = os.Hostname()
			}
			trust, err := os.ReadFile(trustFile)
			if err != nil {
				return err
			}
			reg := prometheus.NewRegistry()
			m := sdk.NewMetrics(reg)
			appM := sim.NewAppMetrics(reg)
			var cl atomic.Pointer[sdk.Client[ratelimits.Config]]
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
			mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
				if cl.Load() == nil {
					http.Error(w, "no config yet", http.StatusServiceUnavailable)
					return
				}
				fmt.Fprintln(w, "ready")
			})
			mux.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
				c := cl.Load()
				if c == nil {
					http.Error(w, "no config yet", http.StatusServiceUnavailable)
					return
				}
				cfg, info := c.Get(), c.Info()
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"instance": instance, "version": info.Version, "generation": info.Generation,
					"release": info.ReleaseID, "timeout_ms": cfg.TimeoutMS, "features": len(cfg.Features), "stale": c.Stale()})
			})
			hs := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
			go func() { _ = hs.ListenAndServe() }()
			defer hs.Close()
			c, err := sdk.Open(cmd.Context(), sdk.Options[ratelimits.Config]{
				Dir: cacheDir, Config: config, Environment: environment, TrustBundle: trust, Parse: ratelimits.Parse,
				Clone: ratelimits.Config.Clone, InstanceID: instance, Metrics: m, Logger: newLogger("info"),
			})
			if err != nil {
				return err
			}
			defer c.Close()
			cl.Store(c)
			w := sim.NewWorkload(instance, rps, appM, uint64(time.Now().UnixNano()), func() (ratelimits.Config, bool) { return c.Get(), true })
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-cmd.Context().Done():
					return nil
				case <-t.C:
					w.Step(250 * time.Millisecond)
				}
			}
		},
	}
	f := cmd.Flags()
	f.StringVar(&cacheDir, "cache-dir", env("CFGCANARY_CACHE_DIR", "/var/lib/cfgcanary"), "cache directory written by the agent")
	f.StringVar(&config, "config", env("CFGCANARY_CONFIG", "ratelimits"), "config name")
	f.StringVar(&environment, "environment", env("CFGCANARY_ENVIRONMENT", "prod"), "environment")
	f.StringVar(&instance, "instance", env("CFGCANARY_INSTANCE", ""), "instance label for metrics (default: hostname)")
	f.StringVar(&trustFile, "trust-bundle", env("CFGCANARY_TRUST_BUNDLE", "/etc/cfgcanary/trust/trust.json"), "trusted signing keys")
	f.StringVar(&listen, "listen", env("CFGCANARY_LISTEN", ":8080"), "HTTP address")
	f.Float64Var(&rps, "rps", 20, "synthetic requests per second")
	return cmd
}
