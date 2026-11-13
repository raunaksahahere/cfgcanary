package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/raunaksahahere/cfgcanary/internal/devpki"
	"github.com/raunaksahahere/cfgcanary/internal/sim"
)

func newSimCmd() *cobra.Command {
	var o sim.Options
	var trustFile, origins, metricsListen, logLevel string
	var seed int64
	cmd := &cobra.Command{
		Use:   "sim",
		Short: "Run a simulated fleet: N real agents and SDK clients plus a synthetic sample application",
		Long: `Run N instances in one process. Each instance is a real agent (watch, verify, atomic apply,
status reporting) and a real SDK client parsing the example ratelimits config; a synthetic
workload produces request metrics whose error rate and latency depend on the served
timeout_ms. Each agent gets its own short-lived certificate minted from the development CA
in --pki-dir, so this command is for development, demos and benchmarks only.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ca, err := devpki.LoadCA(g.pkiDir)
			if err != nil {
				return fail(exitUsage, "sim needs the development CA in %s (run 'cfgcanary dev bootstrap'): %v", g.pkiDir, err)
			}
			o.CA, o.Seed = ca, uint64(seed)
			o.CAFile = filepath.Join(g.pkiDir, "ca.pem")
			o.Server, o.ServerName = g.server, g.serverName
			if o.TrustBundle, err = os.ReadFile(trustFile); err != nil {
				return err
			}
			for _, s := range strings.Split(origins, ",") {
				if s = strings.TrimSpace(s); s != "" {
					o.AllowedOrigins = append(o.AllowedOrigins, s)
				}
			}
			o.Logger = newLogger(logLevel)
			reg := prometheus.NewRegistry()
			reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
			o.Registry = reg
			f, err := sim.NewFleet(o)
			if err != nil {
				return err
			}
			if metricsListen != "" {
				mux := http.NewServeMux()
				mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
				mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
				hs := &http.Server{Addr: metricsListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
				go func() { _ = hs.ListenAndServe() }()
				defer hs.Close()
			}
			f.Start(cmd.Context())
			o.Logger.Info("simulated fleet started", "agents", o.N, "environment", o.Environment, "config", o.Config)
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-cmd.Context().Done():
					f.Stop()
					return nil
				case <-t.C:
					o.Logger.Info("fleet", "serving_by_version", fmt.Sprint(f.Versions()))
				}
			}
		},
	}
	fl := cmd.Flags()
	fl.IntVar(&o.N, "agents", envInt("CFGCANARY_SIM_AGENTS", 50), "number of simulated instances")
	fl.StringVar(&o.Prefix, "prefix", env("CFGCANARY_SIM_PREFIX", "agent"), "instance ID prefix")
	fl.StringVar(&o.Environment, "environment", env("CFGCANARY_ENVIRONMENT", "prod"), "environment")
	fl.StringVar(&o.Config, "config", env("CFGCANARY_SIM_CONFIG", "ratelimits"), "config to subscribe to")
	fl.StringVar(&o.Root, "root", env("CFGCANARY_SIM_ROOT", "/tmp/cfgcanary-sim"), "directory for per-agent caches and certificates")
	fl.StringVar(&trustFile, "trust-bundle", env("CFGCANARY_TRUST_BUNDLE", ".dev/pki/trust.json"), "trusted signing keys")
	fl.StringVar(&origins, "allowed-origins", env("CFGCANARY_ALLOWED_ORIGINS", ""), "comma-separated artifact origins")
	fl.Float64Var(&o.RPS, "rps", float64(envInt("CFGCANARY_SIM_RPS", 40)), "synthetic requests per second per instance (0 disables the workload)")
	fl.StringVar(&metricsListen, "metrics-listen", env("CFGCANARY_METRICS_LISTEN", ":9465"), "metrics address")
	fl.Int64Var(&seed, "seed", int64(envInt("CFGCANARY_SIM_SEED", 1)), "random seed (jitter and workload)")
	fl.DurationVar(&o.Reconcile, "reconcile-interval", 30*time.Second, "agent reconciliation interval")
	fl.DurationVar(&o.StartSpread, "start-spread", 2*time.Second, "spread initial connections over this duration")
	fl.DurationVar(&o.Backoff.Base, "backoff-base", 250*time.Millisecond, "reconnect backoff base")
	fl.DurationVar(&o.Backoff.Cap, "backoff-cap", 10*time.Second, "reconnect backoff cap")
	fl.StringVar(&logLevel, "log-level", env("CFGCANARY_LOG_LEVEL", "info"), "log level")
	return cmd
}
