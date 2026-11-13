package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/raunaksahahere/cfgcanary/internal/agent"
	"github.com/raunaksahahere/cfgcanary/internal/buildinfo"
	"github.com/raunaksahahere/cfgcanary/internal/client"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

func newAgentCmd() *cobra.Command {
	var (
		instance, environment, cacheDir, trustFile, origins, sourceDir, metricsListen, logLevel string
		configs                                                                                 []string
		reconcile                                                                               time.Duration
		maxBytes                                                                                int64
	)
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run the agent: watch, verify and atomically apply configs to the local cache",
		Long: `Run the agent next to a workload. It watches the control plane for the desired state of
its configs, verifies signatures, digest and scope, writes the cache atomically, and reports
what was applied and what the application's SDK accepted. With --source-dir it reads a
projected directory (a Kubernetes ring ConfigMap) instead of a watch stream.

The instance ID must match the agent's certificate identity
(spiffe://<trust-domain>/env/<env>/agent/<id>, or <group>/<suffix> for a group certificate).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if instance == "" || environment == "" || len(configs) == 0 {
				return fail(exitUsage, "--instance, --environment and --config are required")
			}
			trust, err := sign.LoadBundle(trustFile)
			if err != nil {
				return fail(exitUsage, "trust bundle: %v", err)
			}
			log := newLogger(logLevel)
			reg := prometheus.NewRegistry()
			agent.Version, _, _, _ = buildinfo.Info()
			cfg := agent.Config{InstanceID: instance, Environment: environment, Root: cacheDir, Configs: configs, Trust: trust,
				ReconcileInterval: reconcile, MaxBytes: maxBytes, Logger: log, Metrics: agent.NewMetrics(reg)}
			for _, o := range strings.Split(origins, ",") {
				if o = strings.TrimSpace(o); o != "" {
					cfg.AllowedOrigins = append(cfg.AllowedOrigins, o)
				}
			}
			if g.server != "" {
				co, err := clientOptions()
				if err != nil {
					return err
				}
				creds, err := co.Credentials()
				if err != nil {
					return err
				}
				cfg.Dial = func(ctx context.Context) (*grpc.ClientConn, error) { return client.DialWith(co, creds) }
			}
			a, err := agent.New(cfg)
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
			log.Info("agent starting", "instance", instance, "environment", environment, "configs", configs, "source_dir", sourceDir)
			if sourceDir != "" {
				return a.RunFileSource(cmd.Context(), sourceDir, 2*time.Second)
			}
			if cfg.Dial == nil {
				return fail(exitUsage, "--server is required unless --source-dir is used")
			}
			return a.Run(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVar(&instance, "instance", env("CFGCANARY_INSTANCE", ""), "instance ID bound to the certificate [CFGCANARY_INSTANCE]")
	f.StringVar(&environment, "environment", env("CFGCANARY_ENVIRONMENT", ""), "environment [CFGCANARY_ENVIRONMENT]")
	f.StringSliceVar(&configs, "config", splitEnv("CFGCANARY_CONFIGS"), "config to subscribe to (repeatable) [CFGCANARY_CONFIGS, comma-separated]")
	f.StringVar(&cacheDir, "cache-dir", env("CFGCANARY_CACHE_DIR", "/var/lib/cfgcanary"), "cache directory shared with the SDK [CFGCANARY_CACHE_DIR]")
	f.StringVar(&trustFile, "trust-bundle", env("CFGCANARY_TRUST_BUNDLE", ".dev/pki/trust.json"), "trusted signing keys [CFGCANARY_TRUST_BUNDLE]")
	f.StringVar(&origins, "allowed-origins", env("CFGCANARY_ALLOWED_ORIGINS", ""), "comma-separated scheme://host:port artifact origins [CFGCANARY_ALLOWED_ORIGINS]")
	f.StringVar(&sourceDir, "source-dir", env("CFGCANARY_SOURCE_DIR", ""), "read a projected directory instead of watching [CFGCANARY_SOURCE_DIR]")
	f.DurationVar(&reconcile, "reconcile-interval", envDur("CFGCANARY_RECONCILE_INTERVAL", 30*time.Second), "full reconciliation interval [CFGCANARY_RECONCILE_INTERVAL]")
	f.Int64Var(&maxBytes, "max-bytes", int64(envInt("CFGCANARY_MAX_BYTES", 4<<20)), "largest payload accepted [CFGCANARY_MAX_BYTES]")
	f.StringVar(&metricsListen, "metrics-listen", env("CFGCANARY_METRICS_LISTEN", ""), "metrics/health address [CFGCANARY_METRICS_LISTEN]")
	f.StringVar(&logLevel, "log-level", env("CFGCANARY_LOG_LEVEL", "info"), "log level [CFGCANARY_LOG_LEVEL]")
	return cmd
}

func splitEnv(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}
