package main

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/raunaksahahere/cfgcanary/internal/app"
	"github.com/raunaksahahere/cfgcanary/internal/auth"
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

// secretFromFile reads a credential from a file (preferred) or an
// environment variable, never from a flag value visible in process lists.
func secretFromFile(file, envKey string) (string, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	return os.Getenv(envKey), nil
}

func newServerCmd() *cobra.Command {
	var o app.ServerOptions
	var s3AccessFile, s3SecretFile, dbURLFile, logLevel string
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Run a control-plane replica (API, watch fan-out and, when elected, the release controller)",
		Long: `Run a control-plane replica. Every option can also be set with an environment
variable (shown in brackets). Credentials are read from files or the environment, never
from flags. Several replicas may run against the same PostgreSQL: exactly one leads the
release controller at a time (advisory lock plus fencing epoch).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			if o.DBURL, err = secretFromFile(dbURLFile, "CFGCANARY_DB_URL"); err != nil || o.DBURL == "" {
				return fail(exitUsage, "a database URL is required (--db-url-file or CFGCANARY_DB_URL)")
			}
			if o.S3.AccessKey, err = secretFromFile(s3AccessFile, "CFGCANARY_S3_ACCESS_KEY"); err != nil {
				return err
			}
			if o.S3.SecretKey, err = secretFromFile(s3SecretFile, "CFGCANARY_S3_SECRET_KEY"); err != nil {
				return err
			}
			o.Logger = newLogger(logLevel)
			// Reload the RBAC policy on SIGHUP.
			hup := make(chan os.Signal, 1)
			signal.Notify(hup, syscall.SIGHUP)
			defer signal.Stop(hup)
			o.Ready = func(r *app.Running) {
				go func() {
					for range hup {
						if p, err := auth.LoadPolicy(o.RBACFile); err != nil {
							o.Logger.Error("RBAC reload failed; keeping the previous policy", "err", err)
						} else {
							r.Authorizer.Set(p)
							o.Logger.Info("RBAC policy reloaded")
						}
					}
				}()
			}
			return app.RunServer(cmd.Context(), o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.GRPCListen, "listen", env("CFGCANARY_LISTEN", ":8443"), "gRPC (mTLS) listen address [CFGCANARY_LISTEN]")
	f.StringVar(&o.RESTListen, "rest-listen", env("CFGCANARY_REST_LISTEN", ":8444"), "REST gateway (mTLS) listen address; empty disables [CFGCANARY_REST_LISTEN]")
	f.StringVar(&o.MetricsListen, "metrics-listen", env("CFGCANARY_METRICS_LISTEN", "127.0.0.1:9464"), "metrics and health listen address (plain HTTP) [CFGCANARY_METRICS_LISTEN]")
	f.StringVar(&dbURLFile, "db-url-file", env("CFGCANARY_DB_URL_FILE", ""), "file containing the PostgreSQL URL [CFGCANARY_DB_URL_FILE, or CFGCANARY_DB_URL]")
	f.Int32Var(&o.DBMaxConns, "db-max-conns", int32(envInt("CFGCANARY_DB_MAX_CONNS", 30)), "PostgreSQL pool size [CFGCANARY_DB_MAX_CONNS]")
	f.StringVar(&o.S3.Endpoint, "s3-endpoint", env("CFGCANARY_S3_ENDPOINT", ""), "S3 endpoint host:port [CFGCANARY_S3_ENDPOINT]")
	f.StringVar(&o.S3.PublicEndpoint, "s3-public-endpoint", env("CFGCANARY_S3_PUBLIC_ENDPOINT", ""), "endpoint agents download from, if different [CFGCANARY_S3_PUBLIC_ENDPOINT]")
	f.StringVar(&o.S3.Bucket, "s3-bucket", env("CFGCANARY_S3_BUCKET", "cfgcanary"), "private bucket for artifacts [CFGCANARY_S3_BUCKET]")
	f.StringVar(&o.S3.Region, "s3-region", env("CFGCANARY_S3_REGION", "us-east-1"), "S3 region [CFGCANARY_S3_REGION]")
	f.BoolVar(&o.S3.Secure, "s3-tls", envBool("CFGCANARY_S3_TLS", false), "use HTTPS for the object store [CFGCANARY_S3_TLS]")
	f.StringVar(&o.S3.CAFile, "s3-ca", env("CFGCANARY_S3_CA", ""), "CA bundle for the object store [CFGCANARY_S3_CA]")
	f.StringVar(&s3AccessFile, "s3-access-key-file", env("CFGCANARY_S3_ACCESS_KEY_FILE", ""), "file with the S3 access key [or CFGCANARY_S3_ACCESS_KEY]")
	f.StringVar(&s3SecretFile, "s3-secret-key-file", env("CFGCANARY_S3_SECRET_KEY_FILE", ""), "file with the S3 secret key [or CFGCANARY_S3_SECRET_KEY]")
	f.StringVar(&o.TLSCert, "tls-cert", env("CFGCANARY_TLS_CERT", ".dev/pki/server.pem"), "server certificate [CFGCANARY_TLS_CERT]")
	f.StringVar(&o.TLSKey, "tls-key", env("CFGCANARY_TLS_KEY", ".dev/pki/server-key.pem"), "server key [CFGCANARY_TLS_KEY]")
	f.StringVar(&o.TLSCA, "tls-ca", env("CFGCANARY_TLS_CA", ".dev/pki/ca.pem"), "CA bundle for client certificates [CFGCANARY_TLS_CA]")
	f.StringVar(&o.ArtifactKey, "artifact-key", env("CFGCANARY_ARTIFACT_KEY", ".dev/pki/artifact.key"), "ed25519 artifact signing key [CFGCANARY_ARTIFACT_KEY]")
	f.StringVar(&o.DeliveryKey, "delivery-key", env("CFGCANARY_DELIVERY_KEY", ".dev/pki/delivery.key"), "ed25519 delivery signing key [CFGCANARY_DELIVERY_KEY]")
	f.StringVar(&o.RBACFile, "rbac", env("CFGCANARY_RBAC", ".dev/pki/rbac.yaml"), "RBAC policy; reloaded on SIGHUP [CFGCANARY_RBAC]")
	f.StringVar(&o.TrustDomain, "trust-domain", env("CFGCANARY_TRUST_DOMAIN", "cfgcanary.local"), "SPIFFE trust domain [CFGCANARY_TRUST_DOMAIN]")
	f.StringVar(&o.PrometheusURL, "prometheus-url", env("CFGCANARY_PROMETHEUS_URL", ""), "Prometheus for health gates; empty makes gated looks inconclusive [CFGCANARY_PROMETHEUS_URL]")
	f.BoolVar(&o.Controller, "controller", envBool("CFGCANARY_CONTROLLER", true), "campaign for release-controller leadership [CFGCANARY_CONTROLLER]")
	f.DurationVar(&o.ControllerTick, "controller-tick", envDur("CFGCANARY_CONTROLLER_TICK", 250*time.Millisecond), "reconcile interval [CFGCANARY_CONTROLLER_TICK]")
	f.Int64Var(&o.Server.HardMaxBytes, "max-payload-bytes", int64(envInt("CFGCANARY_MAX_PAYLOAD_BYTES", 4<<20)), "server-wide payload limit [CFGCANARY_MAX_PAYLOAD_BYTES]")
	f.IntVar(&o.Server.MaxStreams, "max-streams", envInt("CFGCANARY_MAX_STREAMS", 20000), "concurrent watch streams per replica [CFGCANARY_MAX_STREAMS]")
	f.Float64Var(&o.Server.AdmissionRate, "admission-rate", float64(envInt("CFGCANARY_ADMISSION_RATE", 500)), "new watch streams per second [CFGCANARY_ADMISSION_RATE]")
	f.IntVar(&o.Server.AdmissionBurst, "admission-burst", envInt("CFGCANARY_ADMISSION_BURST", 1000), "admission burst [CFGCANARY_ADMISSION_BURST]")
	f.DurationVar(&o.Server.MembershipWindow, "membership-window", envDur("CFGCANARY_MEMBERSHIP_WINDOW", 2*time.Minute), "instances seen within this window form a release's cohort [CFGCANARY_MEMBERSHIP_WINDOW]")
	f.StringVar(&logLevel, "log-level", env("CFGCANARY_LOG_LEVEL", "info"), "debug, info, warn or error [CFGCANARY_LOG_LEVEL]")
	return cmd
}
