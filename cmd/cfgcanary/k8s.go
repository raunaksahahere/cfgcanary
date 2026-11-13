package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/kube"
)

func newK8sControllerCmd() *cobra.Command {
	var namespace, kubeconfig, leaseNS, healthListen, logLevel string
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "k8s-controller",
		Short: "Run the Kubernetes ConfigRelease controller (a client of the control plane)",
		Long: `Reconcile ConfigRelease resources: submit each payload, create the release, mirror its
status, map spec.paused to pause/resume, and project every ring's desired payload and
signed envelope into <config>-<environment>-ring-<n> ConfigMaps. PostgreSQL remains the
source of release truth. One replica acts at a time (Kubernetes Lease).

Authenticates to the control plane with --cert/--key/--ca (a service identity with the
releaser role).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var cfg *rest.Config
			var err error
			if kubeconfig != "" {
				cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
			} else {
				cfg, err = rest.InClusterConfig()
			}
			if err != nil {
				return fail(exitUsage, "kubernetes client: %v", err)
			}
			dyn, err := dynamic.NewForConfig(cfg)
			if err != nil {
				return err
			}
			core, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				return err
			}
			cc, err := dial()
			if err != nil {
				return err
			}
			defer cc.Close()
			log := newLogger(logLevel)
			ctrl := kube.New(pb.NewControlServiceClient(cc), dyn, core, kube.Options{Namespace: namespace, Interval: interval, Logger: log})
			if healthListen != "" {
				mux := http.NewServeMux()
				mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
				hs := &http.Server{Addr: healthListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
				go func() { _ = hs.ListenAndServe() }()
				defer hs.Close()
			}
			id, _ := os.Hostname()
			lock := &resourcelock.LeaseLock{
				LeaseMeta:  metaFor("cfgcanary-k8s-controller", leaseNS),
				Client:     core.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{Identity: id},
			}
			leaderelection.RunOrDie(cmd.Context(), leaderelection.LeaderElectionConfig{
				Lock: lock, ReleaseOnCancel: true, LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) { log.Info("leading"); ctrl.Run(ctx) },
					OnStoppedLeading: func() { log.Info("stopped leading") },
				},
			})
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&namespace, "namespace", env("CFGCANARY_WATCH_NAMESPACE", ""), "namespace to watch (default: all)")
	f.StringVar(&kubeconfig, "kubeconfig", env("KUBECONFIG", ""), "kubeconfig (default: in-cluster)")
	f.StringVar(&leaseNS, "lease-namespace", env("POD_NAMESPACE", "default"), "namespace of the leader-election Lease")
	f.DurationVar(&interval, "interval", 2*time.Second, "reconcile interval")
	f.StringVar(&healthListen, "health-listen", env("CFGCANARY_HEALTH_LISTEN", ":8081"), "health endpoint")
	f.StringVar(&logLevel, "log-level", env("CFGCANARY_LOG_LEVEL", "info"), "log level")
	return cmd
}
