package main

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

func defaultExamples() string {
	for _, d := range []string{"examples", "/usr/share/cfgcanary/examples"} {
		if _, err := os.Stat(filepath.Join(d, "ratelimits", "spec.yaml")); err == nil {
			return d
		}
	}
	return "examples"
}

func newDemoCmd() *cobra.Command {
	var examples, env string
	var agents int
	cmd := &cobra.Command{
		Use:   "demo",
		Short: "Run the end-to-end demonstration against a running stack",
		Long: `Run the demonstration scenario against a running control plane and simulated fleet
(see deploy/compose): seed a first version, show invalid submissions stopped before any
agent sees them, roll a good change through every ring on gate evidence, and watch a
schema-valid but harmful change get caught in the canary ring and rolled back.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			dir := filepath.Join(examples, "ratelimits")
			step := func(format string, a ...any) { printf("\n== "+format+"\n", a...) }
			ctx := cmd.Context()

			step("identity and server")
			info, err := c.GetServerInfo(ctx, &pb.GetServerInfoRequest{})
			if err != nil {
				return err
			}
			printf("connected to %s as %s %v\n", g.server, info.Identity, info.Roles)

			step("waiting for %d simulated instances to connect", agents)
			deadline := time.Now().Add(3 * time.Minute)
			for {
				r, err := c.ListInstances(ctx, &pb.ListInstancesRequest{Environment: env, PageSize: 1000})
				n := 0
				if err == nil {
					for _, in := range r.Instances {
						if in.Connected {
							n++
						}
					}
				}
				if n >= agents {
					printf("%d instances connected\n", n)
					break
				}
				if time.Now().After(deadline) {
					return fail(exitUnavailable, "only %d/%d instances connected", n, agents)
				}
				time.Sleep(time.Second)
			}

			tgt, err := c.GetTarget(ctx, &pb.GetTargetRequest{Config: "ratelimits", Environment: env})
			if err != nil || tgt.BaselineVersion == 0 {
				step("first deployment: no baseline exists, so v1 goes out under a manual, ungated policy")
				spec, err := loadSpec(filepath.Join(dir, "spec-seed.yaml"))
				if err != nil {
					return err
				}
				if _, err := c.RegisterConfig(ctx, &pb.RegisterConfigRequest{Spec: spec, RequestId: requestID()}); err != nil {
					return err
				}
				v1, err := demoSubmit(cmd, c, filepath.Join(dir, "payloads", "v1-baseline.json"), true)
				if err != nil {
					return err
				}
				cr, err := c.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: "ratelimits", Environment: env, Version: v1, RequestId: requestID(), Note: "demo seed"})
				if err != nil {
					return err
				}
				for {
					r, err := c.GetRelease(ctx, &pb.GetReleaseRequest{Id: cr.Release.Id})
					if err == nil && r.State == pb.ReleaseState_RELEASE_STATE_BAKING {
						break
					}
					time.Sleep(500 * time.Millisecond)
				}
				time.Sleep(5 * time.Second) // the seed policy's minimum bake
				if _, err := c.PromoteRelease(ctx, &pb.PromoteReleaseRequest{Id: cr.Release.Id, RequestId: requestID()}); err != nil {
					return err
				}
				printf("v1 delivered to every instance (release %s)\n", cr.Release.Id)
			}

			step("register the gated policy (rings 15%% / 40%% / 90%% + 10%% holdout, Prometheus gate)")
			spec, err := loadSpec(filepath.Join(dir, "spec.yaml"))
			if err != nil {
				return err
			}
			if _, err := c.RegisterConfig(ctx, &pb.RegisterConfigRequest{Spec: spec, RequestId: requestID()}); err != nil {
				return err
			}
			printf("validation: limits, strict JSON, JSON Schema, CUE, relative change, consumer parser (WASM)\n")

			step("invalid submissions are stopped before any agent can see them")
			for _, f := range []string{"bad-oversized.json", "bad-over-cardinality.json", "bad-doubled.json", "bad-blank-service.json", "bad-parser-rule.json", "bad-malformed.json"} {
				payload, err := os.ReadFile(filepath.Join(dir, "payloads", f))
				if err != nil {
					return err
				}
				r, err := c.SubmitVersion(ctx, &pb.SubmitVersionRequest{Name: "ratelimits", Payload: payload, DryRun: false, RequestId: requestID()})
				if err != nil {
					return err
				}
				if r.Report.Ok {
					return fail(exitGeneral, "%s was accepted; the demo expected a rejection", f)
				}
				i := r.Report.Issues[0]
				msg := i.Message
				if len(msg) > 70 {
					msg = msg[:70] + "..."
				}
				printf("  %-26s rejected  [%s/%s] %s\n", f, i.Stage, i.Code, msg)
			}

			step("a good change rolls out ring by ring on gate evidence")
			v2, err := demoSubmit(cmd, c, filepath.Join(dir, "payloads", "v2-good.json"), true)
			if err != nil {
				return err
			}
			plan, err := c.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: "ratelimits", Environment: env, Version: v2, DryRun: true})
			if err != nil {
				return err
			}
			printPlan(plan.Plan)
			cr, err := c.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: "ratelimits", Environment: env, Version: v2, RequestId: requestID(), Note: "demo good change"})
			if err != nil {
				return err
			}
			if err := watchRelease(cmd, c, cr.Release.Id); err != nil {
				return err
			}

			step("a schema-valid but harmful change (timeout_ms 250 -> 20) is caught in the canary ring")
			v3, err := demoSubmit(cmd, c, filepath.Join(dir, "payloads", "v3-harmful-timeout.json"), true)
			if err != nil {
				return err
			}
			started := time.Now()
			cr, err = c.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: "ratelimits", Environment: env, Version: v3, RequestId: requestID(), Note: "demo harmful change"})
			if err != nil {
				return err
			}
			werr := watchRelease(cmd, c, cr.Release.Id)
			var ce *cliError
			if werr == nil || !errors.As(werr, &ce) || ce.code != exitReleaseFail {
				return fail(exitGeneral, "expected the harmful release to roll back, got %v", werr)
			}
			r, err := c.GetRelease(ctx, &pb.GetReleaseRequest{Id: cr.Release.Id})
			if err != nil {
				return err
			}
			total := int32(0)
			for _, rs := range r.Rings {
				total += rs.Counts.Eligible
			}
			if r.Holdout != nil {
				total += r.Holdout.Eligible
			}
			detect := "n/a"
			if r.Rollback != nil && r.Rollback.Started != nil {
				detect = r.Rollback.Started.AsTime().Sub(r.Created.AsTime()).Round(100 * time.Millisecond).String()
			}
			rollback := "not converged"
			if r.Rollback != nil && r.Rollback.ConvergedAt != nil {
				rollback = r.Rollback.ConvergedAt.AsTime().Sub(r.Rollback.Started.AsTime()).Round(100 * time.Millisecond).String()
			}
			step("result")
			printf("blast radius: %d of %d instances (ring 0) were exposed to v%d; rings 1-2 and the holdout: 0\n", r.Rollback.GetExposed(), total, v3)
			printf("time to detect (release created -> rollback started): %s\n", detect)
			printf("time to roll back (rollback started -> all reachable exposed instances confirmed v%d): %s\n", r.Rollback.GetFallbackVersion(), rollback)
			printf("wall time for this release: %s\n", time.Since(started).Round(time.Second))
			printf("\nsee the Grafana dashboard at http://localhost:3000 and 'cfgcanary audit' for the record\n")
			return nil
		},
	}
	cmd.Flags().StringVar(&examples, "examples", defaultExamples(), "examples directory")
	cmd.Flags().StringVar(&env, "environment", "prod", "environment")
	cmd.Flags().IntVar(&agents, "agents", 50, "instances to wait for")
	return cmd
}

func demoSubmit(cmd *cobra.Command, c pb.ControlServiceClient, path string, show bool) (int64, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	r, err := c.SubmitVersion(cmd.Context(), &pb.SubmitVersionRequest{Name: "ratelimits", Payload: payload, RequestId: requestID(), Description: filepath.Base(path)})
	if err != nil {
		return 0, err
	}
	if !r.Report.Ok {
		printReport(r.Report)
		return 0, fail(exitRejected, "%s was rejected", path)
	}
	if show {
		printf("submitted %s -> v%d (validation passed: %v)\n", filepath.Base(path), r.Version.Version, r.Report.StagesRun)
	}
	return r.Version.Version, nil
}
