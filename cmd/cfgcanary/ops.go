package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

func requestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "cli-" + hex.EncodeToString(b[:])
}

func table() *tabwriter.Writer { return tabwriter.NewWriter(g.stdout, 0, 2, 2, ' ', 0) }

func readInput(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(io.LimitReader(os.Stdin, 64<<20))
	}
	return os.ReadFile(path)
}

// loadSpec reads a ConfigSpec from YAML or JSON. Convenience keys
// validation.json_schema_file, validation.cue_file and
// validation.wasm.module_file are resolved relative to the spec file.
func loadSpec(path string) (*pb.ConfigSpec, error) {
	raw, err := readInput(path)
	if err != nil {
		return nil, err
	}
	js, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var m map[string]any
	if err := json.Unmarshal(js, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if v, ok := m["validation"].(map[string]any); ok {
		for key, target := range map[string]string{"json_schema_file": "json_schema", "cue_file": "cue"} {
			if f, ok := v[key].(string); ok {
				b, err := os.ReadFile(filepath.Join(dir, f))
				if err != nil {
					return nil, err
				}
				v[target] = string(b)
				delete(v, key)
			}
		}
		if w, ok := v["wasm"].(map[string]any); ok {
			if f, ok := w["module_file"].(string); ok {
				b, err := os.ReadFile(filepath.Join(dir, f))
				if err != nil {
					return nil, fmt.Errorf("wasm module: %w (build it with 'make wasm')", err)
				}
				w["module"] = base64.StdEncoding.EncodeToString(b)
				delete(w, "module_file")
			}
		}
	}
	js, _ = json.Marshal(m)
	var spec pb.ConfigSpec
	if err := (protojson.UnmarshalOptions{}).Unmarshal(js, &spec); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &spec, nil
}

// --- configs ----------------------------------------------------------------------

func newConfigsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "configs", Short: "Register and inspect configs"}
	var file string
	var rev int64
	reg := &cobra.Command{
		Use:   "register -f spec.yaml",
		Short: "Create or update a config and its validation and rollout policies",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec, err := loadSpec(file)
			if err != nil {
				return fail(exitUsage, "%v", err)
			}
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			cfg, err := c.RegisterConfig(ctx, &pb.RegisterConfigRequest{Spec: spec, ExpectedRevision: rev, RequestId: requestID()})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("Config", cfg)
			}
			printf("registered %s (revision %d, environments %s)\n", cfg.Spec.Name, cfg.SpecRevision, strings.Join(keys(cfg.Spec.Environments), ", "))
			return nil
		},
	}
	reg.Flags().StringVarP(&file, "file", "f", "", "spec file (YAML or JSON; - for stdin)")
	reg.Flags().Int64Var(&rev, "expected-revision", 0, "update only if the stored revision matches")
	_ = reg.MarkFlagRequired("file")

	get := &cobra.Command{
		Use: "get NAME", Short: "Show a config", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			cfg, err := c.GetConfig(ctx, &pb.GetConfigRequest{Name: args[0]})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("Config", cfg)
			}
			b, _ := protojson.MarshalOptions{Multiline: true, UseProtoNames: true}.Marshal(cfg.Spec)
			y, _ := yaml.JSONToYAML(b)
			printf("# revision %d, latest version %d\n%s", cfg.SpecRevision, cfg.LatestVersion, y)
			return nil
		},
	}
	list := &cobra.Command{
		Use: "list", Short: "List configs", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.ListConfigs(ctx, &pb.ListConfigsRequest{PageSize: 500})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("ConfigList", r)
			}
			w := table()
			fmt.Fprintln(w, "NAME\tREVISION\tLATEST\tFAILURE MODE\tENVIRONMENTS")
			for _, cfg := range r.Configs {
				fmt.Fprintf(w, "%s\t%d\tv%d\t%s\t%s\n", cfg.Spec.Name, cfg.SpecRevision, cfg.LatestVersion,
					strings.ToLower(strings.TrimPrefix(cfg.Spec.FailureMode.String(), "FAILURE_MODE_")), strings.Join(keys(cfg.Spec.Environments), ","))
			}
			return w.Flush()
		},
	}
	cmd.AddCommand(reg, get, list)
	return cmd
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- versions ---------------------------------------------------------------------

func printReport(r *pb.ValidationReport) {
	stages := strings.Join(r.StagesRun, ", ")
	base := "none (first version)"
	if r.BaseVersion > 0 {
		base = "v" + strconv.FormatInt(r.BaseVersion, 10)
	}
	if r.Ok {
		printf("validation passed: stages %s; relative-change base %s\n", stages, base)
		return
	}
	printf("validation FAILED (stages run: %s; base %s):\n", stages, base)
	for _, i := range r.Issues {
		loc := ""
		if i.Path != "" {
			loc = " at " + i.Path
		}
		printf("  [%s/%s]%s %s\n", i.Stage, i.Code, loc, i.Message)
	}
}

func submitCmd(use, short string, dryRun bool) *cobra.Command {
	var file, desc string
	var envs []string
	cmd := &cobra.Command{
		Use: use, Short: short, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			payload, err := readInput(file)
			if err != nil {
				return fail(exitUsage, "%v", err)
			}
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.SubmitVersion(ctx, &pb.SubmitVersionRequest{Name: args[0], Payload: payload, Description: desc,
				Environments: envs, DryRun: dryRun, RequestId: requestID()})
			if err != nil {
				return err
			}
			if !human() {
				if err := emitJSON("SubmitVersionResult", r); err != nil {
					return err
				}
			} else {
				printReport(r.Report)
				if r.Version != nil {
					printf("stored %s v%d  sha256:%s  %d bytes  environments %s\n", r.Version.Config, r.Version.Version,
						r.Version.Sha256[:16], r.Version.Size, strings.Join(r.Version.Environments, ","))
				} else if dryRun && r.Report.Ok {
					printf("dry run: nothing stored\n")
				}
			}
			if !r.Report.Ok {
				return &cliError{exitRejected, fmt.Errorf("payload rejected; it was not stored and can reach no agent")}
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "payload file (- for stdin)")
	cmd.Flags().StringVar(&desc, "description", "", "description of the change")
	cmd.Flags().StringSliceVar(&envs, "environment", nil, "environments this version may be delivered to (default: all of the config's)")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

func newValidateCmd() *cobra.Command {
	return submitCmd("validate CONFIG -f payload.json", "Run the full validation pipeline without storing anything", true)
}

func newVersionsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "versions", Short: "Submit and inspect immutable config versions"}
	list := &cobra.Command{
		Use: "list CONFIG", Short: "List versions, newest first", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.ListVersions(ctx, &pb.ListVersionsRequest{Name: args[0], PageSize: 50})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("VersionList", r)
			}
			w := table()
			fmt.Fprintln(w, "VERSION\tSHA256\tBYTES\tCREATED\tBY\tDESCRIPTION")
			for _, v := range r.Versions {
				sha := v.Sha256[:16]
				if v.Purged {
					sha += " (purged)"
				}
				fmt.Fprintf(w, "v%d\t%s\t%d\t%s\t%s\t%s\n", v.Version, sha, v.Size, v.Created.AsTime().Format(time.RFC3339), short(v.CreatedBy), v.Description)
			}
			return w.Flush()
		},
	}
	payload := &cobra.Command{
		Use: "payload CONFIG VERSION", Short: "Print a version's payload", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := parseVersion(args[1])
			if err != nil {
				return err
			}
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.GetVersionPayload(ctx, &pb.GetVersionRequest{Name: args[0], Version: v})
			if err != nil {
				return err
			}
			_, err = g.stdout.Write(r.Payload)
			return err
		},
	}
	cmd.AddCommand(submitCmd("submit CONFIG -f payload.json", "Validate and store a new immutable version", false), list, payload)
	return cmd
}

func short(uri string) string {
	if i := strings.LastIndex(uri, "/"); i >= 0 {
		return uri[i+1:]
	}
	return uri
}

func parseVersion(s string) (int64, error) {
	v, err := strconv.ParseInt(strings.TrimPrefix(s, "v"), 10, 64)
	if err != nil || v < 0 {
		return 0, fail(exitUsage, "invalid version %q", s)
	}
	return v, nil
}

func printDiff(d *pb.DiffVersionsResponse) {
	from := "empty"
	if d.From > 0 {
		from = fmt.Sprintf("v%d", d.From)
	}
	printf("diff %s -> v%d: %d change(s), %d -> %d bytes\n", from, d.To, d.TotalChanges, d.FromSize, d.ToSize)
	for _, e := range d.Entries {
		if e.Path == "" {
			e.Path = "/"
		}
		switch e.Op {
		case "added":
			printf("  + %s = %s\n", e.Path, e.After)
		case "removed":
			printf("  - %s (was %s)\n", e.Path, e.Before)
		default:
			printf("  ~ %s: %s -> %s\n", e.Path, e.Before, e.After)
		}
	}
	if d.Truncated {
		printf("  ... %d more\n", int(d.TotalChanges)-len(d.Entries))
	}
}

func newDiffCmd() *cobra.Command {
	return &cobra.Command{
		Use: "diff CONFIG FROM TO", Short: "Show a structural diff between two versions (FROM 0 means empty)", Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			from, err := parseVersion(args[1])
			if err != nil {
				return err
			}
			to, err := parseVersion(args[2])
			if err != nil {
				return err
			}
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			d, err := c.DiffVersions(ctx, &pb.DiffVersionsRequest{Name: args[0], From: from, To: to})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("Diff", d)
			}
			printDiff(d)
			return nil
		},
	}
}

// --- releases ---------------------------------------------------------------------

func stateName(s pb.ReleaseState) string { return strings.TrimPrefix(s.String(), "RELEASE_STATE_") }

func verdictName(v pb.GateVerdict) string {
	if v == pb.GateVerdict_GATE_VERDICT_UNSPECIFIED {
		return "-"
	}
	return strings.TrimPrefix(v.String(), "GATE_VERDICT_")
}

func printPlan(p *pb.ReleasePlan) {
	printf("plan: %s/%s v%d (baseline v%d), %d fleet members\n", p.Config, p.Environment, p.Version, p.BaselineVersion, p.TotalMembers)
	w := table()
	fmt.Fprintln(w, "RING\tCUMULATIVE\tMEMBERS\tSAMPLE")
	for _, r := range p.Rings {
		fmt.Fprintf(w, "%d %s\t%.2f%%\t%d\t%s\n", r.Index, r.Name, float64(r.CumulativeBps)/100, r.Members, strings.Join(r.Sample, " "))
	}
	if p.HoldoutMembers > 0 {
		fmt.Fprintf(w, "holdout\t-\t%d\t(control for the final ring; converts on completion)\n", p.HoldoutMembers)
	}
	_ = w.Flush()
	gate := "none (manual promotion)"
	if g := p.Policy.GetGate(); g.GetMode() == pb.GateMode_GATE_MODE_PROMETHEUS {
		var names []string
		for _, m := range g.Metrics {
			n := m.Name
			if m.GuardOnly {
				n += " (guard)"
			}
			names = append(names, n)
		}
		gate = fmt.Sprintf("prometheus: %s; %d looks, min bake %s", strings.Join(names, ", "), g.Looks, g.MinBake.AsDuration())
	}
	printf("promotion: %s; gate: %s\n", strings.ToLower(strings.TrimPrefix(p.Policy.GetPromotion().String(), "PROMOTION_")), gate)
	for _, w := range p.Warnings {
		printf("warning: %s\n", w)
	}
	for _, b := range p.Blockers {
		printf("BLOCKER: %s\n", b)
	}
	if p.Diff != nil {
		printDiff(p.Diff)
	}
}

func printRelease(r *pb.Release) {
	printf("release %s  %s/%s  v%d (baseline v%d)\n", r.Id, r.Config, r.Environment, r.Version, r.BaselineVersion)
	st := stateName(r.State)
	if r.Paused {
		st += " (paused)"
	}
	if r.Reason != "" {
		st += " reason=" + r.Reason
	}
	printf("state: %s   active ring: %d   generation: %d   epoch: %d\n", st, r.ActiveRing, r.Generation, r.Epoch)
	if r.Message != "" {
		printf("last event: %s\n", r.Message)
	}
	w := table()
	fmt.Fprintln(w, "RING\tCUMULATIVE\tPHASE\tELIGIBLE\tREACHABLE\tAPPLIED\tACCEPTED\tREJECTED\tFAILED\tGATE")
	for _, rs := range r.Rings {
		c := rs.Counts
		gate := verdictName(rs.GateVerdict)
		if rs.LooksDone > 0 {
			gate = fmt.Sprintf("%s (look %d)", gate, rs.LooksDone)
		}
		fmt.Fprintf(w, "%d %s\t%.2f%%\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n", rs.Index, rs.Name, float64(rs.CumulativeBps)/100, rs.Phase,
			c.Eligible, c.Reachable, c.Applied, c.Accepted, c.Rejected, c.Failed, gate)
	}
	if h := r.Holdout; h != nil {
		fmt.Fprintf(w, "holdout\t-\tcontrol\t%d\t%d\t-\t-\t-\t-\t-\n", h.Eligible, h.Reachable)
	}
	_ = w.Flush()
	for _, rs := range r.Rings {
		if rs.GateReason != "" {
			printf("gate ring %d: %s\n", rs.Index, rs.GateReason)
		}
	}
	if rb := r.Rollback; rb != nil {
		conv := "converging"
		if rb.Converged {
			conv = "converged"
		} else if r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK {
			conv = "NOT converged: unreachable instances will reconcile when they reconnect"
		}
		printf("rollback: fallback v%d at generation %d: %d/%d exposed instances confirmed, %d pending, %d unreachable (%s)\n",
			rb.FallbackVersion, rb.Generation, rb.Confirmed, rb.Exposed, rb.Pending, rb.Unreachable, conv)
		if len(rb.UnreachableSample) > 0 {
			printf("unreachable: %s\n", strings.Join(rb.UnreachableSample, " "))
		}
	}
}

func terminal(s pb.ReleaseState) bool {
	return s == pb.ReleaseState_RELEASE_STATE_COMPLETE || s == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK
}

func newReleasesCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "releases", Aliases: []string{"release"}, Short: "Plan, run and control releases"}
	var env, note string
	createLike := func(use, short string, dry bool) *cobra.Command {
		var wait bool
		c := &cobra.Command{
			Use: use, Short: short, Args: cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				v, err := parseVersion(args[1])
				if err != nil {
					return err
				}
				c, done, err := control(cmd)
				if err != nil {
					return err
				}
				defer done()
				ctx, cancel := reqCtx(cmd)
				defer cancel()
				r, err := c.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: args[0], Environment: env, Version: v, DryRun: dry, RequestId: requestID(), Note: note})
				if err != nil {
					return err
				}
				if !human() && !wait {
					return emitJSON("CreateReleaseResult", r)
				}
				if human() {
					if dry {
						printPlan(r.Plan)
						printf("dry run: no release was created and no agent was contacted\n")
					} else {
						printf("created release %s (%s/%s v%d); the controller starts ring 0 now\n", r.Release.Id, r.Release.Config, r.Release.Environment, r.Release.Version)
					}
				}
				if dry && len(r.Plan.Blockers) > 0 {
					return &cliError{exitPrecondition, fmt.Errorf("the plan has %d blocker(s)", len(r.Plan.Blockers))}
				}
				if wait && !dry {
					return watchRelease(cmd, c, r.Release.Id)
				}
				return nil
			},
		}
		c.Flags().StringVar(&env, "environment", "prod", "environment")
		c.Flags().StringVar(&note, "note", "", "note recorded in the audit log")
		if !dry {
			c.Flags().BoolVar(&wait, "wait", false, "follow the release until it completes or rolls back")
		}
		return c
	}
	var watch bool
	status := &cobra.Command{
		Use: "status RELEASE", Short: "Show a release with per-ring acknowledgements, gate verdicts and rollback convergence", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			if watch {
				return watchRelease(cmd, c, args[0])
			}
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.GetRelease(ctx, &pb.GetReleaseRequest{Id: args[0]})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("Release", r)
			}
			printRelease(r)
			return nil
		},
	}
	status.Flags().BoolVarP(&watch, "watch", "w", false, "follow until the release completes or rolls back (exit 8 if it rolls back)")
	var listEnv, listCfg string
	var active bool
	list := &cobra.Command{
		Use: "list", Short: "List releases, newest first", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.ListReleases(ctx, &pb.ListReleasesRequest{Config: listCfg, Environment: listEnv, ActiveOnly: active, PageSize: 50})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("ReleaseList", r)
			}
			w := table()
			fmt.Fprintln(w, "ID\tTARGET\tVERSION\tSTATE\tRING\tREASON\tCREATED")
			for _, rel := range r.Releases {
				st := stateName(rel.State)
				if rel.Paused {
					st += "(paused)"
				}
				fmt.Fprintf(w, "%s\t%s/%s\tv%d<-v%d\t%s\t%d\t%s\t%s\n", rel.Id, rel.Config, rel.Environment, rel.Version, rel.BaselineVersion,
					st, rel.ActiveRing, rel.Reason, rel.Created.AsTime().Format(time.RFC3339))
			}
			return w.Flush()
		},
	}
	list.Flags().StringVar(&listCfg, "config", "", "filter by config")
	list.Flags().StringVar(&listEnv, "environment", "", "filter by environment")
	list.Flags().BoolVar(&active, "active", false, "only active releases")

	var expect int32
	var override bool
	var reason string
	promote := &cobra.Command{
		Use: "promote RELEASE", Short: "Expose the next ring (bake and gate still apply unless --override)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			req := &pb.PromoteReleaseRequest{Id: args[0], Override: override, Reason: reason, RequestId: requestID()}
			if cmd.Flags().Changed("expect-ring") {
				req.ExpectedRing, req.HasExpectedRing = expect, true
			}
			r, err := c.PromoteRelease(ctx, req)
			return afterCommand(r, err, "promoted")
		},
	}
	promote.Flags().Int32Var(&expect, "expect-ring", 0, "refuse unless this ring is the active one (guards against double promotion)")
	promote.Flags().BoolVar(&override, "override", false, "skip bake and gate requirements (break-glass role; audited)")
	promote.Flags().StringVar(&reason, "reason", "", "reason (required with --override)")
	simple := func(name, short, verb string, f func(pb.ControlServiceClient) func(cmd *cobra.Command, req *pb.ReleaseCommandRequest) (*pb.Release, error)) *cobra.Command {
		var reason string
		c := &cobra.Command{
			Use: name + " RELEASE", Short: short, Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				c, done, err := control(cmd)
				if err != nil {
					return err
				}
				defer done()
				r, err := f(c)(cmd, &pb.ReleaseCommandRequest{Id: args[0], Reason: reason, RequestId: requestID()})
				return afterCommand(r, err, verb)
			},
		}
		c.Flags().StringVar(&reason, "reason", "", "reason recorded in the audit log")
		return c
	}
	pause := simple("pause", "Stop forward progress (bake time does not count while paused)", "paused",
		func(c pb.ControlServiceClient) func(*cobra.Command, *pb.ReleaseCommandRequest) (*pb.Release, error) {
			return func(cmd *cobra.Command, r *pb.ReleaseCommandRequest) (*pb.Release, error) {
				ctx, cancel := reqCtx(cmd)
				defer cancel()
				return c.PauseRelease(ctx, r)
			}
		})
	resume := simple("resume", "Resume a paused or halted release from fresh evidence", "resumed",
		func(c pb.ControlServiceClient) func(*cobra.Command, *pb.ReleaseCommandRequest) (*pb.Release, error) {
			return func(cmd *cobra.Command, r *pb.ReleaseCommandRequest) (*pb.Release, error) {
				ctx, cancel := reqCtx(cmd)
				defer cancel()
				return c.ResumeRelease(ctx, r)
			}
		})
	rollback := simple("rollback", "Return every ring to the baseline (new generation, older version)", "rolling back",
		func(c pb.ControlServiceClient) func(*cobra.Command, *pb.ReleaseCommandRequest) (*pb.Release, error) {
			return func(cmd *cobra.Command, r *pb.ReleaseCommandRequest) (*pb.Release, error) {
				ctx, cancel := reqCtx(cmd)
				defer cancel()
				return c.RollbackRelease(ctx, r)
			}
		})
	cmd.AddCommand(createLike("plan CONFIG VERSION", "Dry run: show cohorts, blockers and the diff without changing anything", true),
		createLike("create CONFIG VERSION", "Create a release (the controller starts it)", false), status, list, promote, pause, resume, rollback)
	return cmd
}

func afterCommand(r *pb.Release, err error, verb string) error {
	if err != nil {
		return err
	}
	if !human() {
		return emitJSON("Release", r)
	}
	printf("%s %s: state %s, active ring %d\n", verb, r.Id, stateName(r.State), r.ActiveRing)
	return nil
}

func watchRelease(cmd *cobra.Command, c pb.ControlServiceClient, id string) error {
	last := ""
	failures := 0
	for {
		ctx, cancel := reqCtx(cmd)
		r, err := c.GetRelease(ctx, &pb.GetReleaseRequest{Id: id})
		cancel()
		if err != nil {
			if failures++; failures > 10 || cmd.Context().Err() != nil {
				return err
			}
		} else {
			failures = 0
			line := summary(r)
			if line != last {
				last = line
				if human() {
					printf("%s  %s\n", time.Now().Format("15:04:05"), line)
				} else {
					_ = emitJSON("Release", r)
				}
			}
			if terminal(r.State) && (r.Rollback == nil || r.Rollback.Converged || r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK) {
				if human() {
					printRelease(r)
				}
				if r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK {
					return &cliError{exitReleaseFail, fmt.Errorf("release %s was rolled back (%s)", id, r.Reason)}
				}
				return nil
			}
			if r.State == pb.ReleaseState_RELEASE_STATE_HALTED {
				if human() {
					printRelease(r)
				}
				return &cliError{exitReleaseFail, fmt.Errorf("release %s halted (%s); resume or roll it back", id, r.Reason)}
			}
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func summary(r *pb.Release) string {
	s := fmt.Sprintf("%s ring=%d", stateName(r.State), r.ActiveRing)
	if r.Paused {
		s += " paused"
	}
	if r.ActiveRing >= 0 && int(r.ActiveRing) < len(r.Rings) {
		rs := r.Rings[r.ActiveRing]
		s += fmt.Sprintf(" accepted=%d/%d gate=%s", rs.Counts.Accepted, rs.Counts.Eligible, verdictName(rs.GateVerdict))
		if rs.LooksDone > 0 {
			s += fmt.Sprintf("(look %d)", rs.LooksDone)
		}
	}
	if rb := r.Rollback; rb != nil {
		s += fmt.Sprintf(" rollback: confirmed=%d/%d pending=%d unreachable=%d", rb.Confirmed, rb.Exposed, rb.Pending, rb.Unreachable)
	}
	if r.Reason != "" {
		s += " reason=" + r.Reason
	}
	return s
}

// --- emergency, targets, instances, audit, info -----------------------------------

func newKillSwitchCmd() *cobra.Command {
	var env, reason string
	cmd := &cobra.Command{
		Use:     "kill-switch CONFIG",
		Aliases: []string{"emergency-rollback"},
		Short:   "Break-glass: return every ring to the previous known-good version now and freeze the target",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.KillSwitch(ctx, &pb.KillSwitchRequest{Config: args[0], Environment: env, Reason: reason, RequestId: requestID()})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("KillSwitchResult", r)
			}
			printf("kill switch engaged for %s/%s: baseline v%d, generation %d, target frozen (%s)\n", r.Target.Config, r.Target.Environment,
				r.Target.BaselineVersion, r.Target.Generation, r.Target.FrozenReason)
			if r.Release != nil {
				printf("tracking convergence in release %s: cfgcanary releases status %s --watch\n", r.Release.Id, r.Release.Id)
			}
			printf("connected agents converge within a watch round trip; disconnected agents keep their current config until they reconnect\n")
			return nil
		},
	}
	cmd.Flags().StringVar(&env, "environment", "prod", "environment")
	cmd.Flags().StringVar(&reason, "reason", "", "reason (required; audited)")
	_ = cmd.MarkFlagRequired("reason")
	return cmd
}

func newUnfreezeCmd() *cobra.Command {
	var env, reason string
	cmd := &cobra.Command{
		Use: "unfreeze CONFIG", Short: "Lift a kill-switch freeze (admin)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			t, err := c.UnfreezeTarget(ctx, &pb.UnfreezeTargetRequest{Config: args[0], Environment: env, Reason: reason})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("Target", t)
			}
			printf("%s/%s unfrozen (baseline v%d)\n", t.Config, t.Environment, t.BaselineVersion)
			return nil
		},
	}
	cmd.Flags().StringVar(&env, "environment", "prod", "environment")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}

func newTargetCmd() *cobra.Command {
	var env string
	cmd := &cobra.Command{
		Use: "target CONFIG", Short: "Show the desired state of a config in an environment", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			t, err := c.GetTarget(ctx, &pb.GetTargetRequest{Config: args[0], Environment: env})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("Target", t)
			}
			printf("%s/%s generation %d: baseline v%d, candidate v%d (rings <= %d), previous good v%d, release %q, frozen %v %s\n",
				t.Config, t.Environment, t.Generation, t.BaselineVersion, t.CandidateVersion, t.ActiveRing, t.PreviousGoodVersion, t.ReleaseId, t.Frozen, t.FrozenReason)
			return nil
		},
	}
	cmd.Flags().StringVar(&env, "environment", "prod", "environment")
	return cmd
}

func newInstancesCmd() *cobra.Command {
	var env string
	cmd := &cobra.Command{
		Use: "instances", Short: "List known instances and what they applied and serve", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			var all []*pb.Instance
			token := ""
			for {
				r, err := c.ListInstances(ctx, &pb.ListInstancesRequest{Environment: env, PageSize: 500, PageToken: token})
				if err != nil {
					return err
				}
				all = append(all, r.Instances...)
				if token = r.NextPageToken; token == "" {
					break
				}
			}
			if !human() {
				return emitJSON("InstanceList", map[string]any{"instances": all})
			}
			w := table()
			fmt.Fprintln(w, "INSTANCE\tENV\tCONNECTED\tCONFIG\tGEN\tVERSION\tSTATE\tSERVING\tREASON")
			for _, in := range all {
				if len(in.Statuses) == 0 {
					fmt.Fprintf(w, "%s\t%s\t%v\t-\t-\t-\t-\t-\t\n", in.InstanceId, in.Environment, in.Connected)
				}
				for _, s := range in.Statuses {
					fmt.Fprintf(w, "%s\t%s\t%v\t%s\t%d\tv%d\t%s\tv%d\t%s\n", in.InstanceId, in.Environment, in.Connected, s.Config, s.Generation,
						s.Version, strings.TrimPrefix(s.State.String(), "APPLY_STATE_"), s.ServingVersion, s.Reason)
				}
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&env, "environment", "", "filter by environment")
	return cmd
}

func newAuditCmd() *cobra.Command {
	var cfg, env, release, action string
	var limit int32
	cmd := &cobra.Command{
		Use: "audit", Short: "Show the append-only audit log (privileged actions and denied attempts)", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			r, err := c.ListAudit(ctx, &pb.ListAuditRequest{Config: cfg, Environment: env, ReleaseId: release, Action: action, PageSize: limit})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("AuditList", r)
			}
			w := table()
			fmt.Fprintln(w, "ID\tTIME\tACTOR\tACTION\tOUTCOME\tTARGET\tRELEASE\tGEN\tREASON")
			for _, e := range r.Events {
				tgt := e.Config
				if e.Environment != "" {
					tgt += "/" + e.Environment
				}
				fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", e.Id, e.Time.AsTime().Format("15:04:05.000"), short(e.Actor), e.Action,
					e.Outcome, tgt, e.ReleaseId, e.Generation, e.Reason)
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&cfg, "config", "", "filter by config")
	cmd.Flags().StringVar(&env, "environment", "", "filter by environment")
	cmd.Flags().StringVar(&release, "release", "", "filter by release")
	cmd.Flags().StringVar(&action, "action", "", "filter by action, e.g. release.promote")
	cmd.Flags().Int32Var(&limit, "limit", 30, "number of events")
	return cmd
}

func newInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use: "info", Short: "Show the server version, leadership and your identity and roles", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, done, err := control(cmd)
			if err != nil {
				return err
			}
			defer done()
			ctx, cancel := reqCtx(cmd)
			defer cancel()
			i, err := c.GetServerInfo(ctx, &pb.GetServerInfoRequest{})
			if err != nil {
				return err
			}
			if !human() {
				return emitJSON("ServerInfo", i)
			}
			printf("server %s (%s) %s; leader on this replica: %v (epoch %d); %d watch streams\nyou are %s with roles %v\n",
				i.Version, i.Commit, i.GoVersion, i.Leader, i.Epoch, i.Streams, i.Identity, i.Roles)
			return nil
		},
	}
}
