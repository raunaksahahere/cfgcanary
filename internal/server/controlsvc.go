package server

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/auth"
	"github.com/raunaksahahere/cfgcanary/internal/blob"
	"github.com/raunaksahahere/cfgcanary/internal/policy"
	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/validate"
)

var envNameRe = configName

// RegisterConfig creates or updates a config. Policies are validated here, so
// a broken policy is refused when it is written.
func (s *Server) RegisterConfig(ctx context.Context, req *pb.RegisterConfigRequest) (*pb.Config, error) {
	spec := req.GetSpec()
	id, err := s.authorize(ctx, auth.PermAdmin, "config.register", spec.GetName(), "")
	if err != nil {
		return nil, err
	}
	if spec == nil || !configName.MatchString(spec.Name) {
		return nil, status.Error(codes.InvalidArgument, "spec.name must match ^[a-z0-9][a-z0-9._-]{0,62}$")
	}
	spec = proto.Clone(spec).(*pb.ConfigSpec)
	if spec.Format == "" {
		spec.Format = "json"
	}
	if spec.Format != "json" {
		return nil, status.Error(codes.InvalidArgument, "only format \"json\" is supported in v1")
	}
	if spec.FailureMode == pb.FailureMode_FAILURE_MODE_UNSPECIFIED {
		spec.FailureMode = pb.FailureMode_FAILURE_MODE_FAIL_STATIC
	}
	if len(spec.Environments) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one environment with a rollout policy is required")
	}
	for env, p := range spec.Environments {
		if !envNameRe.MatchString(env) || strings.ContainsAny(env, "._") {
			return nil, status.Errorf(codes.InvalidArgument, "invalid environment name %q", env)
		}
		if _, err := policy.Normalize(spec.Name, env, p); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "environments[%s]: %v", env, err)
		}
	}
	if err := validate.CheckPolicy(spec.Validation, s.cfg.HardMaxBytes); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if w := spec.Validation.GetWasm(); w != nil {
		switch {
		case len(w.Module) > 0:
			if len(w.Module) > validate.MaxWasmModuleBytes {
				return nil, status.Errorf(codes.InvalidArgument, "wasm module is larger than %d bytes", validate.MaxWasmModuleBytes)
			}
			// Probe the module against an empty payload so a module that
			// cannot be compiled or instantiated is refused now.
			if _, err := s.validator.Sandbox.Run(ctx, w.Module, w, []byte("{}")); err != nil && !errors.Is(err, validate.ErrWasmTimeout) {
				return nil, status.Errorf(codes.InvalidArgument, "wasm validator does not run: %v", err)
			}
			sha := blob.Digest(w.Module)
			if _, err := s.store.BlobBegin(ctx, sha, int64(len(w.Module)), "wasm"); err != nil {
				return nil, toStatus(err)
			}
			if err := s.blobs.Put(ctx, sha, w.Module); err != nil {
				return nil, status.Error(codes.Unavailable, "object store unavailable")
			}
			if err := s.store.BlobCommit(ctx, sha); err != nil {
				return nil, toStatus(err)
			}
			w.Sha256, w.Required = sha, true
		case w.Sha256 == "":
			return nil, status.Error(codes.InvalidArgument, "validation.wasm needs module bytes")
		default:
			// Keep a module registered earlier: it must exist.
			if ok, err := s.blobs.Exists(ctx, w.Sha256); err != nil || !ok {
				return nil, status.Error(codes.FailedPrecondition, "validation.wasm.sha256 does not name a stored module")
			}
			w.Required = true
		}
	}
	cfg, err := s.store.UpsertConfig(ctx, spec, req.ExpectedRevision, id.URI)
	return cfg, toStatus(err)
}

// GetConfig returns a config.
func (s *Server) GetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.Config, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "config.get", req.Name, ""); err != nil {
		return nil, err
	}
	c, err := s.store.GetConfig(ctx, req.Name)
	return c, toStatus(err)
}

// ListConfigs lists configs.
func (s *Server) ListConfigs(ctx context.Context, req *pb.ListConfigsRequest) (*pb.ListConfigsResponse, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "config.list", "", ""); err != nil {
		return nil, err
	}
	cs, next, err := s.store.ListConfigs(ctx, int(req.PageSize), req.PageToken)
	return &pb.ListConfigsResponse{Configs: cs, NextPageToken: next}, toStatus(err)
}

func (s *Server) wasmModule(ctx context.Context, sha string) ([]byte, error) {
	s.wasmMu.Lock()
	m, ok := s.wasmCache[sha]
	s.wasmMu.Unlock()
	if ok {
		return m, nil
	}
	m, err := s.blobs.Get(ctx, sha, validate.MaxWasmModuleBytes)
	if err != nil {
		return nil, err
	}
	s.wasmMu.Lock()
	if len(s.wasmCache) > 32 {
		s.wasmCache = map[string][]byte{}
	}
	s.wasmCache[sha] = m
	s.wasmMu.Unlock()
	return m, nil
}

func (s *Server) payload(ctx context.Context, config string, version int64) ([]byte, *pb.ConfigVersion, error) {
	v, err := s.store.GetVersion(ctx, config, version)
	if err != nil {
		return nil, nil, err
	}
	if v.Purged {
		return nil, v, fmt.Errorf("%w: version %d was garbage-collected", store.ErrNotFound, version)
	}
	b, err := s.blobs.Get(ctx, v.Sha256, max(v.Size, 1))
	if err != nil {
		return nil, v, status.Errorf(codes.Unavailable, "object store: %v", err)
	}
	return b, v, nil
}

// SubmitVersion validates a payload against the config's policy and the
// correct base, then stores it (blob first, reference second).
func (s *Server) SubmitVersion(ctx context.Context, req *pb.SubmitVersionRequest) (*pb.SubmitVersionResponse, error) {
	id, err := s.authorize(ctx, auth.PermSubmit, "version.submit", req.Name, "")
	if err != nil {
		return nil, err
	}
	if int64(len(req.Payload)) > s.cfg.HardMaxBytes {
		s.metrics.submissions.WithLabelValues("rejected", "limits").Inc()
		return &pb.SubmitVersionResponse{Report: &pb.ValidationReport{StagesRun: []string{"limits"}, Issues: []*pb.ValidationIssue{{
			Stage: "limits", Code: "too_large", Message: fmt.Sprintf("payload is %d bytes; the server limit is %d", len(req.Payload), s.cfg.HardMaxBytes)}}}}, nil
	}
	cfg, err := s.store.GetConfig(ctx, req.Name)
	if err != nil {
		return nil, toStatus(err)
	}
	envs := req.Environments
	if len(envs) == 0 {
		for e := range cfg.Spec.Environments {
			envs = append(envs, e)
		}
	}
	sort.Strings(envs)
	for _, e := range envs {
		if _, ok := cfg.Spec.Environments[e]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "config %q has no environment %q", req.Name, e)
		}
	}
	// Relative guards compare with what is actually deployed: each
	// environment's baseline. With no baseline anywhere, the latest version;
	// with no version at all, there is no base (first version).
	bases := map[int64]bool{}
	for _, e := range envs {
		if t, err := s.store.TargetRow(ctx, req.Name, e); err == nil && t.T.Baseline > 0 {
			bases[t.T.Baseline] = true
		}
	}
	if len(bases) == 0 && cfg.LatestVersion > 0 {
		if v, err := s.store.GetVersion(ctx, req.Name, cfg.LatestVersion); err == nil && !v.Purged {
			bases[cfg.LatestVersion] = true
		}
	}
	var module []byte
	if w := cfg.Spec.Validation.GetWasm(); w != nil && w.Sha256 != "" {
		if module, err = s.wasmModule(ctx, w.Sha256); err != nil {
			s.log.Warn("required validator module unavailable", "config", req.Name, "err", err)
			module = nil // the pipeline rejects with wasm/unavailable
		}
	}
	report := &pb.ValidationReport{}
	baseList := make([]int64, 0, len(bases))
	for b := range bases {
		baseList = append(baseList, b)
	}
	sort.Slice(baseList, func(i, j int) bool { return baseList[i] < baseList[j] })
	if len(baseList) == 0 {
		baseList = []int64{0}
	}
	for _, b := range baseList {
		in := validate.Input{Policy: cfg.Spec.Validation, Payload: req.Payload, WasmModule: module}
		if b > 0 {
			bp, _, err := s.payload(ctx, req.Name, b)
			if err != nil {
				return nil, status.Errorf(codes.Unavailable, "cannot load base version %d for relative checks: %v", b, err)
			}
			in.Base, in.BaseVersion = bp, b
		}
		r := s.validator.Validate(ctx, in)
		if report.StagesRun == nil {
			report.StagesRun, report.BaseVersion = r.StagesRun, r.BaseVersion
		}
		report.Issues = append(report.Issues, r.Issues...)
	}
	report.Ok = len(report.Issues) == 0
	resp := &pb.SubmitVersionResponse{Report: report}
	if !report.Ok {
		s.metrics.submissions.WithLabelValues("rejected", report.Issues[0].Stage).Inc()
		_ = s.store.RecordAudit(ctx, store.Audit{Actor: id.URI, Action: "version.reject", Config: req.Name,
			Detail: map[string]any{"stage": report.Issues[0].Stage, "code": report.Issues[0].Code, "size": len(req.Payload), "dry_run": req.DryRun}})
		return resp, nil
	}
	if req.DryRun {
		s.metrics.submissions.WithLabelValues("dry_run", "").Inc()
		return resp, nil
	}
	sha := blob.Digest(req.Payload)
	committed, err := s.store.BlobBegin(ctx, sha, int64(len(req.Payload)), "payload")
	if err != nil {
		return nil, toStatus(err)
	}
	if !committed {
		if err := s.blobs.Put(ctx, sha, req.Payload); err != nil {
			return nil, status.Error(codes.Unavailable, "object store unavailable; nothing was stored")
		}
	}
	v, err := s.store.InsertVersion(ctx, req.Name, sha, int64(len(req.Payload)), req.Description, envs, id.URI, req.RequestId,
		func(a sign.Artifact) (sign.Signed, error) { return s.artifactSigner.Sign(a) })
	if err != nil {
		return nil, toStatus(err)
	}
	s.metrics.submissions.WithLabelValues("stored", "").Inc()
	resp.Version = v
	return resp, nil
}

// GetVersion returns version metadata.
func (s *Server) GetVersion(ctx context.Context, req *pb.GetVersionRequest) (*pb.ConfigVersion, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "version.get", req.Name, ""); err != nil {
		return nil, err
	}
	v, err := s.store.GetVersion(ctx, req.Name, req.Version)
	return v, toStatus(err)
}

// ListVersions lists versions, newest first.
func (s *Server) ListVersions(ctx context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "version.list", req.Name, ""); err != nil {
		return nil, err
	}
	before, _ := strconv.ParseInt(req.PageToken, 10, 64)
	vs, next, err := s.store.ListVersions(ctx, req.Name, int(req.PageSize), before)
	resp := &pb.ListVersionsResponse{Versions: vs}
	if next > 0 {
		resp.NextPageToken = strconv.FormatInt(next, 10)
	}
	return resp, toStatus(err)
}

// GetVersionPayload returns the payload bytes. Payloads are configuration,
// not secrets, but they are still only served to authorized readers.
func (s *Server) GetVersionPayload(ctx context.Context, req *pb.GetVersionRequest) (*pb.VersionPayload, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "version.payload", req.Name, ""); err != nil {
		return nil, err
	}
	b, v, err := s.payload(ctx, req.Name, req.Version)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.VersionPayload{Version: v, Payload: b}, nil
}

func (s *Server) diff(ctx context.Context, config string, from, to int64) (*pb.DiffVersionsResponse, error) {
	load := func(v int64) (any, int64, error) {
		if v == 0 {
			return nil, 0, nil
		}
		b, _, err := s.payload(ctx, config, v)
		if err != nil {
			return nil, 0, err
		}
		doc, err := validate.ParseStrict(b, 0)
		return doc, int64(len(b)), err
	}
	a, as, err := load(from)
	if err != nil {
		return nil, err
	}
	b, bs, err := load(to)
	if err != nil {
		return nil, err
	}
	changes, total := validate.Diff(a, b, 200)
	resp := &pb.DiffVersionsResponse{From: from, To: to, FromSize: as, ToSize: bs, TotalChanges: int32(total), Truncated: total > len(changes)}
	for _, c := range changes {
		resp.Entries = append(resp.Entries, &pb.DiffEntry{Op: c.Op, Path: c.Path, Before: c.Before, After: c.After})
	}
	return resp, nil
}

// DiffVersions compares two versions structurally.
func (s *Server) DiffVersions(ctx context.Context, req *pb.DiffVersionsRequest) (*pb.DiffVersionsResponse, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "version.diff", req.Name, ""); err != nil {
		return nil, err
	}
	d, err := s.diff(ctx, req.Name, req.From, req.To)
	return d, toStatus(err)
}

// CreateRelease plans and (unless dry_run) creates a release. A dry run
// writes nothing and contacts no agent.
func (s *Server) CreateRelease(ctx context.Context, req *pb.CreateReleaseRequest) (*pb.CreateReleaseResponse, error) {
	id, err := s.authorize(ctx, auth.PermSubmit, "release.create", req.Config, req.Environment)
	if err != nil {
		return nil, err
	}
	in := store.PlanInput{Config: req.Config, Env: req.Environment, Version: req.Version, MembershipWindow: s.cfg.MembershipWindow}
	if req.DryRun {
		plan, err := s.store.PlanRelease(ctx, in)
		if err != nil {
			return nil, toStatus(err)
		}
		if d, err := s.diff(ctx, req.Config, plan.BaselineVersion, req.Version); err == nil {
			plan.Diff = d
		}
		return &pb.CreateReleaseResponse{Plan: plan}, nil
	}
	row, plan, err := s.store.CreateRelease(ctx, in, id.URI, req.RequestId, req.Note)
	if err != nil {
		if errors.Is(err, store.ErrPrecondition) && plan != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "%v (%d blocker(s): %s)", err, len(plan.Blockers), strings.Join(plan.Blockers, "; "))
		}
		return nil, toStatus(err)
	}
	rel, err := s.releaseProto(ctx, row)
	return &pb.CreateReleaseResponse{Plan: plan, Release: rel}, toStatus(err)
}

// GetRelease returns a release with live per-ring counts.
func (s *Server) GetRelease(ctx context.Context, req *pb.GetReleaseRequest) (*pb.Release, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "release.get", "", ""); err != nil {
		return nil, err
	}
	row, err := s.store.ReleaseRow(ctx, req.Id)
	if err != nil {
		return nil, toStatus(err)
	}
	rel, err := s.releaseProto(ctx, row)
	return rel, toStatus(err)
}

// ListReleases lists releases, newest first.
func (s *Server) ListReleases(ctx context.Context, req *pb.ListReleasesRequest) (*pb.ListReleasesResponse, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "release.list", req.Config, req.Environment); err != nil {
		return nil, err
	}
	var before time.Time
	if req.PageToken != "" {
		ns, err := strconv.ParseInt(req.PageToken, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad page_token")
		}
		before = time.Unix(0, ns)
	}
	rows, next, err := s.store.ListReleases(ctx, req.Config, req.Environment, req.ActiveOnly, int(req.PageSize), before)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.ListReleasesResponse{}
	for _, r := range rows {
		resp.Releases = append(resp.Releases, s.releaseSummary(r))
	}
	if !next.IsZero() {
		resp.NextPageToken = strconv.FormatInt(next.UnixNano(), 10)
	}
	return resp, nil
}

func (s *Server) command(ctx context.Context, perm auth.Permission, action, id string, ev rollout.Event, requestID, method string) (*pb.Release, error) {
	row, err := s.store.ReleaseRow(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	who, err := s.authorize(ctx, perm, action, row.Config, row.Env)
	if err != nil {
		return nil, err
	}
	row, _, err = s.store.ApplyEvent(ctx, id, ev, store.EventMeta{Actor: who.URI, RequestID: requestID, Method: method})
	if err != nil {
		return nil, toStatus(err)
	}
	return s.releaseProto(ctx, row)
}

// PromoteRelease exposes the next ring. Safety conditions (bake, gate) still
// apply; override skips them and needs the break-glass role and a reason.
func (s *Server) PromoteRelease(ctx context.Context, req *pb.PromoteReleaseRequest) (*pb.Release, error) {
	perm, action := auth.PermOperate, "release.promote"
	if req.Override {
		perm, action = auth.PermBreakGlass, "release.promote_override"
		if strings.TrimSpace(req.Reason) == "" {
			return nil, status.Error(codes.InvalidArgument, "an override promotion requires a reason")
		}
	}
	ev := rollout.Event{Kind: rollout.EvPromote, Override: req.Override, Reason: req.Reason}
	if req.HasExpectedRing {
		r := req.ExpectedRing
		ev.ExpectRing = &r
	}
	return s.command(ctx, perm, action, req.Id, ev, req.RequestId, "PromoteRelease")
}

// PauseRelease stops forward progress.
func (s *Server) PauseRelease(ctx context.Context, req *pb.ReleaseCommandRequest) (*pb.Release, error) {
	return s.command(ctx, auth.PermOperate, "release.pause", req.Id, rollout.Event{Kind: rollout.EvPause, Reason: req.Reason}, req.RequestId, "PauseRelease")
}

// ResumeRelease resumes a paused or halted release from fresh evidence.
func (s *Server) ResumeRelease(ctx context.Context, req *pb.ReleaseCommandRequest) (*pb.Release, error) {
	return s.command(ctx, auth.PermOperate, "release.resume", req.Id, rollout.Event{Kind: rollout.EvResume, Reason: req.Reason}, req.RequestId, "ResumeRelease")
}

// RollbackRelease returns every ring to the baseline.
func (s *Server) RollbackRelease(ctx context.Context, req *pb.ReleaseCommandRequest) (*pb.Release, error) {
	reason := req.Reason
	if reason == "" {
		reason = "operator rollback"
	}
	return s.command(ctx, auth.PermOperate, "release.rollback", req.Id, rollout.Event{Kind: rollout.EvRollback, Reason: reason}, req.RequestId, "RollbackRelease")
}

// KillSwitch is the audited break-glass path.
func (s *Server) KillSwitch(ctx context.Context, req *pb.KillSwitchRequest) (*pb.KillSwitchResponse, error) {
	id, err := s.authorize(ctx, auth.PermBreakGlass, "target.kill_switch", req.Config, req.Environment)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Reason) == "" {
		return nil, status.Error(codes.InvalidArgument, "the kill switch requires a reason")
	}
	rel, tgt, _, err := s.store.KillSwitch(ctx, req.Config, req.Environment, req.Reason, id.URI, req.RequestId, s.cfg.MembershipWindow)
	if err != nil {
		return nil, toStatus(err)
	}
	if tgt == nil {
		if tgt, err = s.store.TargetRow(ctx, req.Config, req.Environment); err != nil {
			return nil, toStatus(err)
		}
	}
	resp := &pb.KillSwitchResponse{Target: tgt.ProtoTarget()}
	if rel != nil {
		if resp.Release, err = s.releaseProto(ctx, rel); err != nil {
			return nil, toStatus(err)
		}
	}
	return resp, nil
}

// UnfreezeTarget lifts a kill-switch freeze.
func (s *Server) UnfreezeTarget(ctx context.Context, req *pb.UnfreezeTargetRequest) (*pb.Target, error) {
	id, err := s.authorize(ctx, auth.PermAdmin, "target.unfreeze", req.Config, req.Environment)
	if err != nil {
		return nil, err
	}
	t, err := s.store.Unfreeze(ctx, req.Config, req.Environment, req.Reason, id.URI)
	return t, toStatus(err)
}

// GetTarget returns the desired state of a target.
func (s *Server) GetTarget(ctx context.Context, req *pb.GetTargetRequest) (*pb.Target, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "target.get", req.Config, req.Environment); err != nil {
		return nil, err
	}
	t, err := s.store.GetTarget(ctx, req.Config, req.Environment)
	return t, toStatus(err)
}

// GetRingProjection returns each ring's desired version with its signed
// envelope, for sinks such as the Kubernetes controller.
func (s *Server) GetRingProjection(ctx context.Context, req *pb.GetTargetRequest) (*pb.RingProjection, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "target.rings", req.Config, req.Environment); err != nil {
		return nil, err
	}
	k := targetKey{req.Config, req.Environment}
	s.hub.invalidate(k) // read fresh state
	snap, err := s.hub.snapshot(ctx, k)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &pb.RingProjection{Target: snap.row.ProtoTarget()}
	rings := snap.row.RingPolicy.GetRings()
	n := max(len(rings), 1)
	for i := 0; i < n; i++ {
		name := "all"
		if i < len(rings) {
			name = rings[i].Name
		}
		rd := &pb.RingDesired{Ring: int32(i), Name: name}
		// Build exactly what an instance pinned to this ring would receive.
		u, err := s.hub.updateForRing(ctx, k, int32(i))
		if err != nil {
			return nil, toStatus(err)
		}
		if u.Kind == pb.ConfigUpdate_KIND_DESIRED {
			rd.Version, rd.Sha256, rd.Size, rd.Artifact, rd.Delivery = u.Version, u.Sha256, u.Size, u.Artifact, u.Delivery
		}
		out.Rings = append(out.Rings, rd)
	}
	return out, nil
}

// ListInstances lists known instances with their latest statuses.
func (s *Server) ListInstances(ctx context.Context, req *pb.ListInstancesRequest) (*pb.ListInstancesResponse, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "instance.list", "", req.Environment); err != nil {
		return nil, err
	}
	afterEnv, after, _ := strings.Cut(req.PageToken, "/")
	if req.PageToken == "" {
		afterEnv, after = "", ""
	}
	if req.Environment != "" && afterEnv == "" {
		afterEnv = req.Environment
	}
	ins, next, err := s.store.ListInstances(ctx, req.Environment, int(req.PageSize), afterEnv, after, s.cfg.ConnectedWindow)
	return &pb.ListInstancesResponse{Instances: ins, NextPageToken: next}, toStatus(err)
}

// ListAudit lists audit events, newest first.
func (s *Server) ListAudit(ctx context.Context, req *pb.ListAuditRequest) (*pb.ListAuditResponse, error) {
	if _, err := s.authorize(ctx, auth.PermRead, "audit.list", req.Config, req.Environment); err != nil {
		return nil, err
	}
	before, _ := strconv.ParseInt(req.PageToken, 10, 64)
	evs, next, err := s.store.ListAudit(ctx, req.Config, req.Environment, req.ReleaseId, req.Action, int(req.PageSize), before)
	resp := &pb.ListAuditResponse{Events: evs}
	if next > 0 {
		resp.NextPageToken = strconv.FormatInt(next, 10)
	}
	return resp, toStatus(err)
}

// GetServerInfo describes the server and the caller.
func (s *Server) GetServerInfo(ctx context.Context, _ *pb.GetServerInfoRequest) (*pb.ServerInfo, error) {
	id, ok := auth.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no identity")
	}
	info := &pb.ServerInfo{Version: s.deps.Version, Commit: s.deps.Commit, BuildDate: s.deps.BuildDate, GoVersion: runtime.Version(),
		Identity: id.URI, Roles: s.authz.RolesOf(id), Streams: int32(s.hub.streamCount())}
	if s.deps.Leader != nil {
		info.Leader, info.Epoch = s.deps.Leader()
	}
	return info, nil
}

// --- release conversion --------------------------------------------------------

func stateProto(st rollout.State) pb.ReleaseState {
	return pb.ReleaseState(pb.ReleaseState_value["RELEASE_STATE_"+string(st)])
}

func countsProto(c rollout.Counts) *pb.CohortCounts {
	return &pb.CohortCounts{Eligible: c.Eligible, Reachable: c.Reachable, Applied: c.Applied, Accepted: c.Accepted,
		Rejected: c.Rejected, Failed: c.Failed, CrashLoop: c.CrashLoop}
}

func (s *Server) releaseSummary(r *store.ReleaseRow) *pb.Release {
	rel := &pb.Release{Id: r.ID, Config: r.Config, Environment: r.Env, Version: r.R.Candidate, BaselineVersion: r.R.Baseline,
		State: stateProto(r.R.State), Paused: r.R.Paused, ActiveRing: r.R.ActiveRing, Reason: r.R.Reason, Message: r.Message,
		Epoch: r.Epoch, StateVersion: r.R.StateVersion, Policy: r.Policy, CreatedBy: r.CreatedBy, Created: ts(r.Created),
		Updated: ts(r.Updated), Kind: r.R.Kind}
	if r.Finished != nil {
		rel.Finished = ts(*r.Finished)
	}
	rel.Generation = r.R.RingGeneration
	if r.R.RollbackGeneration > 0 {
		rel.Generation = r.R.RollbackGeneration
	}
	return rel
}

func (s *Server) releaseProto(ctx context.Context, r *store.ReleaseRow) (*pb.Release, error) {
	rel := s.releaseSummary(r)
	counts, err := s.store.Counts(ctx, r)
	if err != nil {
		return nil, err
	}
	byRing := map[int32]store.RingCounts{}
	for _, c := range counts {
		byRing[c.Ring] = c
	}
	for i, pr := range r.Policy.GetRings() {
		rs := &pb.RingStatus{Index: int32(i), Name: pr.Name, CumulativeBps: pr.CumulativeBps, Counts: countsProto(byRing[int32(i)].C)}
		if i < len(r.Rings) {
			p := r.Rings[i]
			rs.Phase, rs.Generation, rs.LooksDone, rs.GateReason = p.Phase, p.Generation, p.LooksDone, p.Reason
			rs.GateVerdict = pb.GateVerdict(pb.GateVerdict_value["GATE_VERDICT_"+p.Verdict])
			if p.Started != nil {
				rs.Started = ts(*p.Started)
			}
			if p.BakeStarted != nil {
				rs.BakeStarted = ts(*p.BakeStarted)
			}
		}
		rel.Rings = append(rel.Rings, rs)
	}
	if h, ok := byRing[ring.Holdout]; ok {
		rel.Holdout = countsProto(h.C)
	}
	if r.R.State == rollout.RollingBack || r.R.State == rollout.RolledBack {
		_, rb := store.Summarize(r, counts)
		st := &pb.RollbackStatus{FallbackVersion: r.R.Baseline, Generation: r.R.RollbackGeneration, Exposed: rb.Exposed,
			Confirmed: rb.Confirmed, Pending: rb.Pending, Unreachable: rb.Unreachable,
			Converged: rb.Pending == 0 && rb.Unreachable == 0, Started: ts(r.R.RollbackStarted)}
		if st.Unreachable > 0 {
			st.UnreachableSample, _ = s.store.UnconfirmedSample(ctx, r, 10)
		}
		if st.Converged && r.Finished != nil {
			st.ConvergedAt = ts(*r.Finished)
		}
		rel.Rollback = st
	}
	return rel, nil
}
