package server

import (
	"context"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/auth"
	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/store"
)

var configName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// workload authenticates an agent call and binds the claimed instance ID to
// the certificate identity (docs/invariants.md I2).
func (s *Server) workload(ctx context.Context, method, instance string) (auth.Identity, error) {
	id, ok := auth.FromContext(ctx)
	if !ok {
		return id, status.Error(codes.Unauthenticated, "no authenticated identity")
	}
	if !id.Workload() {
		s.denied(id, method, "", "", "not a workload identity")
		return id, status.Errorf(codes.PermissionDenied, "%s is not an agent identity", id.URI)
	}
	if !id.MayClaim(instance) {
		s.denied(id, method, "", id.Environment, "instance id does not match certificate")
		return id, status.Errorf(codes.PermissionDenied, "instance id %q does not match certificate identity %s", instance, id.URI)
	}
	return id, nil
}

func (s *Server) checkSubs(subs []*pb.Subscription) ([]string, error) {
	if len(subs) > s.cfg.MaxSubscriptions {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d subscriptions", s.cfg.MaxSubscriptions)
	}
	seen := map[string]bool{}
	var out []string
	for _, sub := range subs {
		if !configName.MatchString(sub.Config) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid config name %q", sub.Config)
		}
		if !seen[sub.Config] {
			seen[sub.Config] = true
			out = append(out, sub.Config)
		}
	}
	return out, nil
}

// watcher is one open watch stream.
type watcher struct {
	instance string
	env      string
	configs  []string

	mu    sync.Mutex
	dirty map[string]bool
	wake  chan struct{}
}

func (w *watcher) markDirty(config string) {
	w.mu.Lock()
	w.dirty[config] = true
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *watcher) take() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.dirty))
	for c := range w.dirty {
		out = append(out, c)
	}
	clear(w.dirty)
	return out
}

// Watch streams desired state. Each stream holds one coalescing dirty set: a
// newer change replaces an unsent older one, so memory per stream is bounded
// by its subscriptions and a slow client cannot make the server queue.
func (s *Server) Watch(req *pb.WatchRequest, stream grpc.ServerStreamingServer[pb.ConfigUpdate]) error {
	ctx := stream.Context()
	id, err := s.workload(ctx, "Watch", req.InstanceId)
	if err != nil {
		return err
	}
	configs, err := s.checkSubs(req.Subscriptions)
	if err != nil {
		return err
	}
	select {
	case <-s.draining:
		return status.Error(codes.Unavailable, "server is shutting down")
	default:
	}
	if !s.admission.admit() {
		s.metrics.admissions.WithLabelValues("rejected").Inc()
		return status.Error(codes.ResourceExhausted, "watch admission limit reached; retry with backoff")
	}
	defer s.admission.release()
	s.metrics.admissions.WithLabelValues("admitted").Inc()

	labels := req.Labels
	if len(labels) > 32 {
		labels = nil
	}
	uctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = s.store.UpsertInstance(uctx, id.Environment, req.InstanceId, id.Group(), labels, req.AgentVersion)
	cancel()
	if err != nil {
		return status.Error(codes.Unavailable, "control plane storage unavailable")
	}
	last := map[string]int64{}
	for _, sub := range req.Subscriptions {
		last[sub.Config] = sub.LastGeneration
	}
	w := &watcher{instance: req.InstanceId, env: id.Environment, configs: configs, dirty: map[string]bool{}, wake: make(chan struct{}, 1)}
	s.hub.register(w)
	defer s.hub.unregister(w)
	// Late joiners are added to the active release's cohort.
	for _, c := range configs {
		if snap, err := s.hub.snapshot(ctx, targetKey{c, id.Environment}); err == nil && snap.row.ReleaseID != "" {
			_ = s.store.EnsureMember(ctx, snap.row.ReleaseID, req.InstanceId, snap.ringOf(req.InstanceId))
		}
		w.markDirty(c)
	}
	if err := stream.SendHeader(metadata.Pairs("cfgcanary-admitted", "1")); err != nil {
		return err
	}

	// The sender runs separately so that a send blocked by HTTP/2 flow
	// control can be abandoned: returning from the handler closes the stream
	// and unblocks it.
	var sendingSince atomic.Int64
	sendErr := make(chan error, 1)
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		for {
			select {
			case <-sctx.Done():
				sendErr <- nil
				return
			case <-w.wake:
			}
			for _, c := range w.take() {
				u, err := s.hub.update(sctx, targetKey{c, w.env}, w.instance)
				if err != nil {
					if sctx.Err() != nil {
						sendErr <- nil
						return
					}
					// Retried by the next wake or sweep.
					s.log.Debug("building update failed", "config", c, "err", err)
					continue
				}
				if u.Generation <= last[c] {
					continue
				}
				sendingSince.Store(time.Now().UnixNano())
				err = stream.Send(u)
				sendingSince.Store(0)
				if err != nil {
					sendErr <- err
					return
				}
				last[c] = u.Generation
				s.metrics.sent.WithLabelValues(u.Kind.String()).Inc()
			}
		}
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.draining:
			return status.Error(codes.Unavailable, "server is shutting down")
		case err := <-sendErr:
			return err
		case <-tick.C:
			if t := sendingSince.Load(); t != 0 && time.Since(time.Unix(0, t)) > s.cfg.SendTimeout {
				s.metrics.slow.Inc()
				return status.Error(codes.Unavailable, "slow consumer: update not accepted within the send timeout")
			}
		}
	}
}

// GetDesired returns current desired state for reconciliation.
func (s *Server) GetDesired(ctx context.Context, req *pb.GetDesiredRequest) (*pb.GetDesiredResponse, error) {
	id, err := s.workload(ctx, "GetDesired", req.InstanceId)
	if err != nil {
		return nil, err
	}
	configs, err := s.checkSubs(req.Subscriptions)
	if err != nil {
		return nil, err
	}
	// File-source agents (Kubernetes ring ConfigMaps) never open a watch;
	// reconciliation is how they register membership and reachability.
	uctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = s.store.UpsertInstance(uctx, id.Environment, req.InstanceId, id.Group(), nil, "")
	cancel()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "control plane storage unavailable")
	}
	last := map[string]int64{}
	for _, sub := range req.Subscriptions {
		last[sub.Config] = sub.LastGeneration
	}
	resp := &pb.GetDesiredResponse{}
	for _, c := range configs {
		u, err := s.hub.update(ctx, targetKey{c, id.Environment}, req.InstanceId)
		if err != nil {
			continue
		}
		if u.Generation > last[c] {
			resp.Updates = append(resp.Updates, u)
		}
	}
	s.metrics.rpcs.WithLabelValues("GetDesired", "ok").Inc()
	return resp, nil
}

// ReportStatus queues acknowledgements. Reports are bounded; when the queue
// is full the agent is told to back off and retry.
func (s *Server) ReportStatus(ctx context.Context, req *pb.ReportStatusRequest) (*pb.ReportStatusResponse, error) {
	id, err := s.workload(ctx, "ReportStatus", req.InstanceId)
	if err != nil {
		return nil, err
	}
	if len(req.Statuses) > s.cfg.MaxSubscriptions {
		return nil, status.Error(codes.InvalidArgument, "too many statuses")
	}
	for _, st := range req.Statuses {
		if !configName.MatchString(st.Config) || st.Generation <= 0 || st.Version < 0 {
			continue
		}
		state := st.State.String()
		if len(state) < 12 || st.State == pb.ApplyState_APPLY_STATE_UNSPECIFIED {
			continue
		}
		// A report for a generation the target has not reached is ignored:
		// an agent cannot pre-acknowledge future state.
		if snap, err := s.hub.snapshot(ctx, targetKey{st.Config, id.Environment}); err != nil || st.Generation > snap.row.T.Generation {
			s.metrics.acks.WithLabelValues("ignored").Inc()
			continue
		}
		row := store.StatusRow{Env: id.Environment, Instance: req.InstanceId, Config: st.Config, Generation: st.Generation,
			Version: st.Version, State: state[len("APPLY_STATE_"):], Reason: st.Reason,
			ServingVersion: st.ServingVersion, ServingGeneration: st.ServingGeneration}
		select {
		case s.acks <- row:
		default:
			s.metrics.acks.WithLabelValues("queue_full").Inc()
			return nil, status.Error(codes.ResourceExhausted, "acknowledgement queue full; retry with backoff")
		}
	}
	s.metrics.rpcs.WithLabelValues("ReportStatus", "ok").Inc()
	return &pb.ReportStatusResponse{}, nil
}

// RefreshArtifact issues a fresh URL for a version the caller is entitled to
// right now. It never grants access to another ring's content.
func (s *Server) RefreshArtifact(ctx context.Context, req *pb.RefreshArtifactRequest) (*pb.RefreshArtifactResponse, error) {
	id, err := s.workload(ctx, "RefreshArtifact", req.InstanceId)
	if err != nil {
		return nil, err
	}
	snap, err := s.hub.snapshot(ctx, targetKey{req.Config, id.Environment})
	if err != nil {
		return nil, toStatus(err)
	}
	r := snap.ringOf(req.InstanceId)
	want := snap.row.T.DesiredFor(r)
	if want == 0 || want != req.Version || r == ring.Holdout && snap.row.T.Candidate == req.Version {
		s.denied(id, "RefreshArtifact", req.Config, id.Environment, "version not entitled")
		return nil, status.Errorf(codes.PermissionDenied, "version %d is not the desired version for this instance", req.Version)
	}
	cv := snap.versions[want]
	u, exp, err := s.blobs.Presign(ctx, cv.Sha256, s.cfg.PresignTTL)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "could not sign a download URL")
	}
	return &pb.RefreshArtifactResponse{Url: u, UrlExpires: timestamppb.New(exp)}, nil
}
