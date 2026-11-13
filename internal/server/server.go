// Package server implements the cfgcanary control plane API: the agent-facing
// ConfigService (watch streams, reconciliation, acknowledgements) and the
// operator-facing ControlService (also served as REST by a gateway).
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/auth"
	"github.com/raunaksahahere/cfgcanary/internal/backoff"
	"github.com/raunaksahahere/cfgcanary/internal/blob"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/validate"
)

// Config holds server limits and behaviour.
type Config struct {
	TrustDomain string
	// HardMaxBytes bounds any payload regardless of policy.
	HardMaxBytes int64
	// MaxStreams caps concurrent watch streams per replica.
	MaxStreams int
	// AdmissionRate and AdmissionBurst bound new watch streams per second.
	AdmissionRate  float64
	AdmissionBurst int
	// SendTimeout closes a stream whose client cannot take a message in time.
	SendTimeout time.Duration
	// AckQueue bounds buffered acknowledgement rows.
	AckQueue      int
	AckFlush      time.Duration
	PresignTTL    time.Duration
	SweepInterval time.Duration
	TouchInterval time.Duration
	// MembershipWindow bounds which known instances count as fleet members
	// when a release is planned.
	MembershipWindow time.Duration
	// ConnectedWindow is how recently an instance must have been seen to be
	// reported as connected.
	ConnectedWindow  time.Duration
	MaxSubscriptions int
}

func (c *Config) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	if c.TrustDomain == "" {
		c.TrustDomain = "cfgcanary.local"
	}
	if c.HardMaxBytes <= 0 {
		c.HardMaxBytes = 4 << 20
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = 20000
	}
	if c.AdmissionRate <= 0 {
		c.AdmissionRate = 500
	}
	if c.AdmissionBurst <= 0 {
		c.AdmissionBurst = 1000
	}
	if c.AckQueue <= 0 {
		c.AckQueue = 50000
	}
	if c.MaxSubscriptions <= 0 {
		c.MaxSubscriptions = 32
	}
	set(&c.SendTimeout, 10*time.Second)
	set(&c.AckFlush, 100*time.Millisecond)
	set(&c.PresignTTL, 15*time.Minute)
	set(&c.SweepInterval, 5*time.Second)
	set(&c.TouchInterval, 5*time.Second)
	set(&c.MembershipWindow, 2*time.Minute)
	set(&c.ConnectedWindow, 30*time.Second)
}

// Deps are the server's collaborators.
type Deps struct {
	Store          *store.Store
	Blobs          *blob.Store
	Validator      *validate.Validator
	ArtifactSigner *sign.Signer
	DeliverySigner *sign.Signer
	Authorizer     *auth.Authorizer
	Logger         *slog.Logger
	Registerer     prometheus.Registerer
	// Leader reports whether this replica runs the controller (for /v1/info).
	Leader func() (bool, int64)
	// Version information for /v1/info.
	Version, Commit, BuildDate string
}

// Server implements both services.
type Server struct {
	pb.UnimplementedConfigServiceServer
	pb.UnimplementedControlServiceServer

	cfg            Config
	store          *store.Store
	blobs          *blob.Store
	validator      *validate.Validator
	artifactSigner *sign.Signer
	deliverySigner *sign.Signer
	authz          *auth.Authorizer
	log            *slog.Logger
	deps           Deps
	metrics        *metrics
	backoff        *backoff.Backoff
	hub            *Hub
	admission      *admission
	acks           chan store.StatusRow

	presignMu sync.Mutex
	presigned map[string]presigned

	wasmMu    sync.Mutex
	wasmCache map[string][]byte

	auditDenied chan store.Audit
	draining    chan struct{}
	drainOnce   sync.Once
}

// Drain ends every watch stream with UNAVAILABLE so agents reconnect (with
// backoff) to another replica; new streams are refused.
func (s *Server) Drain() { s.drainOnce.Do(func() { close(s.draining) }) }

type presigned struct {
	url string
	exp time.Time
}

// New builds a server. Call Run to start background work.
func New(cfg Config, d Deps) (*Server, error) {
	cfg.defaults()
	if d.Store == nil || d.Blobs == nil || d.Validator == nil || d.ArtifactSigner == nil || d.DeliverySigner == nil || d.Authorizer == nil {
		return nil, errors.New("server: missing dependency")
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		cfg: cfg, store: d.Store, blobs: d.Blobs, validator: d.Validator, artifactSigner: d.ArtifactSigner,
		deliverySigner: d.DeliverySigner, authz: d.Authorizer, log: d.Logger, deps: d,
		metrics: newMetrics(d.Registerer), backoff: backoff.New(backoff.Policy{Base: 200 * time.Millisecond, Cap: 10 * time.Second}, nil),
		admission: newAdmission(cfg.AdmissionRate, cfg.AdmissionBurst, cfg.MaxStreams),
		acks:      make(chan store.StatusRow, cfg.AckQueue), presigned: map[string]presigned{}, wasmCache: map[string][]byte{},
		auditDenied: make(chan store.Audit, 1024), draining: make(chan struct{}),
	}
	s.hub = newHub(s)
	s.metrics.streamsFn(s.hub.streamCount)
	return s, nil
}

// Run starts background loops and blocks until ctx ends.
func (s *Server) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, f := range []func(context.Context){s.hub.listen, func(c context.Context) { s.hub.sweep(c, s.cfg.SweepInterval) }, s.ackLoop, s.auditLoop} {
		wg.Add(1)
		go func(f func(context.Context)) { defer wg.Done(); f(ctx) }(f)
	}
	wg.Wait()
}

func (s *Server) presign(ctx context.Context, sha string) (string, time.Time, error) {
	s.presignMu.Lock()
	p, ok := s.presigned[sha]
	s.presignMu.Unlock()
	if ok && time.Until(p.exp) > s.cfg.PresignTTL/3 {
		return p.url, p.exp, nil
	}
	u, exp, err := s.blobs.Presign(ctx, sha, s.cfg.PresignTTL)
	if err != nil {
		return "", time.Time{}, err
	}
	s.presignMu.Lock()
	if len(s.presigned) > 10000 {
		s.presigned = map[string]presigned{}
	}
	s.presigned[sha] = presigned{u, exp}
	s.presignMu.Unlock()
	return u, exp, nil
}

// --- admission control -------------------------------------------------------

type admission struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	open   int
	max    int
}

func newAdmission(rate float64, burst, max int) *admission {
	return &admission{rate: rate, burst: float64(burst), tokens: float64(burst), last: time.Now(), max: max}
}

// admit takes a token and a stream slot, or refuses.
func (a *admission) admit() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.tokens = min(a.burst, a.tokens+now.Sub(a.last).Seconds()*a.rate)
	a.last = now
	if a.tokens < 1 || a.open >= a.max {
		return false
	}
	a.tokens--
	a.open++
	return true
}

func (a *admission) release() {
	a.mu.Lock()
	a.open--
	a.mu.Unlock()
}

// --- acknowledgements -------------------------------------------------------

func (s *Server) ackLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.AckFlush)
	defer t.Stop()
	batch := make([]store.StatusRow, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err := s.store.UpsertStatuses(fctx, batch)
		cancel()
		if err != nil {
			// Acks are re-sent by agents on reconnect and by reconciliation;
			// losing a batch delays convergence, it never fakes it.
			s.metrics.acks.WithLabelValues("store_error").Add(float64(len(batch)))
			s.log.Warn("storing acknowledgements failed", "rows", len(batch), "err", err)
		} else {
			s.metrics.acks.WithLabelValues("stored").Add(float64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			// Drain what is buffered.
			for {
				select {
				case r := <-s.acks:
					batch = append(batch, r)
				default:
					flush()
					return
				}
			}
		case r := <-s.acks:
			batch = append(batch, r)
			if len(batch) >= 1000 {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

// --- denied-attempt auditing --------------------------------------------------

func (s *Server) auditLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-s.auditDenied:
			actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			if err := s.store.RecordAudit(actx, a); err != nil {
				s.log.Warn("recording denied attempt failed", "err", err)
			}
			cancel()
		}
	}
}

func (s *Server) denied(id auth.Identity, action, config, env, why string) {
	s.metrics.denied.WithLabelValues(action).Inc()
	a := store.Audit{Actor: id.URI, Action: action, Outcome: "denied", Config: config, Environment: env, Reason: why}
	if a.Actor == "" {
		a.Actor = "unauthenticated"
	}
	select {
	case s.auditDenied <- a:
	default:
		s.metrics.denied.WithLabelValues("audit_dropped").Inc()
	}
}

// authorize checks a permission for the caller. It is called at the top of
// every ControlService method, so gRPC and REST are enforced identically.
func (s *Server) authorize(ctx context.Context, p auth.Permission, action, config, env string) (auth.Identity, error) {
	id, ok := auth.FromContext(ctx)
	if !ok {
		return id, status.Error(codes.Unauthenticated, "no authenticated identity")
	}
	if !s.authz.Allowed(id, p) {
		s.denied(id, action, config, env, "missing permission "+string(p))
		return id, status.Errorf(codes.PermissionDenied, "%s requires the %q permission; %s has roles %v", action, p, id.URI, s.authz.RolesOf(id))
	}
	return id, nil
}

// toStatus maps store and validation errors to gRPC status codes.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrFenced):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	}
	return status.Error(codes.Internal, fmt.Sprintf("internal error: %v", err))
}
