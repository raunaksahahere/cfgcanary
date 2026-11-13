// Package kube implements Kubernetes mode: a ConfigRelease custom resource
// whose controller is a client of the cfgcanary API, plus per-ring ConfigMap
// projection.
//
// PostgreSQL stays the only source of release truth (docs/decisions/0007):
// .spec is intent, .status mirrors the release the control plane runs, and
// ring ConfigMaps are rewritten from the control plane's ring projection on
// every pass, so deleting or editing one by hand is undone.
package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/agent"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// GVR of the ConfigRelease resource.
var GVR = schema.GroupVersionResource{Group: "cfgcanary.io", Version: "v1alpha1", Resource: "configreleases"}

// Spec is ConfigRelease.spec.
type Spec struct {
	Config      string       `json:"config"`
	Environment string       `json:"environment,omitempty"`
	Payload     string       `json:"payload,omitempty"`
	PayloadFrom *PayloadFrom `json:"payloadFrom,omitempty"`
	Description string       `json:"description,omitempty"`
	Paused      bool         `json:"paused,omitempty"`
	// ProjectRings writes <config>-<environment>-ring-<n> ConfigMaps in the
	// resource's namespace (default true).
	ProjectRings *bool `json:"projectRings,omitempty"`
}

// PayloadFrom references a ConfigMap key holding the payload.
type PayloadFrom struct {
	ConfigMapKeyRef *struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	} `json:"configMapKeyRef,omitempty"`
}

// Condition follows the Kubernetes condition convention.
type Condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration"`
	LastTransitionTime string `json:"lastTransitionTime"`
}

// RingSummary is a compact per-ring view in .status.
type RingSummary struct {
	Index    int32  `json:"index"`
	Name     string `json:"name"`
	Phase    string `json:"phase"`
	Eligible int32  `json:"eligible"`
	Accepted int32  `json:"accepted"`
	Rejected int32  `json:"rejected"`
	Gate     string `json:"gate,omitempty"`
}

// Status is ConfigRelease.status.
type Status struct {
	ObservedGeneration int64         `json:"observedGeneration,omitempty"`
	Version            int64         `json:"version,omitempty"`
	ReleaseID          string        `json:"releaseId,omitempty"`
	Phase              string        `json:"phase,omitempty"`
	ActiveRing         int32         `json:"activeRing"`
	Message            string        `json:"message,omitempty"`
	Issues             []string      `json:"issues,omitempty"`
	Rings              []RingSummary `json:"rings,omitempty"`
	Rollback           string        `json:"rollback,omitempty"`
	Conditions         []Condition   `json:"conditions,omitempty"`
}

// Options configure the controller.
type Options struct {
	Namespace string // "" watches every namespace
	Interval  time.Duration
	Logger    *slog.Logger
}

// Controller reconciles ConfigReleases.
type Controller struct {
	api  pb.ControlServiceClient
	dyn  dynamic.Interface
	core kubernetes.Interface
	opt  Options
	log  *slog.Logger

	mu       sync.Mutex
	payloads map[string][]byte // config/version -> payload
}

// New creates a controller.
func New(api pb.ControlServiceClient, dyn dynamic.Interface, core kubernetes.Interface, o Options) *Controller {
	if o.Interval <= 0 {
		o.Interval = 2 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return &Controller{api: api, dyn: dyn, core: core, opt: o, log: o.Logger.With("component", "k8s-controller"), payloads: map[string][]byte{}}
}

// Run reconciles until ctx ends.
func (c *Controller) Run(ctx context.Context) {
	t := time.NewTicker(c.opt.Interval)
	defer t.Stop()
	for {
		if err := c.Pass(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("reconcile pass failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Pass reconciles every ConfigRelease once and refreshes ring projections.
func (c *Controller) Pass(ctx context.Context) error {
	list, err := c.dyn.Resource(GVR).Namespace(c.opt.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing ConfigReleases: %w", err)
	}
	type target struct{ ns, config, env string }
	targets := map[target]bool{}
	for i := range list.Items {
		obj := &list.Items[i]
		spec, err := decodeSpec(obj)
		if err != nil {
			c.setStatus(ctx, obj, func(s *Status) {
				s.Phase, s.Message = "Invalid", err.Error()
				s.ObservedGeneration = obj.GetGeneration()
				setCondition(s, obj.GetGeneration(), "Validated", "False", "InvalidSpec", err.Error())
			})
			continue
		}
		if spec.ProjectRings == nil || *spec.ProjectRings {
			targets[target{obj.GetNamespace(), spec.Config, spec.Environment}] = true
		}
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := c.reconcile(rctx, obj, spec); err != nil {
			c.log.Warn("reconcile failed", "configrelease", obj.GetNamespace()+"/"+obj.GetName(), "err", err)
		}
		cancel()
	}
	keys := make([]target, 0, len(targets))
	for t := range targets {
		keys = append(keys, t)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].ns+keys[i].config < keys[j].ns+keys[j].config })
	for _, t := range keys {
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := c.project(pctx, t.ns, t.config, t.env); err != nil {
			c.log.Warn("ring projection failed", "config", t.config, "environment", t.env, "err", err)
		}
		cancel()
	}
	return nil
}

func decodeSpec(obj *unstructured.Unstructured) (Spec, error) {
	var s Spec
	raw, _, _ := unstructured.NestedMap(obj.Object, "spec")
	b, _ := json.Marshal(raw)
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("spec: %w", err)
	}
	if s.Config == "" {
		return s, fmt.Errorf("spec.config is required")
	}
	if s.Environment == "" {
		s.Environment = "prod"
	}
	if (s.Payload == "") == (s.PayloadFrom == nil) {
		return s, fmt.Errorf("exactly one of spec.payload and spec.payloadFrom is required")
	}
	return s, nil
}

func decodeStatus(obj *unstructured.Unstructured) Status {
	var s Status
	raw, _, _ := unstructured.NestedMap(obj.Object, "status")
	b, _ := json.Marshal(raw)
	_ = json.Unmarshal(b, &s)
	return s
}

func (c *Controller) payloadOf(ctx context.Context, obj *unstructured.Unstructured, s Spec) ([]byte, error) {
	if s.Payload != "" {
		return []byte(s.Payload), nil
	}
	ref := s.PayloadFrom.ConfigMapKeyRef
	if ref == nil {
		return nil, fmt.Errorf("spec.payloadFrom.configMapKeyRef is required")
	}
	cm, err := c.core.CoreV1().ConfigMaps(obj.GetNamespace()).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if v, ok := cm.Data[ref.Key]; ok {
		return []byte(v), nil
	}
	if v, ok := cm.BinaryData[ref.Key]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("ConfigMap %s has no key %q", ref.Name, ref.Key)
}

func (c *Controller) reconcile(ctx context.Context, obj *unstructured.Unstructured, spec Spec) error {
	st := decodeStatus(obj)
	gen := obj.GetGeneration()
	uid := string(obj.GetUID())
	if st.ObservedGeneration != gen {
		// New intent: submit the payload and create a release. Request IDs
		// derived from the object's UID and generation make retries after a
		// crash or a lost status update return the same version and release.
		payload, err := c.payloadOf(ctx, obj, spec)
		if err != nil {
			return c.setStatus(ctx, obj, func(s *Status) {
				s.Phase, s.Message = "Invalid", err.Error()
				setCondition(s, gen, "Validated", "False", "PayloadUnavailable", err.Error())
			})
		}
		sub, err := c.api.SubmitVersion(ctx, &pb.SubmitVersionRequest{Name: spec.Config, Payload: payload, Description: spec.Description,
			Environments: []string{spec.Environment}, RequestId: fmt.Sprintf("k8s-%s-%d-submit", uid, gen)})
		if err != nil {
			return err
		}
		if !sub.Report.Ok {
			var issues []string
			for _, i := range sub.Report.Issues {
				issues = append(issues, fmt.Sprintf("[%s/%s] %s %s", i.Stage, i.Code, i.Path, i.Message))
			}
			return c.setStatus(ctx, obj, func(s *Status) {
				*s = Status{ObservedGeneration: gen, Phase: "Rejected", ActiveRing: -1, Issues: issues, Conditions: s.Conditions,
					Message: "payload rejected by validation; it reaches no instance"}
				setCondition(s, gen, "Validated", "False", "ValidationFailed", strings.Join(issues, "; "))
				setCondition(s, gen, "Released", "False", "ValidationFailed", "nothing was released")
			})
		}
		version := sub.Version.Version
		rel, err := c.api.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: spec.Config, Environment: spec.Environment, Version: version,
			RequestId: fmt.Sprintf("k8s-%s-%d-release", uid, gen), Note: "ConfigRelease " + obj.GetNamespace() + "/" + obj.GetName()})
		if err != nil {
			msg := status.Convert(err).Message()
			code := status.Code(err)
			return c.setStatus(ctx, obj, func(s *Status) {
				*s = Status{ObservedGeneration: gen, Version: version, Phase: "Blocked", ActiveRing: -1, Message: msg, Conditions: s.Conditions}
				setCondition(s, gen, "Validated", "True", "ValidationPassed", "")
				setCondition(s, gen, "Released", "False", code.String(), msg)
			})
		}
		st = Status{ObservedGeneration: gen, Version: version, ReleaseID: rel.Release.Id, ActiveRing: -1, Conditions: st.Conditions}
		setCondition(&st, gen, "Validated", "True", "ValidationPassed", "")
		if err := c.writeStatus(ctx, obj, st); err != nil {
			return err
		}
		c.log.Info("release created", "configrelease", obj.GetNamespace()+"/"+obj.GetName(), "version", version, "release", rel.Release.Id)
	}
	if st.ReleaseID == "" {
		return nil
	}
	r, err := c.api.GetRelease(ctx, &pb.GetReleaseRequest{Id: st.ReleaseID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return c.setStatus(ctx, obj, func(s *Status) { s.Phase, s.Message = "Lost", "release no longer exists" })
		}
		return err
	}
	active := r.State != pb.ReleaseState_RELEASE_STATE_COMPLETE && r.State != pb.ReleaseState_RELEASE_STATE_ROLLED_BACK
	if active && r.State != pb.ReleaseState_RELEASE_STATE_ROLLING_BACK && spec.Paused != r.Paused {
		req := &pb.ReleaseCommandRequest{Id: r.Id, Reason: "ConfigRelease spec.paused=" + fmt.Sprint(spec.Paused)}
		if spec.Paused {
			r, err = c.api.PauseRelease(ctx, req)
		} else {
			r, err = c.api.ResumeRelease(ctx, req)
		}
		if err != nil {
			return err
		}
	}
	return c.setStatus(ctx, obj, func(s *Status) { mirror(s, r, gen) })
}

func mirror(s *Status, r *pb.Release, gen int64) {
	s.Phase = strings.TrimPrefix(r.State.String(), "RELEASE_STATE_")
	if r.Paused {
		s.Phase += "/Paused"
	}
	s.ActiveRing, s.Message = r.ActiveRing, r.Message
	s.Rings = nil
	for _, rs := range r.Rings {
		g := strings.TrimPrefix(rs.GateVerdict.String(), "GATE_VERDICT_")
		if g == "UNSPECIFIED" {
			g = ""
		}
		s.Rings = append(s.Rings, RingSummary{Index: rs.Index, Name: rs.Name, Phase: rs.Phase, Eligible: rs.Counts.Eligible,
			Accepted: rs.Counts.Accepted, Rejected: rs.Counts.Rejected, Gate: g})
	}
	s.Rollback = ""
	if rb := r.Rollback; rb != nil {
		s.Rollback = fmt.Sprintf("fallback v%d: %d/%d confirmed, %d pending, %d unreachable", rb.FallbackVersion, rb.Confirmed, rb.Exposed, rb.Pending, rb.Unreachable)
	}
	switch r.State {
	case pb.ReleaseState_RELEASE_STATE_COMPLETE:
		setCondition(s, gen, "Released", "True", "Complete", "every ring received the version")
	case pb.ReleaseState_RELEASE_STATE_ROLLED_BACK:
		setCondition(s, gen, "Released", "False", "RolledBack", r.Reason)
	case pb.ReleaseState_RELEASE_STATE_HALTED:
		setCondition(s, gen, "Released", "False", "Halted", r.Reason)
	default:
		setCondition(s, gen, "Released", "Unknown", "Progressing", fmt.Sprintf("ring %d, %s", r.ActiveRing, s.Phase))
	}
}

func setCondition(s *Status, gen int64, typ, st, reason, msg string) {
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for i, c := range s.Conditions {
		if c.Type == typ {
			if c.Status != st {
				c.LastTransitionTime = now
			}
			c.Status, c.Reason, c.Message, c.ObservedGeneration = st, reason, msg, gen
			s.Conditions[i] = c
			return
		}
	}
	s.Conditions = append(s.Conditions, Condition{Type: typ, Status: st, Reason: reason, Message: msg, ObservedGeneration: gen, LastTransitionTime: now})
}

func (c *Controller) setStatus(ctx context.Context, obj *unstructured.Unstructured, f func(*Status)) error {
	st := decodeStatus(obj)
	before := st
	before.Conditions = append([]Condition(nil), st.Conditions...)
	f(&st)
	if reflect.DeepEqual(before, st) {
		return nil
	}
	return c.writeStatus(ctx, obj, st)
}

func (c *Controller) writeStatus(ctx context.Context, obj *unstructured.Unstructured, st Status) error {
	b, _ := json.Marshal(st)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	next := obj.DeepCopy()
	if err := unstructured.SetNestedMap(next.Object, m, "status"); err != nil {
		return err
	}
	updated, err := c.dyn.Resource(GVR).Namespace(obj.GetNamespace()).UpdateStatus(ctx, next, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	// Keep the caller's object current (status and resourceVersion) so a
	// second write in the same pass does not conflict.
	obj.Object = updated.Object
	return nil
}

// project writes one ConfigMap per ring with exactly what an instance pinned
// to that ring would receive: the payload and its signed envelope.
func (c *Controller) project(ctx context.Context, ns, config, env string) error {
	proj, err := c.api.GetRingProjection(ctx, &pb.GetTargetRequest{Config: config, Environment: env})
	if err != nil {
		return err
	}
	for _, rd := range proj.Rings {
		name := fmt.Sprintf("%s-%s-ring-%d", config, env, rd.Ring)
		data := map[string]string{}
		if rd.Version > 0 {
			payload, err := c.payload(ctx, config, rd.Version)
			if err != nil {
				return err
			}
			envl := agent.Envelope{Config: config, Environment: env, Version: rd.Version, Generation: proj.Target.Generation,
				SHA256: rd.Sha256, Size: rd.Size, Ring: rd.Ring, ReleaseID: proj.Target.ReleaseId,
				Artifact: sign.Signed{Payload: rd.Artifact.Payload, Signature: rd.Artifact.Signature, KeyID: rd.Artifact.KeyId},
				Delivery: sign.Signed{Payload: rd.Delivery.Payload, Signature: rd.Delivery.Signature, KeyID: rd.Delivery.KeyId}}
			eb, _ := json.MarshalIndent(envl, "", "  ")
			data["payload"], data["envelope.json"] = string(payload), string(eb)
		}
		labels := map[string]string{"app.kubernetes.io/managed-by": "cfgcanary", "cfgcanary.io/config": config,
			"cfgcanary.io/environment": env, "cfgcanary.io/ring": fmt.Sprint(rd.Ring)}
		cms := c.core.CoreV1().ConfigMaps(ns)
		cur, err := cms.Get(ctx, name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			_, err = cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels}, Data: data}, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		case err != nil:
			return err
		case !reflect.DeepEqual(cur.Data, data) || !reflect.DeepEqual(cur.Labels, labels):
			cur = cur.DeepCopy()
			cur.Data, cur.Labels = data, labels
			if _, err := cms.Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Controller) payload(ctx context.Context, config string, version int64) ([]byte, error) {
	key := fmt.Sprintf("%s/%d", config, version)
	c.mu.Lock()
	p, ok := c.payloads[key]
	c.mu.Unlock()
	if ok {
		return p, nil
	}
	r, err := c.api.GetVersionPayload(ctx, &pb.GetVersionRequest{Name: config, Version: version})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if len(c.payloads) > 64 {
		c.payloads = map[string][]byte{}
	}
	c.payloads[key] = r.Payload
	c.mu.Unlock()
	return r.Payload, nil
}
