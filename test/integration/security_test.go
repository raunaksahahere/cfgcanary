package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/client"
	"github.com/raunaksahahere/cfgcanary/internal/devpki"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func writeCert(t *testing.T, ca *devpki.CA, s devpki.Spec) (string, string) {
	t.Helper()
	dir := t.TempDir()
	c, k, err := ca.Issue(s)
	testutil.Must(t, err, "issue")
	cp, kp := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	os.WriteFile(cp, c, 0o600)
	os.WriteFile(kp, k, 0o600)
	return cp, kp
}

func dialAs(t *testing.T, e *testenv.Env, cert, key string) (pb.ConfigServiceClient, pb.ControlServiceClient) {
	t.Helper()
	cc, err := client.Dial(client.Options{Server: e.GRPCAddr(), ServerName: "localhost", Cert: cert, Key: key, CA: filepath.Join(e.PKI, "ca.pem")})
	testutil.Must(t, err, "dial")
	t.Cleanup(func() { cc.Close() })
	return pb.NewConfigServiceClient(cc), pb.NewControlServiceClient(cc)
}

func code(err error) codes.Code { return status.Code(err) }

func TestTransportAndIdentity(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	register(t, admin, testenv.RatelimitsSpec("ratelimits", manualPolicy(5000, 10000), nil))
	td := "spiffe://cfgcanary.local"
	short, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Unknown CA: the TLS handshake fails before any RPC.
	rogue, _ := devpki.NewCA("rogue", time.Hour)
	c, k := writeCert(t, rogue, devpki.Spec{URI: td + "/user/admin", Client: true})
	_, ctl := dialAs(t, e, c, k)
	if _, err := ctl.GetServerInfo(short, &pb.GetServerInfoRequest{}); code(err) != codes.Unavailable {
		t.Fatalf("unknown CA: %v", err)
	}
	// Expired certificate from the real CA.
	c, k = writeCert(t, e.CA, devpki.Spec{URI: td + "/user/admin", Client: true, NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: time.Now().Add(-24 * time.Hour)})
	_, ctl = dialAs(t, e, c, k)
	if _, err := ctl.GetServerInfo(short, &pb.GetServerInfoRequest{}); code(err) != codes.Unavailable {
		t.Fatalf("expired certificate: %v", err)
	}
	// Server-only certificate used as a client (wrong extended key usage).
	c, k = writeCert(t, e.CA, devpki.Spec{URI: td + "/user/admin", Server: true})
	_, ctl = dialAs(t, e, c, k)
	if _, err := ctl.GetServerInfo(short, &pb.GetServerInfoRequest{}); code(err) != codes.Unavailable {
		t.Fatalf("server-auth-only certificate accepted as client: %v", err)
	}
	// No SPIFFE identity: authenticated by TLS, refused by the server.
	c, k = writeCert(t, e.CA, devpki.Spec{CommonName: "nobody", Client: true})
	_, ctl = dialAs(t, e, c, k)
	if _, err := ctl.GetServerInfo(short, &pb.GetServerInfoRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("certificate without identity: %v", err)
	}

	// Agent identity mismatch: agent-1 cannot watch as agent-2.
	c, k = writeCert(t, e.CA, devpki.Spec{URI: td + "/env/prod/agent/agent-1", Client: true})
	cfg, ctl := dialAs(t, e, c, k)
	if _, err := cfg.GetDesired(short, &pb.GetDesiredRequest{InstanceId: "agent-2"}); code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed instance id: %v", err)
	}
	st, err := cfg.Watch(short, &pb.WatchRequest{InstanceId: "agent-2", Subscriptions: []*pb.Subscription{{Config: "ratelimits"}}})
	if err == nil {
		_, err = st.Recv()
	}
	if code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed watch: %v", err)
	}
	// An agent cannot use the operator API at all.
	if _, err := ctl.KillSwitch(short, &pb.KillSwitchRequest{Config: "ratelimits", Environment: "prod", Reason: "x"}); code(err) != codes.PermissionDenied {
		t.Fatalf("agent kill switch: %v", err)
	}
	if _, err := ctl.ListConfigs(short, &pb.ListConfigsRequest{}); code(err) != codes.PermissionDenied {
		t.Fatalf("agent read: %v", err)
	}
	// A group certificate may only claim instances inside its group.
	c, k = writeCert(t, e.CA, devpki.Spec{URI: td + "/env/prod/group/ring1", Client: true})
	cfg, _ = dialAs(t, e, c, k)
	if _, err := cfg.GetDesired(short, &pb.GetDesiredRequest{InstanceId: "ring1/pod-a"}); err != nil {
		t.Fatalf("group member: %v", err)
	}
	if _, err := cfg.GetDesired(short, &pb.GetDesiredRequest{InstanceId: "ring0/pod-a"}); code(err) != codes.PermissionDenied {
		t.Fatalf("group escape: %v", err)
	}
	if _, err := cfg.ReportStatus(short, &pb.ReportStatusRequest{InstanceId: "agent-9"}); code(err) != codes.PermissionDenied {
		t.Fatalf("group reporting as an agent: %v", err)
	}

	// REST gateway enforces the same permissions; identity from mTLS.
	pool := x509.NewCertPool()
	caPEM, _ := os.ReadFile(filepath.Join(e.PKI, "ca.pem"))
	pool.AppendCertsFromPEM(caPEM)
	rest := func(user string) *http.Client {
		cert, err := tls.LoadX509KeyPair(filepath.Join(e.PKI, "users", user+".pem"), filepath.Join(e.PKI, "users", user+"-key.pem"))
		testutil.Must(t, err, "rest cert")
		return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "localhost"}}}
	}
	base := "https://" + e.Run.RESTAddr
	resp, err := rest("viewer").Get(base + "/v1/configs/ratelimits")
	testutil.Must(t, err, "rest get")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("viewer GET: %d", resp.StatusCode)
	}
	resp, err = rest("viewer").Post(base+"/v1/targets/ratelimits/prod:kill", "application/json", strings.NewReader(`{"reason":"x"}`))
	testutil.Must(t, err, "rest post")
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer kill switch over REST: %d", resp.StatusCode)
	}
	resp, err = rest("viewer").Post(base+"/v1/configs/ratelimits/versions", "application/json", bytes.NewReader([]byte(`{"payload":"e30="}`)))
	testutil.Must(t, err, "rest submit")
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer submit over REST: %d", resp.StatusCode)
	}
	// No client certificate: the handshake fails.
	plain := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	if resp, err := plain.Get(base + "/v1/configs"); err == nil {
		resp.Body.Close()
		t.Fatal("REST without a client certificate succeeded")
	}
	// Denied attempts are audited without payloads or secrets.
	testutil.Eventually(t, 10*time.Second, "denied attempts audited", func() bool {
		ev, err := admin.ListAudit(context.Background(), &pb.ListAuditRequest{Action: "target.kill_switch", PageSize: 10})
		if err != nil {
			return false
		}
		denied := 0
		for _, x := range ev.Events {
			if x.Outcome == "denied" {
				denied++
			}
		}
		return denied >= 2
	})
}

// Bytes in the object store are not trusted: a tampered artifact is refused
// by every agent, which keeps its last-known-good and reports FAILED.
func TestTamperedArtifactRefused(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	register(t, admin, testenv.RatelimitsSpec("ratelimits", manualPolicy(10000), nil))
	fleet := e.Fleet(e.FleetOptions(5, "ratelimits"))
	waitInstances(t, admin, 5)
	v1 := mustVersion(t, admin, "ratelimits", readPayload(t, "v1-baseline.json"))
	cr, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1})
	testutil.Must(t, err, "seed")
	promoteThrough(t, admin, cr.Release.Id, 1)
	testutil.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 5 })

	sub := submit(t, admin, "ratelimits", readPayload(t, "v2-good.json"))
	v2 := sub.Version
	// Overwrite the stored object with different bytes under the same key.
	mc, err := minio.New(e.S3.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(e.S3.AccessKey, e.S3.SecretKey, ""), Region: "us-east-1"})
	testutil.Must(t, err, "minio")
	evil := readPayload(t, "v3-harmful-timeout.json")
	_, err = mc.PutObject(context.Background(), e.S3.Bucket, "sha256/"+v2.Sha256, bytes.NewReader(evil), int64(len(evil)), minio.PutObjectOptions{})
	testutil.Must(t, err, "tamper")

	cr, err = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v2.Version})
	testutil.Must(t, err, "release")
	r := waitRelease(t, admin, cr.Release.Id, 30*time.Second, "agents to report failures", func(r *pb.Release) bool { return r.Rings[0].Counts.Failed == 5 })
	if r.Rings[0].Counts.Applied != 0 || r.Rings[0].Counts.Accepted != 0 {
		t.Fatalf("tampered artifact counted as applied: %+v", r.Rings[0].Counts)
	}
	if n := len(fleet.Served(v1)); n != 5 {
		t.Fatalf("%d/5 instances kept the last-known-good", n)
	}
	ins, _ := admin.ListInstances(ctx(t), &pb.ListInstancesRequest{Environment: "prod"})
	for _, in := range ins.Instances {
		if s := in.Statuses[0]; s.State != pb.ApplyState_APPLY_STATE_FAILED || s.Reason != "digest" && s.Reason != "download" {
			t.Fatalf("%s status %v/%s", in.InstanceId, s.State, s.Reason)
		}
	}
	// The release cannot converge and halts at its rollout timeout; an
	// operator rolls it back.
	_, err = admin.RollbackRelease(ctx(t), &pb.ReleaseCommandRequest{Id: cr.Release.Id, Reason: "tampered artifact"})
	testutil.Must(t, err, "rollback")
}

// RefreshArtifact never issues a URL for content the caller's ring is not
// entitled to.
func TestRefreshArtifactEntitlement(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	register(t, admin, testenv.RatelimitsSpec("ratelimits", manualPolicy(5000, 10000), nil))
	fleet := e.Fleet(e.FleetOptions(10, "ratelimits"))
	waitInstances(t, admin, 10)
	v1 := mustVersion(t, admin, "ratelimits", readPayload(t, "v1-baseline.json"))
	cr, _ := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1})
	promoteThrough(t, admin, cr.Release.Id, 2)
	testutil.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 10 })
	v2 := mustVersion(t, admin, "ratelimits", readPayload(t, "v2-good.json"))
	cr, _ = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v2})
	waitRelease(t, admin, cr.Release.Id, 30*time.Second, "ring 0", baking(0))
	exposed := map[string]bool{}
	for _, id := range fleet.Served(v2) {
		exposed[id] = true
	}
	for _, m := range fleet.Members {
		c, k := filepath.Join(e.Dir, "fleet-ratelimits", m.ID, "tls", "agent.pem"), filepath.Join(e.Dir, "fleet-ratelimits", m.ID, "tls", "agent-key.pem")
		cfg, _ := dialAs(t, e, c, k)
		_, err := cfg.RefreshArtifact(ctx(t), &pb.RefreshArtifactRequest{InstanceId: m.ID, Config: "ratelimits", Version: v2})
		if exposed[m.ID] && err != nil {
			t.Fatalf("exposed %s refused: %v", m.ID, err)
		}
		if !exposed[m.ID] && code(err) != codes.PermissionDenied {
			t.Fatalf("unexposed %s got a URL for the candidate: %v", m.ID, err)
		}
		// Labels never change ring assignment.
		st, err := cfg.GetDesired(ctx(t), &pb.GetDesiredRequest{InstanceId: m.ID, Subscriptions: []*pb.Subscription{{Config: "ratelimits"}}})
		testutil.Must(t, err, "desired")
		if got := st.Updates[0].Version == v2; got != exposed[m.ID] {
			t.Fatalf("%s desired v%d, exposed=%v", m.ID, st.Updates[0].Version, exposed[m.ID])
		}
	}
	if len(exposed) == 0 || len(exposed) == 10 {
		t.Fatalf("%d exposed; ring 0 should be a strict subset", len(exposed))
	}
}
