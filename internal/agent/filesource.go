package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// Envelope is the signed description of a payload in a projected directory
// (a Kubernetes ring ConfigMap): files "envelope.json" and "payload".
type Envelope struct {
	Config      string      `json:"config"`
	Environment string      `json:"environment"`
	Version     int64       `json:"version"`
	Generation  int64       `json:"generation"`
	SHA256      string      `json:"sha256"`
	Size        int64       `json:"size"`
	Ring        int32       `json:"ring"`
	ReleaseID   string      `json:"release_id,omitempty"`
	Artifact    sign.Signed `json:"artifact"`
	Delivery    sign.Signed `json:"delivery"`
}

// RunFileSource applies updates from a projected directory instead of a watch
// stream. Kubernetes updates ConfigMap volumes by swapping a symlink, so the
// two files are read from one consistent snapshot; a torn read fails digest
// verification and is retried on the next poll. Statuses are reported to the
// control plane when Dial is configured.
func (a *Agent) RunFileSource(ctx context.Context, dir string, poll time.Duration) error {
	if len(a.cfg.Configs) != 1 {
		return errors.New("file source mode serves exactly one config")
	}
	if poll <= 0 {
		poll = 2 * time.Second
	}
	go a.reportLoop(ctx)
	go a.sdkLoop(ctx)
	if a.cfg.Dial != nil {
		go a.reportConnLoop(ctx)
	}
	name := a.cfg.Configs[0]
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		a.applyFromDir(ctx, name, dir)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (a *Agent) applyFromDir(ctx context.Context, name, dir string) {
	raw, err := os.ReadFile(filepath.Join(dir, "envelope.json"))
	if err != nil || len(raw) > 1<<20 {
		return
	}
	var env Envelope
	if json.Unmarshal(raw, &env) != nil || env.Config != name || env.Version == 0 {
		return
	}
	a.Handle(ctx, nil, &pb.ConfigUpdate{
		Kind: pb.ConfigUpdate_KIND_DESIRED, Config: env.Config, Environment: env.Environment, Generation: env.Generation,
		Version: env.Version, Sha256: env.SHA256, Size: env.Size, Ring: env.Ring, ReleaseId: env.ReleaseID,
		Artifact: &pb.SignedStatement{Payload: env.Artifact.Payload, Signature: env.Artifact.Signature, KeyId: env.Artifact.KeyID},
		Delivery: &pb.SignedStatement{Payload: env.Delivery.Payload, Signature: env.Delivery.Signature, KeyId: env.Delivery.KeyID},
		// The URL scheme "file" is only produced here, never accepted from
		// the network: download() refuses it unless explicitly allowed.
		Url: "file://" + filepath.Join(dir, "payload"),
	})
}

// reportConnLoop keeps a connection for status reports in file-source mode.
func (a *Agent) reportConnLoop(ctx context.Context) {
	attempt := 0
	for ctx.Err() == nil {
		dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := a.cfg.Dial(dctx)
		cancel()
		if err == nil {
			client := pb.NewConfigServiceClient(conn)
			// Probe once so a broken connection is detected.
			pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
			_, err = client.GetDesired(pctx, &pb.GetDesiredRequest{InstanceId: a.cfg.InstanceID, Subscriptions: a.subscriptions()})
			pcancel()
			if err == nil {
				attempt = 0
				a.client.Store(&clientRef{c: client})
				a.contact()
				a.requeueAll()
				t := time.NewTicker(a.cfg.ReconcileInterval)
				for err == nil {
					select {
					case <-ctx.Done():
						err = ctx.Err()
					case <-t.C:
						pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
						_, err = client.GetDesired(pctx, &pb.GetDesiredRequest{InstanceId: a.cfg.InstanceID, Subscriptions: a.subscriptions()})
						pcancel()
						if err == nil {
							a.contact()
						}
					}
				}
				t.Stop()
				a.client.Store(nil)
			}
			conn.Close()
		}
		d := a.bo.Delay(attempt)
		attempt++
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
}

func readLocal(path string, size int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b := make([]byte, size+1)
	n, _ := f.Read(b)
	for n < len(b) {
		m, err := f.Read(b[n:])
		n += m
		if err != nil {
			break
		}
	}
	return b[:n], nil
}
