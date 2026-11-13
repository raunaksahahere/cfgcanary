// Package cache is the agent's durable local store and the format the SDK
// reads. It is designed so that a crash at any point leaves either the old or
// the new consistent state (docs/invariants.md I5).
//
// Layout, per config:
//
//	<root>/<config>/objects/<sha256>   payload bytes, immutable, 0600
//	<root>/<config>/pointer.json       {current, previous}
//	<root>/<config>/pointer.prev.json  the pointer before the last swap
//	<root>/<config>/sdk-state.json     written by the SDK: accepted entry,
//	                                   trial and quarantine records
//	<root>/<config>/sdk-status.json    written by the SDK: what it serves
//	<root>/agent-heartbeat             counter bumped on each server contact
//
// Every file is replaced by write-temp, fsync, rename, fsync(directory). Objects
// are named by digest, so a referenced object is never rewritten; a pointer is
// only swapped after the object it names is durable.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// Entry is one verified delivery, as recorded in a pointer.
type Entry struct {
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
	// FailureMode and MaxStalenessSeconds are copied from the verified
	// delivery statement for convenience; Verify re-derives them.
	FailureMode         string `json:"failure_mode,omitempty"`
	MaxStalenessSeconds int64  `json:"max_staleness_seconds,omitempty"`
}

// Zero reports whether the entry is empty.
func (e Entry) Zero() bool { return e.Version == 0 }

// Pointer names the entries an agent holds for one config.
type Pointer struct {
	Current  Entry `json:"current"`
	Previous Entry `json:"previous"`
}

// Errors.
var (
	ErrNoPointer = errors.New("cache: no pointer")
	ErrCorrupt   = errors.New("cache: corrupt entry")
)

var configName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
var hexDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Dir is the cache of one config.
type Dir struct {
	root, config, path string
}

// Open returns the cache directory for a config, creating it with mode 0700.
func Open(root, config string) (*Dir, error) {
	if !configName.MatchString(config) || strings.Contains(config, "..") {
		return nil, fmt.Errorf("cache: invalid config name %q", config)
	}
	p := filepath.Join(root, config)
	if err := os.MkdirAll(filepath.Join(p, "objects"), 0o700); err != nil {
		return nil, err
	}
	d := &Dir{root: root, config: config, path: p}
	d.cleanTemps()
	return d, nil
}

// Path returns the directory.
func (d *Dir) Path() string { return d.path }

// Config returns the config name.
func (d *Dir) Config() string { return d.config }

func (d *Dir) objectPath(sha string) string { return filepath.Join(d.path, "objects", sha) }

// cleanTemps removes temp files left by a crash.
func (d *Dir) cleanTemps() {
	for _, dir := range []string{d.path, filepath.Join(d.path, "objects")} {
		ents, _ := os.ReadDir(dir)
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), ".tmp-") {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
}

// fsFaults lets tests inject disk errors at named steps.
var fsFaults func(step, path string) error

func fault(step, path string) error {
	if fsFaults != nil {
		return fsFaults(step, path)
	}
	return nil
}

// WriteFileAtomic replaces path with data: temp file in the same directory,
// fsync, rename, fsync of the directory.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := fault("create", path); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if err := f.Chmod(perm); err != nil {
		return err
	}
	if err := fault("write", path); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := fault("fsync", path); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fault("rename", path); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return syncDir(dir)
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}

// PutObject stores payload bytes under their digest. It is a no-op if a
// verified copy is already present.
func (d *Dir) PutObject(sha string, data []byte) error {
	if !hexDigest.MatchString(sha) || digest(data) != sha {
		return ErrCorrupt
	}
	if b, err := os.ReadFile(d.objectPath(sha)); err == nil && digest(b) == sha {
		return nil
	}
	return WriteFileAtomic(d.objectPath(sha), data, 0o600)
}

// ReadObject reads and verifies an object, refusing more than max bytes.
func (d *Dir) ReadObject(sha string, max int64) ([]byte, error) {
	if !hexDigest.MatchString(sha) {
		return nil, ErrCorrupt
	}
	f, err := os.Open(d.objectPath(sha))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max || digest(b) != sha {
		return nil, ErrCorrupt
	}
	return b, nil
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// ReadPointer returns the pointer, falling back to the retained predecessor
// when the current file is missing or unreadable. fromPrev reports whether
// the fallback was used.
func (d *Dir) ReadPointer() (p Pointer, fromPrev bool, err error) {
	if p, err = readPointer(filepath.Join(d.path, "pointer.json")); err == nil {
		return p, false, nil
	}
	if p2, err2 := readPointer(filepath.Join(d.path, "pointer.prev.json")); err2 == nil {
		return p2, true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return Pointer{}, false, ErrNoPointer
	}
	return Pointer{}, false, err
}

func readPointer(path string) (Pointer, error) {
	var p Pointer
	b, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if len(b) > 1<<20 {
		return p, ErrCorrupt
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return p, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return p, nil
}

// Swap makes e current, keeping the old current as previous. The object must
// already be stored. The old pointer file is retained as pointer.prev.json
// before the new one is written, so a corrupt pointer.json never destroys the
// fallback.
func (d *Dir) Swap(e Entry) (Pointer, error) {
	old, _, err := d.ReadPointer()
	if err != nil && !errors.Is(err, ErrNoPointer) {
		old = Pointer{} // unreadable: start over, objects are still present
	}
	if _, err := os.Stat(d.objectPath(e.SHA256)); err != nil {
		return old, fmt.Errorf("cache: object for version %d is not stored: %w", e.Version, err)
	}
	next := Pointer{Current: e, Previous: old.Current}
	if old.Current.SHA256 == e.SHA256 {
		// Same content, newer generation: keep the older distinct fallback.
		next.Previous = old.Previous
	}
	if !old.Current.Zero() {
		ob, _ := json.Marshal(old)
		if err := WriteFileAtomic(filepath.Join(d.path, "pointer.prev.json"), ob, 0o600); err != nil {
			return old, err
		}
	}
	nb, _ := json.MarshalIndent(next, "", "  ")
	if err := WriteFileAtomic(filepath.Join(d.path, "pointer.json"), nb, 0o600); err != nil {
		return old, err
	}
	return next, nil
}

// Verified is an entry whose bytes and statements have been checked.
type Verified struct {
	Entry    Entry
	Payload  []byte
	Delivery sign.Delivery
}

// Verify checks an entry's statements against the trust bundle and scope, and
// its object against the signed digest and size. It is run on apply, and again
// whenever an entry is loaded from disk.
func (d *Dir) Verify(b *sign.Bundle, e Entry, env string, maxBytes int64) (Verified, error) {
	if e.Zero() {
		return Verified{}, ErrNoPointer
	}
	if e.Config != d.config {
		return Verified{}, fmt.Errorf("%w: entry is for config %q", ErrCorrupt, e.Config)
	}
	a, del, err := b.VerifyUpdate(e.Artifact, e.Delivery, sign.Scope{Config: d.config, Environment: env, Ring: e.Ring})
	if err != nil {
		return Verified{}, err
	}
	if a.Version != e.Version || a.SHA256 != e.SHA256 || a.Size != e.Size || del.Generation != e.Generation {
		return Verified{}, fmt.Errorf("%w: entry does not match its signed statements", ErrCorrupt)
	}
	if a.Size > maxBytes {
		return Verified{}, fmt.Errorf("%w: object is larger than the local limit", ErrCorrupt)
	}
	payload, err := d.ReadObject(e.SHA256, a.Size)
	if err != nil {
		return Verified{}, err
	}
	if int64(len(payload)) != a.Size {
		return Verified{}, ErrCorrupt
	}
	return Verified{Entry: e, Payload: payload, Delivery: del}, nil
}

// GC removes objects that are not referenced by any of keep and are not among
// the newest retain objects. Corrupt or partial objects are always removed
// unless referenced (a referenced corrupt object fails verification instead).
func (d *Dir) GC(retain int, keep ...string) (removed int, err error) {
	dir := filepath.Join(d.path, "objects")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	keepSet := map[string]bool{}
	for _, k := range keep {
		if k != "" {
			keepSet[k] = true
		}
	}
	type obj struct {
		name string
		mod  time.Time
	}
	var objs []obj
	for _, e := range ents {
		if !hexDigest.MatchString(e.Name()) || keepSet[e.Name()] {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		objs = append(objs, obj{e.Name(), info.ModTime()})
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].mod.After(objs[j].mod) })
	for i, o := range objs {
		if i < retain {
			continue
		}
		if err := os.Remove(filepath.Join(dir, o.name)); err == nil {
			removed++
		}
	}
	return removed, nil
}

// --- SDK state ----------------------------------------------------------------

// Mark identifies a delivery the SDK made a decision about.
type Mark struct {
	Generation int64  `json:"generation"`
	Version    int64  `json:"version"`
	SHA256     string `json:"sha256"`
}

// Zero reports whether the mark is empty.
func (m Mark) Zero() bool { return m.Generation == 0 }

// SDKState is the SDK's durable record, written only by the SDK.
type SDKState struct {
	// Accepted is the entry the application last accepted: its last-known-
	// good. The agent never deletes its object.
	Accepted Entry `json:"accepted"`
	// Trial is the entry currently being served that has not yet been marked
	// healthy, with the number of process starts while serving it.
	Trial       Mark `json:"trial"`
	TrialStarts int  `json:"trial_starts"`
	// Rejected and Quarantined record decisions so they are not retried on
	// every poll; a newer generation is always evaluated afresh.
	Rejected    Mark `json:"rejected"`
	Quarantined Mark `json:"quarantined"`
}

// ReadSDKState reads the SDK state; a missing or corrupt file yields an empty
// state (the SDK then re-derives everything from the pointer).
func (d *Dir) ReadSDKState() SDKState {
	var s SDKState
	b, err := os.ReadFile(filepath.Join(d.path, "sdk-state.json"))
	if err != nil || len(b) > 1<<20 {
		return SDKState{}
	}
	if json.Unmarshal(b, &s) != nil {
		return SDKState{}
	}
	return s
}

// WriteSDKState replaces the SDK state.
func (d *Dir) WriteSDKState(s SDKState) error {
	b, _ := json.MarshalIndent(s, "", "  ")
	return WriteFileAtomic(filepath.Join(d.path, "sdk-state.json"), b, 0o600)
}

// SDKStatus is what the SDK reports through the agent.
type SDKStatus struct {
	Generation        int64  `json:"generation"`
	Version           int64  `json:"version"`
	State             string `json:"state"` // ACCEPTED, REJECTED, CRASH_LOOP
	Reason            string `json:"reason,omitempty"`
	ServingVersion    int64  `json:"serving_version"`
	ServingGeneration int64  `json:"serving_generation"`
	// Seq increases with every write so readers can detect changes.
	Seq int64 `json:"seq"`
}

// ReadSDKStatus reads the SDK status file.
func (d *Dir) ReadSDKStatus() (SDKStatus, error) {
	var s SDKStatus
	b, err := os.ReadFile(filepath.Join(d.path, "sdk-status.json"))
	if err != nil {
		return s, err
	}
	if len(b) > 1<<16 {
		return s, ErrCorrupt
	}
	return s, json.Unmarshal(b, &s)
}

// WriteSDKStatus replaces the SDK status file.
func (d *Dir) WriteSDKStatus(s SDKStatus) error {
	b, _ := json.Marshal(s)
	return WriteFileAtomic(filepath.Join(d.path, "sdk-status.json"), b, 0o600)
}

// --- heartbeat ------------------------------------------------------------------

// BumpHeartbeat records a successful contact with the control plane. The value
// is a counter, not a timestamp: readers measure time since it last changed
// with their own monotonic clock, so clock skew cannot make a stale cache
// look fresh.
func BumpHeartbeat(root string, n int64) error {
	return WriteFileAtomic(filepath.Join(root, "agent-heartbeat"), []byte(strconv.FormatInt(n, 10)), 0o600)
}

// ReadHeartbeat returns the heartbeat counter (0 when absent).
func ReadHeartbeat(root string) int64 {
	b, err := os.ReadFile(filepath.Join(root, "agent-heartbeat"))
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return n
}

// SetFaultInjector installs a hook that can fail named filesystem steps
// ("create", "write", "fsync", "rename") for a path. Tests only; pass nil to
// remove it. Not safe to change while writes are in progress.
func SetFaultInjector(f func(step, path string) error) { fsFaults = f }

// Digest returns the hex SHA-256 of b.
func Digest(b []byte) string { return digest(b) }
