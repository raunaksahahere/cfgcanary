package validate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

// Sandbox limits. A policy can lower them, never raise them above the Max
// values.
const (
	DefaultWasmTimeout   = 2 * time.Second
	MaxWasmTimeout       = 30 * time.Second
	DefaultWasmMemoryMiB = 64
	MaxWasmMemoryMiB     = 256
	MaxWasmOutputBytes   = 64 << 10
	MaxWasmModuleBytes   = 16 << 20
)

// Sandbox errors. Every one of them rejects the payload.
var (
	ErrWasmTimeout = errors.New("timed out")
	ErrWasmTrap    = errors.New("trapped or exited with an error")
	ErrWasmOutput  = errors.New("produced malformed or oversized output")
	ErrWasmModule  = errors.New("module could not be compiled or instantiated")
	ErrWasmBusy    = errors.New("sandbox is at capacity")
)

func sandboxCode(err error) string {
	switch {
	case errors.Is(err, ErrWasmTimeout):
		return "timeout"
	case errors.Is(err, ErrWasmTrap):
		return "trap"
	case errors.Is(err, ErrWasmOutput):
		return "bad_output"
	case errors.Is(err, ErrWasmBusy):
		return "busy"
	}
	return "unavailable"
}

// Result is the verdict a validator module prints (ABI cfgcanary-validator/v1).
type Result struct {
	OK     bool     `json:"ok"`
	Errors []string `json:"errors"`
}

// Sandbox runs validator modules under wazero.
//
// ABI cfgcanary-validator/v1: a WASI preview 1 command. Standard input is the
// payload. The module prints one JSON object {"ok":bool,"errors":[string]} on
// standard output and exits 0. The host gives it no files, no environment, no
// arguments, no sockets, a fixed clock, a memory ceiling and a deadline.
type Sandbox struct {
	sem chan struct{}

	mu       sync.Mutex
	runtimes map[uint32]wazero.Runtime // by memory limit in pages
	modules  map[string]wazero.CompiledModule
}

// NewSandbox returns a sandbox that runs at most concurrency validations at
// once.
func NewSandbox(concurrency int) *Sandbox {
	if concurrency <= 0 {
		concurrency = 2
	}
	return &Sandbox{sem: make(chan struct{}, concurrency), runtimes: map[uint32]wazero.Runtime{}, modules: map[string]wazero.CompiledModule{}}
}

// Close releases compiled modules.
func (s *Sandbox) Close(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.runtimes {
		_ = r.Close(ctx)
	}
	s.runtimes, s.modules = map[uint32]wazero.Runtime{}, map[string]wazero.CompiledModule{}
}

func limits(w *pb.WasmValidator) (time.Duration, uint32) {
	timeout := DefaultWasmTimeout
	if w != nil && w.Timeout != nil && w.Timeout.AsDuration() > 0 {
		timeout = w.Timeout.AsDuration()
	}
	if timeout > MaxWasmTimeout {
		timeout = MaxWasmTimeout
	}
	mib := uint32(DefaultWasmMemoryMiB)
	if w != nil && w.MemoryLimitMib > 0 {
		mib = uint32(w.MemoryLimitMib)
	}
	if mib > MaxWasmMemoryMiB {
		mib = MaxWasmMemoryMiB
	}
	return timeout, mib * 16 // 64 KiB pages
}

func (s *Sandbox) compiled(ctx context.Context, module []byte, pages uint32) (wazero.Runtime, wazero.CompiledModule, error) {
	if len(module) > MaxWasmModuleBytes {
		return nil, nil, fmt.Errorf("%w: module larger than %d bytes", ErrWasmModule, MaxWasmModuleBytes)
	}
	sum := sha256.Sum256(module)
	key := fmt.Sprintf("%s/%d", hex.EncodeToString(sum[:]), pages)
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[pages]
	if !ok {
		rt = wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
			WithMemoryLimitPages(pages).
			WithCloseOnContextDone(true))
		if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
			_ = rt.Close(ctx)
			return nil, nil, fmt.Errorf("%w: %v", ErrWasmModule, err)
		}
		s.runtimes[pages] = rt
	}
	if m, ok := s.modules[key]; ok {
		return rt, m, nil
	}
	m, err := rt.CompileModule(ctx, module)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrWasmModule, err)
	}
	// Only WASI imports are provided. A module that wants anything else
	// fails at instantiation.
	for _, imp := range m.ImportedFunctions() {
		if mod, _, _ := imp.Import(); mod != wasi_snapshot_preview1.ModuleName {
			return nil, nil, fmt.Errorf("%w: imports from %q are not provided", ErrWasmModule, mod)
		}
	}
	if len(s.modules) > 64 {
		for k, old := range s.modules {
			_ = old.Close(ctx)
			delete(s.modules, k)
		}
	}
	s.modules[key] = m
	return rt, m, nil
}

type capped struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (c *capped) Write(p []byte) (int, error) {
	if c.buf.Len()+len(p) > c.max {
		c.overflow = true
		return 0, errors.New("output limit exceeded")
	}
	return c.buf.Write(p)
}

// Run validates payload with the module. Any error means "reject".
func (s *Sandbox) Run(ctx context.Context, module []byte, w *pb.WasmValidator, payload []byte) (Result, error) {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return Result{}, ErrWasmBusy
	}
	timeout, pages := limits(w)
	rt, compiled, err := s.compiled(ctx, module, pages)
	if err != nil {
		return Result{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out := &capped{max: MaxWasmOutputBytes}
	errOut := &capped{max: 4 << 10}
	cfg := wazero.NewModuleConfig().
		WithName(""). // anonymous: instances never collide
		WithArgs("validator").
		WithStdin(bytes.NewReader(payload)).
		WithStdout(out).
		WithStderr(errOut)
	// Deliberately absent: WithFS/WithFSConfig, WithEnv, WithSysWalltime,
	// WithSysNanotime, WithRandSource. The module sees no files, an empty
	// environment, a fixed fake clock and a deterministic random source.
	mod, err := rt.InstantiateModule(runCtx, compiled, cfg)
	if mod != nil {
		_ = mod.Close(ctx)
	}
	if err != nil {
		var exit *sys.ExitError
		switch {
		case runCtx.Err() != nil && ctx.Err() == nil:
			return Result{}, ErrWasmTimeout
		case errors.As(err, &exit) && exit.ExitCode() == sys.ExitCodeDeadlineExceeded:
			return Result{}, ErrWasmTimeout
		case errors.As(err, &exit) && exit.ExitCode() == 0:
			// Normal termination through proc_exit(0).
		case out.overflow:
			return Result{}, ErrWasmOutput
		default:
			return Result{}, ErrWasmTrap
		}
	}
	if out.overflow {
		return Result{}, ErrWasmOutput
	}
	var res Result
	dec := json.NewDecoder(bytes.NewReader(out.buf.Bytes()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil || dec.More() {
		return Result{}, ErrWasmOutput
	}
	if len(res.Errors) > 20 {
		res.Errors = res.Errors[:20]
	}
	for i, e := range res.Errors {
		if len(e) > 300 {
			res.Errors[i] = e[:300] + "..."
		}
	}
	return res, nil
}
