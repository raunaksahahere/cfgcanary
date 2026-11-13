// Command cfgcanary is the single binary for the cfgcanary control plane,
// agent, simulator and operator CLI. Run `cfgcanary --help`.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/buildinfo"
	"github.com/raunaksahahere/cfgcanary/internal/client"
)

// Exit codes. Documented in docs/cli.md.
const (
	exitOK           = 0
	exitGeneral      = 1
	exitUsage        = 2
	exitRejected     = 3 // validation rejected the payload
	exitPrecondition = 4 // precondition failed or conflict
	exitDenied       = 5 // unauthenticated or permission denied
	exitNotFound     = 6
	exitUnavailable  = 7
	exitReleaseFail  = 8 // a waited-for release ended rolled back or halted
)

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string { return e.err.Error() }

func fail(code int, format string, a ...any) error {
	return &cliError{code, fmt.Errorf(format, a...)}
}

// globals holds persistent flags.
type globals struct {
	server, serverName string
	cert, key, ca      string
	pkiDir, identity   string
	output             string
	timeout            time.Duration
	stdout, stderr     io.Writer
}

var g = &globals{stdout: os.Stdout, stderr: os.Stderr}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	root := newRoot()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := root.ExecuteContext(ctx)
	stop()
	if err != nil {
		code := exitGeneral
		var ee *cliError
		if errors.As(err, &ee) {
			code = ee.code
		} else if st, ok := status.FromError(err); ok {
			code = codeFor(st.Code())
			err = errors.New(st.Message())
		} else if strings.Contains(err.Error(), "unknown command") || strings.Contains(err.Error(), "flag") || strings.Contains(err.Error(), "arg(s)") {
			code = exitUsage
		}
		if g.output == "json" {
			_ = json.NewEncoder(g.stdout).Encode(map[string]any{"apiVersion": "cfgcanary/v1", "kind": "Error",
				"data": map[string]any{"message": err.Error(), "exit_code": code}})
		} else {
			fmt.Fprintln(g.stderr, "error:", err)
		}
		os.Exit(code)
	}
}

func codeFor(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return exitUsage
	case codes.FailedPrecondition, codes.Aborted, codes.AlreadyExists:
		return exitPrecondition
	case codes.PermissionDenied, codes.Unauthenticated:
		return exitDenied
	case codes.NotFound:
		return exitNotFound
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return exitUnavailable
	}
	return exitGeneral
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "cfgcanary",
		Short: "Progressive delivery for configuration: validate, canary, promote and roll back config like code.",
		Long: `cfgcanary validates structured runtime configuration, rolls it out ring by ring with
health gates, and rolls it back automatically. Agents and the Go SDK keep applications
running on their last-known-good configuration.

Operator commands talk to the control plane over mutual TLS. Identity comes from the
client certificate: --cert/--key/--ca, or --as NAME with a development PKI directory
(created by 'cfgcanary dev bootstrap').`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       versionString(),
	}
	pf := root.PersistentFlags()
	pf.StringVar(&g.server, "server", env("CFGCANARY_SERVER", "localhost:8443"), "control plane gRPC address (env CFGCANARY_SERVER)")
	pf.StringVar(&g.serverName, "server-name", env("CFGCANARY_SERVER_NAME", ""), "TLS name to verify (default: host of --server)")
	pf.StringVar(&g.cert, "cert", env("CFGCANARY_CERT", ""), "client certificate (env CFGCANARY_CERT)")
	pf.StringVar(&g.key, "key", env("CFGCANARY_KEY", ""), "client key (env CFGCANARY_KEY)")
	pf.StringVar(&g.ca, "ca", env("CFGCANARY_CA", ""), "CA bundle (env CFGCANARY_CA)")
	pf.StringVar(&g.pkiDir, "pki-dir", env("CFGCANARY_PKI_DIR", ".dev/pki"), "development PKI directory (env CFGCANARY_PKI_DIR)")
	pf.StringVar(&g.identity, "as", env("CFGCANARY_AS", "admin"), "development identity: users/<name> or services/<name> in --pki-dir (env CFGCANARY_AS)")
	pf.StringVarP(&g.output, "output", "o", env("CFGCANARY_OUTPUT", "human"), "output format: human or json (versioned envelope)")
	pf.DurationVar(&g.timeout, "timeout", 30*time.Second, "per-request timeout")
	root.SetVersionTemplate("{{.Version}}\n")

	root.AddCommand(
		newServerCmd(), newAgentCmd(), newSimCmd(),
		newConfigsCmd(), newVersionsCmd(), newValidateCmd(), newDiffCmd(), newReleasesCmd(),
		newKillSwitchCmd(), newUnfreezeCmd(), newTargetCmd(), newInstancesCmd(), newAuditCmd(), newInfoCmd(),
		newInitCmd(), newDevCmd(), newDoctorCmd(), newVersionCmd(),
	)
	return root
}

func versionString() string {
	v, c, d, gv := buildinfo.Info()
	s := "cfgcanary " + v
	if c != "" {
		s += " (" + c + ")"
	}
	if d != "" {
		s += " built " + d
	}
	return s + " " + gv
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version and build information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			v, c, d, gv := buildinfo.Info()
			if g.output == "json" {
				return emitJSON("BuildInfo", map[string]string{"version": v, "commit": c, "date": d, "go": gv})
			}
			fmt.Fprintln(g.stdout, versionString())
			return nil
		},
	}
}

// --- connections -------------------------------------------------------------

func clientOptions() (client.Options, error) {
	o := client.Options{Server: g.server, ServerName: g.serverName, Cert: g.cert, Key: g.key, CA: g.ca}
	if o.Cert == "" || o.Key == "" {
		id := g.identity
		if !strings.Contains(id, "/") {
			id = "users/" + id
		}
		o.Cert = filepath.Join(g.pkiDir, id+".pem")
		o.Key = filepath.Join(g.pkiDir, id+"-key.pem")
	}
	if o.CA == "" {
		o.CA = filepath.Join(g.pkiDir, "ca.pem")
	}
	for _, f := range []string{o.Cert, o.Key, o.CA} {
		if _, err := os.Stat(f); err != nil {
			return o, fail(exitDenied, "client credentials not found (%s). Pass --cert/--key/--ca, or run 'cfgcanary dev bootstrap' for a development PKI", f)
		}
	}
	return o, nil
}

func dial() (*grpc.ClientConn, error) {
	o, err := clientOptions()
	if err != nil {
		return nil, err
	}
	return client.Dial(o)
}

func control(cmd *cobra.Command) (pb.ControlServiceClient, func(), error) {
	cc, err := dial()
	if err != nil {
		return nil, nil, err
	}
	return pb.NewControlServiceClient(cc), func() { cc.Close() }, nil
}

func reqCtx(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), g.timeout)
}

// --- output -------------------------------------------------------------------

var pjson = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}

func emitJSON(kind string, data any) error {
	var raw json.RawMessage
	if m, ok := data.(proto.Message); ok {
		b, err := pjson.Marshal(m)
		if err != nil {
			return err
		}
		raw = b
	} else {
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		raw = b
	}
	enc := json.NewEncoder(g.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{"apiVersion": "cfgcanary/v1", "kind": kind, "data": raw})
}

func printf(format string, a ...any) { fmt.Fprintf(g.stdout, format, a...) }

func human() bool { return g.output != "json" }
