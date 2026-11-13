package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/devpki"
)

func randomSecret() string {
	var b [18]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func newDevCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "dev", Short: "Local development helpers"}
	var dir, dbHost string
	var shared bool
	boot := &cobra.Command{
		Use:   "bootstrap",
		Short: "Create development certificates, signing keys, RBAC policy and local credentials (idempotent)",
		Long: `Create everything a local deployment needs under --dir (default .dev, which is git-ignored):
a development CA, server and user certificates (admin, operator, viewer, oncall), a
service certificate for the Kubernetes controller, ed25519 artifact and delivery signing
keys with their trust bundle, an RBAC policy, and random database, object-store and
Grafana credentials (one file per secret under secrets/). Existing files are kept, so re-running it never rotates anything.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pki := filepath.Join(dir, "pki")
			created, err := devpki.Bootstrap(pki, devpki.DefaultOptions())
			if err != nil {
				return err
			}
			// Credentials for the local Compose stack, one file per secret so
			// that services read them with *_FILE variables rather than from
			// the environment.
			secrets := filepath.Join(dir, "secrets")
			if err := os.MkdirAll(secrets, 0o755); err != nil {
				return err
			}
			want := map[string]string{"postgres-password": randomSecret(), "s3-access-key": "cfgcanary", "s3-secret-key": randomSecret(), "grafana-admin-password": randomSecret()}
			for name, value := range want {
				p := filepath.Join(secrets, name)
				if _, err := os.Stat(p); err == nil {
					continue
				}
				if err := os.WriteFile(p, []byte(value), 0o644); err != nil {
					return err
				}
				created = append(created, filepath.Join("secrets", name))
			}
			dbURL := filepath.Join(secrets, "db-url")
			if _, err := os.Stat(dbURL); err != nil {
				pw, err := os.ReadFile(filepath.Join(secrets, "postgres-password"))
				if err != nil {
					return err
				}
				url := fmt.Sprintf("postgres://cfgcanary:%s@%s/cfgcanary?sslmode=disable", strings.TrimSpace(string(pw)), dbHost)
				if err := os.WriteFile(dbURL, []byte(url), 0o644); err != nil {
					return err
				}
				created = append(created, "secrets/db-url")
			}
			// Containers run as non-root users and must read what they mount;
			// the top-level directory stays private to this user.
			_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
				if err == nil && !info.IsDir() {
					_ = os.Chmod(p, 0o644)
				} else if err == nil {
					_ = os.Chmod(p, 0o755)
				}
				return nil
			})
			// Private to this user on a workstation; traversable when the
			// directory is a volume shared with non-root containers.
			if shared {
				_ = os.Chmod(dir, 0o755)
			} else {
				_ = os.Chmod(dir, 0o700)
			}
			if !human() {
				return emitJSON("Bootstrap", map[string]any{"dir": dir, "created": created})
			}
			if len(created) == 0 {
				printf("%s is already bootstrapped; nothing changed\n", dir)
				return nil
			}
			printf("bootstrapped %s (%d files): %s\n", dir, len(created), strings.Join(created, ", "))
			printf("these are development credentials: keep %s out of version control (it is git-ignored)\n", dir)
			return nil
		},
	}
	boot.Flags().StringVar(&dir, "dir", ".dev", "output directory")
	boot.Flags().BoolVar(&shared, "shared", false, "make the directory readable by other users (a volume shared with non-root containers)")
	boot.Flags().StringVar(&dbHost, "db-host", "postgres:5432", "PostgreSQL host:port written into secrets/db-url")
	cmd.AddCommand(boot, newDevIssueCmd())
	return cmd
}

func newDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check prerequisites for the local demo and connectivity to the control plane",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			failed := 0
			check := func(name string, ok bool, detail string, required bool) {
				mark := "ok  "
				if !ok {
					if required {
						mark, failed = "FAIL", failed+1
					} else {
						mark = "warn"
					}
				}
				printf("[%s] %-28s %s\n", mark, name, detail)
			}
			out, err := exec.CommandContext(cmd.Context(), "docker", "version", "--format", "{{.Server.Version}}").Output()
			check("docker engine", err == nil, strings.TrimSpace(string(out)), true)
			out, err = exec.CommandContext(cmd.Context(), "docker", "compose", "version", "--short").Output()
			check("docker compose", err == nil, strings.TrimSpace(string(out)), true)
			for _, p := range []string{"8443", "8444", "3000", "9090"} {
				l, err := net.Listen("tcp", "127.0.0.1:"+p)
				if err == nil {
					l.Close()
				}
				detail := "free"
				if err != nil {
					detail = "in use (fine if the cfgcanary stack is already running)"
				}
				check("port "+p, err == nil, detail, false)
			}
			_, err = os.Stat(filepath.Join(g.pkiDir, "ca.pem"))
			check("development PKI", err == nil, g.pkiDir+" (create with 'cfgcanary dev bootstrap')", false)
			if err == nil {
				if c, done, err := control(cmd); err == nil {
					ctx, cancel := reqCtx(cmd)
					i, err := c.GetServerInfo(ctx, &pb.GetServerInfoRequest{})
					cancel()
					done()
					var detail string
					if err == nil {
						detail = fmt.Sprintf("%s: server %s, you are %s %v", g.server, i.Version, short(i.Identity), i.Roles)
					} else {
						detail = fmt.Sprintf("%s: %v", g.server, err)
					}
					check("control plane", err == nil, detail, false)
				}
			}
			if failed > 0 {
				return &cliError{exitGeneral, fmt.Errorf("%d required check(s) failed", failed)}
			}
			return nil
		},
	}
}

func newDevIssueCmd() *cobra.Command {
	var uri, out, name string
	var days int
	cmd := &cobra.Command{
		Use:   "issue",
		Short: "Issue a client certificate from the development CA (for agents, groups or services)",
		Example: `  cfgcanary dev issue --uri spiffe://cfgcanary.local/env/prod/group/ring1 --out .dev/pki/groups --name ring1
  cfgcanary dev issue --uri spiffe://cfgcanary.local/env/prod/agent/host-17 --out certs --name host-17`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ca, err := devpki.LoadCA(g.pkiDir)
			if err != nil {
				return fail(exitUsage, "development CA not found in %s: %v", g.pkiDir, err)
			}
			if err := ca.WriteIssued(out, name, devpki.Spec{CommonName: name, URI: uri, Client: true,
				NotAfter: time.Now().Add(time.Duration(days) * 24 * time.Hour)}); err != nil {
				return err
			}
			printf("wrote %s/%s.pem and %s-key.pem for %s\n", out, name, name, uri)
			return nil
		},
	}
	cmd.Flags().StringVar(&uri, "uri", "", "SPIFFE ID")
	cmd.Flags().StringVar(&out, "out", ".", "output directory")
	cmd.Flags().StringVar(&name, "name", "client", "file base name")
	cmd.Flags().IntVar(&days, "days", 90, "validity in days")
	_ = cmd.MarkFlagRequired("uri")
	return cmd
}
