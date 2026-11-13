// Package client opens mTLS connections to the control plane.
package client

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/auth"
)

// Options describe how to reach the server.
type Options struct {
	Server     string // host:port
	ServerName string // TLS name to verify; defaults to the host part
	Cert, Key  string
	CA         string
	// MaxMsgBytes bounds message sizes (default 8 MiB).
	MaxMsgBytes int
	// Dialer overrides the network dialer (tests route through Toxiproxy).
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

// Credentials returns reloadable transport credentials.
func (o Options) Credentials() (credentials.TransportCredentials, error) {
	r, err := auth.NewReloader(o.Cert, o.Key, o.CA)
	if err != nil {
		return nil, err
	}
	name := o.ServerName
	if name == "" {
		host, _, err := net.SplitHostPort(o.Server)
		if err != nil {
			return nil, fmt.Errorf("server address %q: %w", o.Server, err)
		}
		name = host
	}
	return credentials.NewTLS(r.ClientTLS(name)), nil
}

// Dial returns a lazily connecting client.
func Dial(o Options) (*grpc.ClientConn, error) {
	creds, err := o.Credentials()
	if err != nil {
		return nil, err
	}
	return DialWith(o, creds)
}

// DialWith reuses credentials (agents dial on every watch attempt).
func DialWith(o Options, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	if o.MaxMsgBytes <= 0 {
		o.MaxMsgBytes = 8 << 20
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(o.MaxMsgBytes), grpc.MaxCallSendMsgSize(o.MaxMsgBytes)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
	}
	if o.Dialer != nil {
		opts = append(opts, grpc.WithContextDialer(o.Dialer))
	}
	return grpc.NewClient(o.Server, opts...)
}

// Control returns a ControlService client.
func Control(cc *grpc.ClientConn) pb.ControlServiceClient { return pb.NewControlServiceClient(cc) }
