package server

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/auth"
)

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// GRPCServer returns a gRPC server with mTLS, identity interceptors, bounded
// message sizes and keepalive enforcement, serving both services.
func (s *Server) GRPCServer(tlsCfg *tls.Config, extra ...grpc.ServerOption) *grpc.Server {
	opts := append([]grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(s.cfg.TrustDomain)),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(s.cfg.TrustDomain)),
		grpc.MaxRecvMsgSize(int(s.cfg.HardMaxBytes) + 1<<20),
		grpc.MaxSendMsgSize(int(s.cfg.HardMaxBytes) + 1<<20),
		grpc.ConnectionTimeout(10 * time.Second),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		grpc.MaxConcurrentStreams(64),
	}, extra...)
	g := grpc.NewServer(opts...)
	pb.RegisterConfigServiceServer(g, s)
	pb.RegisterControlServiceServer(g, s)
	return g
}

// Gateway returns the REST handler. It calls the same methods as gRPC, so
// authorization is identical; identity comes from the client certificate.
func (s *Server) Gateway(ctx context.Context) (http.Handler, error) {
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions:   protojson.MarshalOptions{EmitUnpopulated: false, UseProtoNames: false},
		UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: false},
	}))
	if err := pb.RegisterControlServiceHandlerServer(ctx, mux, s); err != nil {
		return nil, err
	}
	limit := s.cfg.HardMaxBytes*4/3 + 1<<20 // base64 payloads in JSON
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
	return auth.HTTPMiddleware(s.cfg.TrustDomain, h), nil
}
