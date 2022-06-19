package server

import (
	logV1 "ashishkujoy/ds-log/api/v1"
	log "ashishkujoy/ds-log/internal/log"
	"context"
	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"time"
)

const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type SubjectContextKey struct{}

func subject(ctx context.Context) string {
	return ctx.Value(SubjectContextKey{}).(string)
}

type CommitLog struct {
	*log.Log
}

type grpcServer struct {
	logV1.UnimplementedLogServer
	*Config
}

func (g *grpcServer) Produce(ctx context.Context, req *logV1.ProduceRequest) (*logV1.ProduceResponse, error) {
	if err := g.Authorizer.Authorize(subject(ctx), objectWildCard, produceAction); err != nil {
		return nil, err
	}
	u, err := g.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &logV1.ProduceResponse{Offset: u}, nil
}

func (g *grpcServer) Consume(ctx context.Context, req *logV1.ConsumeRequest) (*logV1.ConsumeResponse, error) {
	if err := g.Authorizer.Authorize(subject(ctx), objectWildCard, consumeAction); err != nil {
		return nil, err
	}
	record, err := g.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &logV1.ConsumeResponse{Record: record}, nil
}

func (g *grpcServer) ProduceStream(srv logV1.Log_ProduceStreamServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return err
		}
		res, err := g.Produce(srv.Context(), req)
		if err != nil {
			return err
		}
		if err = srv.Send(res); err != nil {
			return err
		}
	}
}

func (g *grpcServer) ConsumeStream(req *logV1.ConsumeRequest, srv logV1.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-srv.Context().Done():
			return nil
		default:
			response, err := g.Consume(srv.Context(), req)
			switch err.(type) {
			case nil:
			case log.ErrOutOfRange:
				continue
			default:
				return err
			}
			if err = srv.Send(response); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

var _ logV1.LogServer = (*grpcServer)(nil)

func newGrpcServer(config *Config) (server *grpcServer, err error) {
	server = &grpcServer{Config: config}
	return server, nil
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{grpc_zap.WithDurationField(func(duration time.Duration) zapcore.Field {
		return zap.Int64("grpc_time_ns", duration.Nanoseconds())
	})}
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}
	opts = append(
		opts,
		grpc.StreamInterceptor(
			grpcMiddleware.ChainStreamServer(
				grpcAuth.StreamServerInterceptor(authenticate),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_ctxtags.StreamServerInterceptor(),
			)),
		grpc.UnaryInterceptor(
			grpcMiddleware.ChainUnaryServer(
				grpcAuth.UnaryServerInterceptor(authenticate),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				grpc_ctxtags.UnaryServerInterceptor(),
			)),
	)
	gsrv := grpc.NewServer(opts...)
	server, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}
	logV1.RegisterLogServer(gsrv, server)
	return gsrv, err
}

func authenticate(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}
	if p.AuthInfo == nil {
		return context.WithValue(ctx, SubjectContextKey{}, ""), nil
	}
	tlsInfo := p.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, SubjectContextKey{}, subject)
	return ctx, nil
}
