package server

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	log "ashishkujoy/ds-log/internal/log"
	"context"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

type CommitLog struct {
	*log.Log
}

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

func (g *grpcServer) Produce(ctx context.Context, req *log_v1.ProduceRequest) (*log_v1.ProduceResponse, error) {
	u, err := g.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: u}, nil
}
func (g *grpcServer) Consume(ctx context.Context, req *log_v1.ConsumeRequest) (*log_v1.ConsumeResponse, error) {
	record, err := g.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &log_v1.ConsumeResponse{Record: record}, nil
}

func (g *grpcServer) ProduceStream(srv log_v1.Log_ProduceStreamServer) error {
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

func (g *grpcServer) ConsumeStream(req *log_v1.ConsumeRequest, srv log_v1.Log_ConsumeStreamServer) error {
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

var _ log_v1.LogServer = (*grpcServer)(nil)

func newGrpcServer(config *Config) (server *grpcServer, err error) {
	server = &grpcServer{Config: config}
	return server, nil
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	server, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}
	log_v1.RegisterLogServer(gsrv, server)
	return gsrv, err
}
