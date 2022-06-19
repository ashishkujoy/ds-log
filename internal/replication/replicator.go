package replication

import (
	api "ashishkujoy/ds-log/api/v1"
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
)

type Replicator struct {
	DialOption  []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	clientConn, err := grpc.Dial(addr, r.DialOption...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
	}
	defer clientConn.Close()
	logClient := api.NewLogClient(clientConn)
	ctx := context.Background()
	stream, err := logClient.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logError(err, "Failed to consume", addr)
		return
	}
	records := make(chan *api.Record)
	go func(records chan *api.Record) {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive record", addr)
			}
			records <- recv.Record
		}
	}(records)
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) logError(err error, msg string, addr string) {
	r.logger.Error(msg, zap.String("addr", addr), zap.Error(err))
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}
