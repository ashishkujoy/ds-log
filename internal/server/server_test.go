package server

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	"ashishkujoy/ds-log/internal/log"
	"context"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
	"net"
	"testing"
)

func TestGrpcServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client log_v1.LogClient, config *Config){
		"produce/consume message from log successfully": testProduceConsume,
		"produce/consume stream succeeds":               testProduceConsumeStream,
		"consume past log boundary fails":               testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, tearDown := setupTest(t, nil)
			defer tearDown()
			fn(t, client, config)
		})
	}
}

func testConsumePastBoundary(t *testing.T, client log_v1.LogClient, config *Config) {

}

func testProduceConsumeStream(t *testing.T, client log_v1.LogClient, config *Config) {
	ctx := context.Background()
	records := []*log_v1.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err := stream.Send(&log_v1.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset %d, required %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &log_v1.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for offset, record := range records {
			response, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, response.Record.Offset, uint64(offset))
			require.Equal(t, response.Record.Value, record.Value)
		}
	}
}

func setupTest(t *testing.T, fn func(*Config)) (log_v1.LogClient, *Config, func()) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	clientConn, err := grpc.Dial(l.Addr().String(), dialOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	log, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	config := &Config{CommitLog{log}}

	if fn != nil {
		fn(config)
	}

	server, err := NewGRPCServer(config)
	require.NoError(t, err)

	go func(server *grpc.Server) {
		_ = server.Serve(l)
	}(server)
	client := log_v1.NewLogClient(clientConn)

	return client, config, func() {
		server.Stop()
		clientConn.Close()
		l.Close()
		log.Remove()
	}
}

func testProduceConsume(t *testing.T, client log_v1.LogClient, config *Config) {
	ctx := context.Background()
	record := &log_v1.Record{Value: []byte("hello world")}
	response, err := client.Produce(ctx, &log_v1.ProduceRequest{Record: record})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &log_v1.ConsumeRequest{Offset: response.Offset})
	require.NoError(t, err)
	require.Equal(t, record.Value, consume.Record.Value)
	require.Equal(t, record.Offset, consume.Record.Offset)
}
