package server

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	"ashishkujoy/ds-log/internal/auth"
	config "ashishkujoy/ds-log/internal/config"
	"ashishkujoy/ds-log/internal/log"
	"context"
	"flag"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
)

func TestGrpcServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, rootClient, noBodyClient log_v1.LogClient, cfg *Config){
		"produce/consume message from log successfully": testProduceConsume,
		"produce/consume stream succeeds":               testProduceConsumeStream,
		"consume past log boundary fails":               testConsumePastBoundary,
		"test unauthorized":                             testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, noBodyClient, cfg, tearDown := setupTest(t, nil)
			defer tearDown()
			fn(t, rootClient, noBodyClient, cfg)
		})
	}
}

func testUnauthorized(t *testing.T, _, noBodyClient log_v1.LogClient, c *Config) {
	ctx := context.Background()
	record := &log_v1.Record{Value: []byte("Hello")}
	_, err := noBodyClient.Produce(ctx, &log_v1.ProduceRequest{Record: record})
	//require.Nil(t, produceRes, "Produce response should be nil")
	require.Error(t, err)
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := noBodyClient.Consume(ctx, &log_v1.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}

func testConsumePastBoundary(t *testing.T, rootClient, noBodyClient log_v1.LogClient, config *Config) {

}

func testProduceConsumeStream(t *testing.T, rootClient, nobodyClient log_v1.LogClient, config *Config) {
	ctx := context.Background()
	records := []*log_v1.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}
	{
		stream, err := rootClient.ProduceStream(ctx)
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
		stream, err := rootClient.ConsumeStream(ctx, &log_v1.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for offset, record := range records {
			response, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, response.Record.Offset, uint64(offset))
			require.Equal(t, response.Record.Value, record.Value)
		}
	}
}

func setupTest(t *testing.T, fn func(*Config)) (log_v1.LogClient, log_v1.LogClient, *Config, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	rootClientConnection, rootClient, _ := newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
		l.Addr().String(),
		t,
	)
	nobodyClientConnection, noBodyClient, _ := newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
		l.Addr().String(),
		t,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg := &Config{
		CommitLog:  CommitLog{clog},
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}
	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, noBodyClient, cfg, func() {
		server.Stop()
		rootClientConnection.Close()
		nobodyClientConnection.Close()
		l.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

func testProduceConsume(t *testing.T, rootClient, noBodyClient log_v1.LogClient, config *Config) {
	ctx := context.Background()
	record := &log_v1.Record{Value: []byte("hello world")}
	response, err := rootClient.Produce(ctx, &log_v1.ProduceRequest{Record: record})
	require.NoError(t, err)
	consume, err := rootClient.Consume(ctx, &log_v1.ConsumeRequest{Offset: response.Offset})
	require.NoError(t, err)
	require.Equal(t, record.Value, consume.Record.Value)
	require.Equal(t, record.Offset, consume.Record.Offset)
}

func newClient(certPath, keyPath, serverAddress string, t *testing.T) (*grpc.ClientConn, log_v1.LogClient, []grpc.DialOption) {
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: certPath,
		KeyFile:  keyPath,
		CAFile:   config.CAFile,
		Server:   false,
	})

	require.NoError(t, err)
	clientCreds := credentials.NewTLS(tlsConfig)
	options := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
	clientConn, err := grpc.Dial(serverAddress, options...)

	require.NoError(t, err)
	client := log_v1.NewLogClient(clientConn)
	return clientConn, client, options
}

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}
