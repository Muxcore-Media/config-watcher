package client_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Muxcore-Media/config-watcher/internal"
	"github.com/Muxcore-Media/config-watcher/internal/adminv1"
	"github.com/Muxcore-Media/config-watcher/pkg/client"
	"github.com/Muxcore-Media/core/pkg/contracts"
	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"
)

const bufSize = 1 << 20

func startTestModule(t *testing.T) *internal.Module {
	t.Helper()
	mod := internal.NewModule(internal.Config{GRPCAddr: ":0"})
	ctx := context.Background()
	if err := mod.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
	})
	return mod
}

func dialBufconn(t *testing.T, mod *internal.Module) (*grpc.ClientConn, *bufconn.Listener) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	configwatcherv1.RegisterConfigWatcherServiceServer(gs, mod)
	adminv1.RegisterConfigWatcherAdminServiceServer(gs, mod)
	go gs.Serve(lis)
	t.Cleanup(func() {
		gs.Stop()
		lis.Close()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, lis
}

func authCtx() context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-caller-id", "test-module"))
}

func TestClientOnChange(t *testing.T) {
	mod := startTestModule(t)
	conn, _ := dialBufconn(t, mod)

	w := client.New(conn)
	var calls atomic.Int32
	cancel := w.OnChange(context.Background(), "secrets", func() {
		calls.Add(1)
	})
	defer cancel()

	admin := adminv1.NewConfigWatcherAdminServiceClient(conn)
	_, err := admin.ReportModuleChange(authCtx(), &adminv1.ReportModuleChangeRequest{
		EventType:  contracts.EventModuleRegistered,
		ModuleId:   "secrets-file",
		Capability: "secrets",
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for OnChange handler")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var _ contracts.ConfigWatcher = (*client.Client)(nil)
