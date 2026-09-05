package internal

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"

	"github.com/Muxcore-Media/config-watcher/internal/adminv1"
)

func testConfig() Config {
	return Config{GRPCAddr: ":0"}
}

func enableInsecureGRPCForTest(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
}

func TestModuleInfo(t *testing.T) {
	m := NewModule(Config{})
	info := m.Info()
	if info.ID == "" {
		t.Error("module ID must not be empty")
	}
	if info.Version != "0.1.1" {
		t.Errorf("version = %q", info.Version)
	}
	if info.MinCoreVersion == "" {
		t.Error("MinCoreVersion must not be empty")
	}
	if len(info.Contracts) == 0 {
		t.Error("Contracts must not be empty")
	}
	if len(info.Capabilities) == 0 {
		t.Error("Capabilities must not be empty")
	}
	if info.Capabilities[0] != "config.watcher" {
		t.Errorf("expected capability config.watcher, got %s", info.Capabilities[0])
	}
	foundSettings := false
	for _, c := range info.Capabilities {
		if c == "settings" {
			foundSettings = true
		}
	}
	if !foundSettings {
		t.Error("expected settings capability")
	}
	if info.HTTPAddr != "127.0.0.1:9611" {
		t.Errorf("HTTPAddr = %q, want 127.0.0.1:9611", info.HTTPAddr)
	}
}

func TestEnvGRPCAddrAllInterfaces(t *testing.T) {
	t.Setenv("CONFIG_WATCHER_GRPC_ADDR", ":9611")
	m := NewModule(Config{})
	if m.Info().HTTPAddr != ":9611" {
		t.Fatalf("HTTPAddr = %q, want :9611", m.Info().HTTPAddr)
	}
}

func TestSettings_StreamBuffer(t *testing.T) {
	m := NewModule(Config{StreamBuffer: 8})
	if err := m.UpdateSetting("stream_buffer", "32"); err != nil {
		t.Fatal(err)
	}
	if m.Settings()[0].Value != "32" {
		t.Fatalf("value=%q", m.Settings()[0].Value)
	}
	if err := m.UpdateSetting("stream_buffer", "0"); err == nil {
		t.Fatal("expected error")
	}
}

func TestWatchThenReport(t *testing.T) {
	enableInsecureGRPCForTest(t)
	m := NewModule(testConfig())
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop(ctx)

	addr := m.lis.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	watchClient := configwatcherv1.NewConfigWatcherServiceClient(conn)
	adminClient := adminv1.NewConfigWatcherAdminServiceClient(conn)

	stream, err := watchClient.WatchChanges(ctx, &configwatcherv1.WatchChangesRequest{Capability: "secrets"})
	if err != nil {
		t.Fatalf("WatchChanges: %v", err)
	}

	recvCh := make(chan *configwatcherv1.WatchChangesResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		recvCh <- resp
	}()

	// Allow the server-side watch registration to complete before reporting.
	deadline := time.Now().Add(2 * time.Second)
	for {
		m.mu.RLock()
		n := len(m.watchers["secrets"])
		m.mu.RUnlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for watcher registration")
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, err = adminClient.ReportModuleChange(ctx, &adminv1.ReportModuleChangeRequest{
		EventType:  "module.registered",
		ModuleId:   "secrets-provider",
		Capability: "secrets",
	})
	if err != nil {
		t.Fatalf("ReportModuleChange: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("stream Recv: %v", err)
	case resp := <-recvCh:
		if resp.GetEventType() != "module.registered" {
			t.Errorf("expected event_type module.registered, got %s", resp.GetEventType())
		}
		if resp.GetModuleId() != "secrets-provider" {
			t.Errorf("expected module_id secrets-provider, got %s", resp.GetModuleId())
		}
		if resp.GetCapability() != "secrets" {
			t.Errorf("expected capability secrets, got %s", resp.GetCapability())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}
func TestMultipleWatchers(t *testing.T) {
	enableInsecureGRPCForTest(t)
	m := NewModule(testConfig())
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop(ctx)

	addr := m.lis.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	watchClient := configwatcherv1.NewConfigWatcherServiceClient(conn)
	adminClient := adminv1.NewConfigWatcherAdminServiceClient(conn)

	s1, err := watchClient.WatchChanges(ctx, &configwatcherv1.WatchChangesRequest{Capability: "secrets"})
	if err != nil {
		t.Fatalf("WatchChanges s1: %v", err)
	}
	s2, err := watchClient.WatchChanges(ctx, &configwatcherv1.WatchChangesRequest{Capability: "secrets"})
	if err != nil {
		t.Fatalf("WatchChanges s2: %v", err)
	}

	recvCh := make(chan int, 2)
	errCh := make(chan error, 2)
	for _, s := range []grpc.ServerStreamingClient[configwatcherv1.WatchChangesResponse]{s1, s2} {
		s := s
		go func() {
			resp, err := s.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if resp.GetEventType() == "module.registered" {
				recvCh <- 1
			}
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		m.mu.RLock()
		n := len(m.watchers["secrets"])
		m.mu.RUnlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for watchers, have %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, err = adminClient.ReportModuleChange(ctx, &adminv1.ReportModuleChangeRequest{
		EventType:  "module.registered",
		ModuleId:   "secrets-provider",
		Capability: "secrets",
	})
	if err != nil {
		t.Fatalf("ReportModuleChange: %v", err)
	}

	received := 0
	timeout := time.After(5 * time.Second)
	for received < 2 {
		select {
		case err := <-errCh:
			t.Fatalf("stream Recv: %v", err)
		case <-recvCh:
			received++
		case <-timeout:
			t.Fatalf("timed out: got %d/2 events", received)
		}
	}
}

func TestModuleLifecycle(t *testing.T) {
	enableInsecureGRPCForTest(t)
	m := NewModule(testConfig())
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
