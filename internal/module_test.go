package internal

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"
	discoveryv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/discovery/v1"

	"github.com/Muxcore-Media/config-watcher/internal/adminv1"
)

func testConfig() Config {
	return Config{GRPCAddr: ":0"}
}

func authCtx(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("x-caller-id", "test-module"))
}

func startModule(t *testing.T) (*Module, context.Context) {
	t.Helper()
	m := NewModule(testConfig())
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := m.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return m, ctx
}

func dialModule(t *testing.T, m *Module) (*grpc.ClientConn, configwatcherv1.ConfigWatcherServiceClient, adminv1.ConfigWatcherAdminServiceClient) {
	t.Helper()
	addr := m.lis.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close: %v", err)
		}
	})
	return conn, configwatcherv1.NewConfigWatcherServiceClient(conn), adminv1.NewConfigWatcherAdminServiceClient(conn)
}

func waitWatchers(t *testing.T, m *Module, capability string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		m.mu.RLock()
		n := len(m.watchers[capability])
		m.mu.RUnlock()
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d watchers on %q, have %d", want, capability, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
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
	if len(info.Roles) != 1 || info.Roles[0] != "config" {
		t.Errorf("roles = %v", info.Roles)
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
}

func TestHealthNotServingBeforeStart(t *testing.T) {
	m := NewModule(testConfig())
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Health(ctx); err == nil {
		t.Fatal("expected Health error before Start")
	}
}

func TestHealthServingAfterStart(t *testing.T) {
	m, ctx := startModule(t)
	if err := m.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
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
	if err := m.UpdateSetting("unknown_key", "1"); err == nil {
		t.Fatal("expected error for unknown setting")
	}
}

func TestNewModuleStreamBufferEnv(t *testing.T) {
	t.Setenv("CONFIG_WATCHER_STREAM_BUFFER", "24")
	m := NewModule(Config{})
	if m.streamBuf() != 24 {
		t.Fatalf("streamBuf()=%d want 24", m.streamBuf())
	}
}

func TestWatchThenReport(t *testing.T) {
	m, ctx := startModule(t)
	_, watchClient, adminClient := dialModule(t, m)

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

	waitWatchers(t, m, "secrets", 1)

	_, err = adminClient.ReportModuleChange(authCtx(ctx), &adminv1.ReportModuleChangeRequest{
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

func TestWildcardWatcherReceivesAllCapabilities(t *testing.T) {
	m, ctx := startModule(t)
	_, watchClient, adminClient := dialModule(t, m)

	stream, err := watchClient.WatchChanges(ctx, &configwatcherv1.WatchChangesRequest{Capability: ""})
	if err != nil {
		t.Fatalf("WatchChanges: %v", err)
	}

	recvCh := make(chan *configwatcherv1.WatchChangesResponse, 1)
	go func() {
		resp, err := stream.Recv()
		if err != nil {
			return
		}
		recvCh <- resp
	}()

	waitWatchers(t, m, "", 1)

	_, err = adminClient.ReportModuleChange(authCtx(ctx), &adminv1.ReportModuleChangeRequest{
		EventType:  "module.registered",
		ModuleId:   "cache-local",
		Capability: "cache",
	})
	if err != nil {
		t.Fatalf("ReportModuleChange: %v", err)
	}

	select {
	case resp := <-recvCh:
		if resp.GetCapability() != "cache" {
			t.Errorf("capability=%q", resp.GetCapability())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for wildcard event")
	}
}

func TestReportModuleChangeValidation(t *testing.T) {
	m, ctx := startModule(t)
	_, _, adminClient := dialModule(t, m)

	_, err := adminClient.ReportModuleChange(authCtx(ctx), &adminv1.ReportModuleChangeRequest{
		EventType:  "module.registered",
		ModuleId:   "x",
		Capability: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty capability: code=%v err=%v", status.Code(err), err)
	}

	_, err = adminClient.ReportModuleChange(authCtx(ctx), &adminv1.ReportModuleChangeRequest{
		EventType:  "",
		ModuleId:   "x",
		Capability: "secrets",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty event_type: code=%v err=%v", status.Code(err), err)
	}
}

func TestReportModuleChangeRequiresAuth(t *testing.T) {
	m, ctx := startModule(t)
	_, _, adminClient := dialModule(t, m)

	_, err := adminClient.ReportModuleChange(ctx, &adminv1.ReportModuleChangeRequest{
		EventType:  "module.registered",
		ModuleId:   "x",
		Capability: "secrets",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code=%v err=%v", status.Code(err), err)
	}
}

func TestSlowWatcherDrop(t *testing.T) {
	m := NewModule(Config{StreamBuffer: 1})
	fast := make(chan *configwatcherv1.WatchChangesResponse, 1)
	slow := make(chan *configwatcherv1.WatchChangesResponse, 1)
	slow <- &configwatcherv1.WatchChangesResponse{EventType: "module.registered", ModuleId: "block", Capability: "secrets"}

	m.mu.Lock()
	m.watchers["secrets"] = map[chan *configwatcherv1.WatchChangesResponse]struct{}{
		fast: struct{}{},
		slow: struct{}{},
	}
	m.mu.Unlock()

	m.fanOut(&configwatcherv1.WatchChangesResponse{
		EventType:  "module.registered",
		ModuleId:   "secrets-provider",
		Capability: "secrets",
	})

	select {
	case evt := <-fast:
		if evt.GetModuleId() != "secrets-provider" {
			t.Fatalf("module_id=%q", evt.GetModuleId())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fast watcher did not receive event while slow watcher was full")
	}
}

func TestStreamCancelRemovesWatcher(t *testing.T) {
	m, ctx := startModule(t)
	_, watchClient, _ := dialModule(t, m)

	watchCtx, cancel := context.WithCancel(ctx)
	stream, err := watchClient.WatchChanges(watchCtx, &configwatcherv1.WatchChangesRequest{Capability: "db"})
	if err != nil {
		t.Fatal(err)
	}
	waitWatchers(t, m, "db", 1)
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for {
		m.mu.RLock()
		n := len(m.watchers["db"])
		m.mu.RUnlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watcher still registered after cancel, count=%d", n)
		}
		_, _ = stream.Recv()
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMultipleWatchers(t *testing.T) {
	m, ctx := startModule(t)
	_, watchClient, adminClient := dialModule(t, m)

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
			if resp.GetEventType() == "module.registered" && resp.GetModuleId() == "secrets-provider" {
				recvCh <- 1
			}
		}()
	}

	waitWatchers(t, m, "secrets", 2)

	_, err = adminClient.ReportModuleChange(authCtx(ctx), &adminv1.ReportModuleChangeRequest{
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

func TestSendSnapshotMatchingCapability(t *testing.T) {
	m := NewModule(testConfig())
	ctx := context.Background()

	snap := &snapshotRecorder{}
	if err := m.emitSnapshotDirect(ctx, "secrets", []*discoveryv1.ModuleInfoProto{
		{Id: "secrets-file", Capabilities: []string{"secrets", "settings"}},
	}, snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.events) != 1 {
		t.Fatalf("events=%d want 1", len(snap.events))
	}
	if snap.events[0].GetModuleId() != "secrets-file" || snap.events[0].GetCapability() != "secrets" {
		t.Fatalf("unexpected snapshot event: %+v", snap.events[0])
	}
}

type snapshotRecorder struct {
	events []*configwatcherv1.WatchChangesResponse
}

func (s *snapshotRecorder) Send(evt *configwatcherv1.WatchChangesResponse) error {
	s.events = append(s.events, evt)
	return nil
}

func TestModuleLifecycle(t *testing.T) {
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
	if err := m.Health(ctx); err == nil {
		t.Fatal("expected Health error after Stop")
	}
}

func TestUpdateSettingEnvAlias(t *testing.T) {
	m := NewModule(Config{})
	if err := m.UpdateSetting("CONFIG_WATCHER_STREAM_BUFFER", "12"); err != nil {
		t.Fatal(err)
	}
	if m.streamBuf() != 12 {
		t.Fatalf("streamBuf=%d", m.streamBuf())
	}
}

func TestModuleTokenAuth(t *testing.T) {
	t.Setenv("CONFIG_WATCHER_MODULE_TOKEN", "secret-token")
	m := NewModule(testConfig())
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	_, _, adminClient := dialModule(t, m)
	tokenCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer secret-token"))
	_, err := adminClient.ReportModuleChange(tokenCtx, &adminv1.ReportModuleChangeRequest{
		EventType:  "module.registered",
		ModuleId:   "x",
		Capability: "secrets",
	})
	if err != nil {
		t.Fatalf("token auth: %v", err)
	}
}
