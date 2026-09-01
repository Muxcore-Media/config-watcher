package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Muxcore-Media/core/pkg/contracts"
	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"

	"github.com/Muxcore-Media/config-watcher/internal/adminv1"
)

const defaultStreamBuffer = 16

//nolint:govet // struct field alignment
type Module struct {
	configwatcherv1.UnimplementedConfigWatcherServiceServer
	adminv1.UnimplementedConfigWatcherAdminServiceServer

	watchers map[string]map[chan *configwatcherv1.WatchChangesResponse]struct{}
	grpcSrv  *grpc.Server
	lis      net.Listener

	id           string
	grpcAddr     string
	moduleToken  string
	streamBuffer int

	cfgMu      sync.RWMutex
	mu         sync.RWMutex
	meshMu     sync.RWMutex
	registryMu sync.RWMutex

	mc         *client.Client
	moduleCaps map[string][]string

	eventsCancel context.CancelFunc
	eventsDone   chan struct{}
}

type Config struct {
	ID           string
	GRPCAddr     string
	StreamBuffer int
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "config-watcher"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9614"
	}
	if v := os.Getenv("CONFIG_WATCHER_GRPC_ADDR"); v != "" {
		cfg.GRPCAddr = v
	}
	buf := cfg.StreamBuffer
	if buf <= 0 {
		buf = defaultStreamBuffer
	}
	if v := strings.TrimSpace(os.Getenv("CONFIG_WATCHER_STREAM_BUFFER")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			buf = n
		}
	}
	return &Module{
		id:           cfg.ID,
		grpcAddr:     cfg.GRPCAddr,
		moduleToken:  moduleTokenFromEnv(),
		streamBuffer: buf,
		watchers:     make(map[string]map[chan *configwatcherv1.WatchChangesResponse]struct{}),
		moduleCaps:   make(map[string][]string),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           m.id,
		Name:         "Config Watcher",
		Version:      "0.1.1",
		Roles:        []string{"config"},
		Description:  "Notifies modules when infrastructure services change at runtime.",
		Author:       "MuxCore Media",
		Capabilities: []string{"config.watcher", "settings"},
		Contracts: []contracts.ContractDeclaration{
			{Repo: "github.com/Muxcore-Media/core/pkg/contracts", Interface: "ConfigWatcher", Version: "v0.4.0"},
		},
		MinCoreVersion: "0.5.8",
		HTTPAddr:       m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", m.grpcAddr, err)
	}
	m.lis = lis
	m.cfgMu.RLock()
	buf := m.streamBuffer
	m.cfgMu.RUnlock()
	slog.Info("config-watcher initialized", "addr", m.grpcAddr, "stream_buffer", buf)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	srv := grpc.NewServer(grpc.UnaryInterceptor(authUnaryInterceptor(m.moduleToken)))
	lis := m.lis
	m.grpcSrv = srv
	configwatcherv1.RegisterConfigWatcherServiceServer(srv, m)
	adminv1.RegisterConfigWatcherAdminServiceServer(srv, m)
	modulesdk.RegisterSettings(srv, m.id, m)
	go func() {
		slog.Info("config-watcher gRPC started", "addr", m.grpcAddr)
		if err := srv.Serve(lis); err != nil {
			slog.Error("config-watcher gRPC error", "error", err)
		}
	}()

	m.dialCore()
	m.bootstrapRegistryCache(ctx)

	eventsCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.eventsCancel = cancel
	m.eventsDone = done
	go func() {
		defer close(done)
		if delay := registrySyncDelay(); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-eventsCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		m.subscribeRegistryEvents(eventsCtx)
	}()

	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.eventsCancel != nil {
		m.eventsCancel()
	}
	if m.eventsDone != nil {
		<-m.eventsDone
	}
	srv := m.grpcSrv
	if srv != nil {
		srv.GracefulStop()
		m.grpcSrv = nil
	}
	if m.lis != nil {
		_ = m.lis.Close()
		m.lis = nil
	}
	m.meshMu.Lock()
	if m.mc != nil {
		_ = m.mc.Close()
		m.mc = nil
	}
	m.meshMu.Unlock()
	slog.Info("config-watcher stopped")
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	if m.lis == nil || m.grpcSrv == nil {
		return fmt.Errorf("gRPC server not serving")
	}
	return nil
}

func (m *Module) WatchChanges(req *configwatcherv1.WatchChangesRequest, stream grpc.ServerStreamingServer[configwatcherv1.WatchChangesResponse]) error {
	capability := req.GetCapability()
	slog.Debug("config-watcher: new watch", "capability", capability)

	if err := m.sendSnapshot(stream.Context(), capability, stream); err != nil {
		return err
	}

	ch := make(chan *configwatcherv1.WatchChangesResponse, m.streamBuf())
	m.mu.Lock()
	if m.watchers[capability] == nil {
		m.watchers[capability] = make(map[chan *configwatcherv1.WatchChangesResponse]struct{})
	}
	m.watchers[capability][ch] = struct{}{}
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.watchers[capability], ch)
		if len(m.watchers[capability]) == 0 {
			delete(m.watchers, capability)
		}
		m.mu.Unlock()
		close(ch)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(evt); err != nil {
				return err
			}
		}
	}
}

func (m *Module) ReportModuleChange(ctx context.Context, req *adminv1.ReportModuleChangeRequest) (*adminv1.ReportModuleChangeResponse, error) {
	if req.GetCapability() == "" {
		return nil, status.Error(codes.InvalidArgument, "capability is required")
	}
	if req.GetEventType() == "" {
		return nil, status.Error(codes.InvalidArgument, "event_type is required")
	}

	slog.Info("config-watcher: reporting module change",
		"event_type", req.GetEventType(),
		"module_id", req.GetModuleId(),
		"capability", req.GetCapability(),
	)

	m.fanOut(&configwatcherv1.WatchChangesResponse{
		EventType:  req.GetEventType(),
		ModuleId:   req.GetModuleId(),
		Capability: req.GetCapability(),
	})

	return &adminv1.ReportModuleChangeResponse{}, nil
}

func (m *Module) fanOut(evt *configwatcherv1.WatchChangesResponse) {
	caps := []string{evt.GetCapability(), ""}
	m.mu.RLock()
	targets := make([]chan *configwatcherv1.WatchChangesResponse, 0)
	for _, capability := range caps {
		for ch := range m.watchers[capability] {
			targets = append(targets, ch)
		}
	}
	m.mu.RUnlock()

	for _, ch := range targets {
		select {
		case ch <- evt:
		case <-time.After(100 * time.Millisecond):
			slog.Warn("config-watcher: dropping event for slow watcher", "capability", evt.GetCapability())
		}
	}
}
