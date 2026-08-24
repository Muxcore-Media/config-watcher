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
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"

	"github.com/Muxcore-Media/config-watcher/internal/adminv1"
)

const defaultStreamBuffer = 16

type Module struct {
	configwatcherv1.UnimplementedConfigWatcherServiceServer
	adminv1.UnimplementedConfigWatcherAdminServiceServer

	watchers map[string]map[chan *configwatcherv1.WatchChangesResponse]struct{}
	grpcSrv  *grpc.Server
	lis      net.Listener

	id       string
	grpcAddr string

	streamBuffer int

	cfgMu sync.RWMutex
	mu    sync.RWMutex
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
		streamBuffer: buf,
		watchers:     make(map[string]map[chan *configwatcherv1.WatchChangesResponse]struct{}),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           m.id,
		Name:         "Config Watcher",
		Version:      "0.1.1",
		Roles:        []string{},
		Description:  "Notifies modules when infrastructure services change at runtime.",
		Author:       "MuxCore Media",
		Capabilities: []string{"config.watcher", "settings"},
		Contracts: []contracts.ContractDeclaration{
			{Repo: "github.com/Muxcore-Media/core/pkg/contracts", Interface: "ConfigWatcher", Version: "v0.4.0"},
		},
		MinCoreVersion: "0.4.0",
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
	m.grpcSrv = grpc.NewServer()
	configwatcherv1.RegisterConfigWatcherServiceServer(m.grpcSrv, m)
	adminv1.RegisterConfigWatcherAdminServiceServer(m.grpcSrv, m)
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)
	go func() {
		slog.Info("config-watcher gRPC started", "addr", m.grpcAddr)
		if err := m.grpcSrv.Serve(m.lis); err != nil {
			slog.Error("config-watcher gRPC error", "error", err)
		}
	}()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	slog.Info("config-watcher stopped")
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	return nil
}

func (m *Module) WatchChanges(req *configwatcherv1.WatchChangesRequest, stream grpc.ServerStreamingServer[configwatcherv1.WatchChangesResponse]) error {
	capability := req.GetCapability()
	slog.Debug("config-watcher: new watch", "capability", capability)

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

	evt := &configwatcherv1.WatchChangesResponse{
		EventType:  req.GetEventType(),
		ModuleId:   req.GetModuleId(),
		Capability: req.GetCapability(),
	}

	caps := []string{req.GetCapability(), ""}
	m.mu.RLock()
	for _, capability := range caps {
		watchers, ok := m.watchers[capability]
		if !ok {
			continue
		}
		for ch := range watchers {
			select {
			case ch <- evt:
			case <-time.After(100 * time.Millisecond):
				slog.Warn("config-watcher: dropping event for slow watcher", "capability", capability)
			}
		}
	}
	m.mu.RUnlock()

	return &adminv1.ReportModuleChangeResponse{}, nil
}
