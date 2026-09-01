package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"
	discoveryv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/discovery/v1"
	"github.com/Muxcore-Media/core/sdk/go/client"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) dialCore() {
	addr := strings.TrimSpace(os.Getenv("MUXCORE_GRPC_ADDR"))
	if addr == "" {
		return
	}
	var opts []client.Option
	if os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true" {
		opts = append(opts, client.WithInsecure())
	}
	c, err := client.Dial(addr, opts...)
	if err != nil {
		slog.Warn("config-watcher: dial core for registry", "error", err)
		return
	}
	m.meshMu.Lock()
	if m.mc != nil {
		_ = m.mc.Close()
	}
	m.mc = c
	m.meshMu.Unlock()
	slog.Info("config-watcher: connected to core for registry sync", "addr", addr)
}

func (m *Module) meshClient() *client.Client {
	m.meshMu.RLock()
	defer m.meshMu.RUnlock()
	return m.mc
}

func (m *Module) resolveModuleCapabilities(ctx context.Context, moduleID string) []string {
	mc := m.meshClient()
	if mc == nil {
		return nil
	}
	info, err := mc.Discovery.Resolve(ctx, moduleID)
	if err != nil || info == nil {
		return nil
	}
	return info.GetCapabilities()
}

func (m *Module) modulesForSnapshot(ctx context.Context, capability string) ([]*discoveryv1.ModuleInfoProto, error) {
	mc := m.meshClient()
	if mc == nil {
		return nil, nil
	}
	if capability == "" {
		resp, err := mc.Discovery.Raw().ListAll(ctx, &discoveryv1.ListAllRequest{})
		if err != nil {
			return nil, fmt.Errorf("ListAll: %w", err)
		}
		var modules []*discoveryv1.ModuleInfoProto
		for _, entry := range resp.GetEntries() {
			if info := entry.GetInfo(); info != nil && info.GetId() != "" {
				modules = append(modules, info)
			}
		}
		return modules, nil
	}
	modules, err := mc.Discovery.FindByCapability(ctx, capability)
	if err != nil {
		return nil, fmt.Errorf("FindByCapability: %w", err)
	}
	return modules, nil
}

func (m *Module) sendSnapshot(ctx context.Context, capability string, stream interface {
	Send(*configwatcherv1.WatchChangesResponse) error
}) error {
	modules, err := m.modulesForSnapshot(ctx, capability)
	if err != nil {
		return err
	}
	return m.emitSnapshotDirect(ctx, capability, modules, stream)
}

func (m *Module) emitSnapshotDirect(ctx context.Context, capability string, modules []*discoveryv1.ModuleInfoProto, stream interface {
	Send(*configwatcherv1.WatchChangesResponse) error
}) error {
	_ = ctx
	for _, info := range modules {
		moduleID := info.GetId()
		caps := info.GetCapabilities()
		m.rememberModuleCapabilities(moduleID, caps)
		if capability == "" {
			for _, cap := range caps {
				if err := stream.Send(&configwatcherv1.WatchChangesResponse{
					EventType:  contracts.EventModuleRegistered,
					ModuleId:   moduleID,
					Capability: cap,
				}); err != nil {
					return err
				}
			}
			continue
		}
		if err := stream.Send(&configwatcherv1.WatchChangesResponse{
			EventType:  contracts.EventModuleRegistered,
			ModuleId:   moduleID,
			Capability: capability,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) bootstrapRegistryCache(ctx context.Context) {
	modules, err := m.modulesForSnapshot(ctx, "")
	if err != nil {
		slog.Warn("config-watcher: bootstrap registry cache", "error", err)
		return
	}
	for _, info := range modules {
		m.rememberModuleCapabilities(info.GetId(), info.GetCapabilities())
	}
	if len(modules) > 0 {
		slog.Info("config-watcher: registry cache bootstrapped", "modules", len(modules))
	}
}
