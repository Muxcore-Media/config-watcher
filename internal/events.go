package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	configwatcherv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/configwatcher/v1"
	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
)

func (m *Module) subscribeRegistryEvents(ctx context.Context) {
	if m.mc == nil {
		return
	}

	type stream struct { //nolint:govet // struct field alignment
		eventType string
		ch        <-chan *eventsv1.Event
		cancel    context.CancelFunc
	}
	var streams []stream
	defer func() {
		for _, s := range streams {
			s.cancel()
		}
	}()

	for _, et := range []string{contracts.EventModuleRegistered, contracts.EventModuleUnregistered} {
		ch, cancel, err := m.mc.Events.Subscribe(ctx, et)
		if err != nil {
			slog.Warn("config-watcher: subscribe registry events", "type", et, "error", err)
			return
		}
		streams = append(streams, stream{eventType: et, ch: ch, cancel: cancel})
		slog.Info("config-watcher: subscribed for registry events", "type", et)
	}

	errCh := make(chan error, len(streams))
	for _, s := range streams {
		go func(st stream) {
			errCh <- m.handleRegistryEventStream(ctx, st.eventType, st.ch)
		}(s)
	}

	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		if err != nil && ctx.Err() == nil {
			slog.Warn("config-watcher: registry event stream ended", "error", err)
		}
	}
}

func (m *Module) handleRegistryEventStream(ctx context.Context, eventType string, ch <-chan *eventsv1.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-ch:
			if !ok {
				return fmt.Errorf("registry event stream closed: %s", eventType)
			}
			switch eventType {
			case contracts.EventModuleRegistered:
				m.applyModuleRegistered(ctx, evt)
			case contracts.EventModuleUnregistered:
				m.applyModuleUnregistered(ctx, evt)
			}
		}
	}
}

func (m *Module) applyModuleRegistered(ctx context.Context, evt *eventsv1.Event) {
	moduleID := moduleIDFromEvent(evt)
	if moduleID == "" {
		return
	}
	caps := m.resolveModuleCapabilities(ctx, moduleID)
	m.rememberModuleCapabilities(moduleID, caps)
	for _, cap := range caps {
		m.fanOut(&configwatcherv1.WatchChangesResponse{
			EventType:  contracts.EventModuleRegistered,
			ModuleId:   moduleID,
			Capability: cap,
		})
	}
}

func (m *Module) applyModuleUnregistered(ctx context.Context, evt *eventsv1.Event) {
	moduleID := moduleIDFromEvent(evt)
	if moduleID == "" {
		return
	}
	caps := m.forgetModuleCapabilities(moduleID)
	if len(caps) == 0 {
		caps = m.resolveModuleCapabilities(ctx, moduleID)
	}
	for _, cap := range caps {
		m.fanOut(&configwatcherv1.WatchChangesResponse{
			EventType:  contracts.EventModuleUnregistered,
			ModuleId:   moduleID,
			Capability: cap,
		})
	}
}

func moduleIDFromEvent(evt *eventsv1.Event) string {
	if evt == nil {
		return ""
	}
	switch evt.GetType() {
	case contracts.EventModuleRegistered:
		var payload contracts.ModuleRegisteredPayload
		if len(evt.Payload) > 0 {
			_ = json.Unmarshal(evt.Payload, &payload)
		}
		if id := strings.TrimSpace(payload.ModuleID); id != "" {
			return id
		}
	case contracts.EventModuleUnregistered:
		var payload contracts.ModuleUnregisteredPayload
		if len(evt.Payload) > 0 {
			_ = json.Unmarshal(evt.Payload, &payload)
		}
		if id := strings.TrimSpace(payload.ModuleID); id != "" {
			return id
		}
	}
	return strings.TrimSpace(evt.Source)
}

func (m *Module) rememberModuleCapabilities(moduleID string, caps []string) {
	if moduleID == "" {
		return
	}
	cp := append([]string(nil), caps...)
	m.registryMu.Lock()
	if m.moduleCaps == nil {
		m.moduleCaps = make(map[string][]string)
	}
	m.moduleCaps[moduleID] = cp
	m.registryMu.Unlock()
}

func (m *Module) forgetModuleCapabilities(moduleID string) []string {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()
	caps := append([]string(nil), m.moduleCaps[moduleID]...)
	delete(m.moduleCaps, moduleID)
	return caps
}

func registrySyncDelay() time.Duration {
	delay := 2 * time.Second
	if v := strings.TrimSpace(os.Getenv("CONFIG_WATCHER_REGISTRY_SYNC_DELAY")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			delay = d
		}
	}
	return delay
}
