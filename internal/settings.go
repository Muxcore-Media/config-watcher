package internal

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	return m.settingsDefs()
}

func (m *Module) UpdateSetting(key, value string) error {
	return m.updateSetting(key, value)
}

func (m *Module) streamBuf() int {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	if m.streamBuffer <= 0 {
		return defaultStreamBuffer
	}
	return m.streamBuffer
}

func (m *Module) settingsDefs() []contracts.SettingDef {
	buf := m.streamBuf()
	return []contracts.SettingDef{
		{
			Key:         "stream_buffer",
			Label:       "Watch Stream Buffer",
			Type:        contracts.SettingTypeInt,
			Value:       strconv.Itoa(buf),
			Default:     strconv.Itoa(defaultStreamBuffer),
			Description: "Per-watcher channel buffer size (CONFIG_WATCHER_STREAM_BUFFER); applies to new watches",
			Group:       "Streaming",
		},
	}
}

func (m *Module) updateSetting(key, value string) error {
	value = strings.TrimSpace(value)
	switch key {
	case "stream_buffer", "CONFIG_WATCHER_STREAM_BUFFER":
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return fmt.Errorf("stream_buffer must be a positive integer")
		}
		m.cfgMu.Lock()
		m.streamBuffer = n
		m.cfgMu.Unlock()
		return nil
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
}
