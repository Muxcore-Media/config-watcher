# Config Watcher

[![Go Version](https://img.shields.io/badge/Go-1.26-blue)](https://go.dev/)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)

**Notifies modules when infrastructure services change at runtime.**

A MuxCore sidecar module that provides the `config.watcher` capability. Modules subscribe by capability string via `WatchChanges` (server-streaming gRPC) and receive events when a matching module registers or unregisters. The module subscribes to core `module.registered` / `module.unregistered` events and fans out internally; `ReportModuleChange` remains available for trusted mesh callers.

---

## How It Works

```
Module ──→ WatchChanges(stream) ──→ config-watcher
                                         ↑
Core events ──→ module.registered ───────┤
              module.unregistered ────────┘
Mesh/admin ──→ ReportModuleChange ────────┘ (authenticated)
```

### WatchChanges (server-streaming)

Clients call `WatchChanges` with a capability string. On subscribe the server emits a snapshot of currently registered modules matching that capability (registry lookup), then keeps the stream open and pushes live events.

Use `pkg/client` for the documented `contracts.ConfigWatcher.OnChange` interface over gRPC.

### ReportModuleChange (unary admin)

Authenticated mesh callers may push synthetic events. Requires `x-caller-id` mesh metadata or `CONFIG_WATCHER_MODULE_TOKEN` / `MUXCORE_MODULE_TOKEN` bearer auth.

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_WATCHER_GRPC_ADDR` | `:9614` | gRPC listen address |
| `CONFIG_WATCHER_STREAM_BUFFER` | `16` | Per-watcher channel buffer |
| `CONFIG_WATCHER_REGISTRY_SYNC_DELAY` | `2s` | Delay before subscribing to core registry events |
| `CONFIG_WATCHER_MODULE_TOKEN` | — | Bearer token for `ReportModuleChange` |
| `MUXCORE_GRPC_ADDR` | — | Core mesh address (registry + events) |
| `MUXCORE_MODULE_ID` | `config-watcher` | Module identity |
| `MUXCORE_INSECURE_DISABLE_TLS` | — | Disable TLS to core (`true`, dev only) |

---

## Quick Start

```bash
# Build
make build

# Run against local core (dev mode)
export MUXCORE_INSECURE_DISABLE_TLS=true
./config-watcher --muxcore-mesh-addr localhost:9090
```

---

## Deployment

### Docker

Build from the umbrella workspace root (needs sibling `core/`):

```bash
make docker
docker run -d --restart=unless-stopped \
  -e CONFIG_WATCHER_GRPC_ADDR=:9614 \
  -p 9614:9614 \
  -e MUXCORE_GRPC_ADDR=core:9090 \
  ghcr.io/muxcore-media/config-watcher:latest
```

### docker-compose

```bash
docker compose -f deploy/docker-compose.yml up
```

### systemd

```bash
sudo cp deploy/systemd/muxcore-config-watcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now muxcore-config-watcher
```

---

## Development

```bash
make build   # compile
make test    # run tests
make lint    # golangci-lint
make fmt     # format code
make proto   # regenerate admin proto
```

---

## Implementation

- Registers with capability: `"config.watcher"`
- Declares contract `ConfigWatcher` (`v0.4.0`); remote client in `pkg/client`
- Exposes two gRPC services:
  - `ConfigWatcherService.WatchChanges` (server-streaming)
  - `ConfigWatcherAdminService.ReportModuleChange` (unary, authenticated)
- Subscribes to core registry events and emits capability snapshots for late joiners
- Event fan-out per capability with non-blocking channel sends

---

## License

GPL-3.0
