# Config Watcher

[![Go Version](https://img.shields.io/badge/Go-1.26-blue)](https://go.dev/)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)

**Notifies modules when infrastructure services change at runtime.**

A MuxCore sidecar module that provides the `config.watcher` capability. Modules subscribe by capability string via `WatchChanges` (server-streaming gRPC) and receive events when a matching module registers or unregisters. Events are pushed by core or other modules through the `ReportModuleChange` admin RPC.

---

## How It Works

```
Module ──→ WatchChanges(stream) ──→ config-watcher
                                         ↑
Core/Admin ──→ ReportModuleChange ────────┘
```

### WatchChanges (server-streaming)

Clients call `WatchChanges` with a capability string. The server keeps the stream open and pushes `WatchChangesResponse` events as they arrive.

### ReportModuleChange (unary admin)

Core or another module calls `ReportModuleChange` with an event type, module ID, and capability. The module fans out the event to all watchers matching that capability (plus wildcard watchers watching `""`).

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_WATCHER_GRPC_ADDR` | `:9611` | gRPC listen address |
| `MUXCORE_GRPC_ADDR` | — | Core mesh address |
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

```bash
make docker
docker run -d --restart=unless-stopped \
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
- Declares contract `ConfigWatcher` (`v0.4.0`); serves gRPC (not in-process `OnChange`)
- Exposes two gRPC services:
  - `ConfigWatcherService.WatchChanges` (server-streaming)
  - `ConfigWatcherAdminService.ReportModuleChange` (unary)
- Event fan-out per capability with non-blocking channel sends

---

## License

GPL-3.0
