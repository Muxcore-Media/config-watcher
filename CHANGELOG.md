# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0/).

## [0.1.5] - 2026-10-05

### Changed
- Built on core v0.6.14 / sdk/go/module v0.6.4: unregisters on shutdown and re-registers after core restarts (ADR-0022).

## [0.1.4] - 2026-10-05


### Changed
- Reported version comes from muxcore.json (ADR-0021); built on core v0.6.12 / sdk/go/module v0.6.3 (mesh enrollment, ADR-0017).

## [0.1.2] - 2026-10-05

### Changed
- CI runs on GitHub-hosted runners from the umbrella template; retired-origin workflows removed.
- Dependencies resolve from published GitHub tags (no filesystem `replace`); requires core v0.6.0.

### Changed

- Default gRPC listen bind is loopback (`127.0.0.1:9611`); set `CONFIG_WATCHER_GRPC_ADDR` to `:9611` or `0.0.0.0:9611` for all interfaces

## [0.1.1] — 2026-08-10

### Added

- SettingsProvider for `stream_buffer` (`CONFIG_WATCHER_STREAM_BUFFER`, default 16)
- Advertises `settings` capability for admin-ui discovery

## [0.1.0] — 2026-08-09

### Added

- `config.watcher` sidecar: `ConfigWatcherService.WatchChanges` (server-streaming) and `ConfigWatcherAdminService.ReportModuleChange`.
- Per-capability watcher fan-out with thread-safe channel dispatch (wildcard `""` supported).
- Unit coverage: info, lifecycle, watch-then-report, multiple watchers.
