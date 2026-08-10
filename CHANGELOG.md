# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0/).

## [0.1.1] — 2026-08-10

### Added

- SettingsProvider for `stream_buffer` (`CONFIG_WATCHER_STREAM_BUFFER`, default 16)
- Advertises `settings` capability for admin-ui discovery

## [0.1.0] — 2026-08-09

### Added

- `config.watcher` sidecar: `ConfigWatcherService.WatchChanges` (server-streaming) and `ConfigWatcherAdminService.ReportModuleChange`.
- Per-capability watcher fan-out with thread-safe channel dispatch (wildcard `""` supported).
- Unit coverage: info, lifecycle, watch-then-report, multiple watchers.
