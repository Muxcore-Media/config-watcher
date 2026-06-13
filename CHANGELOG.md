# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial project scaffold from muxcore-module-starter
- Module with ConfigWatcherService.WatchChanges (server-streaming)
- Module with ConfigWatcherAdminService.ReportModuleChange (unary admin)
- Per-capacity watcher fan-out with thread-safe channel dispatch
- Comprehensive test suite (info, lifecycle, watch-then-report, multiple watchers)
