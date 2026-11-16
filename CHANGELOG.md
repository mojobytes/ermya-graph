# Changelog

All notable changes to ErmyaGraph Community are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.13.4] - 2026-11-16

- Preserve committed MVCC versions through checkpoints, failed flushes and shutdown.
- Preserve old-reader property blobs and reject inconsistent free-list references.
- Rebuild label indexes after WAL recovery and retain sparse record IDs on reopen.
- Report missing property blobs and propagate shutdown persistence errors.
- Add bounded offline recovery with provenance and explicit historical-data decisions.
- Test acknowledged writes across graceful shutdown, process death and repeated reopen.

## [0.13.3] - 2026-11-16

- Bind and authorize databases on Bolt BEGIN, including the Community default.
- Support parameters in node and relationship MATCH property maps for reads and mutations.
- Reject database switches during transactions and preserve rollback state.
- Test official sync/async drivers and Python 3.15.2 in CI.

## [0.13.2] - 2026-11-16

- Enforce UNIQUE and write/write conflicts atomically at transaction commit.
- Fix MVCC deletion before materialization and historical-index vacuum cleanup.
- Return explicit Bolt constraint/conflict errors and release rejected transactions.
- Update TLS dependencies and add official-driver concurrency regressions.
- Provide a versioned Community container build with source revision metadata.

## [0.13.1] - 2026-08-23

### Changed

- Completed the rebrand from Tessera to ErmyaGraph across code, packages,
  documentation, automation and published metadata.

## [0.13.0] - 2026-08-09

### Changed

- Split licensing by deliverable: the embeddable core and Python bindings use
  MIT; the server-side Community components use BSL 1.1 and convert to
  Apache-2.0 after four years.
- Updated the minimum supported Rust version to 1.88.
- Migrated the Python bindings to PyO3 0.29.
- Replaced absolute-duration performance assertions with relative baselines.

### Security

- Updated vulnerable transitive dependencies reported by `cargo audit`.
- Replaced the deprecated `rustls-pemfile` parser with rustls PKI types.

### Operations

- Added Rust, Python, security-audit and container CI gates.
- Added reproducible Python wheel builds and tagged GitHub release automation.

[Unreleased]: https://github.com/mojobytes/ermya-graph/compare/v0.13.2...HEAD
[0.13.1]: https://github.com/mojobytes/ermya-graph/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/mojobytes/ermya-graph/releases/tag/v0.13.0

[0.13.2]: https://github.com/mojobytes/ermya-graph/compare/v0.13.1...v0.13.2
