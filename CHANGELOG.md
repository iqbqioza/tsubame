# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added

- Cross-transaction page reuse for long-running writers. Pages freed by a
  commit become reusable after both recoverable meta snapshots no longer
  reference them and no older reader can observe them. Commit-record pages are
  recycled as well.
- Clearing a database now releases its old tree pages for reuse.

### Fixed

- Inserting a key that sorts before an existing leaf entry replaced the
  existing entry instead of inserting the new key.

### Notes

- The free list is maintained in memory. Reopening an environment starts with
  an empty free list; run `Env::compact` to reclaim pages left free by a
  previous process.

## [0.1.0] - 2026-10-21

### Added

- Typed embedded key-value store with `Env`, `Database`, `RoTxn`, and `RwTxn`.
- Copy-on-write B+tree with leaf and branch splits, deletes, and contiguous
  overflow extents for large values.
- Crash-safe commit protocol with a single `fdatasync` and validated
  commit-intent records.
- Snapshot-isolated readers and a single writer.
- Named databases backed by a catalog tree.
- `Bytes`, `Str`, `Unit`, and fixed-width integer codecs.
- Optional `SerdeJson<T>` and `SerdeBincode<T>` codecs behind the
  `serde-json` and `serde-bincode` features.
- Forward, reverse, range, prefix, and write-transaction iteration.
- Offline compaction through `Env::compact(path)`.
- Standalone `benchmarks` crate comparing Tsubame with heed.

### Notes

- The on-disk format is version 2.
- One process per environment; the file lock is exclusive.
- Cross-transaction page reuse is not implemented; offline compaction is
  available through `Env::compact`.
- APIs may change before 1.0.
