# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added

- Cross-transaction page reuse for long-running writers. Pages freed by a
  commit become reusable after both recoverable meta snapshots no longer
  reference them and no older reader can observe them. Commit-record pages are
  recycled as well.
- Lazy free-list reconstruction on the first write after reopening, so pages
  freed by a previous process become reusable without a compaction.
- `get_rw`, `range_rw`, `rev_iter_rw`, `rev_range_rw`, `prefix_rw`, and
  `rev_prefix_rw` on write transactions, including staged puts and deletes.
- Clearing a database now releases its old tree pages for reuse.

### Performance

- Bulk loading builds leaf pages from borrowed entries instead of cloning
  every key and value.
- Verified page reads skip redundant validation, and hot page/btree accessors
  are inlined.
- Leaves with changed-length values are rebuilt from borrowed slots, and
  adjacent dirty pages are written in batches.

### Fixed

- Inserting a key that sorts before an existing leaf entry replaced the
  existing entry instead of inserting the new key.

### Notes

- The free list is maintained in memory. Read-only reopening does not scan the
  tree; the first write transaction reconstructs the free list from the latest
  reachable state.

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
