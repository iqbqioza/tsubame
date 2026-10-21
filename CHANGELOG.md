# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added

- Cross-transaction page reuse for long-running writers, including in
  `multi_process` mode. Pages freed by a commit become reusable after both
  recoverable meta snapshots no longer reference them and no older reader, in
  this process or another read-only process, can observe them. Readers reserve
  a shared registry slot before loading a snapshot, closing the window between
  snapshot selection and registration. Commit-record pages are recycled as
  well.
- Lazy free-list reconstruction on the first write after reopening, so pages
  freed by a previous process become reusable without a compaction.
- `get_rw`, `range_rw`, `rev_iter_rw`, `rev_range_rw`, `prefix_rw`, and
  `rev_prefix_rw` on write transactions, including staged puts and deletes.
- Clearing a database now releases its old tree pages for reuse.
- `Env::compact_online` while a writable environment remains open. Active read
  transactions and database handles keep their previous mmap generation; a
  writer transaction must be idle.
- Environment-wide custom key comparators through `KeyComparator` and
  `EnvOpenOptions::comparator`. A comparator ID is persisted in
  `comparator.tsubame` and checked on reopen and compaction; custom-order
  `Env::compact_with` is available for offline compaction.
- Read-only environments and shared read locks. Multiple read-only processes
  can share an environment while a writable environment holds the exclusive
  lock.
- `multi_process(true)` lets a writable environment coexist with read-only
  processes while retaining cross-process page reuse. Reader snapshots are
  advertised in `readers.tsubame`; offline compaction clears stale slots.

### Performance

- CRC32C checksums are computed over the page and metadata buffers without
  copying the full page first, and write paths use a faster hardware-backed
  CRC32C implementation. Single-key commit CPU time drops substantially while
  the on-disk checksum algorithm and format remain unchanged.
- Bulk loading builds leaf pages from borrowed entries instead of cloning
  every key and value.
- Verified page reads bypass redundant header checks, and binary-search slot
  access avoids repeated bounds checks; hot page/btree accessors are inlined.
- Leaves with changed-length values shift a packed cell block in place instead
  of rebuilding the page entry by entry, and adjacent dirty pages are written
  in batches.
- Small pending values are stored inline, removing a heap allocation per put.

### Fixed

- Inserting a key that sorts before an existing leaf entry replaced the
  existing entry instead of inserting the new key.

### Notes

- The minimum supported Rust version is now 1.88.
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
