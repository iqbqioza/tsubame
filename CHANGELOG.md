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
- `Database::first`, `last`, `len`, and `is_empty` inspection helpers, plus
  `first_rw`, `last_rw`, `len_rw`, and `is_empty_rw` for staged transaction
  views.
- `Database::put_reserved`, `get_or_put_reserved`, and
  `get_or_put_reserved_with_flags` write values through `ReservedSpace`
  without encoding them through the value codec.
- `Database::remap_types`, `remap_key_type`, and `remap_data_type` create
  typed handles for the same underlying database using different codecs.
- `PutFlags`, `Database::put_with_flags`, and
  `Database::get_or_put_with_flags`. `NO_OVERWRITE` is supported; append and
  duplicate-data flags are reserved and return an error.
- `Database::get_or_put` inserts only when a key is absent and returns the
  existing decoded value without overwriting it. Staged puts and deletes are
  respected.
- `Database::delete_range` removes all visible entries in a range and returns
  the number deleted. A full range is handled through the same path as
  `Database::clear` after counting.
- Heed-style write-transaction iterator aliases: `iter_mut`, `range_mut`,
  `rev_iter_mut`, `rev_range_mut`, `prefix_iter_mut`, and `rev_prefix_iter_mut`.
- `Env::force_sync` flushes committed data and the environment directory while
  ensuring no write transaction is active.
- `EnvOpenOptions::max_readers` and `Env::max_readers` configure the number of
  read-only transaction slots used by multi-process readers. The limit is also
  exposed through `EnvInfo`.
- `Env::clear_stale_readers` clears stale cross-process reader slots when the
  calling writable multi-process environment is the only one open.
- `Env::real_disk_size` reports the data file's allocated bytes on disk.
- `Env::stat` reports the same structural statistics for the unnamed
  database, with `Env::non_free_pages_size` and `Env::max_key_size` helpers.
  `EnvStat` is exported for callers that name the result type.
- `Database::stat` reports page size, depth, branch/leaf/overflow page counts,
  and entry count for a database snapshot.
- `Database::get_greater_than`, `get_greater_than_or_equal_to`,
  `get_lower_than`, and `get_lower_than_or_equal_to` directional lookups.
- `Env::copy_to_path` creates a consistent backup of the committed snapshot
  into an empty directory. It copies only the committed page range and the
  comparator identity; free-list and reader sidecars are reconstructed when the
  backup is opened.
- Clearing a database now releases its old tree pages for reuse.
- `Env::compact_online` while a writable environment remains open. Active read
  transactions and database handles keep their previous mmap generation; a
  writer transaction must be idle.
- `BytesEncode::bytes_encode_into` lets integer key codecs encode into a
  caller-provided stack buffer for allocation-free point lookups.
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
- Online compaction is now available to a `multi_process` writer while
  read-only environments are open. Existing read transactions keep the old
  file generation and new read transactions remap the compacted file. The
  environment-open gate prevents new environments from mapping the old
  generation during the swap.
- A multi-process writer clears stale reader registry state when it can acquire
  the lifecycle lock exclusively, so crashed read-only processes do not block
  page reuse or online compaction indefinitely.
- Reopening a multi-process writer advances the reader generation, so existing
  read-only environments remap after a compaction interrupted before its
  generation was published.
- Cleanly closed writable environments persist their free list in a checksummed
  `free.tsubame` sidecar. A writer loads it during open when the transaction
  and page count match, avoiding both a full reachable-page scan and cache I/O
  on the first write. Stale or corrupt caches are ignored and the free list is
  reconstructed on first use.

### Performance

- CRC32C checksums are computed over the page and metadata buffers without
  copying the full page first, and write paths use a faster hardware-backed
  CRC32C implementation. Single-key commit CPU time drops substantially while
  keeping the same page checksum algorithm.
- Commits reuse a pre-zeroed meta buffer and write only the metadata prefix
  instead of a full page; single-page commits skip commit-entry vector
  construction.
- Multi-process reader registration uses shared atomic memory-mapped slots,
  removing per-transaction registry file I/O and the global registry lock from
  the read path. The writer skips the slot scan entirely when the registry is
  empty.
- Point lookups with fixed-width integer keys avoid allocating an encoded key
  buffer on every `get`.
- Staged pending keys up to 16 bytes are stored inline instead of allocating a
  `Vec` per write, and bulk loading borrows staged keys instead of first
  converting every key to an owned buffer.
- Large pending write batches stage in an unordered vector, then sort and
  deduplicate once at commit. Fixed-width keys use numeric sorting fast paths
  instead of repeated byte-slice comparisons.
- Lexicographic comparisons for 2-, 4-, 8-, 10-, and 16-byte keys use native
  integer comparisons in page searches and lookups.
- Reusable free runs are coalesced when adjacent, and the smallest larger run
  is split only when no exact-size run is available. This lets small leaf and
  branch allocations reclaim space left by large overflow values without
  sacrificing every contiguous extent.
- Small commits build inline commit descriptors directly without allocating a
  `CommitEntry` vector or cloning it. Pages written by a successful commit are
  marked verified to avoid rechecking their checksums on the next writer read.
- Writer page reads copy validated bytes directly into the owned page buffer
  instead of zero-filling the whole page first.
- Leaf and branch updates mutate the owned page buffer in place instead of
  copying the whole page a second time before writing it.
- Free-list reconstruction marks the tree pages it validated as verified, so
  the first writer after reopening does not checksum them again.
- Bulk loading builds leaf pages from borrowed entries instead of cloning
  every key and value.
- Verified page reads bypass redundant header checks, and binary-search slot
  access avoids repeated bounds checks; hot page/btree accessors are inlined.
- Leaves with changed-length values shift a packed cell block in place instead
  of rebuilding the page entry by entry, and adjacent dirty pages are written
  in batches.
- Small pending values are stored inline, removing a heap allocation per put.
- Page slots shrink from 12 to 8 bytes by packing the overflow flag into the
  high bit of the value-length word. The larger slot capacity improves point
  lookup throughput and reduces B+tree fanout pressure.
- Meta and commit-record checksums cover only the bytes that carry metadata,
  avoiding a full-page hash on every commit. Page checksums use XXH3 over the
  used page regions, skipping uncommitted free space.
- Committed pages, overflow extents, and the new meta page are written directly
  into writable shared mappings, removing per-page `pwrite` syscalls and overlay
  copies from the commit path. POSIX systems flush the mappings with one
  `fdatasync`; Windows explicitly flushes dirty mapped ranges first.
- Copy-on-write leaf and branch updates copy the old page directly into the
  writable mapping and edit it there. This removes one full-page copy at every
  tree level from the common insert and update path.
- Leaf and branch deletes shift the packed cells and slot array in place instead
  of rebuilding the page. Large delete batches are applied bottom-up so each
  affected page is rebuilt once rather than once per key.
- In-place page edits defer checksum recomputation until commit, so repeated
  changes to a transaction-owned page hash it only once. Dirty and unsealed
  page IDs use identity-hashed sets to keep the update path allocation-light.
- Large overflow values are written into their final writable-mapped extent at
  `put` time and referenced directly by the tree writer, avoiding the pending
  full-value copy as well as a temporary encoding buffer. Verified overflow
  extents return their payload without re-reading extent headers, so large-value
  point reads avoid repeated header validation.
- Forward prefix iteration stops on a prefix mismatch instead of materializing
  a successor key and comparing a full range bound for every entry.
- Ordered iteration keeps the current leaf page in the cursor instead of looking
  it up again for every entry. Range, prefix, and reverse scans avoid one page
  lookup per key.
- Range and reverse-range iterators fetch each key/value pair once per step
  instead of separately reading the key for bound checks.
- Large pending batches that exceed a byte threshold use bottom-up bulk loading
  even when their entry count is below the normal threshold.
- Large mixed write batches are applied bottom-up in one pass: each affected
  leaf is rewritten once, and branch pages are rebuilt from the resulting
  child list instead of bulk-deleting first and then inserting keys one by one.
- Tree rewrite entries keep values up to 96 bytes inline, avoiding a heap
  allocation per entry on large updates.
- Large pending delete batches are applied bottom-up before staged puts, while
  updates reuse transaction-owned pages in place. This keeps mixed update and
  delete workloads from rebuilding the same path once per key.
- Writer startup defers state and free-list setup until the first allocation,
  and transaction-owned page sets are boxed/lazily initialized where possible.
  Startup write bursts therefore avoid work not required by the transaction.

### Fixed

- Inserting a key that sorts before an existing leaf entry replaced the
  existing entry instead of inserting the new key.

### Notes

- The minimum supported Rust version is now 1.88.
- The free list is maintained in memory. Read-only reopening does not scan the
  tree; the first write transaction reconstructs the free list from the latest
  reachable state.
- The on-disk format is version 5. Earlier format versions are rejected and must
  be recreated; there is no in-place format migration yet.

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
