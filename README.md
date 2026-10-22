# Tsubame

[![CI](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml/badge.svg)](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml)

**Tsubame** (`tsubame` on crates.io) is a typed, embedded key-value store for Rust,
inspired by [heed].

> [!WARNING]
> Tsubame is pre-1.0. The current codebase includes the environment format,
> B+tree, transactions, named databases, crash-safe commit records, ordered
> iteration, serde codecs, generation-tagged page reuse across processes,
> online compaction, and offline compaction. APIs may still change before 1.0.

## Why Tsubame

[heed] provides a typed Rust API on top of LMDB. Tsubame aims for the same
ergonomics — typed databases, explicit read/write transactions, zero-copy
codecs, and ordered iteration — while keeping the storage engine under our
control.

## Workspace layout

| Crate | Purpose |
| --- | --- |
| [`tsubame`](crates/tsubame) | Environments, typed databases, transactions, and iteration |
| [`tsubame-traits`](crates/tsubame-traits) | `BytesEncode`, `BytesDecode`, and key comparison traits |
| [`tsubame-types`](crates/tsubame-types) | Built-in codecs such as `Str`, `Bytes`, `Unit`, and byte-ordered integers |

The traits and codecs are separate crates so applications can use them
without depending on the storage engine, and so additional codec crates can be
added without changing the main API.

## Status

- [x] Cargo workspace with publishable crate metadata
- [x] Encoding, decoding, and comparator traits
- [x] `Bytes`, `Str`, `Unit`, and integer codecs
- [x] Public API skeleton for environments, databases, and transactions
- [x] On-disk meta format and environment lifecycle
- [x] B+tree page format and read path
- [x] B+tree writes, splits, deletes, and overflow extents
- [x] Read/write transactions and crash-safe commits
- [x] Single-fsync durable commits with validated commit records
- [x] Typed database operations on the unnamed database
- [x] Named databases and catalog
- [x] Ordered forward iteration
- [x] Range, prefix, and reverse iterators
- [x] Write transaction reads and iteration with pending writes
- [x] Serde-based codecs behind features
- [x] Cross-transaction page reuse within and across processes
- [x] Coalescing and splitting of reusable free extents
- [x] Lazy free-list reconstruction after reopening
- [x] Read-only environments with shared locks
- [x] Concurrent writer and read-only processes (`multi_process`)
- [x] Atomic cross-process reader registration
- [x] Custom key comparators
- [x] Online compaction while an environment remains open
- [x] Multi-process online compaction with reader generation remapping
- [x] Offline compaction
- [x] Consistent environment backup with `Env::copy_to_path`
- [x] Database structural statistics
- [x] Heed-style write-transaction iterator aliases and `Env::force_sync`
- [x] Range deletion with `Database::delete_range`

## Codec features

`tsubame` and `tsubame-types` expose optional serde codecs:

```toml
tsubame = { version = "0.1", features = ["serde-json"] }
# or
tsubame = { version = "0.1", features = ["serde-bincode"] }
```

| Feature | Codec |
| --- | --- |
| `serde-json` | `types::SerdeJson<T>` |
| `serde-bincode` | `types::SerdeBincode<T>` |

Bincode is compact but not self-describing; take care when evolving stored
schemas.

## Quick start

The following example uses a named database.

```rust
use tsubame::byteorder::NativeEndian;
use tsubame::types::{Str, U32};
use tsubame::{EnvOpenOptions, Result};

fn main() -> Result<()> {
    let env = EnvOpenOptions::new().open("/tmp/tsubame")?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, Some("scores"))?;
    db.put(&mut wtxn, "alice", &42)?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "alice")?, Some(42));

    Ok(())
}
```

## Custom key order

The default order is lexicographic byte order. `KeyComparator` can be supplied
to every database in an environment:

```rust
use std::cmp::Ordering;
use tsubame::{EnvOpenOptions, KeyComparator};

fn reverse(left: &[u8], right: &[u8]) -> Ordering {
    right.cmp(left)
}

fn main() -> tsubame::Result<()> {
    let mut options = EnvOpenOptions::new();
    options
        .comparator(KeyComparator::custom(reverse))
        .comparator_id("reverse-v1");
    let env = options.open("/tmp/tsubame-reverse")?;
    Ok(())
}
```

The comparator identifier is persisted in `comparator.tsubame`. The same ID
must be supplied on every reopen and to `Env::compact_with`.

## Examples

```sh
cargo run --example basic
cargo run --example serde_json --features serde-json
```

The examples use a temporary directory and clean up after themselves.

## Limitations

- A writable environment normally holds an exclusive file lock. Multiple
  read-only environments use a shared lock and can share the same data file.
  A writable environment can coexist with read-only processes when opened with
  `multi_process(true)`. Readers advertise their snapshots through
  `readers.tsubame`, and the writer only recycles pages that no advertised
  snapshot can still need. The registry supports up to 4096 simultaneous
  cross-process readers. Stale slots left by a crashed reader conservatively
  block reuse while another environment remains open; a multi-process writer
  clears them once it can acquire the lifecycle lock exclusively, and offline
  compaction always clears them. Read-only environments therefore need write
  access to their directory for the registry sidecar.
- Freed pages are reused across transactions once the two recoverable meta
  snapshots no longer reference them and no active reader, in any process, can
  still observe them. Readers reserve a registry slot before loading a
  snapshot, so a writer cannot recycle a page during the snapshot/registration
  window. This keeps long-running update workloads from growing the file
  indefinitely. Commit-record pages are recycled with one additional
  generation of delay.
- The free list is in memory. Cleanly closed writable environments persist it in
  `free.tsubame`; the next writer validates the cache against the committed
  transaction and page count, then falls back to scanning reachable pages after
  an unclean shutdown or corrupt cache. Read-only reopening does not scan the
  tree. Offline compaction is still available:
  ```rust
  Env::compact("/path/to/env")?;
  ```
  The environment must be closed while offline compaction runs.
- `Env::compact_online` keeps an open writable environment mapped to the
  compacted file. Read transactions and `Database` handles may remain active;
  existing readers keep their previous file generation. A write transaction
  must not be active. A `multi_process` writer may compact while read-only
  environments are open; their existing transactions keep the old generation,
  and their next read transaction remaps the compacted file. Read-only
  environments cannot initiate compaction. A writer restart also advances the
  generation so readers recover from an interrupted compaction.
- Custom key comparators are environment-wide and must be supplied again on
  every open and offline compaction. A stable comparator ID is persisted in
  `comparator.tsubame` and must match. Prefix iteration requires lexicographic
  order.
- Writable environments preallocate the data file to `map_size` so committed
  pages can be written through a writable mapping. The file is sparse on Unix,
  but tools that copy the file may copy its logical size rather than its used
  pages.
- The on-disk format is version 5. Earlier format versions are rejected and are
  not migrated in place.
- Performance is benchmarked rather than assumed; see [`benchmarks`](benchmarks).

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo package --workspace --no-verify
```

## Publishing

The crates are published in dependency order:

```sh
cargo package --workspace --no-verify
cargo publish -p tsubame-traits
cargo publish -p tsubame-types
cargo publish -p tsubame
```

Tarball verification is deferred until lower-level crates are published,
because the higher-level crates may use APIs not yet present in the published
versions.

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## Benchmarks

The standalone [`benchmarks`](benchmarks) crate compares Tsubame with heed on
batch inserts, point reads, forward/reverse/prefix scans, large values, durable
commit latency, startup writes, bulk deletes, and reopen time:

```sh
cd benchmarks
cargo run --release
```

It is excluded from the workspace so heed and its LMDB build are not part of
the published crates or CI.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

[heed]: https://github.com/meilisearch/heed
