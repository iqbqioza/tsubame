# Tsubame

[![CI](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml/badge.svg)](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml)

**Tsubame** (`tsubame` on crates.io) is a typed, embedded key-value store for Rust,
inspired by [heed].

> [!WARNING]
> Tsubame is pre-1.0. The current codebase includes the environment format,
> B+tree, transactions, named databases, crash-safe commit records, ordered
> iteration, serde codecs, in-process page reuse, and offline compaction. APIs
> may still change before 1.0.

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
- [x] Write transaction iteration with pending writes
- [x] Serde-based codecs behind features
- [x] Cross-transaction page reuse within a process
- [x] Lazy free-list reconstruction after reopening
- [x] Offline compaction

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

## Examples

```sh
cargo run --example basic
cargo run --example serde_json --features serde-json
```

The examples use a temporary directory and clean up after themselves.

## Limitations

- One process per environment; the file lock is exclusive.
- Freed pages are reused across transactions once the two recoverable meta
  snapshots no longer reference them and no active reader can still observe
  them. This keeps long-running update workloads from growing the file
  indefinitely. Commit-record pages are recycled with one additional
  generation of delay.
- The free list is in memory. The first write transaction after reopening
  rebuilds it by scanning pages reachable from the latest commit, so pages
  freed by a previous process become reusable without waiting for compaction.
  Read-only reopening does not scan the tree. Offline compaction is still
  available:
  ```rust
  Env::compact("/path/to/env")?;
  ```
  The environment must be closed while compaction runs.
- Custom key comparators are not exposed yet.
- Performance is benchmarked rather than assumed; see [`benchmarks`](benchmarks).

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
```

## Publishing

The crates are published in dependency order:

```sh
cargo package --workspace
cargo publish -p tsubame-traits
cargo publish -p tsubame-types
cargo publish -p tsubame
```

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## Benchmarks

The standalone [`benchmarks`](benchmarks) crate compares Tsubame with heed on
batch inserts, point reads, durable commit latency, and reopen time:

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
