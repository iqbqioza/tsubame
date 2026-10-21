# Tsubame

[![CI](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml/badge.svg)](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml)

**Tsubame** (`tsubame` on crates.io) is a typed, embedded key-value store for Rust,
inspired by [heed].

> [!WARNING]
> Tsubame is under construction. The environment format, B+tree, transactions,
> and typed operations on the unnamed database work. Named databases, cursors,
> and iteration are not implemented yet.

## Why Tsubame

[heed] provides a typed Rust API on top of LMDB. Tsubame aims for the same
ergonomics — typed databases, explicit read/write transactions, zero-copy
codecs, and ordered iteration — while keeping the storage engine under our
control.

## Workspace layout

| Crate | Purpose |
| --- | --- |
| [`tsubame`](crates/tsubame) | Environments, typed databases, transactions, cursors, and iteration |
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
- [ ] Named databases and catalog
- [ ] Cursors and iterators
- [ ] Ordered, range, prefix, and reverse iterators
- [ ] Serde-based codecs behind features

## Target API

The following example uses the unnamed database. Named databases are not
implemented yet.

```rust
use tsubame::byteorder::NativeEndian;
use tsubame::types::{Str, U32};
use tsubame::{EnvOpenOptions, Result};

fn main() -> Result<()> {
    let env = EnvOpenOptions::new().open("/tmp/tsubame")?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, None)?;
    db.put(&mut wtxn, "alice", &42)?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "alice")?, Some(42));

    Ok(())
}
```

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
cargo publish -p tsubame-traits
cargo publish -p tsubame-types
cargo publish -p tsubame
```

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
