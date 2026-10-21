# Tsubame

[![CI](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml/badge.svg)](https://github.com/iqbqioza/tsubame/actions/workflows/ci.yml)

**Tsubame** (`tsubame` on crates.io) is a typed, embedded key-value store for Rust,
inspired by [heed].

> [!WARNING]
> Tsubame is under construction. The workspace layout, public API surface,
> encoding traits, built-in codecs, environment format, and B+tree read path
> exist, but transactions and B+tree writes are not implemented yet. Opening
> an environment works; starting a transaction currently returns an error.

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
- [ ] B+tree writes, transactions, and typed database operations
- [ ] Read/write transactions and cursors
- [ ] Ordered, range, prefix, and reverse iterators
- [ ] Serde-based codecs behind features

## Target API

The following is the intended shape of the API. It does not run until the
storage engine lands.

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

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

[heed]: https://github.com/meilisearch/heed
