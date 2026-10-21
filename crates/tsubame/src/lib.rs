//! Tsubame is a typed, embedded key-value store for Rust, inspired by [`heed`].
//!
//! The `tsubame` crate exposes environments, typed databases, and explicit
//! read/write transactions. Keys and values are described by codec types from
//! [`types`] or by implementations of [`traits::BytesEncode`] and
//! [`traits::BytesDecode`].
//!
//! # Status
//!
//! The environment lifecycle, the on-disk meta format, the B+tree read and
//! write paths, transactions, and typed database operations are implemented.
//! Named databases, cursors, and iteration are still under construction. APIs
//! may change while the engine is being built.
//!
//! # Example
//!
//! The following example shows the intended API. It compiles, but does not run
//! until the storage engine lands.
//!
//! ```no_run
//! use tsubame::byteorder::NativeEndian;
//! use tsubame::types::{Str, U32};
//! use tsubame::{EnvOpenOptions, Result};
//!
//! # fn main() -> Result<()> {
//! let env = EnvOpenOptions::new().open("/tmp/tsubame")?;
//!
//! let mut wtxn = env.write_txn()?;
//! let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, None)?;
//! db.put(&mut wtxn, "alice", &42)?;
//! wtxn.commit()?;
//!
//! let rtxn = env.read_txn()?;
//! assert_eq!(db.get(&rtxn, "alice")?, Some(42));
//! # Ok(())
//! # }
//! ```
//!
//! [`heed`]: https://github.com/meilisearch/heed

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod cursor;
pub mod database;
pub mod env;
pub mod error;
pub mod iter;
mod storage;
pub mod txn;

pub use self::database::Database;
pub use self::env::{DEFAULT_MAP_SIZE, DEFAULT_MAX_DBS, Env, EnvInfo, EnvOpenOptions};
pub use self::error::{Error, Result};
pub use self::txn::{RoTxn, RwTxn};

/// The [`byteorder`] crate, re-exported for codecs such as [`types::U32`].
pub use byteorder;

/// Encoding, decoding, and comparison traits, re-exported from `tsubame-traits`.
pub use tsubame_traits as traits;

/// Built-in codecs, re-exported from `tsubame-types`.
pub use tsubame_types as types;
