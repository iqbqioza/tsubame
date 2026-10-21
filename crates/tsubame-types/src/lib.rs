//! Built-in codecs for the `tsubame` typed key-value store.
//!
//! A codec is a pair of types implementing [`tsubame_traits::BytesEncode`] and
//! [`tsubame_traits::BytesDecode`]. The types in this crate cover the common
//! cases: raw bytes, UTF-8 strings, the unit type, and fixed-width integers
//! with an explicit byte order.
//!
//! Optional `serde-json` and `serde-bincode` features add `SerdeJson` and
//! `SerdeBincode` codecs for serde-serializable values.

mod bytes;
mod integer;
#[cfg(feature = "serde-bincode")]
mod serde_bincode;
#[cfg(feature = "serde-json")]
mod serde_json;
mod str;
mod unit;

pub use self::bytes::Bytes;
pub use self::integer::{I8, I16, I32, I64, I128, U8, U16, U32, U64, U128};
#[cfg(feature = "serde-bincode")]
pub use self::serde_bincode::SerdeBincode;
#[cfg(feature = "serde-json")]
pub use self::serde_json::SerdeJson;
pub use self::str::Str;
pub use self::unit::{NonEmptyError, Unit};
