//! Error types shared by the `tsubame` API.

pub use anyhow::Error;

/// A [`Result`] alias whose error type defaults to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
