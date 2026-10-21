//! Internal storage engine primitives.
//!
//! Nothing in this module is part of the public API. The on-disk format is
//! documented in `docs/storage-engine.md`.

pub(crate) mod io;
pub(crate) mod meta;
pub(crate) mod mmap;

/// The fixed page size used by the on-disk format.
pub(crate) const PAGE_SIZE: usize = 4096;
