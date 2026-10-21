//! Internal storage engine primitives.
//!
//! Nothing in this module is part of the public API. The on-disk format is
//! documented in `docs/storage-engine.md`.

#[allow(dead_code)] // The transaction layer wires these modules in next.
pub(crate) mod btree;
pub(crate) mod io;
pub(crate) mod meta;
pub(crate) mod mmap;
#[allow(dead_code)] // The transaction layer wires these modules in next.
pub(crate) mod page;

/// The fixed page size used by the on-disk format.
pub(crate) const PAGE_SIZE: usize = 4096;
