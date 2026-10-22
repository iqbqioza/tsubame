//! Internal storage engine primitives.
//!
//! Nothing in this module is part of the public API. The on-disk format is
//! documented in `docs/storage-engine.md`.

pub(crate) mod btree;
pub(crate) mod checksum;
pub(crate) mod commit;
pub(crate) mod compact;
pub(crate) mod io;
pub(crate) mod meta;
pub(crate) mod mmap;
pub(crate) mod overflow;
pub(crate) mod page;
pub(crate) mod store;

/// The fixed page size used by the on-disk format.
pub(crate) const PAGE_SIZE: usize = 4096;

/// A fast identity-hashed set for internal page IDs.
pub(crate) type PageSet =
    std::collections::HashSet<u64, std::hash::BuildHasherDefault<PageIdHasher>>;

/// Identity hasher for page IDs.
#[derive(Default)]
pub(crate) struct PageIdHasher(u64);

impl std::hash::Hasher for PageIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut value = 0u64;
        for &byte in bytes.iter().take(8) {
            value = (value << 8) | u64::from(byte);
        }
        self.0 = value;
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}
