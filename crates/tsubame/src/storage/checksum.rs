//! Fast checksums for on-disk structures.

use xxhash_rust::xxh3::{Xxh3, xxh3_64};

/// Computes a checksum over the complete byte slice.
#[inline]
pub(crate) fn hash(bytes: &[u8]) -> u32 {
    xxh3_64(bytes) as u32
}

/// Computes a checksum while treating `start..end` as zero bytes.
#[inline]
pub(crate) fn hash_zeroed(bytes: &[u8], start: usize, end: usize) -> u32 {
    let mut hasher = Xxh3::new();
    hasher.update(&bytes[..start]);
    hasher.update(&[0u8; 4]);
    hasher.update(&bytes[end..]);
    hasher.digest() as u32
}

/// Computes a checksum over the used regions of a page, excluding free space.
/// The caller has already validated the free-area bounds.
#[inline]
pub(crate) fn hash_page_used(page: &[u8], free_start: usize, free_end: usize) -> u32 {
    let mut hasher = Xxh3::new();
    hasher.update(&[0u8; 4]);
    hasher.update(&page[4..free_start]);
    hasher.update(&page[free_end..]);
    hasher.digest() as u32
}
