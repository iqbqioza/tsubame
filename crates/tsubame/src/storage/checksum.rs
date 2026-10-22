//! Fast checksums for on-disk structures.

/// Computes a checksum over the complete byte slice.
#[inline]
pub(crate) fn hash(bytes: &[u8]) -> u32 {
    crc32c_fast::crc32c(bytes)
}

/// Computes a checksum while treating `start..end` as zero bytes.
///
/// This avoids copying a page just to blank its checksum field.
#[inline]
pub(crate) fn hash_zeroed(bytes: &[u8], start: usize, end: usize) -> u32 {
    let checksum = crc32c::crc32c(&bytes[..start]);
    let checksum = crc32c::crc32c_append(checksum, &[0u8; 4]);
    crc32c::crc32c_append(checksum, &bytes[end..])
}
