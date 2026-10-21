//! Contiguous overflow extents for values that do not fit inline.

use anyhow::{Context, Result, ensure};
use byteorder::{ByteOrder, LittleEndian};

use super::PAGE_SIZE;

/// The size of an overflow reference stored in a leaf slot.
pub(crate) const OVERFLOW_REF_SIZE: usize = 24;

/// The extent header prepended to the payload.
pub(crate) const EXTENT_HEADER_SIZE: usize = 32;

const CHECKSUM_OFFSET: usize = 0;

/// A reference from a leaf slot to a contiguous overflow extent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OverflowRef {
    /// The number of payload bytes.
    pub(crate) total_len: u32,
    /// The first page of the extent.
    pub(crate) start_page: u64,
    /// The number of contiguous pages in the extent.
    pub(crate) page_count: u64,
}

impl OverflowRef {
    /// Decodes a 24-byte reference.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == OVERFLOW_REF_SIZE,
            "overflow reference has {} bytes, expected {OVERFLOW_REF_SIZE}",
            bytes.len()
        );
        ensure!(
            bytes[20..24].iter().all(|byte| *byte == 0),
            "overflow reference has non-zero reserved bytes"
        );

        let reference = Self {
            total_len: LittleEndian::read_u32(&bytes[0..4]),
            start_page: LittleEndian::read_u64(&bytes[4..12]),
            page_count: LittleEndian::read_u64(&bytes[12..20]),
        };
        reference.validate_shape()?;

        Ok(reference)
    }

    /// Encodes the reference as 24 bytes.
    pub(crate) fn encode(self) -> [u8; OVERFLOW_REF_SIZE] {
        let mut bytes = [0u8; OVERFLOW_REF_SIZE];
        LittleEndian::write_u32(&mut bytes[0..4], self.total_len);
        LittleEndian::write_u64(&mut bytes[4..12], self.start_page);
        LittleEndian::write_u64(&mut bytes[12..20], self.page_count);
        bytes
    }

    fn validate_shape(self) -> Result<()> {
        ensure!(
            self.start_page >= 2,
            "overflow extent points at a reserved page"
        );
        ensure!(self.page_count > 0, "overflow extent has no pages");
        let required = pages_for(self.total_len as usize)?;
        ensure!(
            self.page_count >= required,
            "overflow extent has {} pages, needs at least {required}",
            self.page_count
        );

        Ok(())
    }
}

/// Returns the number of pages needed for a payload of `value_len` bytes.
pub(crate) fn pages_for(value_len: usize) -> Result<u64> {
    let total = value_len
        .checked_add(EXTENT_HEADER_SIZE)
        .context("overflow value size overflow")?;
    let pages = total.div_ceil(PAGE_SIZE);
    ensure!(pages > 0, "overflow value needs at least one page");
    u64::try_from(pages).context("overflow extent is too large")
}

/// Builds the complete bytes of an overflow extent.
pub(crate) fn encode_extent(reference: OverflowRef, value: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        value.len() <= u32::MAX as usize,
        "overflow value is larger than 4 GiB"
    );
    ensure!(
        reference.total_len as usize == value.len(),
        "overflow reference length {} does not match the value length {}",
        reference.total_len,
        value.len()
    );
    let pages = pages_for(value.len())?;
    ensure!(
        pages == reference.page_count,
        "overflow reference has {} pages, expected {pages}",
        reference.page_count
    );
    reference.validate_shape()?;

    let len = usize::try_from(pages)
        .context("overflow extent is too large for this platform")?
        .checked_mul(PAGE_SIZE)
        .context("overflow extent size overflow")?;
    let mut bytes = vec![0u8; len];

    LittleEndian::write_u32(&mut bytes[4..8], reference.total_len);
    LittleEndian::write_u64(&mut bytes[8..16], reference.start_page);
    LittleEndian::write_u64(&mut bytes[16..24], reference.page_count);
    LittleEndian::write_u64(&mut bytes[24..32], 0);
    bytes[EXTENT_HEADER_SIZE..EXTENT_HEADER_SIZE + value.len()].copy_from_slice(value);

    let checksum = checksum(&bytes);
    LittleEndian::write_u32(&mut bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4], checksum);

    Ok(bytes)
}

/// Validates an extent and returns its payload.
#[allow(dead_code)] // Wired into the mmap page source with transactions.
pub(crate) fn validate_extent(bytes: &[u8], reference: OverflowRef) -> Result<&[u8]> {
    let expected = usize::try_from(reference.page_count)
        .context("overflow extent is too large for this platform")?
        .checked_mul(PAGE_SIZE)
        .context("overflow extent size overflow")?;
    ensure!(
        bytes.len() == expected,
        "overflow extent has {} bytes, expected {expected}",
        bytes.len()
    );

    let stored = LittleEndian::read_u32(&bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]);
    let actual = checksum(bytes);
    ensure!(
        stored == actual,
        "overflow checksum mismatch (stored {stored:#010x}, computed {actual:#010x})"
    );

    ensure!(
        LittleEndian::read_u32(&bytes[4..8]) == reference.total_len,
        "overflow length does not match its reference"
    );
    ensure!(
        LittleEndian::read_u64(&bytes[8..16]) == reference.start_page,
        "overflow start page does not match its reference"
    );
    ensure!(
        LittleEndian::read_u64(&bytes[16..24]) == reference.page_count,
        "overflow page count does not match its reference"
    );
    ensure!(
        LittleEndian::read_u64(&bytes[24..32]) == 0,
        "overflow extent has unsupported flags"
    );

    let total_len = reference.total_len as usize;
    let payload = &bytes[EXTENT_HEADER_SIZE..EXTENT_HEADER_SIZE + total_len];
    ensure!(
        payload.len() == total_len,
        "overflow payload is shorter than its reference"
    );

    Ok(payload)
}

/// Computes the extent checksum with the checksum field zeroed.
fn checksum(bytes: &[u8]) -> u32 {
    debug_assert!(bytes.len() >= EXTENT_HEADER_SIZE);
    let mut header = [0u8; EXTENT_HEADER_SIZE];
    header.copy_from_slice(&bytes[..EXTENT_HEADER_SIZE]);
    header[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);

    let checksum = crc32c::crc32c(&header);
    crc32c::crc32c_append(checksum, &bytes[EXTENT_HEADER_SIZE..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(value_len: usize) -> Result<OverflowRef> {
        Ok(OverflowRef {
            total_len: value_len as u32,
            start_page: 2,
            page_count: pages_for(value_len)?,
        })
    }

    #[test]
    fn extent_round_trip() {
        let value = vec![0x5a; 10_000];
        let reference = reference(value.len()).unwrap();
        let bytes = encode_extent(reference, &value).unwrap();

        assert_eq!(validate_extent(&bytes, reference).unwrap(), value);
    }

    #[test]
    fn reference_round_trip() {
        let reference = OverflowRef {
            total_len: 1234,
            start_page: 9,
            page_count: 2,
        };

        assert_eq!(OverflowRef::decode(&reference.encode()).unwrap(), reference);
    }

    #[test]
    fn rejects_corrupt_extent() {
        let value = vec![0x5a; 5_000];
        let reference = reference(value.len()).unwrap();
        let mut bytes = encode_extent(reference, &value).unwrap();
        bytes[100] ^= 0xff;

        assert!(validate_extent(&bytes, reference).is_err());
    }

    #[test]
    fn rejects_zero_page_extent() {
        let reference = OverflowRef {
            total_len: 1,
            start_page: 2,
            page_count: 0,
        };

        assert!(reference.validate_shape().is_err());
    }
}
