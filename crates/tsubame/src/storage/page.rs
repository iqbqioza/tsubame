//! Fixed-size page encoding and decoding.
//!
//! A page is a slotted container. The slot array grows upward from the header
//! and cells are packed downward from the end of the page. Every page carries
//! a CRC32C checksum over its bytes with the checksum field zeroed.

use anyhow::{Context, Result, bail, ensure};
use byteorder::{ByteOrder, LittleEndian};

use super::PAGE_SIZE;
use super::overflow::{OVERFLOW_REF_SIZE, OverflowRef};

/// The maximum encoded key size accepted by format version 1.
pub(crate) const MAX_KEY_SIZE: usize = 1024;

/// The maximum inline entry size, including its slot.
pub(crate) const INLINE_BUDGET: usize = PAGE_SIZE / 4;

pub(crate) const HEADER_SIZE: usize = 32;
pub(crate) const SLOT_SIZE: usize = 12;
const CHECKSUM_OFFSET: usize = 0;
const BRANCH_CHILD_SIZE: usize = 8;
const FLAG_OVERFLOW: u8 = 1 << 0;

/// The kind of a B+tree page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageType {
    /// An internal page holding separator keys and child page IDs.
    Branch,
    /// A page holding keys and values.
    Leaf,
}

impl PageType {
    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            1 => Ok(Self::Branch),
            2 => Ok(Self::Leaf),
            _ => bail!("unknown page type {byte}"),
        }
    }

    fn as_byte(self) -> u8 {
        match self {
            Self::Branch => 1,
            Self::Leaf => 2,
        }
    }
}

/// A leaf value, which may be stored inline or in an overflow extent.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LeafValue<'a> {
    /// The value bytes are stored directly in the page.
    Inline(&'a [u8]),
    /// The value is stored in a contiguous overflow extent.
    Overflow(OverflowRef),
}

/// A validated view over one page.
pub(crate) struct Page<'a> {
    bytes: &'a [u8],
    page_type: PageType,
    slot_count: usize,
}

impl<'a> Page<'a> {
    /// Validates and parses a page.
    ///
    /// When `expected_id` is provided, the page ID in the header must match it.
    pub(crate) fn parse(bytes: &'a [u8], expected_id: Option<u64>) -> Result<Self> {
        ensure!(
            bytes.len() == PAGE_SIZE,
            "page has {} bytes, expected {PAGE_SIZE}",
            bytes.len()
        );

        let stored = LittleEndian::read_u32(&bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]);
        let mut copy = [0u8; PAGE_SIZE];
        copy.copy_from_slice(bytes);
        copy[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
        let actual = crc32c::crc32c(&copy);
        ensure!(
            stored == actual,
            "page checksum mismatch (stored {stored:#010x}, computed {actual:#010x})"
        );

        let page_type = PageType::from_byte(bytes[4])?;
        ensure!(bytes[5] == 0, "unsupported page flags {:#04x}", bytes[5]);

        let slot_count = LittleEndian::read_u16(&bytes[6..8]) as usize;
        let page_id = LittleEndian::read_u64(&bytes[8..16]);
        ensure!(page_id >= 2, "data page ID {page_id} is reserved");
        if let Some(expected_id) = expected_id {
            ensure!(
                page_id == expected_id,
                "page ID mismatch: header has {page_id}, expected {expected_id}"
            );
        }

        let txn_id = LittleEndian::read_u64(&bytes[16..24]);
        ensure!(txn_id > 0, "page transaction ID must be non-zero");

        let free_start = LittleEndian::read_u16(&bytes[24..26]) as usize;
        let free_end = LittleEndian::read_u16(&bytes[26..28]) as usize;
        let slot_array_end = HEADER_SIZE
            .checked_add(
                slot_count
                    .checked_mul(SLOT_SIZE)
                    .context("slot array size overflow")?,
            )
            .context("slot array size overflow")?;

        ensure!(
            slot_array_end == free_start,
            "page free_start {free_start} does not follow the slot array ({slot_array_end})"
        );
        ensure!(
            free_start <= free_end,
            "page free area is inverted: {free_start}..{free_end}"
        );
        ensure!(
            free_end <= PAGE_SIZE,
            "page free_end {free_end} exceeds the page size"
        );

        let mut previous_cell_start = PAGE_SIZE;
        for index in 0..slot_count {
            let slot = HEADER_SIZE + index * SLOT_SIZE;
            let cell_offset = LittleEndian::read_u16(&bytes[slot..slot + 2]) as usize;
            let key_len = LittleEndian::read_u16(&bytes[slot + 2..slot + 4]) as usize;
            let value_len = LittleEndian::read_u32(&bytes[slot + 4..slot + 8]) as usize;
            let flags = bytes[slot + 8];

            ensure!(
                flags & !FLAG_OVERFLOW == 0,
                "unsupported slot flags {flags:#04x}"
            );
            ensure!(
                key_len <= MAX_KEY_SIZE,
                "slot {index} key is {key_len} bytes, maximum is {MAX_KEY_SIZE}"
            );

            let cell_len = key_len
                .checked_add(value_len)
                .context("cell size overflow")?;
            let cell_end = cell_offset
                .checked_add(cell_len)
                .context("cell end overflow")?;
            ensure!(
                cell_offset >= free_end,
                "slot {index} starts before the free area"
            );
            ensure!(cell_end <= PAGE_SIZE, "slot {index} exceeds the page");
            ensure!(
                cell_end <= previous_cell_start,
                "slot {index} overlaps a previous cell"
            );
            previous_cell_start = cell_offset;

            match page_type {
                PageType::Branch => {
                    ensure!(
                        value_len == BRANCH_CHILD_SIZE,
                        "branch slot {index} has a {value_len} byte child, expected {BRANCH_CHILD_SIZE}"
                    );
                    ensure!(
                        flags & FLAG_OVERFLOW == 0,
                        "branch slot {index} is marked as an overflow reference"
                    );
                }
                PageType::Leaf => {
                    if flags & FLAG_OVERFLOW != 0 {
                        ensure!(
                            value_len == OVERFLOW_REF_SIZE,
                            "overflow slot {index} has {value_len} bytes, expected {OVERFLOW_REF_SIZE}"
                        );
                    }
                }
            }
        }

        ensure!(
            free_end == previous_cell_start,
            "page free_end {free_end} does not match the packed cells ({previous_cell_start})"
        );

        Ok(Self {
            bytes,
            page_type,
            slot_count,
        })
    }

    /// Returns the page type.
    pub(crate) fn page_type(&self) -> PageType {
        self.page_type
    }

    /// Returns the number of slots in the page.
    pub(crate) fn slot_count(&self) -> usize {
        self.slot_count
    }

    /// Returns the key stored in a slot.
    pub(crate) fn key(&self, index: usize) -> Result<&'a [u8]> {
        let (offset, key_len, _, _) = self.slot(index)?;
        Ok(&self.bytes[offset..offset + key_len])
    }

    /// Returns the value stored in a leaf slot.
    pub(crate) fn value(&self, index: usize) -> Result<&'a [u8]> {
        let (offset, key_len, value_len, _) = self.slot(index)?;
        let start = offset + key_len;
        Ok(&self.bytes[start..start + value_len])
    }

    /// Returns the child page ID stored in a branch slot.
    pub(crate) fn child(&self, index: usize) -> Result<u64> {
        let (offset, key_len, value_len, _) = self.slot(index)?;
        ensure!(
            value_len == BRANCH_CHILD_SIZE,
            "branch child slot has {value_len} bytes"
        );
        let start = offset + key_len;
        Ok(LittleEndian::read_u64(&self.bytes[start..start + 8]))
    }

    /// Returns whether a leaf slot points to an overflow extent.
    pub(crate) fn is_overflow(&self, index: usize) -> Result<bool> {
        let (_, _, _, flags) = self.slot(index)?;
        Ok(flags & FLAG_OVERFLOW != 0)
    }

    /// Returns the slot index containing `key`, if it exists.
    pub(crate) fn leaf_index(&self, key: &[u8]) -> Result<Option<usize>> {
        ensure!(
            self.page_type == PageType::Leaf,
            "leaf_index called on a branch page"
        );

        let index = self.lower_bound(key)?;
        if index == self.slot_count || self.key(index)? != key {
            Ok(None)
        } else {
            Ok(Some(index))
        }
    }

    /// Returns the value stored in a leaf slot.
    pub(crate) fn leaf_value(&self, index: usize) -> Result<LeafValue<'a>> {
        if self.is_overflow(index)? {
            Ok(LeafValue::Overflow(OverflowRef::decode(
                self.value(index)?,
            )?))
        } else {
            Ok(LeafValue::Inline(self.value(index)?))
        }
    }

    /// Returns the child to follow for `key` in a branch page.
    pub(crate) fn branch_child(&self, key: &[u8]) -> Result<u64> {
        let index = self.branch_child_index(key)?;
        self.child(index)
    }

    /// Returns the child slot index to follow for `key` in a branch page.
    pub(crate) fn branch_child_index(&self, key: &[u8]) -> Result<usize> {
        ensure!(
            self.page_type == PageType::Branch,
            "branch_child_index called on a leaf page"
        );
        ensure!(self.slot_count > 0, "branch page has no children");

        let mut left = 0;
        let mut right = self.slot_count;
        while left < right {
            let mid = (left + right) / 2;
            if self.key(mid)? <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left.checked_sub(1)
            .context("branch page has no child for the requested key")
    }

    /// Returns the first slot whose key is `>= key`.
    fn lower_bound(&self, key: &[u8]) -> Result<usize> {
        let mut left = 0;
        let mut right = self.slot_count;
        while left < right {
            let mid = (left + right) / 2;
            if self.key(mid)? < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(left)
    }

    /// Reads the raw slot fields.
    fn slot(&self, index: usize) -> Result<(usize, usize, usize, u8)> {
        ensure!(
            index < self.slot_count,
            "slot index {index} is out of range ({} slots)",
            self.slot_count
        );

        let slot = HEADER_SIZE + index * SLOT_SIZE;
        let offset = LittleEndian::read_u16(&self.bytes[slot..slot + 2]) as usize;
        let key_len = LittleEndian::read_u16(&self.bytes[slot + 2..slot + 4]) as usize;
        let value_len = LittleEndian::read_u32(&self.bytes[slot + 4..slot + 8]) as usize;
        let flags = self.bytes[slot + 8];

        Ok((offset, key_len, value_len, flags))
    }
}

/// Builds a leaf page from sorted key/value pairs.
#[allow(dead_code)] // Convenience wrapper used by page tests and future callers.
pub(crate) fn build_leaf(
    page_id: u64,
    txn_id: u64,
    entries: &[(&[u8], &[u8])],
) -> Result<[u8; PAGE_SIZE]> {
    for pair in entries.windows(2) {
        ensure!(
            pair[0].0 < pair[1].0,
            "leaf keys must be strictly ascending"
        );
    }

    let mut builder = PageBuilder::new(PageType::Leaf);
    for (key, value) in entries {
        builder.push(key, value, 0)?;
    }

    builder.finish(page_id, txn_id)
}

/// Builds a leaf page from sorted entries that may reference overflow extents.
pub(crate) fn build_leaf_entries(
    page_id: u64,
    txn_id: u64,
    entries: &[(&[u8], LeafValue<'_>)],
) -> Result<[u8; PAGE_SIZE]> {
    for pair in entries.windows(2) {
        ensure!(
            pair[0].0 < pair[1].0,
            "leaf keys must be strictly ascending"
        );
    }

    let mut builder = PageBuilder::new(PageType::Leaf);
    for (key, value) in entries {
        match value {
            LeafValue::Inline(value) => builder.push(key, value, 0)?,
            LeafValue::Overflow(reference) => {
                builder.push(key, &reference.encode(), FLAG_OVERFLOW)?;
            }
        }
    }

    builder.finish(page_id, txn_id)
}

/// Builds a branch page from separator keys and child page IDs.
///
/// The first entry must use an empty key, which represents negative infinity.
pub(crate) fn build_branch(
    page_id: u64,
    txn_id: u64,
    entries: &[(&[u8], u64)],
) -> Result<[u8; PAGE_SIZE]> {
    ensure!(!entries.is_empty(), "branch page needs at least one child");
    ensure!(
        entries[0].0.is_empty(),
        "the first branch key must be empty"
    );
    for pair in entries.windows(2) {
        ensure!(
            pair[0].0 < pair[1].0,
            "branch keys must be strictly ascending"
        );
    }

    let mut builder = PageBuilder::new(PageType::Branch);
    for (key, child) in entries {
        builder.push(key, &child.to_le_bytes(), 0)?;
    }

    builder.finish(page_id, txn_id)
}

struct PageBuilder {
    page: [u8; PAGE_SIZE],
    page_type: PageType,
    slot_count: usize,
    cell_end: usize,
}

impl PageBuilder {
    fn new(page_type: PageType) -> Self {
        Self {
            page: [0u8; PAGE_SIZE],
            page_type,
            slot_count: 0,
            cell_end: PAGE_SIZE,
        }
    }

    fn push(&mut self, key: &[u8], value: &[u8], flags: u8) -> Result<()> {
        ensure!(
            key.len() <= MAX_KEY_SIZE,
            "key is {} bytes, maximum is {MAX_KEY_SIZE}",
            key.len()
        );

        let cell_len = key
            .len()
            .checked_add(value.len())
            .context("cell size overflow")?;
        let new_end = self
            .cell_end
            .checked_sub(cell_len)
            .context("page does not have room for the cell")?;
        let slot = HEADER_SIZE + self.slot_count * SLOT_SIZE;
        ensure!(
            new_end >= slot + SLOT_SIZE,
            "page does not have room for another slot"
        );

        self.page[new_end..new_end + key.len()].copy_from_slice(key);
        let value_start = new_end + key.len();
        self.page[value_start..value_start + value.len()].copy_from_slice(value);

        LittleEndian::write_u16(&mut self.page[slot..slot + 2], new_end as u16);
        LittleEndian::write_u16(&mut self.page[slot + 2..slot + 4], key.len() as u16);
        LittleEndian::write_u32(&mut self.page[slot + 4..slot + 8], value.len() as u32);
        self.page[slot + 8] = flags;

        self.slot_count += 1;
        self.cell_end = new_end;

        Ok(())
    }

    fn finish(mut self, page_id: u64, txn_id: u64) -> Result<[u8; PAGE_SIZE]> {
        ensure!(page_id >= 2, "data page ID {page_id} is reserved");
        ensure!(txn_id > 0, "page transaction ID must be non-zero");

        self.page[4] = self.page_type.as_byte();
        self.page[5] = 0;
        LittleEndian::write_u16(&mut self.page[6..8], self.slot_count as u16);
        LittleEndian::write_u64(&mut self.page[8..16], page_id);
        LittleEndian::write_u64(&mut self.page[16..24], txn_id);

        let free_start = HEADER_SIZE + self.slot_count * SLOT_SIZE;
        LittleEndian::write_u16(&mut self.page[24..26], free_start as u16);
        LittleEndian::write_u16(&mut self.page[26..28], self.cell_end as u16);

        let checksum = page_checksum(&self.page);
        LittleEndian::write_u32(
            &mut self.page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4],
            checksum,
        );

        Ok(self.page)
    }
}

/// Computes the CRC32C of a page with its checksum field zeroed.
fn page_checksum(page: &[u8; PAGE_SIZE]) -> u32 {
    let mut copy = *page;
    copy[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    crc32c::crc32c(&copy)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf() -> [u8; PAGE_SIZE] {
        build_leaf(2, 7, &[(b"a", b"1"), (b"b", b"2")]).unwrap()
    }

    #[test]
    fn parses_a_leaf() {
        let bytes = leaf();
        let page = Page::parse(&bytes, Some(2)).unwrap();

        assert_eq!(page.page_type(), PageType::Leaf);
        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.key(0).unwrap(), b"a");
        assert_eq!(page.value(1).unwrap(), b"2");
    }

    #[test]
    fn leaf_lookup_hits_and_misses() {
        let bytes = leaf();
        let page = Page::parse(&bytes, Some(2)).unwrap();

        assert_eq!(page.leaf_index(b"a").unwrap(), Some(0));
        assert_eq!(page.leaf_index(b"b").unwrap(), Some(1));
        assert_eq!(page.leaf_index(b"c").unwrap(), None);
        assert_eq!(page.leaf_index(b"").unwrap(), None);
        assert!(matches!(
            page.leaf_value(0).unwrap(),
            LeafValue::Inline(value) if value == b"1"
        ));
    }

    #[test]
    fn parses_a_branch() {
        let page_bytes = build_branch(3, 7, &[(b"", 2), (b"m", 4)]).unwrap();
        let page = Page::parse(&page_bytes, Some(3)).unwrap();

        assert_eq!(page.page_type(), PageType::Branch);
        assert_eq!(page.child(0).unwrap(), 2);
        assert_eq!(page.child(1).unwrap(), 4);
        assert_eq!(page.branch_child(b"a").unwrap(), 2);
        assert_eq!(page.branch_child(b"m").unwrap(), 4);
        assert_eq!(page.branch_child(b"zzz").unwrap(), 4);
    }

    #[test]
    fn rejects_a_corrupt_checksum() {
        let mut page = leaf();
        page[100] ^= 0xff;

        assert!(Page::parse(&page, Some(2)).is_err());
    }

    #[test]
    fn rejects_a_truncated_page() {
        assert!(Page::parse(&leaf()[..PAGE_SIZE - 1], Some(2)).is_err());
    }

    #[test]
    fn rejects_a_page_id_mismatch() {
        assert!(Page::parse(&leaf(), Some(3)).is_err());
    }

    #[test]
    fn rejects_oversized_keys() {
        let key = vec![b'x'; MAX_KEY_SIZE + 1];
        assert!(build_leaf(2, 7, &[(&key, b"value")]).is_err());
    }

    #[test]
    fn rejects_unsorted_keys() {
        assert!(build_leaf(2, 7, &[(b"b", b"1"), (b"a", b"2")]).is_err());
    }

    #[test]
    fn rejects_out_of_range_cells() {
        let mut page = leaf();
        // Point the first slot at the end of the page, then make the checksum
        // valid again so that bounds validation is what rejects the page.
        LittleEndian::write_u16(&mut page[HEADER_SIZE..HEADER_SIZE + 2], PAGE_SIZE as u16);
        let checksum = page_checksum(&page);
        LittleEndian::write_u32(&mut page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4], checksum);

        assert!(Page::parse(&page, Some(2)).is_err());
    }

    #[test]
    fn empty_leaf_is_valid() {
        let page_bytes = build_leaf(2, 7, &[]).unwrap();
        let page = Page::parse(&page_bytes, Some(2)).unwrap();

        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.leaf_index(b"a").unwrap(), None);
    }
}
