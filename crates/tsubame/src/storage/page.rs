//! Fixed-size page encoding and decoding.
//!
//! A page is a slotted container. The slot array grows upward from the header
//! and cells are packed downward from the end of the page. Every page carries
//! a CRC32C checksum over its bytes with the checksum field zeroed.

use std::cmp::Ordering;

use anyhow::{Context, Result, bail, ensure};
use byteorder::{ByteOrder, LittleEndian};

use super::PAGE_SIZE;
use super::checksum;
use super::overflow::{OVERFLOW_REF_SIZE, OverflowRef};
use crate::comparator::KeyComparator;

/// The maximum encoded key size accepted by format version 1.
pub(crate) const MAX_KEY_SIZE: usize = 1024;

/// The maximum inline entry size, including its slot.
pub(crate) const INLINE_BUDGET: usize = PAGE_SIZE / 4;

pub(crate) const HEADER_SIZE: usize = 32;
pub(crate) const SLOT_SIZE: usize = 8;
const CHECKSUM_OFFSET: usize = 0;
const BRANCH_CHILD_SIZE: usize = 8;
const FLAG_OVERFLOW: u8 = 1 << 0;
const OVERFLOW_BIT: u32 = 1 << 31;
const VALUE_LEN_MASK: u32 = OVERFLOW_BIT - 1;

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

/// Decodes the value length and flags packed into a slot.
#[inline]
fn decode_slot_value(word: u32) -> (usize, u8) {
    let flags = if word & OVERFLOW_BIT != 0 {
        FLAG_OVERFLOW
    } else {
        0
    };
    ((word & VALUE_LEN_MASK) as usize, flags)
}

/// Packs a value length and flags into a slot word.
#[inline]
fn encode_slot_value(value_len: usize, flags: u8) -> Result<u32> {
    ensure!(
        flags & !FLAG_OVERFLOW == 0,
        "unsupported slot flags {flags:#04x}"
    );
    let value_len = u32::try_from(value_len).context("value is too large")?;
    ensure!(
        value_len <= VALUE_LEN_MASK,
        "value is too large for the page format"
    );
    Ok(value_len
        | if flags & FLAG_OVERFLOW != 0 {
            OVERFLOW_BIT
        } else {
            0
        })
}

/// A validated view over one page.
pub(crate) struct Page<'a> {
    bytes: &'a [u8],
    page_type: PageType,
    slot_count: usize,
}

impl<'a> Page<'a> {
    /// Validates and parses a page, including its checksum.
    ///
    /// When `expected_id` is provided, the page ID in the header must match it.
    pub(crate) fn parse(bytes: &'a [u8], expected_id: Option<u64>) -> Result<Self> {
        Self::parse_inner(bytes, expected_id, true, true)
    }

    /// Parses a page that was produced by this crate and is already verified.
    ///
    /// Only the header is validated. The caller must only use this for pages
    /// that were fully validated by [`Page::parse`] earlier or built by this
    /// crate's page builder.
    pub(crate) fn parse_trusted(bytes: &'a [u8], expected_id: Option<u64>) -> Result<Self> {
        Self::parse_inner(bytes, expected_id, false, false)
    }

    /// Parses a page whose complete contents were already validated.
    ///
    /// This reads only the page kind and slot count. The caller must have
    /// validated this page with [`Page::parse`] in this process and must not
    /// have observed external modifications to the mapped file.
    #[inline]
    pub(crate) fn parse_verified(bytes: &'a [u8]) -> Self {
        let page_type = if bytes[4] == PageType::Branch.as_byte() {
            PageType::Branch
        } else {
            PageType::Leaf
        };
        let slot_count = LittleEndian::read_u16(&bytes[6..8]) as usize;
        Self {
            bytes,
            page_type,
            slot_count,
        }
    }

    /// Copies the validated page into an owned buffer without first zeroing it.
    fn copy_page(&self) -> Result<[u8; PAGE_SIZE]> {
        let bytes: &[u8; PAGE_SIZE] = self
            .bytes
            .try_into()
            .context("page has an unexpected size")?;
        Ok(*bytes)
    }

    fn parse_inner(
        bytes: &'a [u8],
        expected_id: Option<u64>,
        verify_checksum: bool,
        validate_all_slots: bool,
    ) -> Result<Self> {
        ensure!(
            bytes.len() == PAGE_SIZE,
            "page has {} bytes, expected {PAGE_SIZE}",
            bytes.len()
        );

        if verify_checksum {
            let stored = LittleEndian::read_u32(&bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]);
            let actual = page_checksum(bytes);
            ensure!(
                stored == actual,
                "page checksum mismatch (stored {stored:#010x}, computed {actual:#010x})"
            );
        }

        let page_type = PageType::from_byte(bytes[4])?;
        ensure!(bytes[5] == 0, "unsupported page flags {:#04x}", bytes[5]);

        let slot_count = LittleEndian::read_u16(&bytes[6..8]) as usize;
        ensure!(
            slot_count <= (PAGE_SIZE - HEADER_SIZE) / SLOT_SIZE,
            "page has too many slots: {slot_count}"
        );
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

        if validate_all_slots {
            let mut previous_cell_start = PAGE_SIZE;
            for index in 0..slot_count {
                let slot = HEADER_SIZE + index * SLOT_SIZE;
                let cell_offset = LittleEndian::read_u16(&bytes[slot..slot + 2]) as usize;
                let key_len = LittleEndian::read_u16(&bytes[slot + 2..slot + 4]) as usize;
                let value_word = LittleEndian::read_u32(&bytes[slot + 4..slot + 8]);
                let (value_len, flags) = decode_slot_value(value_word);
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
        }

        Ok(Self {
            bytes,
            page_type,
            slot_count,
        })
    }

    /// Returns the page type.
    #[inline]
    pub(crate) fn page_type(&self) -> PageType {
        self.page_type
    }

    /// Returns the transaction ID stored in the page header.
    pub(crate) fn txn_id(&self) -> u64 {
        LittleEndian::read_u64(&self.bytes[16..24])
    }

    /// Returns the raw page bytes.
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Returns the start of the free area (the end of the slot array).
    pub(crate) fn free_end(&self) -> usize {
        LittleEndian::read_u16(&self.bytes[26..28]) as usize
    }

    /// Returns whether a leaf entry can be appended without rebuilding.
    pub(crate) fn can_append(&self, key_len: usize, value_len: usize) -> bool {
        if self.page_type != PageType::Leaf || key_len > MAX_KEY_SIZE {
            return false;
        }
        let slot_end = HEADER_SIZE + (self.slot_count + 1) * SLOT_SIZE;
        match key_len.checked_add(value_len) {
            Some(cell_len) => self
                .free_end()
                .checked_sub(cell_len)
                .is_some_and(|cell_start| cell_start >= slot_end),
            None => false,
        }
    }

    /// Replaces a branch child page ID.
    pub(crate) fn replace_child(
        &self,
        page_id: u64,
        txn_id: u64,
        index: usize,
        child: u64,
    ) -> Result<[u8; PAGE_SIZE]> {
        ensure!(
            self.page_type == PageType::Branch,
            "cannot replace a child in a leaf page"
        );
        let (offset, key_len, value_len, _) = self.slot(index)?;
        ensure!(
            value_len == BRANCH_CHILD_SIZE,
            "branch child slot has {value_len} bytes"
        );

        let mut page = self.copy_page()?;
        let start = offset + key_len;
        LittleEndian::write_u64(&mut page[start..start + 8], child);

        reseal(&mut page, page_id, txn_id);
        Ok(page)
    }

    /// Returns the number of slots in the page.
    #[inline]
    pub(crate) fn slot_count(&self) -> usize {
        self.slot_count
    }

    /// Returns the key stored in a slot.
    #[inline]
    pub(crate) fn key(&self, index: usize) -> Result<&'a [u8]> {
        let (offset, key_len, _, _) = self.slot(index)?;
        Ok(&self.bytes[offset..offset + key_len])
    }

    /// Returns the value stored in a leaf slot.
    #[allow(dead_code)] // Used by page tests and kept for low-level callers.
    #[inline]
    pub(crate) fn value(&self, index: usize) -> Result<&'a [u8]> {
        let (offset, key_len, value_len, _) = self.slot(index)?;
        let start = offset + key_len;
        Ok(&self.bytes[start..start + value_len])
    }

    /// Returns the child page ID stored in a branch slot.
    #[inline]
    pub(crate) fn child(&self, index: usize) -> Result<u64> {
        let (offset, key_len, value_len, _) = self.slot(index)?;
        ensure!(
            value_len == BRANCH_CHILD_SIZE,
            "branch child slot has {value_len} bytes"
        );
        let start = offset + key_len;
        Ok(LittleEndian::read_u64(&self.bytes[start..start + 8]))
    }

    /// Returns the slot index containing `key`, if it exists.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn leaf_index(&self, key: &[u8]) -> Result<Option<usize>> {
        self.leaf_index_with(key, &KeyComparator::lexicographic())
    }

    pub(crate) fn leaf_index_with(
        &self,
        key: &[u8],
        comparator: &KeyComparator,
    ) -> Result<Option<usize>> {
        ensure!(
            self.page_type == PageType::Leaf,
            "leaf_index called on a branch page"
        );

        let index = self.lower_bound_with(key, comparator)?;
        if index == self.slot_count
            || comparator.compare(self.key_at(index), key) != Ordering::Equal
        {
            Ok(None)
        } else {
            Ok(Some(index))
        }
    }

    /// Returns the key and value of a leaf slot with one slot walk.
    #[inline]
    pub(crate) fn leaf_entry(&self, index: usize) -> Result<(&'a [u8], LeafValue<'a>)> {
        let (offset, key_len, value_len, flags) = self.slot_at(index);
        let key = &self.bytes[offset..offset + key_len];
        let value_start = offset + key_len;
        let value_bytes = &self.bytes[value_start..value_start + value_len];
        let value = if flags & FLAG_OVERFLOW != 0 {
            LeafValue::Overflow(OverflowRef::decode(value_bytes)?)
        } else {
            LeafValue::Inline(value_bytes)
        };
        Ok((key, value))
    }

    /// Returns the value stored in a leaf slot.
    #[inline]
    pub(crate) fn leaf_value(&self, index: usize) -> Result<LeafValue<'a>> {
        Ok(self.leaf_entry(index)?.1)
    }

    /// Returns the child to follow for `key` in a branch page.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn branch_child(&self, key: &[u8]) -> Result<u64> {
        self.branch_child_with(key, &KeyComparator::lexicographic())
    }

    pub(crate) fn branch_child_with(&self, key: &[u8], comparator: &KeyComparator) -> Result<u64> {
        let index = self.branch_child_index_with(key, comparator)?;
        self.child(index)
    }

    /// Returns the child slot index to follow for `key` in a branch page.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn branch_child_index(&self, key: &[u8]) -> Result<usize> {
        self.branch_child_index_with(key, &KeyComparator::lexicographic())
    }

    pub(crate) fn branch_child_index_with(
        &self,
        key: &[u8],
        comparator: &KeyComparator,
    ) -> Result<usize> {
        ensure!(
            self.page_type == PageType::Branch,
            "branch_child_index called on a leaf page"
        );
        ensure!(self.slot_count > 0, "branch page has no children");

        // The first child is the negative-infinity slot. Its key is only a
        // placeholder and may not be the minimum under a custom comparator.
        let mut left = 1;
        let mut right = self.slot_count;
        if comparator.is_lexicographic() {
            while left < right {
                let mid = (left + right) / 2;
                if crate::comparator::compare_lexicographic(self.key_at(mid), key)
                    != Ordering::Greater
                {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
        } else {
            while left < right {
                let mid = (left + right) / 2;
                if comparator.compare(self.key_at(mid), key) != Ordering::Greater {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
        }

        left.checked_sub(1)
            .context("branch page has no child for the requested key")
    }

    /// Returns the first slot whose key is `>= key`.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn lower_bound(&self, key: &[u8]) -> Result<usize> {
        self.lower_bound_with(key, &KeyComparator::lexicographic())
    }

    pub(crate) fn lower_bound_with(&self, key: &[u8], comparator: &KeyComparator) -> Result<usize> {
        let mut left = 0;
        let mut right = self.slot_count;
        while left < right {
            let mid = (left + right) / 2;
            if comparator.compare(self.key_at(mid), key) == Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(left)
    }

    /// Returns the first slot whose key is `> key`.
    #[allow(dead_code)]
    pub(crate) fn upper_bound(&self, key: &[u8]) -> Result<usize> {
        self.upper_bound_with(key, &KeyComparator::lexicographic())
    }

    pub(crate) fn upper_bound_with(&self, key: &[u8], comparator: &KeyComparator) -> Result<usize> {
        let mut left = 0;
        let mut right = self.slot_count;
        while left < right {
            let mid = (left + right) / 2;
            if comparator.compare(self.key_at(mid), key) != Ordering::Greater {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(left)
    }

    /// Reads a slot that the caller has already bounds-checked.
    #[inline]
    fn slot_at(&self, index: usize) -> (usize, usize, usize, u8) {
        let slot = HEADER_SIZE + index * SLOT_SIZE;
        let offset = LittleEndian::read_u16(&self.bytes[slot..slot + 2]) as usize;
        let key_len = LittleEndian::read_u16(&self.bytes[slot + 2..slot + 4]) as usize;
        let value_word = LittleEndian::read_u32(&self.bytes[slot + 4..slot + 8]);
        let (value_len, flags) = decode_slot_value(value_word);
        (offset, key_len, value_len, flags)
    }

    /// Returns a key for a slot that the caller has already bounds-checked.
    #[inline]
    fn key_at(&self, index: usize) -> &'a [u8] {
        let (offset, key_len, _, _) = self.slot_at(index);
        &self.bytes[offset..offset + key_len]
    }

    /// Reads the raw slot fields.
    #[inline]
    fn slot(&self, index: usize) -> Result<(usize, usize, usize, u8)> {
        ensure!(
            index < self.slot_count,
            "slot index {index} is out of range ({} slots)",
            self.slot_count
        );

        let slot = HEADER_SIZE + index * SLOT_SIZE;
        let offset = LittleEndian::read_u16(&self.bytes[slot..slot + 2]) as usize;
        let key_len = LittleEndian::read_u16(&self.bytes[slot + 2..slot + 4]) as usize;
        let value_word = LittleEndian::read_u32(&self.bytes[slot + 4..slot + 8]);
        let (value_len, flags) = decode_slot_value(value_word);

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
#[allow(dead_code)] // Validation wrapper for callers that do not already know key order.
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

    build_leaf_entries_from_iter(
        page_id,
        txn_id,
        entries.iter().map(|(key, value)| (*key, *value)),
    )
}

/// Builds a leaf page from an ordered iterator of entries.
pub(crate) fn build_leaf_entries_from_iter<'a, I>(
    page_id: u64,
    txn_id: u64,
    entries: I,
) -> Result<[u8; PAGE_SIZE]>
where
    I: IntoIterator<Item = (&'a [u8], LeafValue<'a>)>,
{
    let mut builder = PageBuilder::new(PageType::Leaf);
    for (key, value) in entries {
        push_leaf_entry(&mut builder, key, &value)?;
    }
    builder.finish(page_id, txn_id)
}

fn push_leaf_entry(builder: &mut PageBuilder, key: &[u8], value: &LeafValue<'_>) -> Result<()> {
    match value {
        LeafValue::Inline(value) => builder.push(key, value, 0),
        LeafValue::Overflow(reference) => builder.push(key, &reference.encode(), FLAG_OVERFLOW),
    }
}

/// Builds a branch page from separator keys and child page IDs.
///
/// The first entry must use an empty key, which represents negative infinity.
#[allow(dead_code)] // Validation wrapper used by tests and external builders.
pub(crate) fn build_branch(
    page_id: u64,
    txn_id: u64,
    entries: &[(&[u8], u64)],
) -> Result<[u8; PAGE_SIZE]> {
    for pair in entries.windows(2) {
        ensure!(
            pair[0].0 < pair[1].0,
            "branch keys must be strictly ascending"
        );
    }
    build_branch_sorted(page_id, txn_id, entries)
}

/// Builds a branch page whose keys are already in tree order.
pub(crate) fn build_branch_sorted(
    page_id: u64,
    txn_id: u64,
    entries: &[(&[u8], u64)],
) -> Result<[u8; PAGE_SIZE]> {
    ensure!(!entries.is_empty(), "branch page needs at least one child");
    ensure!(
        entries[0].0.is_empty(),
        "the first branch key must be empty"
    );

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
        let value_word = encode_slot_value(value.len(), flags)?;
        LittleEndian::write_u32(&mut self.page[slot + 4..slot + 8], value_word);

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

        let checksum = page_checksum_for_write(&mut self.page);
        LittleEndian::write_u32(
            &mut self.page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4],
            checksum,
        );

        Ok(self.page)
    }
}

/// Computes the checksum of a page with its checksum field treated as zero.
fn page_checksum(page: &[u8]) -> u32 {
    checksum::hash_zeroed(page, CHECKSUM_OFFSET, CHECKSUM_OFFSET + 4)
}

/// Computes a page checksum when the caller owns the page and can zero the
/// checksum field before hashing. This uses the faster full-buffer CRC path.
fn page_checksum_for_write(page: &mut [u8; PAGE_SIZE]) -> u32 {
    page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    checksum::hash(page)
}

/// Appends a leaf entry directly into an owned page buffer.
pub(crate) fn append_entry_in_place(
    page: &mut [u8; PAGE_SIZE],
    page_id: u64,
    txn_id: u64,
    key: &[u8],
    value: &[u8],
    flags: u8,
) -> Result<()> {
    let (slot_count, free_end) = {
        let parsed = Page::parse_trusted(page, None)?;
        ensure!(
            parsed.page_type() == PageType::Leaf,
            "cannot append an entry to a branch page"
        );
        (parsed.slot_count(), parsed.free_end())
    };
    ensure!(
        key.len() <= MAX_KEY_SIZE,
        "key is {} bytes, maximum is {MAX_KEY_SIZE}",
        key.len()
    );
    ensure!(
        flags & !FLAG_OVERFLOW == 0,
        "unsupported slot flags {flags:#04x}"
    );

    let slot = HEADER_SIZE + slot_count * SLOT_SIZE;
    let cell_len = key
        .len()
        .checked_add(value.len())
        .context("cell size overflow")?;
    let cell_start = free_end
        .checked_sub(cell_len)
        .context("page does not have room for the cell")?;
    ensure!(
        cell_start >= slot + SLOT_SIZE,
        "page does not have room for another slot"
    );

    page[cell_start..cell_start + key.len()].copy_from_slice(key);
    let value_start = cell_start + key.len();
    page[value_start..value_start + value.len()].copy_from_slice(value);
    LittleEndian::write_u16(&mut page[slot..slot + 2], cell_start as u16);
    LittleEndian::write_u16(&mut page[slot + 2..slot + 4], key.len() as u16);
    let value_word = encode_slot_value(value.len(), flags)?;
    LittleEndian::write_u32(&mut page[slot + 4..slot + 8], value_word);
    LittleEndian::write_u16(&mut page[6..8], (slot_count + 1) as u16);
    LittleEndian::write_u16(&mut page[24..26], (slot + SLOT_SIZE) as u16);
    LittleEndian::write_u16(&mut page[26..28], cell_start as u16);
    reseal(page, page_id, txn_id);
    Ok(())
}

/// Replaces a same-length leaf value directly in an owned page buffer.
pub(crate) fn replace_value_in_place(
    page: &mut [u8; PAGE_SIZE],
    page_id: u64,
    txn_id: u64,
    index: usize,
    value: &[u8],
    flags: u8,
) -> Result<()> {
    let (offset, key_len, value_len) = {
        let parsed = Page::parse_trusted(page, None)?;
        ensure!(
            parsed.page_type() == PageType::Leaf,
            "cannot replace a value in a branch page"
        );
        let (offset, key_len, value_len, _) = parsed.slot(index)?;
        (offset, key_len, value_len)
    };
    ensure!(
        value_len == value.len(),
        "replacement value has {} bytes, expected {value_len}",
        value.len()
    );
    ensure!(
        flags & !FLAG_OVERFLOW == 0,
        "unsupported slot flags {flags:#04x}"
    );
    let start = offset + key_len;
    page[start..start + value_len].copy_from_slice(value);
    let slot = HEADER_SIZE + index * SLOT_SIZE;
    let value_word = encode_slot_value(value_len, flags)?;
    LittleEndian::write_u32(&mut page[slot + 4..slot + 8], value_word);
    reseal(page, page_id, txn_id);
    Ok(())
}

/// Replaces a changed-length leaf value directly in an owned page buffer.
pub(crate) fn replace_value_resized_in_place(
    page: &mut [u8; PAGE_SIZE],
    page_id: u64,
    txn_id: u64,
    index: usize,
    value: &[u8],
    flags: u8,
) -> Result<()> {
    let (slot_count, free_end, offset, key_len, value_len) = {
        let parsed = Page::parse_trusted(page, None)?;
        ensure!(
            parsed.page_type() == PageType::Leaf,
            "cannot replace a value in a branch page"
        );
        let (offset, key_len, value_len, _) = parsed.slot(index)?;
        (
            parsed.slot_count(),
            parsed.free_end(),
            offset,
            key_len,
            value_len,
        )
    };
    ensure!(
        flags & !FLAG_OVERFLOW == 0,
        "unsupported slot flags {flags:#04x}"
    );

    let old_cell_len = key_len
        .checked_add(value_len)
        .context("cell size overflow")?;
    let new_cell_len = key_len
        .checked_add(value.len())
        .context("cell size overflow")?;
    let old_cell_end = offset
        .checked_add(old_cell_len)
        .context("cell end overflow")?;
    let new_start = old_cell_end
        .checked_sub(new_cell_len)
        .context("cell start underflow")?;
    let new_free_end = if value.len() >= value_len {
        free_end.checked_sub(value.len() - value_len)
    } else {
        free_end.checked_add(value_len - value.len())
    }
    .context("leaf free area overflow")?;
    let slot_end = HEADER_SIZE
        .checked_add(slot_count * SLOT_SIZE)
        .context("slot array overflow")?;
    ensure!(
        new_free_end >= slot_end,
        "resized leaf cell overlaps the slot array"
    );
    ensure!(
        new_start >= new_free_end && new_start + new_cell_len <= PAGE_SIZE,
        "resized leaf cell is outside the page"
    );

    if value.len() < value_len {
        // Shrinking shifts earlier cells into the old target cell. Move the
        // target key out of the way first; the shifted block ends at new_start.
        page.copy_within(offset..offset + key_len, new_start);
        if new_free_end != free_end {
            page.copy_within(free_end..offset, new_free_end);
        }
    } else {
        // Growing shifts earlier cells left, ending before the target key.
        if new_free_end != free_end {
            page.copy_within(free_end..offset, new_free_end);
        }
        page.copy_within(offset..offset + key_len, new_start);
    }
    let value_start = new_start + key_len;
    page[value_start..value_start + value.len()].copy_from_slice(value);

    let slot = HEADER_SIZE + index * SLOT_SIZE;
    LittleEndian::write_u16(&mut page[slot..slot + 2], new_start as u16);
    let value_word = encode_slot_value(value.len(), flags)?;
    LittleEndian::write_u32(&mut page[slot + 4..slot + 8], value_word);

    if value.len() != value_len {
        let delta = value.len() as isize - value_len as isize;
        for later in index + 1..slot_count {
            let later_slot = HEADER_SIZE + later * SLOT_SIZE;
            let old_offset = LittleEndian::read_u16(&page[later_slot..later_slot + 2]) as isize;
            let new_offset = old_offset - delta;
            ensure!(
                new_offset >= slot_end as isize && new_offset <= PAGE_SIZE as isize,
                "resized leaf slot is outside the page"
            );
            LittleEndian::write_u16(&mut page[later_slot..later_slot + 2], new_offset as u16);
        }
    }

    LittleEndian::write_u16(&mut page[26..28], new_free_end as u16);
    reseal(page, page_id, txn_id);
    Ok(())
}

/// Replaces a branch child directly in an owned page buffer.
pub(crate) fn replace_child_in_place(
    page: &mut [u8; PAGE_SIZE],
    page_id: u64,
    txn_id: u64,
    index: usize,
    child: u64,
) -> Result<()> {
    let (offset, key_len, value_len) = {
        let parsed = Page::parse_trusted(page, None)?;
        ensure!(
            parsed.page_type() == PageType::Branch,
            "cannot replace a child in a leaf page"
        );
        let (offset, key_len, value_len, _) = parsed.slot(index)?;
        (offset, key_len, value_len)
    };
    ensure!(
        value_len == BRANCH_CHILD_SIZE,
        "branch child slot has {value_len} bytes"
    );
    let start = offset + key_len;
    LittleEndian::write_u64(&mut page[start..start + 8], child);
    reseal(page, page_id, txn_id);
    Ok(())
}

/// Updates the page identity and checksum after an in-place edit.
fn reseal(page: &mut [u8; PAGE_SIZE], page_id: u64, txn_id: u64) {
    LittleEndian::write_u64(&mut page[8..16], page_id);
    LittleEndian::write_u64(&mut page[16..24], txn_id);
    let checksum = page_checksum_for_write(page);
    LittleEndian::write_u32(&mut page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4], checksum);
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
        page[8] ^= 0xff;

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
    fn fast_checksum_matches_reference_implementation() {
        let mut page = leaf();
        let fast = page_checksum_for_write(&mut page);
        let mut reference = page;
        reference[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
        assert_eq!(fast, checksum::hash(&reference));
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
        let checksum = page_checksum_for_write(&mut page);
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
