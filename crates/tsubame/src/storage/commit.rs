//! Commit-intent records.
//!
//! A commit writes its pages, a chain of commit-record pages, and the new meta
//! page before a single `fdatasync`. The record lists every page and extent
//! written by the transaction. Recovery accepts a meta page only when the
//! record chain and every listed page are intact and carry the same txn ID.

use anyhow::{Context, Result, ensure};
use byteorder::{ByteOrder, LittleEndian};

use super::PAGE_SIZE;
use super::meta::{INLINE_COMMIT_CAPACITY, Meta};
use super::overflow::{self, OverflowRef};
use super::page::Page;
use super::store::PageStore;

const MAGIC: [u8; 8] = *b"TSUBCOM\0";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 40;
const CHECKSUM_OFFSET: usize = 0;
const ENTRY_SIZE: usize = 16;
const CAPACITY: usize = (PAGE_SIZE - HEADER_SIZE) / ENTRY_SIZE;

/// One page or overflow extent in a commit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitEntry {
    /// A single B+tree page.
    Page(u64),
    /// A contiguous overflow extent.
    Extent { start_page: u64, page_count: u64 },
}

/// One page of the commit-record chain.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CommitPage {
    txn_id: u64,
    next_page: u64,
    entries: Vec<CommitEntry>,
}

impl CommitPage {
    fn encode(&self) -> Result<[u8; PAGE_SIZE]> {
        ensure!(
            self.entries.len() <= CAPACITY,
            "commit record page has too many entries"
        );

        let mut page = [0u8; PAGE_SIZE];
        page[4..12].copy_from_slice(&MAGIC);
        LittleEndian::write_u32(&mut page[12..16], VERSION);
        LittleEndian::write_u64(&mut page[16..24], self.txn_id);
        LittleEndian::write_u64(&mut page[24..32], self.next_page);
        LittleEndian::write_u32(&mut page[32..36], self.entries.len() as u32);

        for (index, entry) in self.entries.iter().enumerate() {
            let offset = HEADER_SIZE + index * ENTRY_SIZE;
            let (page_id, page_count) = match entry {
                CommitEntry::Page(page_id) => (*page_id, 0),
                CommitEntry::Extent {
                    start_page,
                    page_count,
                } => (*start_page, *page_count),
            };
            LittleEndian::write_u64(&mut page[offset..offset + 8], page_id);
            LittleEndian::write_u64(&mut page[offset + 8..offset + 16], page_count);
        }

        let checksum = crc32c_fast::crc32c(&page);
        LittleEndian::write_u32(&mut page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4], checksum);

        Ok(page)
    }

    fn decode(page: &[u8]) -> Result<Self> {
        ensure!(page.len() == PAGE_SIZE, "commit record page has wrong size");
        ensure!(
            &page[4..12] == MAGIC.as_slice(),
            "invalid commit record magic"
        );
        ensure!(
            LittleEndian::read_u32(&page[12..16]) == VERSION,
            "unsupported commit record version"
        );

        let stored = LittleEndian::read_u32(&page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]);
        let actual = checksum(page);
        ensure!(
            stored == actual,
            "commit record checksum mismatch (stored {stored:#010x}, computed {actual:#010x})"
        );

        let txn_id = LittleEndian::read_u64(&page[16..24]);
        ensure!(txn_id > 0, "commit record txn ID must be non-zero");
        let next_page = LittleEndian::read_u64(&page[24..32]);
        ensure!(
            next_page == 0 || next_page >= 2,
            "commit record points at a reserved page"
        );
        let entry_count = LittleEndian::read_u32(&page[32..36]) as usize;
        ensure!(
            entry_count <= CAPACITY,
            "commit record has too many entries"
        );

        let mut entries = Vec::with_capacity(entry_count);
        for index in 0..entry_count {
            let offset = HEADER_SIZE + index * ENTRY_SIZE;
            let page_id = LittleEndian::read_u64(&page[offset..offset + 8]);
            let page_count = LittleEndian::read_u64(&page[offset + 8..offset + 16]);
            ensure!(page_id >= 2, "commit record points at a reserved page");
            if page_count == 0 {
                entries.push(CommitEntry::Page(page_id));
            } else {
                let end = page_id
                    .checked_add(page_count)
                    .context("commit extent page overflow")?;
                ensure!(end >= page_id, "commit extent page overflow");
                entries.push(CommitEntry::Extent {
                    start_page: page_id,
                    page_count,
                });
            }
        }

        Ok(Self {
            txn_id,
            next_page,
            entries,
        })
    }
}

/// Where a commit stored its list of new pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Descriptor {
    /// No pages were written.
    None,
    /// Exactly one page, stored directly in the meta page.
    Page(u64),
    /// A small number of pages stored inline in the meta page.
    Inline(Vec<CommitEntry>),
    /// A chain of commit-record pages.
    Chain(u64),
}

/// Returns the number of pages needed for a commit-record chain.
pub(crate) fn chain_page_count(entry_count: usize) -> Result<u64> {
    u64::try_from(entry_count.div_ceil(CAPACITY)).context("commit record is too large")
}

/// Allocates and writes the commit-record chain, returning its first page.
pub(crate) fn build<S: PageStore>(
    store: &mut S,
    txn_id: u64,
    entries: &[CommitEntry],
) -> Result<Descriptor> {
    if entries.is_empty() {
        return Ok(Descriptor::None);
    }
    if let [CommitEntry::Page(page_id)] = entries {
        return Ok(Descriptor::Page(*page_id));
    }
    if entries.len() <= INLINE_COMMIT_CAPACITY {
        return Ok(Descriptor::Inline(entries.to_vec()));
    }

    let page_count = entries.len().div_ceil(CAPACITY);
    let start_page = store.allocate_pages(chain_page_count(entries.len())?)?;

    for index in 0..page_count {
        let from = index * CAPACITY;
        let to = (from + CAPACITY).min(entries.len());
        let next_page = if index + 1 < page_count {
            start_page
                .checked_add(index as u64 + 1)
                .context("commit record page overflow")?
        } else {
            0
        };
        let record = CommitPage {
            txn_id,
            next_page,
            entries: entries[from..to].to_vec(),
        };
        let page = record.encode()?;
        store.write_page(
            start_page
                .checked_add(index as u64)
                .context("commit record page overflow")?,
            page,
        )?;
    }

    Ok(Descriptor::Chain(start_page))
}

/// Validates the commit record and every page it references.
pub(crate) fn validate(data: &[u8], meta: &Meta) -> Result<()> {
    if meta.inline_count > 0 {
        for entry in &meta.inline[..meta.inline_count as usize] {
            if entry.page_count == 0 {
                validate_page(data, meta, entry.page_id)?;
            } else {
                validate_extent(data, meta, entry.page_id, entry.page_count)?;
            }
        }
        return Ok(());
    }

    if meta.commit_page != 0 {
        validate_page(data, meta, meta.commit_page)?;
        return Ok(());
    }
    if meta.commit_record == 0 {
        return Ok(());
    }

    let mut page_id = meta.commit_record;
    let mut previous = 0u64;
    let mut record_pages = 0u64;

    loop {
        ensure!(
            page_id >= 2 && page_id < meta.page_count,
            "commit record page {page_id} is outside the committed page range"
        );
        if previous != 0 {
            ensure!(page_id > previous, "commit record chain is not increasing");
        }
        record_pages += 1;
        ensure!(
            record_pages <= meta.page_count,
            "commit record chain contains a cycle"
        );

        let record = CommitPage::decode(page_slice(data, page_id)?)?;
        ensure!(
            record.txn_id == meta.txn_id,
            "commit record txn ID {} does not match meta txn ID {}",
            record.txn_id,
            meta.txn_id
        );

        for entry in &record.entries {
            match *entry {
                CommitEntry::Page(page_id) => {
                    validate_page(data, meta, page_id)?;
                }
                CommitEntry::Extent {
                    start_page,
                    page_count,
                } => validate_extent(data, meta, start_page, page_count)?,
            }
        }

        previous = page_id;
        if record.next_page == 0 {
            break;
        }
        page_id = record.next_page;
    }

    Ok(())
}

fn validate_extent(data: &[u8], meta: &Meta, start_page: u64, page_count: u64) -> Result<()> {
    let end = start_page
        .checked_add(page_count)
        .context("commit extent page overflow")?;
    ensure!(
        start_page >= 2 && end <= meta.page_count,
        "committed extent {start_page}..{end} is outside the page range"
    );
    let byte_len = usize::try_from(page_count)
        .context("commit extent is too large")?
        .checked_mul(PAGE_SIZE)
        .context("commit extent size overflow")?;
    let start = page_offset(start_page)?;
    let end = start
        .checked_add(byte_len)
        .context("commit extent offset overflow")?;
    ensure!(
        end <= data.len(),
        "committed extent is outside the mapped region"
    );
    let bytes = &data[start..end];
    let total_len = LittleEndian::read_u32(&bytes[4..8]);
    let reference = OverflowRef {
        total_len,
        start_page,
        page_count,
    };
    overflow::validate_extent(bytes, reference)?;
    ensure!(
        overflow::txn_id(bytes)? == meta.txn_id,
        "overflow extent at page {start_page} has the wrong txn ID"
    );
    Ok(())
}

fn validate_page(data: &[u8], meta: &Meta, page_id: u64) -> Result<()> {
    ensure!(
        page_id >= 2 && page_id < meta.page_count,
        "committed page {page_id} is outside the page range"
    );
    let page = Page::parse(page_slice(data, page_id)?, Some(page_id))?;
    ensure!(
        page.txn_id() == meta.txn_id,
        "page {page_id} has txn ID {}, expected {}",
        page.txn_id(),
        meta.txn_id
    );
    Ok(())
}

fn page_offset(page_id: u64) -> Result<usize> {
    let index = usize::try_from(page_id - 2).context("page ID is too large")?;
    index.checked_mul(PAGE_SIZE).context("page offset overflow")
}

fn page_slice(data: &[u8], page_id: u64) -> Result<&[u8]> {
    let start = page_offset(page_id)?;
    let end = start
        .checked_add(PAGE_SIZE)
        .context("page offset overflow")?;
    ensure!(end <= data.len(), "page is outside the mapped region");
    Ok(&data[start..end])
}

fn checksum(page: &[u8]) -> u32 {
    let checksum = crc32c::crc32c(&page[..CHECKSUM_OFFSET]);
    let checksum = crc32c::crc32c_append(checksum, &[0u8; 4]);
    crc32c::crc32c_append(checksum, &page[CHECKSUM_OFFSET + 4..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::build_leaf;
    use crate::storage::store::memory::MemoryStore;

    #[test]
    fn record_page_round_trip() {
        let record = CommitPage {
            txn_id: 4,
            next_page: 9,
            entries: vec![
                CommitEntry::Page(2),
                CommitEntry::Extent {
                    start_page: 3,
                    page_count: 2,
                },
            ],
        };

        assert_eq!(
            CommitPage::decode(&record.encode().unwrap()).unwrap(),
            record
        );
    }

    #[test]
    fn builds_a_chain() {
        let mut store = MemoryStore::new();
        let entries: Vec<CommitEntry> = (0..CAPACITY + 3)
            .map(|index| CommitEntry::Page(2 + index as u64))
            .collect();
        let first = build(&mut store, 7, &entries).unwrap();
        assert!(matches!(first, Descriptor::Chain(_)));
    }

    #[test]
    fn validates_pages() {
        let mut store = MemoryStore::new();
        let page_id = store.allocate_pages(1).unwrap();
        let page = build_leaf(page_id, 7, &[(b"a", b"1")]).unwrap();
        store.write_page(page_id, page).unwrap();
        let entries = [CommitEntry::Page(page_id)];
        let descriptor = build(&mut store, 7, &entries).unwrap();
        assert_eq!(descriptor, Descriptor::Page(page_id));
        let mut meta = Meta::initial();
        meta.txn_id = 7;
        meta.page_count = page_id + 1;
        meta.commit_page = page_id;

        // Build a flat data region containing pages 2..page_count.
        let mut data = vec![0u8; ((meta.page_count - 2) as usize) * PAGE_SIZE];
        for page_id in 2..meta.page_count {
            let page = store.read_page(page_id).unwrap();
            let start = ((page_id - 2) as usize) * PAGE_SIZE;
            data[start..start + PAGE_SIZE].copy_from_slice(&page);
        }

        validate(&data, &meta).unwrap();
    }
}
