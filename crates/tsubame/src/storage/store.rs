//! Low-level page storage used by the B+tree writer.
//!
//! The read-only traversal uses [`crate::storage::btree::PageSource`]. The
//! write path uses this trait so that a transaction can stage pages in an
//! overlay without knowing whether the base environment is memory-mapped.

use anyhow::Result;

use super::PAGE_SIZE;

/// A mutable page store.
pub(crate) trait PageStore {
    /// Reads a page into an owned buffer.
    fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE]>;

    /// Allocates a contiguous run of pages and returns its first page ID.
    fn allocate_pages(&mut self, pages: u64) -> Result<u64>;

    /// Writes a complete page.
    fn write_page(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) -> Result<()>;

    /// Writes a complete overflow extent.
    fn write_extent(&mut self, start_page: u64, bytes: &[u8]) -> Result<()>;

    /// Marks a contiguous run of pages as free.
    fn free_pages(&mut self, start_page: u64, pages: u64) -> Result<()>;
}

#[cfg(test)]
pub(crate) mod memory {
    use std::collections::BTreeMap;

    use anyhow::{Context, Result, ensure};

    use super::super::PAGE_SIZE;
    use super::super::btree::PageSource;
    use super::super::overflow::{self, OverflowRef};
    use super::PageStore;
    use crate::storage::page::Page;

    /// An append-only in-memory page store used by the storage tests.
    pub(crate) struct MemoryStore {
        pages: BTreeMap<u64, [u8; PAGE_SIZE]>,
        extents: BTreeMap<u64, Vec<u8>>,
        next_page: u64,
        pub(crate) freed: Vec<(u64, u64)>,
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MemoryStore {
        pub(crate) fn new() -> Self {
            Self {
                pages: BTreeMap::new(),
                extents: BTreeMap::new(),
                next_page: 2,
                freed: Vec::new(),
            }
        }
    }

    impl PageStore for MemoryStore {
        fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
            let page = self
                .pages
                .get(&page_id)
                .with_context(|| format!("page {page_id} not found"))?;
            Ok(*page)
        }

        fn allocate_pages(&mut self, pages: u64) -> Result<u64> {
            ensure!(pages > 0, "allocation must contain at least one page");
            let start = self.next_page;
            self.next_page = self
                .next_page
                .checked_add(pages)
                .context("page ID overflow")?;
            Ok(start)
        }

        fn write_page(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) -> Result<()> {
            self.pages.insert(page_id, page);
            Ok(())
        }

        fn write_extent(&mut self, start_page: u64, bytes: &[u8]) -> Result<()> {
            self.extents.insert(start_page, bytes.to_vec());
            Ok(())
        }

        fn free_pages(&mut self, start_page: u64, pages: u64) -> Result<()> {
            self.freed.push((start_page, pages));
            Ok(())
        }
    }

    impl PageSource for MemoryStore {
        fn page(&self, page_id: u64) -> Result<Page<'_>> {
            let page = self
                .pages
                .get(&page_id)
                .with_context(|| format!("page {page_id} not found"))?;
            Page::parse(page, Some(page_id))
        }

        fn overflow(&self, reference: OverflowRef) -> Result<&[u8]> {
            let bytes = self
                .extents
                .get(&reference.start_page)
                .with_context(|| format!("overflow extent {} not found", reference.start_page))?;
            overflow::validate_extent(bytes, reference)
        }

        fn comparator(&self) -> &crate::comparator::KeyComparator {
            &crate::comparator::LEXICOGRAPHIC
        }
    }
}
