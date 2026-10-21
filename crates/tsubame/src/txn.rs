//! Read-only and read-write transactions.
//!
//! Every database operation runs inside a transaction. Read-only transactions
//! provide a consistent snapshot and can coexist; a read-write transaction has
//! exclusive access to the environment.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, MutexGuard};

use anyhow::{Context, Result, bail, ensure};

use crate::database::DatabaseKind;
use crate::env::{Committed, Env, EnvInner};
use crate::storage::btree;
use crate::storage::btree::PageSource;
use crate::storage::commit::{self, CommitEntry, Descriptor};
use crate::storage::meta::{self, InlineCommit, Meta};
use crate::storage::overflow::{self, OverflowRef};
use crate::storage::page::Page;
use crate::storage::store::PageStore;
use crate::storage::{PAGE_SIZE, io};

/// Transactions writing at least this many entries into an empty database
/// build the tree bottom-up instead of inserting one path at a time.
const BULK_LOAD_THRESHOLD: usize = 1024;

/// A read-only transaction.
///
/// Read-only transactions see a stable snapshot of the environment until they
/// are dropped. They are created with [`crate::Env::read_txn`].
pub struct RoTxn<'env> {
    env: &'env Env,
    meta: Meta,
}

impl<'env> RoTxn<'env> {
    pub(crate) fn new(env: &'env Env, meta: Meta) -> Self {
        Self { env, meta }
    }

    pub(crate) fn root_for(&self, kind: DatabaseKind) -> u64 {
        match kind {
            DatabaseKind::Unnamed => self.meta.unnamed_root,
        }
    }

    pub(crate) fn belongs_to(&self, env: &Arc<EnvInner>) -> bool {
        Arc::ptr_eq(&self.env.inner, env)
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        self.env.inner.release_reader(self.meta.txn_id);
    }
}

impl PageSource for RoTxn<'_> {
    fn page(&self, page_id: u64) -> Result<Page<'_>> {
        self.env.inner.read_page(page_id, &self.meta)
    }

    fn overflow(&self, reference: OverflowRef) -> Result<&[u8]> {
        self.env.inner.read_overflow(reference, &self.meta)
    }
}

/// A read-write transaction.
///
/// Writes are made through a [`RwTxn`] and become visible to new read
/// transactions only after [`RwTxn::commit`] succeeds.
pub struct RwTxn<'env> {
    env: &'env Env,
    base: Meta,
    base_slot: u8,
    txn_id: u64,
    overlay: BTreeMap<u64, [u8; PAGE_SIZE]>,
    extents: BTreeMap<u64, Vec<u8>>,
    next_page: u64,
    free_single: BTreeSet<u64>,
    free_runs: BTreeMap<u64, u64>,
    freed: Vec<(u64, u64)>,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    root: Option<u64>,
    failed: bool,
    _writer: MutexGuard<'env, ()>,
}

impl<'env> RwTxn<'env> {
    pub(crate) fn new(
        env: &'env Env,
        writer: MutexGuard<'env, ()>,
        committed: Committed,
    ) -> Result<Self> {
        let txn_id = committed
            .meta
            .txn_id
            .checked_add(1)
            .context("transaction ID overflow")?;

        Ok(Self {
            env,
            base: committed.meta,
            base_slot: committed.slot,
            txn_id,
            overlay: BTreeMap::new(),
            extents: BTreeMap::new(),
            next_page: committed.meta.page_count,
            free_single: BTreeSet::new(),
            free_runs: BTreeMap::new(),
            freed: Vec::new(),
            pending: BTreeMap::new(),
            root: None,
            failed: false,
            _writer: writer,
        })
    }

    pub(crate) fn belongs_to(&self, env: &Arc<EnvInner>) -> bool {
        Arc::ptr_eq(&self.env.inner, env)
    }

    pub(crate) fn put_raw(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.pending.insert(key, Some(value));
    }

    pub(crate) fn delete_raw(&mut self, key: Vec<u8>) {
        self.pending.insert(key, None);
    }

    pub(crate) fn clear_raw(&mut self) {
        self.pending.clear();
        self.root = Some(0);
    }

    pub(crate) fn mark_failed(&mut self) {
        self.failed = true;
    }

    /// Commits the transaction and publishes its writes.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged entry cannot be applied, or if a page or
    /// the meta page cannot be written or synced. After a storage error the
    /// environment is marked unusable and must be reopened.
    pub fn commit(mut self) -> Result<()> {
        if self.failed {
            bail!("transaction cannot be committed after a failed operation");
        }

        let mut root = self.root.unwrap_or(self.base.unnamed_root);
        self.apply_pending(&mut root)?;

        let entries: Vec<CommitEntry> = self
            .overlay
            .keys()
            .map(|page_id| CommitEntry::Page(*page_id))
            .chain(
                self.extents
                    .iter()
                    .map(|(start_page, bytes)| CommitEntry::Extent {
                        start_page: *start_page,
                        page_count: (bytes.len() / PAGE_SIZE) as u64,
                    }),
            )
            .collect();
        let txn_id = self.txn_id;
        let descriptor = commit::build(&mut self, txn_id, &entries)?;
        let mut inline = [InlineCommit::default(); meta::INLINE_COMMIT_CAPACITY];
        let (commit_record, commit_page, inline_count) = match descriptor {
            Descriptor::None => (0, 0, 0),
            Descriptor::Page(page_id) => (0, page_id, 0),
            Descriptor::Inline(entries) => {
                for (slot, entry) in entries.iter().enumerate() {
                    inline[slot] = match *entry {
                        CommitEntry::Page(page_id) => InlineCommit {
                            page_id,
                            page_count: 0,
                        },
                        CommitEntry::Extent {
                            start_page,
                            page_count,
                        } => InlineCommit {
                            page_id: start_page,
                            page_count,
                        },
                    };
                }
                (0, 0, entries.len() as u32)
            }
            Descriptor::Chain(page_id) => (page_id, 0, 0),
        };

        let new_meta = Meta {
            txn_id,
            unnamed_root: root,
            catalog_root: self.base.catalog_root,
            page_count: self.next_page,
            database_count: self.base.database_count,
            flags: 0,
            commit_record,
            commit_page,
            inline_count,
            inline,
        };
        new_meta.validate()?;

        if let Err(error) = self.write_and_sync(&new_meta) {
            self.env.inner.poison();
            return Err(error);
        }

        Ok(())
    }

    fn apply_pending(&mut self, root: &mut u64) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let pending = std::mem::take(&mut self.pending);
        if *root == 0 && pending.len() >= BULK_LOAD_THRESHOLD {
            let entries: Vec<(Vec<u8>, Vec<u8>)> = pending
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect();
            if !entries.is_empty() {
                *root = btree::bulk_load(self, &entries, self.txn_id)?;
            }
            return Ok(());
        }

        for (key, value) in pending {
            *root = match value {
                Some(value) => btree::insert(self, *root, &key, &value, self.txn_id)?,
                None => btree::delete(self, *root, &key, self.txn_id)?,
            };
        }

        Ok(())
    }

    /// Aborts the transaction and discards its writes.
    ///
    /// Dropping a [`RwTxn`] without committing it aborts it as well.
    ///
    /// # Errors
    ///
    /// This implementation never fails; the method exists for symmetry with
    /// [`RwTxn::commit`].
    pub fn abort(self) -> Result<()> {
        Ok(())
    }

    fn write_and_sync(&self, new_meta: &Meta) -> Result<()> {
        let data = &self.env.inner.data;

        for (page_id, page) in &self.overlay {
            ensure!(
                *page_id >= 2 && *page_id < self.next_page,
                "page {page_id} is outside the transaction allocation range"
            );
            let offset = page_id
                .checked_mul(PAGE_SIZE as u64)
                .context("page offset overflow")?;
            io::write_all_at(data, page, offset)
                .with_context(|| format!("failed to write page {page_id}"))?;
        }

        for (start_page, bytes) in &self.extents {
            ensure!(
                bytes.len() % PAGE_SIZE == 0,
                "overflow extent is not page-aligned"
            );
            let offset = start_page
                .checked_mul(PAGE_SIZE as u64)
                .context("extent offset overflow")?;
            io::write_all_at(data, bytes, offset)
                .with_context(|| format!("failed to write overflow extent at page {start_page}"))?;
        }

        let slot = 1 - self.base_slot;
        meta::write_slot(data, slot as usize, new_meta)?;
        data.sync_data().context("failed to sync the commit")?;

        let mut committed = self
            .env
            .inner
            .committed
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *committed = Committed {
            meta: *new_meta,
            slot,
        };

        Ok(())
    }
}

impl PageSource for RwTxn<'_> {
    fn page(&self, page_id: u64) -> Result<Page<'_>> {
        if let Some(page) = self.overlay.get(&page_id) {
            Page::parse_trusted(page, Some(page_id))
        } else {
            self.env.inner.read_page(page_id, &self.base)
        }
    }

    fn overflow(&self, reference: OverflowRef) -> Result<&[u8]> {
        if let Some(bytes) = self.extents.get(&reference.start_page) {
            overflow::validate_extent(bytes, reference)
        } else {
            self.env.inner.read_overflow(reference, &self.base)
        }
    }
}

impl PageStore for RwTxn<'_> {
    fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
        if let Some(page) = self.overlay.get(&page_id) {
            return Ok(*page);
        }

        let mut page = [0u8; PAGE_SIZE];
        page.copy_from_slice(self.env.inner.read_page(page_id, &self.base)?.bytes());
        Ok(page)
    }

    fn allocate_pages(&mut self, pages: u64) -> Result<u64> {
        ensure!(pages > 0, "allocation must contain at least one page");

        if pages == 1 {
            if let Some(page) = self.free_single.pop_first() {
                return Ok(page);
            }
            if let Some((&start, &len)) = self.free_runs.iter().next() {
                self.free_runs.remove(&start);
                if len == 2 {
                    self.free_single.insert(start + 1);
                } else {
                    self.free_runs.insert(start + 1, len - 1);
                }
                return Ok(start);
            }
        } else if let Some((&start, &len)) = self.free_runs.iter().find(|(_, len)| **len >= pages) {
            self.free_runs.remove(&start);
            if len > pages {
                self.free_runs.insert(start + pages, len - pages);
            }
            return Ok(start);
        }

        let start = self.next_page;
        self.next_page = self
            .next_page
            .checked_add(pages)
            .context("page ID overflow")?;
        let max_pages =
            u64::try_from(self.env.inner.map_size / PAGE_SIZE).context("map size is too large")?;
        ensure!(
            self.next_page <= max_pages,
            "environment map is full at {} pages; reopen it with a larger map_size",
            max_pages
        );

        Ok(start)
    }

    fn write_page(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) -> Result<()> {
        ensure!(
            page_id >= 2 && page_id < self.next_page,
            "page {page_id} is outside the transaction allocation range"
        );
        self.overlay.insert(page_id, page);
        Ok(())
    }

    fn write_extent(&mut self, start_page: u64, bytes: &[u8]) -> Result<()> {
        ensure!(
            start_page >= 2 && start_page < self.next_page,
            "extent start {start_page} is outside the transaction allocation range"
        );
        ensure!(
            bytes.len() % PAGE_SIZE == 0,
            "overflow extent is not page-aligned"
        );
        let pages =
            u64::try_from(bytes.len() / PAGE_SIZE).context("overflow extent is too large")?;
        let end = start_page
            .checked_add(pages)
            .context("overflow extent page overflow")?;
        ensure!(
            end <= self.next_page,
            "overflow extent extends beyond the transaction allocation range"
        );

        self.extents.insert(start_page, bytes.to_vec());
        Ok(())
    }

    fn free_pages(&mut self, start_page: u64, pages: u64) -> Result<()> {
        ensure!(pages > 0, "free must contain at least one page");
        let end = start_page
            .checked_add(pages)
            .context("free range overflow")?;
        let base = self.base.page_count;

        if start_page < base {
            let base_end = end.min(base);
            self.freed.push((start_page, base_end - start_page));
            if end > base {
                self.free_new_pages(base, end - base)?;
            }
        } else {
            self.free_new_pages(start_page, pages)?;
        }

        Ok(())
    }
}

impl RwTxn<'_> {
    fn free_new_pages(&mut self, start_page: u64, pages: u64) -> Result<()> {
        ensure!(
            start_page >= self.base.page_count,
            "page {start_page} is not owned by this transaction"
        );
        let end = start_page
            .checked_add(pages)
            .context("free range overflow")?;
        ensure!(
            end <= self.next_page,
            "free range {start_page}..{end} exceeds the transaction high-water mark"
        );

        if pages == 1 {
            self.overlay.remove(&start_page);
            self.free_single.insert(start_page);
        } else {
            self.extents.remove(&start_page);
            self.free_runs.insert(start_page, pages);
        }

        Ok(())
    }
}
