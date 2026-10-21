//! Read-only and read-write transactions.
//!
//! Every database operation runs inside a transaction. Read-only transactions
//! provide a consistent snapshot and can coexist; a read-write transaction has
//! exclusive access to the environment.

use std::collections::BTreeMap;
use std::sync::{Arc, MutexGuard};

use anyhow::{Context, Result, bail, ensure};

use crate::database::DatabaseKind;
use crate::env::{Committed, Env, EnvInner};
use crate::storage::btree::PageSource;
use crate::storage::meta::{self, Meta};
use crate::storage::overflow::{self, OverflowRef};
use crate::storage::page::Page;
use crate::storage::store::PageStore;
use crate::storage::{PAGE_SIZE, io};

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
    #[allow(dead_code)] // Reused by the future free-space allocator.
    freed: Vec<(u64, u64)>,
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
            freed: Vec::new(),
            root: None,
            failed: false,
            _writer: writer,
        })
    }

    pub(crate) fn root_for(&self, kind: DatabaseKind) -> u64 {
        match kind {
            DatabaseKind::Unnamed => self.root.unwrap_or(self.base.unnamed_root),
        }
    }

    pub(crate) fn set_root(&mut self, root: u64) {
        self.root = Some(root);
    }

    pub(crate) fn belongs_to(&self, env: &Arc<EnvInner>) -> bool {
        Arc::ptr_eq(&self.env.inner, env)
    }

    pub(crate) fn txn_id(&self) -> u64 {
        self.txn_id
    }

    pub(crate) fn mark_failed(&mut self) {
        self.failed = true;
    }

    /// Commits the transaction and publishes its writes.
    ///
    /// # Errors
    ///
    /// Returns an error if a page or the meta page cannot be written or synced.
    /// After a storage error the environment is marked unusable and must be
    /// reopened.
    pub fn commit(self) -> Result<()> {
        if self.failed {
            bail!("transaction cannot be committed after a failed operation");
        }

        let new_meta = Meta {
            txn_id: self.txn_id,
            unnamed_root: self.root.unwrap_or(self.base.unnamed_root),
            catalog_root: self.base.catalog_root,
            page_count: self.next_page,
            database_count: self.base.database_count,
            flags: 0,
        };
        new_meta.validate()?;

        if let Err(error) = self.write_and_sync(&new_meta) {
            self.env.inner.poison();
            return Err(error);
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

        data.sync_all().context("failed to sync data pages")?;

        let slot = 1 - self.base_slot;
        meta::write_slot(data, slot as usize, new_meta)?;
        data.sync_all().context("failed to sync the meta page")?;

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
            Page::parse_unchecked(page, Some(page_id))
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
        self.freed.push((start_page, pages));
        Ok(())
    }
}
