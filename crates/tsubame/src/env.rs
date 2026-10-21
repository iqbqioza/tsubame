//! Environments and environment configuration.
//!
//! An environment is a directory that holds one or more databases. Every
//! database belongs to exactly one environment, and transactions are created
//! from the environment.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, bail, ensure};
use fs4::TryLockError;
use memmap2::Mmap;

use crate::Result;
use crate::database::{Database, DatabaseKind};
use crate::storage::commit;
use crate::storage::meta::{self, Meta};
use crate::storage::overflow::{self, OverflowRef};
use crate::storage::page::{LeafValue, Page, PageType};
use crate::storage::{PAGE_SIZE, io, mmap};
use crate::txn::{RoTxn, RwTxn};

/// The default maximum size of an environment: 1 GiB.
pub const DEFAULT_MAP_SIZE: usize = 1024 * 1024 * 1024;

/// The default maximum number of named databases in an environment.
pub const DEFAULT_MAX_DBS: usize = 10;

const DATA_FILE: &str = "data.tsubame";
const LOCK_FILE: &str = "lock.tsubame";
const WRITER_LOCK_FILE: &str = "writer.tsubame.lock";
const CLEAN_FILE: &str = "clean.tsubame";
const MIN_MAP_SIZE: usize = 3 * PAGE_SIZE;
const CLEAN_MAGIC: [u8; 8] = *b"TSUBCLN\0";
const CLEAN_LEN: usize = 32;

/// An open environment.
///
/// Environments are created with [`EnvOpenOptions::open`]. All databases in an
/// environment share the same data file and transaction namespace.
pub struct Env {
    pub(crate) inner: Arc<EnvInner>,
}

impl Env {
    /// Returns the directory that backs this environment.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Returns whether this environment is read-only.
    pub fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    /// Returns whether this writable environment allows concurrent readers.
    pub fn is_multi_process(&self) -> bool {
        self.inner.multi_process
    }

    /// Returns a consistent snapshot of the environment metadata.
    pub fn info(&self) -> EnvInfo {
        let committed = self.inner.read_committed();
        EnvInfo {
            map_size: self.inner.map_size,
            page_size: PAGE_SIZE,
            page_count: committed.meta.page_count,
            txn_id: committed.meta.txn_id,
            database_count: committed.meta.database_count,
            max_dbs: self.inner.max_dbs,
            format_version: meta::FORMAT_VERSION,
            read_only: self.inner.read_only,
            multi_process: self.inner.multi_process,
        }
    }

    /// Starts a read-only transaction.
    ///
    /// Read-only transactions can run concurrently with each other and see a
    /// consistent snapshot of the environment.
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        self.inner.ensure_usable()?;
        if self.inner.read_only {
            self.inner.refresh_read_only()?;
        }
        let committed = self.inner.read_committed();
        self.inner.register_reader(committed.meta.txn_id);
        Ok(RoTxn::new(self, committed.meta))
    }

    /// Starts a read-write transaction.
    ///
    /// Only one read-write transaction can be active at a time. The first
    /// write transaction after opening scans the reachable pages once to
    /// reconstruct the free list.
    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        ensure!(
            !self.inner.read_only,
            "environment `{}` is read-only",
            self.inner.path.display()
        );
        self.inner.ensure_usable()?;
        let writer = self
            .inner
            .writer
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction lock is poisoned"))?;
        let committed = self.inner.read_committed();
        self.inner.ensure_free_pool();
        RwTxn::new(self, writer, committed)
    }

    /// Opens the unnamed default database or a named database.
    pub fn open_database<KC, DC>(
        &self,
        txn: &RoTxn<'_>,
        name: Option<&str>,
    ) -> Result<Database<KC, DC>> {
        ensure!(
            txn.belongs_to(&self.inner),
            "transaction belongs to a different environment"
        );

        let kind = match name {
            None => DatabaseKind::Unnamed,
            Some(name) => {
                validate_database_name(name)?;
                ensure!(
                    txn.catalog_lookup(name)?.is_some(),
                    "database `{name}` does not exist"
                );
                DatabaseKind::Named(Arc::from(name))
            }
        };

        Ok(Database::new(Arc::clone(&self.inner), kind))
    }

    /// Creates a named database, or opens it if it already exists.
    ///
    /// Pass `None` to use the unnamed default database.
    pub fn create_database<KC, DC>(
        &self,
        txn: &mut RwTxn<'_>,
        name: Option<&str>,
    ) -> Result<Database<KC, DC>> {
        ensure!(
            txn.belongs_to(&self.inner),
            "transaction belongs to a different environment"
        );

        let kind = match name {
            None => DatabaseKind::Unnamed,
            Some(name) => {
                validate_database_name(name)?;
                if txn.catalog_lookup(name)?.is_none() {
                    let count = usize::try_from(txn.catalog_count())
                        .context("database count is too large")?;
                    ensure!(
                        count < self.inner.max_dbs,
                        "maximum number of named databases ({}) reached",
                        self.inner.max_dbs
                    );
                    txn.catalog_put(name, 0)?;
                    txn.increment_database_count()?;
                }
                DatabaseKind::Named(Arc::from(name))
            }
        };

        Ok(Database::new(Arc::clone(&self.inner), kind))
    }

    /// Compacts the environment at `path` into a fresh data file.
    ///
    /// The environment must be closed; if it is open, this returns an error.
    /// Compaction removes pages that are no longer reachable, including pages
    /// left behind by updates and deletes.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment is open, its commit cannot be
    /// validated, or the replacement file cannot be written.
    pub fn compact(path: impl AsRef<Path>) -> Result<()> {
        crate::storage::compact::compact(path.as_ref())
    }
}

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Env")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

/// Metadata about an open [`Env`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EnvInfo {
    /// The effective mapped size of the data file, in bytes.
    pub map_size: usize,
    /// The page size used by the on-disk format.
    pub page_size: usize,
    /// The page count high-water mark for the current commit.
    pub page_count: u64,
    /// The transaction ID of the current commit.
    pub txn_id: u64,
    /// The number of named databases.
    pub database_count: u32,
    /// The configured maximum number of named databases.
    pub max_dbs: usize,
    /// The on-disk format version.
    pub format_version: u32,
    /// Whether this environment was opened read-only.
    pub read_only: bool,
    /// Whether concurrent readers were enabled for this writable environment.
    pub multi_process: bool,
}

/// A page or contiguous run that is no longer reachable from a committed meta.
///
/// `freed_txn` records the commit generation that released the extent. Data
/// pages freed by commit N are referenced by the snapshot at N - 1, so they
/// are reusable only once commit N + 1 has replaced that fallback snapshot.
/// Commit-record pages are referenced by their own commit, which requires one
/// additional generation of delay.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FreeExtent {
    pub(crate) start_page: u64,
    pub(crate) pages: u64,
    pub(crate) freed_txn: u64,
}

const RECONSTRUCT_MAX_DEPTH: usize = 64;

/// Rebuilds the in-memory free list from the pages reachable in a snapshot.
///
/// The reconstruction deliberately does not mark commit-record pages as live:
/// they are unreachable from the B+tree and are returned to the pool with the
/// snapshot generation plus one, which is late enough for both recoverable
/// meta pages to have stopped referring to them.
struct FreeListReconstructor<'a> {
    mmap: &'a [u8],
    meta: &'a Meta,
    visited: BTreeSet<u64>,
    live: Vec<(u64, u64)>,
    named_roots: Vec<u64>,
}

impl FreeListReconstructor<'_> {
    fn page(&self, page_id: u64) -> Result<Page<'_>> {
        ensure!(
            page_id >= 2 && page_id < self.meta.page_count,
            "page {page_id} is outside snapshot {}..{}",
            2,
            self.meta.page_count
        );
        let index = usize::try_from(page_id - 2).context("page ID is too large")?;
        let start = index
            .checked_mul(PAGE_SIZE)
            .context("page offset overflow")?;
        let end = start
            .checked_add(PAGE_SIZE)
            .context("page offset overflow")?;
        ensure!(
            end <= self.mmap.len(),
            "page {page_id} is outside the mapped region"
        );
        Page::parse(&self.mmap[start..end], Some(page_id))
    }

    fn walk(&mut self, root: u64, catalog: bool, depth: usize) -> Result<()> {
        if root == 0 {
            return Ok(());
        }
        ensure!(
            depth < RECONSTRUCT_MAX_DEPTH,
            "tree exceeds the maximum depth during free-list reconstruction"
        );
        ensure!(
            self.visited.insert(root),
            "tree references page {root} more than once"
        );

        let mut children = Vec::new();
        let mut overflows = Vec::new();
        let mut roots = Vec::new();

        {
            let page = self.page(root)?;
            match page.page_type() {
                PageType::Branch => {
                    for index in 0..page.slot_count() {
                        children.push(page.child(index)?);
                    }
                }
                PageType::Leaf => {
                    for index in 0..page.slot_count() {
                        match page.leaf_value(index)? {
                            LeafValue::Overflow(reference) => {
                                ensure!(!catalog, "catalog value is stored in an overflow extent");
                                overflows.push((reference.start_page, reference.page_count));
                            }
                            LeafValue::Inline(value) if catalog => {
                                ensure!(
                                    value.len() == 8,
                                    "catalog entry has {} bytes, expected 8",
                                    value.len()
                                );
                                let mut root = [0u8; 8];
                                root.copy_from_slice(value);
                                let root = u64::from_le_bytes(root);
                                if root != 0 {
                                    roots.push(root);
                                }
                            }
                            LeafValue::Inline(_) => {}
                        }
                    }
                }
            }
        }

        self.live.push((root, root + 1));

        for child in children {
            self.walk(child, catalog, depth + 1)?;
        }
        for (start_page, pages) in overflows {
            ensure!(pages > 0, "overflow extent has zero pages");
            let end = start_page
                .checked_add(pages)
                .context("overflow extent page overflow")?;
            ensure!(
                start_page >= 2 && end <= self.meta.page_count,
                "overflow extent {start_page}..{end} is outside snapshot {}..{}",
                2,
                self.meta.page_count
            );
            self.live.push((start_page, end));
        }
        self.named_roots.extend(roots);

        Ok(())
    }
}

fn reconstruct_free_list(mmap: &[u8], meta: &Meta) -> Result<Vec<FreeExtent>> {
    let mut reconstructor = FreeListReconstructor {
        mmap,
        meta,
        visited: BTreeSet::new(),
        live: Vec::new(),
        named_roots: Vec::new(),
    };

    reconstructor.walk(meta.unnamed_root, false, 0)?;
    reconstructor.walk(meta.catalog_root, true, 0)?;
    let named_roots = std::mem::take(&mut reconstructor.named_roots);
    for root in named_roots {
        reconstructor.walk(root, false, 0)?;
    }

    let mut live = reconstructor.live;
    live.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(live.len());
    for (start, end) in live {
        ensure!(
            start >= 2 && start < end && end <= meta.page_count,
            "live range {start}..{end} is outside snapshot {}..{}",
            2,
            meta.page_count
        );
        if let Some(last) = merged.last_mut() {
            if start <= last.1 {
                last.1 = last.1.max(end);
                continue;
            }
        }
        merged.push((start, end));
    }

    let freed_txn = meta.txn_id.saturating_add(1);
    let mut free = Vec::new();
    let mut cursor = 2;
    for (start, end) in merged {
        if start > cursor {
            free.push(FreeExtent {
                start_page: cursor,
                pages: start - cursor,
                freed_txn,
            });
        }
        cursor = cursor.max(end);
    }
    if cursor < meta.page_count {
        free.push(FreeExtent {
            start_page: cursor,
            pages: meta.page_count - cursor,
            freed_txn,
        });
    }

    Ok(free)
}

/// The committed meta state and the slot that contains it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Committed {
    pub(crate) meta: Meta,
    pub(crate) slot: u8,
}

/// The in-memory state shared by an environment and its handles.
pub(crate) struct EnvInner {
    pub(crate) path: PathBuf,
    pub(crate) map_size: usize,
    pub(crate) max_dbs: usize,
    pub(crate) data: File,
    pub(crate) mmap: Mmap,
    pub(crate) read_only: bool,
    pub(crate) multi_process: bool,
    pub(crate) page_reuse: bool,
    pub(crate) committed: RwLock<Committed>,
    pub(crate) writer: Mutex<()>,
    pub(crate) readers: Mutex<BTreeMap<u64, usize>>,
    /// Extents that are no longer reachable from the current commit, tagged
    /// with the generation that released them. The writer moves reusable
    /// extents into its transaction and returns the unused ones.
    pub(crate) free_pool: Mutex<Vec<FreeExtent>>,
    free_pool_ready: AtomicBool,
    pub(crate) poisoned: AtomicBool,
    clean_marker_on_drop: AtomicBool,
    verified: Vec<AtomicU64>,
    _lock: File,
    _writer_lock: Option<File>,
}

impl EnvInner {
    pub(crate) fn read_committed(&self) -> Committed {
        *self
            .committed
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn ensure_usable(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            bail!(
                "environment `{}` must be reopened after a failed commit",
                self.path.display()
            );
        }
        Ok(())
    }

    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    #[inline]
    fn is_verified(&self, page_id: u64) -> bool {
        let index = (page_id / 64) as usize;
        let bit = 1u64 << (page_id % 64);
        self.verified
            .get(index)
            .is_some_and(|word| word.load(Ordering::Relaxed) & bit != 0)
    }

    #[inline]
    fn mark_verified(&self, page_id: u64) {
        let index = (page_id / 64) as usize;
        let bit = 1u64 << (page_id % 64);
        if let Some(word) = self.verified.get(index) {
            word.fetch_or(bit, Ordering::Relaxed);
        }
    }

    pub(crate) fn register_reader(&self, txn_id: u64) {
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *readers.entry(txn_id).or_insert(0) += 1;
    }

    pub(crate) fn release_reader(&self, txn_id: u64) {
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = readers.get_mut(&txn_id) {
            *count -= 1;
            if *count == 0 {
                readers.remove(&txn_id);
            }
        }
    }

    pub(crate) fn oldest_reader_snapshot(&self, fallback: u64) -> u64 {
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .next()
            .copied()
            .unwrap_or(fallback)
    }

    pub(crate) fn take_reusable_free(&self, limit_txn: u64) -> Vec<FreeExtent> {
        let mut pool = self
            .free_pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut reusable = Vec::new();
        let mut retained = Vec::with_capacity(pool.len());

        for extent in pool.drain(..) {
            if extent.freed_txn <= limit_txn {
                reusable.push(extent);
            } else {
                retained.push(extent);
            }
        }

        *pool = retained;
        reusable
    }

    pub(crate) fn return_free(&self, extents: impl IntoIterator<Item = FreeExtent>) {
        let mut pool = self
            .free_pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pool.extend(extents);
    }

    /// Reconstructs the free list on the first write after opening.
    ///
    /// Reconstruction scans the reachable tree, so doing it eagerly would make
    /// read-only reopen latency proportional to the database size. The writer
    /// lock is already held when this is called, so the scan runs once.
    pub(crate) fn ensure_free_pool(&self) {
        if !self.page_reuse || self.free_pool_ready.swap(true, Ordering::AcqRel) {
            return;
        }
        let committed = self.read_committed();
        // A malformed reachable tree already fails reads. Do not turn that
        // into an open error; leave the pool empty if reconstruction cannot
        // complete.
        let free = reconstruct_free_list(&self.mmap, &committed.meta).unwrap_or_default();
        self.return_free(free);
    }

    #[inline]
    /// Refreshes the committed snapshot from disk for a read-only environment.
    pub(crate) fn refresh_read_only(&self) -> Result<()> {
        let data_path = self.path.join(DATA_FILE);
        let loaded = meta::load_all(&self.data, &data_path)?;
        let clean_txn = read_clean_marker(&self.path);
        let mut chosen = None;
        let mut errors = Vec::new();

        for candidate in loaded {
            let required = pages_to_bytes(candidate.meta.page_count)?;
            let required_data = required.saturating_sub(2 * PAGE_SIZE);
            if self.mmap.len() < required_data {
                errors.push(format!(
                    "slot {}: committed pages exceed the read-only mapping",
                    candidate.slot
                ));
                continue;
            }
            if clean_txn == Some(candidate.meta.txn_id) {
                chosen = Some(candidate);
                break;
            }
            match commit::validate(&self.mmap, &candidate.meta) {
                Ok(()) => {
                    chosen = Some(candidate);
                    break;
                }
                Err(error) => errors.push(format!("slot {}: {error:#}", candidate.slot)),
            }
        }

        let chosen = chosen.with_context(|| {
            format!(
                "no recoverable commit in `{}`: {}",
                self.path.display(),
                errors.join("; ")
            )
        })?;
        let mut committed = self
            .committed
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *committed = Committed {
            meta: chosen.meta,
            slot: chosen.slot,
        };
        Ok(())
    }

    #[inline]
    pub(crate) fn read_page(&self, page_id: u64, snapshot: &Meta) -> Result<Page<'_>> {
        ensure!(
            page_id >= 2 && page_id < snapshot.page_count,
            "page {page_id} is outside snapshot {}..{}",
            2,
            snapshot.page_count
        );
        let index = usize::try_from(page_id - 2).context("page ID is too large")?;
        let start = index
            .checked_mul(PAGE_SIZE)
            .context("page offset overflow")?;
        let end = start
            .checked_add(PAGE_SIZE)
            .context("page offset overflow")?;
        ensure!(
            end <= self.mmap.len(),
            "page {page_id} is outside the mapped region"
        );

        let page = &self.mmap[start..end];
        if self.is_verified(page_id) {
            Ok(Page::parse_verified(page))
        } else {
            let parsed = Page::parse(page, Some(page_id))?;
            self.mark_verified(page_id);
            Ok(parsed)
        }
    }

    pub(crate) fn read_overflow(&self, reference: OverflowRef, snapshot: &Meta) -> Result<&[u8]> {
        ensure!(
            reference.start_page >= 2,
            "overflow extent points at reserved page {}",
            reference.start_page
        );
        let end_page = reference
            .start_page
            .checked_add(reference.page_count)
            .context("overflow extent page overflow")?;
        ensure!(
            end_page <= snapshot.page_count,
            "overflow extent {}..{end_page} is outside snapshot {}..{}",
            reference.start_page,
            2,
            snapshot.page_count
        );

        let index = usize::try_from(reference.start_page - 2)
            .context("overflow start page is too large")?;
        let start = index
            .checked_mul(PAGE_SIZE)
            .context("overflow offset overflow")?;
        let len = usize::try_from(reference.page_count)
            .context("overflow page count is too large")?
            .checked_mul(PAGE_SIZE)
            .context("overflow length overflow")?;
        let end = start.checked_add(len).context("overflow offset overflow")?;
        ensure!(
            end <= self.mmap.len(),
            "overflow extent is outside the mapped region"
        );

        let bytes = &self.mmap[start..end];
        if self.is_verified(reference.start_page) {
            overflow::validate_extent_unchecked(bytes, reference)
        } else {
            let payload = overflow::validate_extent(bytes, reference)?;
            self.mark_verified(reference.start_page);
            Ok(payload)
        }
    }
}

impl Drop for EnvInner {
    fn drop(&mut self) {
        if self.poisoned.load(Ordering::Acquire)
            || !self.clean_marker_on_drop.load(Ordering::Acquire)
        {
            return;
        }

        let committed = self.read_committed();
        let _ = write_clean_marker(&self.path, committed.meta.txn_id);
    }
}

/// Reads the clean-shutdown marker, if it is present and valid.
fn read_clean_marker(path: &Path) -> Option<u64> {
    let bytes = fs::read(path.join(CLEAN_FILE)).ok()?;
    if bytes.len() != CLEAN_LEN || &bytes[0..8] != CLEAN_MAGIC.as_slice() {
        return None;
    }
    if u32::from_le_bytes(bytes[8..12].try_into().ok()?) != 1 {
        return None;
    }

    let stored = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
    let mut copy = bytes.clone();
    copy[24..28].fill(0);
    if stored != crc32c::crc32c(&copy) {
        return None;
    }

    let txn_id = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
    (txn_id > 0).then_some(txn_id)
}

/// Writes the clean-shutdown marker.
fn write_clean_marker(path: &Path, txn_id: u64) -> Result<()> {
    let mut bytes = [0u8; CLEAN_LEN];
    bytes[0..8].copy_from_slice(&CLEAN_MAGIC);
    bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&txn_id.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes);
    bytes[24..28].copy_from_slice(&checksum.to_le_bytes());

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join(CLEAN_FILE))
        .with_context(|| format!("failed to open `{}`", path.join(CLEAN_FILE).display()))?;
    io::write_all_at(&file, &bytes, 0)?;
    file.set_len(CLEAN_LEN as u64)?;
    file.sync_data()?;

    Ok(())
}

/// Options used to open an [`Env`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnvOpenOptions {
    /// The maximum size of the environment, in bytes.
    ///
    /// The data file is extended sparsely to this size. When the logical page
    /// range reaches the end of the map, writes fail with a clear error.
    pub map_size: usize,
    /// The maximum number of named databases that can be created.
    pub max_dbs: usize,
    /// Whether the environment is opened read-only.
    pub read_only: bool,
    /// Whether a writable environment may coexist with read-only processes.
    ///
    /// This disables cross-transaction page reuse so snapshots held by other
    /// processes remain valid until they are dropped.
    pub multi_process: bool,
}

impl Default for EnvOpenOptions {
    fn default() -> Self {
        Self {
            map_size: DEFAULT_MAP_SIZE,
            max_dbs: DEFAULT_MAX_DBS,
            read_only: false,
            multi_process: false,
        }
    }
}

impl EnvOpenOptions {
    /// Creates options with the default map size and database limit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum size of the environment, in bytes.
    pub fn map_size(&mut self, map_size: usize) -> &mut Self {
        self.map_size = map_size;
        self
    }

    /// Sets the maximum number of named databases.
    pub fn max_dbs(&mut self, max_dbs: usize) -> &mut Self {
        self.max_dbs = max_dbs;
        self
    }

    /// Opens the environment without write access.
    ///
    /// Read-only environments take a shared file lock. Multiple read-only
    /// processes can share an environment. A writable environment can be open
    /// at the same time only when it enables [`Self::multi_process`].
    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Allows a writable environment to coexist with read-only environments.
    ///
    /// Cross-transaction page reuse is disabled in this mode so pages retained
    /// by another process's snapshot are never overwritten. The data file can
    /// grow until it is compacted offline.
    pub fn multi_process(&mut self, multi_process: bool) -> &mut Self {
        self.multi_process = multi_process;
        self
    }

    /// Opens the environment in `path`.
    ///
    /// A writable environment creates the directory and data file when needed.
    /// A read-only environment requires an existing, initialized data file and
    /// takes a shared lock.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or opened, the
    /// environment is already locked by another process, the on-disk format is
    /// invalid, or the data file cannot be mapped.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Env> {
        let path = path.as_ref();
        if self.read_only {
            ensure!(
                path.is_dir(),
                "environment `{}` does not exist",
                path.display()
            );
        } else {
            fs::create_dir_all(path).with_context(|| {
                format!(
                    "failed to create the environment directory `{}`",
                    path.display()
                )
            })?;
        }

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join(LOCK_FILE))
            .with_context(|| {
                format!(
                    "failed to open the environment lock `{}`",
                    path.join(LOCK_FILE).display()
                )
            })?;

        let shared_lifecycle = self.read_only || self.multi_process;
        let lock_result = if shared_lifecycle {
            fs4::FileExt::try_lock_shared(&lock)
        } else {
            fs4::FileExt::try_lock(&lock)
        };
        match lock_result {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                if shared_lifecycle {
                    bail!("environment `{}` is open for writing", path.display())
                } else {
                    bail!("environment `{}` is already locked", path.display())
                }
            }
            Err(TryLockError::Error(error)) => {
                return Err(error).with_context(|| {
                    format!("failed to lock the environment `{}`", path.display())
                });
            }
        }

        let writer_lock = if self.multi_process && !self.read_only {
            let writer_path = path.join(WRITER_LOCK_FILE);
            let writer_lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&writer_path)
                .with_context(|| format!("failed to open `{}`", writer_path.display()))?;
            match fs4::FileExt::try_lock(&writer_lock) {
                Ok(()) => Some(writer_lock),
                Err(TryLockError::WouldBlock) => {
                    bail!(
                        "environment `{}` already has an active writable owner",
                        path.display()
                    )
                }
                Err(TryLockError::Error(error)) => {
                    return Err(error)
                        .with_context(|| format!("failed to lock `{}`", writer_path.display()));
                }
            }
        } else {
            None
        };

        let data_path = path.join(DATA_FILE);
        let data = if self.read_only {
            OpenOptions::new()
                .read(true)
                .open(&data_path)
                .with_context(|| format!("failed to open `{}`", data_path.display()))?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&data_path)
                .with_context(|| format!("failed to open `{}`", data_path.display()))?
        };

        let existing_len = data
            .metadata()
            .with_context(|| format!("failed to inspect `{}`", data_path.display()))?
            .len();
        let requested_map_size = normalize_map_size(self.map_size)?;

        let (candidates, map_size, file_covers_newest) = if self.read_only {
            ensure!(
                existing_len >= (2 * PAGE_SIZE) as u64,
                "environment `{}` is not initialized",
                path.display()
            );
            let loaded = meta::load_all(&data, &data_path)?;
            let newest = &loaded[0];
            let minimum = pages_to_bytes(newest.meta.page_count)?;
            let file_len =
                usize::try_from(existing_len).context("data file is too large to map")?;
            let map_size = requested_map_size.max(file_len);
            ensure!(
                map_size >= minimum,
                "read-only map size is smaller than the committed page count"
            );
            let covers = existing_len >= minimum as u64;
            (loaded, map_size, covers)
        } else if existing_len == 0 {
            #[cfg(windows)]
            let initial_len = requested_map_size as u64;
            #[cfg(not(windows))]
            let initial_len = (2 * PAGE_SIZE) as u64;
            data.set_len(initial_len)
                .with_context(|| format!("failed to size `{}`", data_path.display()))?;
            let meta = meta::initialize(&data).context("failed to initialize the environment")?;
            data.sync_all()
                .with_context(|| format!("failed to sync `{}`", data_path.display()))?;
            io::sync_dir(path).with_context(|| format!("failed to sync `{}`", path.display()))?;
            (
                vec![meta::LoadedMeta { meta, slot: 0 }],
                requested_map_size,
                true,
            )
        } else {
            let loaded = meta::load_all(&data, &data_path)?;
            let newest = &loaded[0];
            let minimum = pages_to_bytes(newest.meta.page_count)?;
            #[cfg(windows)]
            let mut covers = existing_len >= minimum as u64;
            #[cfg(not(windows))]
            let covers = existing_len >= minimum as u64;
            if !covers {
                data.set_len(minimum as u64)
                    .with_context(|| format!("failed to size `{}`", data_path.display()))?;
            }

            let map_size = requested_map_size.max(minimum);

            #[cfg(windows)]
            if data.metadata()?.len() < map_size as u64 {
                data.set_len(map_size as u64)
                    .with_context(|| format!("failed to size `{}`", data_path.display()))?;
                data.sync_all()
                    .with_context(|| format!("failed to sync `{}`", data_path.display()))?;
                covers = false;
            }

            (loaded, map_size, covers)
        };

        let data_offset = (2 * PAGE_SIZE) as u64;
        let mapped_len = map_size - 2 * PAGE_SIZE;
        let mmap = if mapped_len == 0 {
            mmap::map_empty().context("failed to map an empty data region")?
        } else {
            mmap::map_data(&data, data_offset, mapped_len)
                .with_context(|| format!("failed to map `{}`", data_path.display()))?
        };
        debug_assert!(mmap.len() >= mapped_len.max(1));

        let mut chosen = None;
        let mut errors = Vec::new();
        let clean_txn = read_clean_marker(path);
        for candidate in candidates {
            if clean_txn == Some(candidate.meta.txn_id) && file_covers_newest {
                chosen = Some(candidate);
                break;
            }
            match commit::validate(&mmap, &candidate.meta) {
                Ok(()) => {
                    chosen = Some(candidate);
                    break;
                }
                Err(error) => errors.push(format!("slot {}: {error:#}", candidate.slot)),
            }
        }
        let chosen = match chosen {
            Some(committed) => committed,
            None => bail!(
                "no recoverable commit in `{}`: {}",
                path.display(),
                errors.join("; ")
            ),
        };
        let meta = chosen.meta;
        let meta_slot = chosen.slot;

        let verified_words = (map_size / PAGE_SIZE).div_ceil(64);
        let verified = (0..verified_words).map(|_| AtomicU64::new(0)).collect();

        let inner = EnvInner {
            path: path.to_path_buf(),
            map_size,
            max_dbs: self.max_dbs,
            data,
            mmap,
            read_only: self.read_only,
            multi_process: self.multi_process && !self.read_only,
            page_reuse: !self.read_only && !self.multi_process,
            committed: RwLock::new(Committed {
                meta,
                slot: meta_slot,
            }),
            writer: Mutex::new(()),
            readers: Mutex::new(BTreeMap::new()),
            free_pool: Mutex::new(Vec::new()),
            free_pool_ready: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            clean_marker_on_drop: AtomicBool::new(!self.read_only),
            verified,
            _lock: lock,
            _writer_lock: writer_lock,
        };

        Ok(Env {
            inner: Arc::new(inner),
        })
    }
}

/// Validates a user-provided named database name.
fn validate_database_name(name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "database name must not be empty");
    ensure!(
        name.len() <= 255,
        "database name is {} bytes, maximum is 255",
        name.len()
    );
    Ok(())
}

/// Rounds `map_size` up to a page boundary and enforces the minimum size.
fn normalize_map_size(requested: usize) -> Result<usize> {
    ensure!(
        requested >= MIN_MAP_SIZE,
        "map_size must be at least {MIN_MAP_SIZE} bytes, got {requested}"
    );

    requested
        .checked_add(PAGE_SIZE - 1)
        .map(|size| size / PAGE_SIZE * PAGE_SIZE)
        .context("map_size is too large")
}

/// Converts a page count to a byte size, checking for overflow.
fn pages_to_bytes(page_count: u64) -> Result<usize> {
    let pages = usize::try_from(page_count).context("page count does not fit in usize")?;
    pages
        .checked_mul(PAGE_SIZE)
        .context("environment size overflows usize")
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};

    use tempfile::tempdir;

    use super::*;

    fn options() -> EnvOpenOptions {
        let mut options = EnvOpenOptions::new();
        options.map_size(16 * 1024 * 1024);
        options
    }

    #[test]
    fn defaults_are_applied() {
        let options = EnvOpenOptions::new();

        assert_eq!(options.map_size, DEFAULT_MAP_SIZE);
        assert_eq!(options.max_dbs, DEFAULT_MAX_DBS);
        assert!(!options.read_only);
        assert!(!options.multi_process);
    }

    #[test]
    fn builder_updates_options() {
        let mut options = EnvOpenOptions::new();
        options
            .map_size(4096 * 4)
            .max_dbs(2)
            .read_only(true)
            .multi_process(true);

        assert_eq!(options.map_size, 4096 * 4);
        assert_eq!(options.max_dbs, 2);
        assert!(options.read_only);
        assert!(options.multi_process);
    }

    #[test]
    fn open_creates_environment_and_reports_info() {
        let dir = tempdir().unwrap();
        let env = options().open(dir.path()).unwrap();

        assert_eq!(env.path(), dir.path());
        assert!(dir.path().join(DATA_FILE).is_file());
        assert!(dir.path().join(LOCK_FILE).is_file());

        let info = env.info();
        assert_eq!(info.map_size, 16 * 1024 * 1024);
        assert_eq!(info.page_size, PAGE_SIZE);
        assert_eq!(info.page_count, 2);
        assert_eq!(info.txn_id, 1);
        assert_eq!(info.database_count, 0);
        assert_eq!(info.max_dbs, DEFAULT_MAX_DBS);
        assert_eq!(info.format_version, meta::FORMAT_VERSION);
    }

    #[test]
    fn reopen_reads_the_same_meta() {
        let dir = tempdir().unwrap();
        let info = {
            let env = options().open(dir.path()).unwrap();
            env.info()
        };

        let env = options().open(dir.path()).unwrap();
        assert_eq!(env.info(), info);
    }

    #[test]
    fn map_size_is_rounded_up_to_a_page() {
        let dir = tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(MIN_MAP_SIZE + 1);

        let env = options.open(dir.path()).unwrap();
        assert_eq!(env.info().map_size, 4 * PAGE_SIZE);
    }

    #[test]
    fn map_size_below_the_meta_area_is_rejected() {
        let dir = tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(MIN_MAP_SIZE - 1);

        assert!(options.open(dir.path()).is_err());
    }

    #[test]
    fn second_open_is_rejected_while_locked() {
        let dir = tempdir().unwrap();
        let _env = options().open(dir.path()).unwrap();

        let error = options().open(dir.path()).unwrap_err();
        assert!(error.to_string().contains("already locked"));
    }

    #[test]
    fn lock_is_released_on_drop() {
        let dir = tempdir().unwrap();
        drop(options().open(dir.path()).unwrap());

        assert!(options().open(dir.path()).is_ok());
    }

    #[test]
    fn truncated_data_file_is_rejected() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(DATA_FILE), b"x").unwrap();

        assert!(options().open(dir.path()).is_err());
    }

    #[test]
    fn corrupt_meta_is_rejected() {
        let dir = tempdir().unwrap();
        drop(options().open(dir.path()).unwrap());

        let path = dir.path().join(DATA_FILE);
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(std::io::SeekFrom::Start(100)).unwrap();
        file.write_all(&[0xff]).unwrap();

        assert!(options().open(dir.path()).is_err());
    }

    #[test]
    fn corrupt_inactive_slot_falls_back_to_the_active_slot() {
        let dir = tempdir().unwrap();
        drop(options().open(dir.path()).unwrap());

        let path = dir.path().join(DATA_FILE);
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(std::io::SeekFrom::Start(PAGE_SIZE as u64 + 100))
            .unwrap();
        file.write_all(&[0xff]).unwrap();

        let env = options().open(dir.path()).unwrap();
        assert_eq!(env.info().txn_id, 1);
    }
}
