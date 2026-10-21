//! Offline environment compaction.
//!
//! Compaction requires the environment to be closed. It copies every live page
//! into a fresh data file, writes a new meta page, and atomically replaces the
//! old file.

use std::fs::{self, File, OpenOptions};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use fs4::TryLockError;
use memmap2::Mmap;

use crate::comparator::{KeyComparator, LEXICOGRAPHIC};
use crate::iter::Cursor;
use crate::storage::PAGE_SIZE;
use crate::storage::btree::{self, PageSource};
use crate::storage::commit;
use crate::storage::meta::{self, Meta};
use crate::storage::mmap;
use crate::storage::overflow::{self, OverflowRef};
use crate::storage::page::Page;
use crate::storage::store::PageStore;

const DATA_FILE: &str = "data.tsubame";
const LOCK_FILE: &str = "lock.tsubame";
const COMPACT_FILE: &str = "data.tsubame.compact";

/// Rewrites an environment into a compact data file.
pub(crate) fn compact_with(
    path: &Path,
    comparator: KeyComparator,
    comparator_id: &str,
) -> Result<()> {
    ensure!(path.is_dir(), "`{}` is not a directory", path.display());
    match crate::env::read_comparator_id(path)? {
        Some(stored) => ensure!(
            stored == comparator_id,
            "comparator id mismatch: data file uses `{stored}`, requested `{comparator_id}`"
        ),
        None => ensure!(
            comparator.is_lexicographic() && comparator_id == "lexicographic",
            "comparator identity is missing for a custom comparator"
        ),
    }
    let data_path = path.join(DATA_FILE);
    ensure!(
        data_path.is_file(),
        "`{}` is not a sparkdb environment",
        path.display()
    );

    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join(LOCK_FILE))
        .with_context(|| format!("failed to open `{}`", path.join(LOCK_FILE).display()))?;

    match fs4::FileExt::try_lock(&lock) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            bail!(
                "environment `{}` is open; close it before compaction",
                path.display()
            )
        }
        Err(TryLockError::Error(error)) => {
            return Err(error)
                .with_context(|| format!("failed to lock the environment `{}`", path.display()));
        }
    }

    crate::env::reset_reader_registry(path)
        .context("failed to reset the reader registry before compaction")?;

    let data = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&data_path)
        .with_context(|| format!("failed to open `{}`", data_path.display()))?;
    let data_len = data
        .metadata()
        .with_context(|| format!("failed to inspect `{}`", data_path.display()))?
        .len();
    ensure!(
        data_len >= (2 * PAGE_SIZE) as u64,
        "data file `{}` is truncated",
        data_path.display()
    );

    let candidates = meta::load_all(&data, &data_path)?;
    let data_offset = (2 * PAGE_SIZE) as u64;
    let mapped_len = usize::try_from(data_len - data_offset).context("data file is too large")?;
    let mapped: Option<Mmap> = if mapped_len == 0 {
        None
    } else {
        Some(
            mmap::map_data(&data, data_offset, mapped_len)
                .with_context(|| format!("failed to map `{}`", data_path.display()))?,
        )
    };
    let map_slice = mapped.as_deref().unwrap_or(&[]);

    let mut chosen = None;
    let mut errors = Vec::new();
    for candidate in candidates {
        match commit::validate(map_slice, &candidate.meta) {
            Ok(()) => {
                chosen = Some(candidate.meta);
                break;
            }
            Err(error) => errors.push(format!("slot {}: {error:#}", candidate.slot)),
        }
    }
    let old_meta = match chosen {
        Some(meta) => meta,
        None => bail!(
            "no recoverable commit in `{}`: {}",
            path.display(),
            errors.join("; ")
        ),
    };

    let (unnamed_entries, named_entries) = {
        let source = MmapSource {
            data: map_slice,
            page_count: old_meta.page_count,
            comparator,
        };
        let catalog_source = MmapSource {
            data: map_slice,
            page_count: old_meta.page_count,
            comparator: LEXICOGRAPHIC,
        };
        let unnamed = collect_entries(&source, old_meta.unnamed_root)?;
        let catalog = collect_entries(&catalog_source, old_meta.catalog_root)?;
        let mut named = Vec::with_capacity(catalog.len());

        for (name, root_bytes) in catalog {
            ensure!(
                root_bytes.len() == 8,
                "catalog entry has {} bytes, expected 8",
                root_bytes.len()
            );
            let mut root = [0u8; 8];
            root.copy_from_slice(&root_bytes);
            let root = u64::from_le_bytes(root);
            let entries = collect_entries(&source, root)?;
            named.push((name, entries));
        }

        ensure!(
            u32::try_from(named.len()).context("too many named databases")?
                == old_meta.database_count,
            "catalog has {} entries, meta records {}",
            named.len(),
            old_meta.database_count
        );

        (unnamed, named)
    };

    drop(mapped);
    drop(data);

    let temp_path = path.join(COMPACT_FILE);
    if temp_path.exists() {
        fs::remove_file(&temp_path)
            .with_context(|| format!("failed to remove `{}`", temp_path.display()))?;
    }

    let result = (|| {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .with_context(|| format!("failed to create `{}`", temp_path.display()))?;
        file.set_len((2 * PAGE_SIZE) as u64)?;
        let max_pages = data_len / PAGE_SIZE as u64;
        let mut store = CompactStore {
            file,
            next_page: 2,
            max_pages,
        };

        let txn_id = old_meta
            .txn_id
            .checked_add(1)
            .context("transaction ID overflow")?;
        let unnamed_root =
            btree::bulk_load_with(&mut store, &unnamed_entries, txn_id, &comparator)?;

        let mut catalog_entries = Vec::with_capacity(named_entries.len());
        for (name, entries) in named_entries {
            let root = btree::bulk_load_with(&mut store, &entries, txn_id, &comparator)?;
            catalog_entries.push((name, root.to_le_bytes().to_vec()));
        }
        let catalog_root =
            btree::bulk_load_with(&mut store, &catalog_entries, txn_id, &LEXICOGRAPHIC)?;

        let mut meta = Meta::initial();
        meta.txn_id = txn_id;
        meta.unnamed_root = unnamed_root;
        meta.catalog_root = catalog_root;
        meta.page_count = store.next_page;
        meta.database_count =
            u32::try_from(catalog_entries.len()).context("too many named databases")?;
        meta.validate()?;

        let zero = [0u8; PAGE_SIZE];
        crate::storage::io::write_all_at(&store.file, &zero, PAGE_SIZE as u64)?;
        meta::write_slot(&store.file, 0, &meta)?;
        store.file.sync_all()?;
        drop(store);

        fs::rename(&temp_path, &data_path).with_context(|| {
            format!(
                "failed to replace `{}` with `{}`",
                data_path.display(),
                temp_path.display()
            )
        })?;
        crate::storage::io::sync_dir(path)?;

        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    result
}

struct MmapSource<'a> {
    data: &'a [u8],
    page_count: u64,
    comparator: KeyComparator,
}

impl PageSource for MmapSource<'_> {
    fn page(&self, page_id: u64) -> Result<Page<'_>> {
        ensure!(
            page_id >= 2 && page_id < self.page_count,
            "page {page_id} is outside the committed page range"
        );
        let index = usize::try_from(page_id - 2).context("page ID is too large")?;
        let start = index
            .checked_mul(PAGE_SIZE)
            .context("page offset overflow")?;
        let end = start
            .checked_add(PAGE_SIZE)
            .context("page offset overflow")?;
        ensure!(end <= self.data.len(), "page is outside the mapped region");
        Page::parse(&self.data[start..end], Some(page_id))
    }

    fn overflow(&self, reference: OverflowRef) -> Result<&[u8]> {
        ensure!(
            reference.start_page >= 2,
            "overflow extent points at a reserved page"
        );
        let end_page = reference
            .start_page
            .checked_add(reference.page_count)
            .context("overflow extent page overflow")?;
        ensure!(
            end_page <= self.page_count,
            "overflow extent is outside the committed page range"
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
            end <= self.data.len(),
            "overflow extent is outside the mapped region"
        );
        overflow::validate_extent(&self.data[start..end], reference)
    }

    fn comparator(&self) -> &KeyComparator {
        &self.comparator
    }
}

fn collect_entries<S: PageSource>(source: &S, root: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if root == 0 {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let mut cursor = Cursor::new(source);
    cursor.seek_first(root)?;
    while let Some((key, value)) = cursor.current()? {
        entries.push((key.to_vec(), value.to_vec()));
        cursor.next()?;
    }
    Ok(entries)
}

struct CompactStore {
    file: File,
    next_page: u64,
    max_pages: u64,
}

impl PageStore for CompactStore {
    fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
        let mut page = [0u8; PAGE_SIZE];
        crate::storage::io::read_exact_at(
            &self.file,
            &mut page,
            page_id
                .checked_mul(PAGE_SIZE as u64)
                .context("page offset overflow")?,
        )?;
        Ok(page)
    }

    fn allocate_pages(&mut self, pages: u64) -> Result<u64> {
        ensure!(pages > 0, "allocation must contain at least one page");
        let start = self.next_page;
        self.next_page = self
            .next_page
            .checked_add(pages)
            .context("page ID overflow")?;
        ensure!(
            self.next_page <= self.max_pages,
            "compacted environment exceeds its original size"
        );
        Ok(start)
    }

    fn write_page(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) -> Result<()> {
        ensure!(
            page_id >= 2 && page_id < self.next_page,
            "page {page_id} is outside the allocation range"
        );
        crate::storage::io::write_all_at(
            &self.file,
            &page,
            page_id
                .checked_mul(PAGE_SIZE as u64)
                .context("page offset overflow")?,
        )?;
        Ok(())
    }

    fn write_extent(&mut self, start_page: u64, bytes: &[u8]) -> Result<()> {
        ensure!(
            start_page >= 2 && start_page < self.next_page,
            "extent start is outside the allocation range"
        );
        ensure!(
            bytes.len() % PAGE_SIZE == 0,
            "overflow extent is not page-aligned"
        );
        let pages =
            u64::try_from(bytes.len() / PAGE_SIZE).context("overflow extent is too large")?;
        ensure!(
            start_page
                .checked_add(pages)
                .context("overflow extent page overflow")?
                <= self.next_page,
            "overflow extent exceeds the allocation range"
        );
        crate::storage::io::write_all_at(
            &self.file,
            bytes,
            start_page
                .checked_mul(PAGE_SIZE as u64)
                .context("extent offset overflow")?,
        )?;
        Ok(())
    }

    fn free_pages(&mut self, _start_page: u64, _pages: u64) -> Result<()> {
        Ok(())
    }
}
