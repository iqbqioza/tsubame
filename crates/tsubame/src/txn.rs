//! Read-only and read-write transactions.
//!
//! Every database operation runs inside a transaction. Read-only transactions
//! provide a consistent snapshot and can coexist; a read-write transaction has
//! exclusive access to the environment.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, MutexGuard, OnceLock};

use anyhow::{Context, Result, bail, ensure};

use crate::comparator::KeyComparator;
use crate::database::DatabaseKind;
use crate::env::{Committed, Env, EnvInner, EnvState, FreeExtent};
use crate::storage::btree::PageSource;
use crate::storage::btree::{self, BulkValue, ValueRef};
use crate::storage::commit::{self, CommitEntry, Descriptor};
use crate::storage::meta::{self, InlineCommit, Meta};
use crate::storage::overflow::{self, OverflowRef};
use crate::storage::page;
use crate::storage::page::{INLINE_BUDGET, Page};
use crate::storage::store::PageStore;
use crate::storage::{PAGE_SIZE, PageSet, io};

/// Transactions writing at least this many entries into an empty database
/// build the tree bottom-up instead of inserting one path at a time.
const BULK_LOAD_THRESHOLD: usize = 1024;

/// Maximum number of contiguous overlay pages combined into one write.
const WRITE_BATCH_PAGES: u64 = 256;

/// Keep small page overlays in a vector to avoid a B-tree node per page.
const SMALL_OVERLAY_CAPACITY: usize = 8;

enum PageOverlay {
    Small(Vec<(u64, [u8; PAGE_SIZE])>),
    Large(BTreeMap<u64, [u8; PAGE_SIZE]>),
}

impl PageOverlay {
    fn new() -> Self {
        Self::Small(Vec::new())
    }

    fn len(&self) -> usize {
        match self {
            Self::Small(entries) => entries.len(),
            Self::Large(pages) => pages.len(),
        }
    }

    fn get(&self, page_id: u64) -> Option<&[u8; PAGE_SIZE]> {
        match self {
            Self::Small(entries) => entries
                .iter()
                .find(|(id, _)| *id == page_id)
                .map(|(_, page)| page),
            Self::Large(pages) => pages.get(&page_id),
        }
    }

    fn get_mut(&mut self, page_id: u64) -> Option<&mut [u8; PAGE_SIZE]> {
        match self {
            Self::Small(entries) => entries
                .iter_mut()
                .find(|(id, _)| *id == page_id)
                .map(|(_, page)| page),
            Self::Large(pages) => pages.get_mut(&page_id),
        }
    }

    fn insert(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) {
        match self {
            Self::Small(entries) => {
                if let Some((_, existing)) = entries.iter_mut().find(|(id, _)| *id == page_id) {
                    *existing = page;
                    return;
                }
                if entries.len() < SMALL_OVERLAY_CAPACITY {
                    entries.push((page_id, page));
                    return;
                }

                let mut pages: BTreeMap<u64, [u8; PAGE_SIZE]> = entries.drain(..).collect();
                pages.insert(page_id, page);
                *self = Self::Large(pages);
            }
            Self::Large(pages) => {
                pages.insert(page_id, page);
            }
        }
    }

    fn remove(&mut self, page_id: u64) {
        match self {
            Self::Small(entries) => entries.retain(|(id, _)| *id != page_id),
            Self::Large(pages) => {
                pages.remove(&page_id);
            }
        }
    }

    fn iter(&self) -> PageOverlayIter<'_> {
        match self {
            Self::Small(entries) => PageOverlayIter::Small(entries.iter()),
            Self::Large(pages) => PageOverlayIter::Large(pages.iter()),
        }
    }
}

enum PageOverlayIter<'a> {
    Small(std::slice::Iter<'a, (u64, [u8; PAGE_SIZE])>),
    Large(std::collections::btree_map::Iter<'a, u64, [u8; PAGE_SIZE]>),
}

impl<'a> Iterator for PageOverlayIter<'a> {
    type Item = (&'a u64, &'a [u8; PAGE_SIZE]);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(entries) => entries.next().map(|(id, page)| (id, page)),
            Self::Large(pages) => pages.next(),
        }
    }
}

const INLINE_PENDING_VALUE: usize = 32;

#[derive(Clone)]
enum PendingValue {
    Inline {
        bytes: [u8; INLINE_PENDING_VALUE],
        len: u8,
    },
    Owned(Vec<u8>),
    Mapped {
        reference: OverflowRef,
        state: Arc<EnvState>,
        offset: usize,
        len: usize,
    },
}

impl PendingValue {
    fn new(value: &[u8]) -> Self {
        if value.len() <= INLINE_PENDING_VALUE {
            let mut bytes = [0u8; INLINE_PENDING_VALUE];
            bytes[..value.len()].copy_from_slice(value);
            Self::Inline {
                bytes,
                len: value.len() as u8,
            }
        } else {
            Self::Owned(value.to_vec())
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, len } => &bytes[..*len as usize],
            Self::Owned(value) => value,
            Self::Mapped {
                state, offset, len, ..
            } => &state.mmap[*offset..*offset + *len],
        }
    }
}

impl PendingValue {
    fn is_mapped(&self) -> bool {
        matches!(self, Self::Mapped { .. })
    }
}

impl BulkValue for PendingValue {
    fn value_ref(&self) -> ValueRef<'_> {
        match self {
            Self::Mapped { reference, .. } => ValueRef::Overflow(*reference),
            _ => ValueRef::Inline(self.as_slice()),
        }
    }
}

const INLINE_PENDING_KEY: usize = 16;

/// A staged key that stays inline for short keys such as fixed-width integers.
#[derive(Clone)]
enum PendingKey {
    Inline {
        bytes: [u8; INLINE_PENDING_KEY],
        len: u8,
    },
    Owned(Vec<u8>),
}

impl PendingKey {
    fn new(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE_PENDING_KEY {
            let mut inline = [0u8; INLINE_PENDING_KEY];
            inline[..bytes.len()].copy_from_slice(bytes);
            Self::Inline {
                bytes: inline,
                len: bytes.len() as u8,
            }
        } else {
            Self::Owned(bytes.to_vec())
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, len } => &bytes[..*len as usize],
            Self::Owned(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for PendingKey {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for PendingKey {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for PendingKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for PendingKey {}

impl PartialOrd for PendingKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

const UNORDERED_PENDING_THRESHOLD: usize = 1024;

struct UnorderedPending {
    entries: Vec<(PendingKey, Option<PendingValue>, usize)>,
    mapped: BTreeMap<PendingKey, PendingValue>,
    materialized: OnceLock<BTreeMap<PendingKey, Option<PendingValue>>>,
}

impl UnorderedPending {
    fn materialized(&self) -> &BTreeMap<PendingKey, Option<PendingValue>> {
        self.materialized.get_or_init(|| {
            let mut map = BTreeMap::new();
            for (key, value, _) in &self.entries {
                map.insert(key.clone(), value.clone());
            }
            map
        })
    }
}

enum Pending {
    Empty,
    One(PendingKey, Option<PendingValue>),
    Map(BTreeMap<PendingKey, Option<PendingValue>>),
    Unordered(UnorderedPending),
}

impl Pending {
    fn new() -> Self {
        Self::Empty
    }

    fn insert(&mut self, key: PendingKey, value: Option<PendingValue>) -> Option<PendingValue> {
        match std::mem::replace(self, Self::Empty) {
            Self::Empty => {
                *self = Self::One(key, value);
                None
            }
            Self::One(existing_key, existing_value) => {
                if existing_key == key {
                    *self = Self::One(existing_key, value);
                    existing_value
                } else {
                    let mut map = BTreeMap::new();
                    map.insert(existing_key, existing_value);
                    map.insert(key, value);
                    *self = Self::Map(map);
                    None
                }
            }
            Self::Map(mut map) => {
                let previous = map.insert(key, value).flatten();
                if map.len() >= UNORDERED_PENDING_THRESHOLD {
                    let entries: Vec<_> = map
                        .into_iter()
                        .enumerate()
                        .map(|(sequence, (key, value))| (key, value, sequence))
                        .collect();
                    let mapped = entries
                        .iter()
                        .filter_map(|(key, value, _)| match value {
                            Some(value) if value.is_mapped() => Some((key.clone(), value.clone())),
                            _ => None,
                        })
                        .collect();
                    *self = Self::Unordered(UnorderedPending {
                        entries,
                        mapped,
                        materialized: OnceLock::new(),
                    });
                } else {
                    *self = Self::Map(map);
                }
                previous
            }
            Self::Unordered(mut pending) => {
                let previous = match value.as_ref() {
                    Some(mapped @ PendingValue::Mapped { .. }) => {
                        pending.mapped.insert(key.clone(), mapped.clone())
                    }
                    _ if !pending.mapped.is_empty() => pending.mapped.remove(&key),
                    _ => None,
                };
                if let Some(PendingValue::Mapped { reference, .. }) = &previous
                    && let Some(index) = pending.entries.iter().rposition(|(existing, value, _)| {
                        existing == &key
                            && matches!(
                                value,
                                Some(PendingValue::Mapped { reference: old, .. })
                                    if old.start_page == reference.start_page
                            )
                    })
                {
                    pending.entries.remove(index);
                }
                let sequence = pending.entries.len();
                if let Some(map) = pending.materialized.get_mut() {
                    map.insert(key.clone(), value.clone());
                }
                pending.entries.push((key, value, sequence));
                *self = Self::Unordered(pending);
                previous
            }
        }
    }

    fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        match self {
            Self::Empty => None,
            Self::One(pending_key, value) => (pending_key.as_slice() == key)
                .then_some(value.as_ref().map(PendingValue::as_slice)),
            Self::Map(map) => map
                .get(key)
                .map(|value| value.as_ref().map(PendingValue::as_slice)),
            Self::Unordered(pending) => pending
                .materialized()
                .get(key)
                .map(|value| value.as_ref().map(PendingValue::as_slice)),
        }
    }

    fn iter(&self, _comparator: KeyComparator) -> PendingIter<'_> {
        match self {
            Self::Empty => Box::new(std::iter::empty()),
            Self::One(key, value) => Box::new(std::iter::once((
                key.as_slice(),
                value.as_ref().map(PendingValue::as_slice),
            ))),
            Self::Map(map) => {
                Box::new(map.iter().map(|(key, value)| {
                    (key.as_slice(), value.as_ref().map(PendingValue::as_slice))
                }))
            }
            Self::Unordered(pending) => {
                Box::new(pending.materialized().iter().map(|(key, value)| {
                    (key.as_slice(), value.as_ref().map(PendingValue::as_slice))
                }))
            }
        }
    }

    fn iter_rev(&self, _comparator: KeyComparator) -> PendingRevIter<'_> {
        match self {
            Self::Empty => Box::new(std::iter::empty()),
            Self::One(key, value) => Box::new(std::iter::once((
                key.as_slice(),
                value.as_ref().map(PendingValue::as_slice),
            ))),
            Self::Map(map) => {
                Box::new(map.iter().rev().map(|(key, value)| {
                    (key.as_slice(), value.as_ref().map(PendingValue::as_slice))
                }))
            }
            Self::Unordered(pending) => {
                Box::new(pending.materialized().iter().rev().map(|(key, value)| {
                    (key.as_slice(), value.as_ref().map(PendingValue::as_slice))
                }))
            }
        }
    }
}

fn fixed_key_u64(key: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes[..key.len()].copy_from_slice(key);
    u64::from_be_bytes(bytes)
}

fn sort_unordered_entries(
    entries: &mut [(PendingKey, Option<PendingValue>, usize)],
    comparator: KeyComparator,
) {
    if comparator.is_lexicographic()
        && let Some((first, rest)) = entries.split_first()
    {
        let key_len = first.0.as_slice().len();
        let same_length = rest.iter().all(|entry| entry.0.as_slice().len() == key_len);
        if same_length && key_len <= 8 {
            entries.sort_by_cached_key(|entry| (fixed_key_u64(entry.0.as_slice()), entry.2));
            return;
        }
        if same_length && key_len == 16 {
            entries.sort_by_cached_key(|entry| {
                let mut key = [0u8; 16];
                key.copy_from_slice(entry.0.as_slice());
                (u128::from_be_bytes(key), entry.2)
            });
            return;
        }
    }

    if comparator.is_lexicographic() {
        entries.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2))
        });
    } else {
        entries.sort_unstable_by(|left, right| {
            comparator
                .compare(left.0.as_slice(), right.0.as_slice())
                .then_with(|| left.2.cmp(&right.2))
        });
    }
}

fn deduplicate_unordered(
    entries: Vec<(PendingKey, Option<PendingValue>, usize)>,
) -> Vec<(PendingKey, Option<PendingValue>)> {
    let mut deduplicated: Vec<(PendingKey, Option<PendingValue>)> =
        Vec::with_capacity(entries.len());
    for (key, value, _) in entries {
        if let Some((last_key, last_value)) = deduplicated.last_mut()
            && *last_key == key
        {
            *last_value = value;
            continue;
        }
        deduplicated.push((key, value));
    }
    deduplicated
}

pub(crate) type PendingIter<'a> = Box<dyn Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a>;
pub(crate) type PendingRevIter<'a> = Box<dyn Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a>;

fn sort_pending<'a>(
    pending: PendingIter<'a>,
    comparator: KeyComparator,
    reverse: bool,
) -> PendingIter<'a> {
    if comparator.is_lexicographic() {
        return pending;
    }
    let mut entries: Vec<_> = pending.collect();
    entries.sort_by(|left, right| comparator.compare(left.0, right.0));
    if reverse {
        entries.reverse();
    }
    Box::new(entries.into_iter())
}

/// A read-only transaction.
///
/// Read-only transactions see a stable snapshot of the environment until they
/// are dropped. They are created with [`crate::Env::read_txn`].
pub struct RoTxn<'env> {
    env: &'env Env,
    state: Arc<EnvState>,
    meta: Meta,
    reader_slot: Option<u64>,
}

impl<'env> RoTxn<'env> {
    pub(crate) fn new(
        env: &'env Env,
        state: Arc<EnvState>,
        meta: Meta,
        reader_slot: Option<u64>,
    ) -> Self {
        Self {
            env,
            state,
            meta,
            reader_slot,
        }
    }

    #[inline]
    pub(crate) fn comparator(&self) -> KeyComparator {
        self.env.inner.comparator
    }

    pub(crate) fn root_for(&self, kind: &DatabaseKind) -> Result<u64> {
        match kind {
            DatabaseKind::Unnamed => Ok(self.meta.unnamed_root),
            DatabaseKind::Named(name) => {
                let Some(root) = self.catalog_lookup(name)? else {
                    bail!("database `{name}` does not exist");
                };
                Ok(root)
            }
        }
    }

    pub(crate) fn catalog_lookup(&self, name: &str) -> Result<Option<u64>> {
        let Some(bytes) = btree::get(self, self.meta.catalog_root, name.as_bytes())? else {
            return Ok(None);
        };
        ensure!(
            bytes.len() == 8,
            "catalog entry for `{name}` has {} bytes, expected 8",
            bytes.len()
        );
        let mut root = [0u8; 8];
        root.copy_from_slice(bytes);
        Ok(Some(u64::from_le_bytes(root)))
    }

    pub(crate) fn belongs_to(&self, env: &Arc<EnvInner>) -> bool {
        Arc::ptr_eq(&self.env.inner, env)
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        self.env.inner.release_reader(self.meta.txn_id);
        if let Some(slot) = self.reader_slot {
            self.env.inner.release_external_reader(slot);
        }
    }
}

impl PageSource for RoTxn<'_> {
    #[inline]
    fn page(&self, page_id: u64) -> Result<Page<'_>> {
        self.state.read_page(page_id, &self.meta)
    }

    fn overflow(&self, reference: OverflowRef) -> Result<&[u8]> {
        self.state.read_overflow(reference, &self.meta)
    }

    fn comparator(&self) -> &crate::comparator::KeyComparator {
        &self.env.inner.comparator
    }
}

/// A read-write transaction.
///
/// Writes are made through a [`RwTxn`] and become visible to new read
/// transactions only after [`RwTxn::commit`] succeeds.
pub struct RwTxn<'env> {
    env: &'env Env,
    state: Arc<EnvState>,
    base: Meta,
    base_slot: u8,
    txn_id: u64,
    overlay: PageOverlay,
    dirty_pages: PageSet,
    unsealed_pages: PageSet,
    direct_extent_pages: BTreeMap<u64, u64>,
    extents: BTreeMap<u64, Vec<u8>>,
    next_page: u64,
    free_single: BTreeSet<u64>,
    free_runs: BTreeMap<u64, u64>,
    // Free extents taken from the environment at the start of this
    // transaction. They are safe for the committed snapshots and active
    // readers, but are not part of this transaction yet.
    cross_free: Vec<FreeExtent>,
    // Pages from `cross_free` that this transaction has allocated. They are
    // returned to the environment if the transaction is aborted.
    cross_allocated: Vec<FreeExtent>,
    // Pages reachable from the previous committed snapshot that this
    // transaction replaced. They become reusable after two more commits.
    base_freed: Vec<FreeExtent>,
    // Pages allocated by this transaction. Page IDs alone do not distinguish
    // an old base page from a reused free page below the old high-water mark.
    owned_pages: PageSet,
    deferred_free: Vec<FreeExtent>,
    unnamed_pending: Pending,
    named_pending: BTreeMap<Arc<str>, Pending>,
    cleared_named: BTreeSet<Arc<str>>,
    catalog_root: u64,
    database_count: u32,
    root: Option<u64>,
    failed: bool,
    _writer: MutexGuard<'env, ()>,
}

impl<'env> RwTxn<'env> {
    pub(crate) fn new(
        env: &'env Env,
        writer: MutexGuard<'env, ()>,
        committed: Committed,
        state: Arc<EnvState>,
    ) -> Result<Self> {
        let txn_id = committed
            .meta
            .txn_id
            .checked_add(1)
            .context("transaction ID overflow")?;
        let cross_free = if env.inner.page_reuse {
            let oldest_reader = env.inner.oldest_reader_snapshot(committed.meta.txn_id);
            let free_limit = committed.meta.txn_id.saturating_sub(1).min(oldest_reader);
            env.inner.take_reusable_free(free_limit)
        } else {
            Vec::new()
        };

        Ok(Self {
            env,
            state,
            base: committed.meta,
            base_slot: committed.slot,
            txn_id,
            overlay: PageOverlay::new(),
            dirty_pages: PageSet::default(),
            unsealed_pages: PageSet::default(),
            direct_extent_pages: BTreeMap::new(),
            extents: BTreeMap::new(),
            next_page: committed.meta.page_count,
            free_single: BTreeSet::new(),
            free_runs: BTreeMap::new(),
            cross_free,
            cross_allocated: Vec::new(),
            base_freed: Vec::new(),
            owned_pages: PageSet::default(),
            deferred_free: Vec::new(),
            unnamed_pending: Pending::new(),
            named_pending: BTreeMap::new(),
            cleared_named: BTreeSet::new(),
            catalog_root: committed.meta.catalog_root,
            database_count: committed.meta.database_count,
            root: None,
            failed: false,
            _writer: writer,
        })
    }

    pub(crate) fn belongs_to(&self, env: &Arc<EnvInner>) -> bool {
        Arc::ptr_eq(&self.env.inner, env)
    }

    pub(crate) fn put_raw(&mut self, kind: DatabaseKind, key: &[u8], value: &[u8]) -> Result<()> {
        let value = if value.len() > INLINE_BUDGET && self.state.has_write_mmap() {
            let pages = overflow::pages_for(value.len())?;
            let start_page = self.allocate_pages(pages)?;
            let reference = OverflowRef {
                total_len: u32::try_from(value.len()).context("value is larger than 4 GiB")?,
                start_page,
                page_count: pages,
            };
            self.state
                .write_overflow_direct(reference, value, self.txn_id)?;
            self.direct_extent_pages.insert(start_page, pages);
            let offset = usize::try_from(start_page - 2)
                .context("page ID is too large")?
                .checked_mul(PAGE_SIZE)
                .and_then(|offset| offset.checked_add(overflow::EXTENT_HEADER_SIZE))
                .context("overflow payload offset overflow")?;
            PendingValue::Mapped {
                reference,
                state: Arc::clone(&self.state),
                offset,
                len: value.len(),
            }
        } else {
            PendingValue::new(value)
        };
        let key = PendingKey::new(key);
        let previous = match kind {
            DatabaseKind::Unnamed => self.unnamed_pending.insert(key, Some(value)),
            DatabaseKind::Named(name) => self
                .named_pending
                .entry(name)
                .or_insert_with(Pending::new)
                .insert(key, Some(value)),
        };
        self.release_pending_value(previous)
    }

    pub(crate) fn delete_raw(&mut self, kind: DatabaseKind, key: &[u8]) -> Result<()> {
        let key = PendingKey::new(key);
        let previous = match kind {
            DatabaseKind::Unnamed => self.unnamed_pending.insert(key, None),
            DatabaseKind::Named(name) => self
                .named_pending
                .entry(name)
                .or_insert_with(Pending::new)
                .insert(key, None),
        };
        self.release_pending_value(previous)
    }

    fn release_pending_value(&mut self, value: Option<PendingValue>) -> Result<()> {
        if let Some(PendingValue::Mapped { reference, .. }) = value
            && self
                .direct_extent_pages
                .remove(&reference.start_page)
                .is_some()
        {
            self.free_pages(reference.start_page, reference.page_count)?;
        }
        Ok(())
    }

    fn release_pending(&mut self, pending: Pending) -> Result<()> {
        match pending {
            Pending::Empty => Ok(()),
            Pending::One(_, value) => self.release_pending_value(value),
            Pending::Map(map) => {
                for value in map.into_values() {
                    self.release_pending_value(value)?;
                }
                Ok(())
            }
            Pending::Unordered(pending) => {
                for (_, value, _) in pending.entries {
                    self.release_pending_value(value)?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn clear_raw(&mut self, kind: DatabaseKind) -> Result<()> {
        match kind {
            DatabaseKind::Unnamed => {
                let root = self.root.unwrap_or(self.base.unnamed_root);
                if root != 0 {
                    btree::free_tree(self, root)?;
                }
                let pending = std::mem::replace(&mut self.unnamed_pending, Pending::new());
                self.release_pending(pending)?;
                self.root = Some(0);
            }
            DatabaseKind::Named(name) => {
                if !self.cleared_named.contains(&name)
                    && let Some(root) = self.catalog_lookup(&name)?
                    && root != 0
                {
                    btree::free_tree(self, root)?;
                }
                let pending = self.named_pending.remove(&name);
                if let Some(pending) = pending {
                    self.release_pending(pending)?;
                }
                self.cleared_named.insert(name);
            }
        }
        Ok(())
    }

    pub(crate) fn catalog_lookup(&self, name: &str) -> Result<Option<u64>> {
        let Some(bytes) = btree::get(self, self.catalog_root, name.as_bytes())? else {
            return Ok(None);
        };
        ensure!(
            bytes.len() == 8,
            "catalog entry for `{name}` has {} bytes, expected 8",
            bytes.len()
        );
        let mut root = [0u8; 8];
        root.copy_from_slice(bytes);
        Ok(Some(u64::from_le_bytes(root)))
    }

    pub(crate) fn catalog_put(&mut self, name: &str, root: u64) -> Result<()> {
        let bytes = root.to_le_bytes();
        let catalog_root = self.catalog_root;
        let new_root = btree::insert(self, catalog_root, name.as_bytes(), &bytes, self.txn_id)?;
        self.catalog_root = new_root;
        Ok(())
    }

    pub(crate) fn catalog_count(&self) -> u32 {
        self.database_count
    }

    pub(crate) fn comparator(&self) -> KeyComparator {
        self.env.inner.comparator
    }

    pub(crate) fn read_root(&self, kind: &DatabaseKind) -> Result<u64> {
        match kind {
            DatabaseKind::Unnamed => Ok(self.root.unwrap_or(self.base.unnamed_root)),
            DatabaseKind::Named(name) => {
                if self.cleared_named.contains(name) {
                    Ok(0)
                } else {
                    Ok(self.catalog_lookup(name)?.unwrap_or(0))
                }
            }
        }
    }

    pub(crate) fn pending_for<'a>(
        &'a self,
        kind: &DatabaseKind,
        comparator: KeyComparator,
    ) -> PendingIter<'a> {
        let pending: PendingIter<'a> = match kind {
            DatabaseKind::Unnamed => self.unnamed_pending.iter(comparator),
            DatabaseKind::Named(name) => self
                .named_pending
                .get(name.as_ref())
                .map(|pending| pending.iter(comparator))
                .unwrap_or_else(|| Box::new(std::iter::empty())),
        };
        sort_pending(pending, comparator, false)
    }

    pub(crate) fn pending_for_rev<'a>(
        &'a self,
        kind: &DatabaseKind,
        comparator: KeyComparator,
    ) -> PendingRevIter<'a> {
        let pending: PendingRevIter<'a> = match kind {
            DatabaseKind::Unnamed => self.unnamed_pending.iter_rev(comparator),
            DatabaseKind::Named(name) => self
                .named_pending
                .get(name.as_ref())
                .map(|pending| pending.iter_rev(comparator))
                .unwrap_or_else(|| Box::new(std::iter::empty())),
        };
        sort_pending(pending, comparator, true)
    }

    pub(crate) fn pending_get(&self, kind: &DatabaseKind, key: &[u8]) -> Option<Option<&[u8]>> {
        match kind {
            DatabaseKind::Unnamed => self.unnamed_pending.get(key),
            DatabaseKind::Named(name) => self
                .named_pending
                .get(name.as_ref())
                .and_then(|pending| pending.get(key)),
        }
    }

    pub(crate) fn get_raw<'a>(
        &'a self,
        kind: &DatabaseKind,
        key: &[u8],
    ) -> Result<Option<&'a [u8]>> {
        if let Some(value) = self.pending_get(kind, key) {
            return Ok(value);
        }
        let root = self.read_root(kind)?;
        btree::get_with(self, root, key, &self.env.inner.comparator)
    }

    pub(crate) fn increment_database_count(&mut self) -> Result<()> {
        self.database_count = self
            .database_count
            .checked_add(1)
            .context("database count overflow")?;
        Ok(())
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

        let txn_id = self.txn_id;
        let overlay_len = self.overlay.len();
        let dirty_len = self.dirty_pages.len();
        let extent_len = self.extents.len() + self.direct_extent_pages.len();
        let entry_count = overlay_len + dirty_len + extent_len;
        let page_count = overlay_len + dirty_len;
        let (commit_record, commit_page, inline, inline_count) =
            if entry_count == 0 {
                (0, 0, None, 0)
            } else if page_count == 1 && extent_len == 0 {
                let page_id = if let Some(&page_id) = self.dirty_pages.iter().next() {
                    page_id
                } else {
                    self.overlay
                        .iter()
                        .next()
                        .map(|(page_id, _)| *page_id)
                        .context("commit has no page")?
                };
                (0, page_id, None, 0)
            } else if entry_count <= meta::INLINE_COMMIT_CAPACITY {
                let mut inline = [InlineCommit::default(); meta::INLINE_COMMIT_CAPACITY];
                let mut slot = 0;
                for (page_id, _) in self.overlay.iter() {
                    inline[slot] = InlineCommit {
                        page_id: *page_id,
                        page_count: 0,
                    };
                    slot += 1;
                }
                for page_id in &self.dirty_pages {
                    inline[slot] = InlineCommit {
                        page_id: *page_id,
                        page_count: 0,
                    };
                    slot += 1;
                }
                for (start_page, bytes) in &self.extents {
                    inline[slot] = InlineCommit {
                        page_id: *start_page,
                        page_count: (bytes.len() / PAGE_SIZE) as u64,
                    };
                    slot += 1;
                }
                for (start_page, page_count) in &self.direct_extent_pages {
                    inline[slot] = InlineCommit {
                        page_id: *start_page,
                        page_count: *page_count,
                    };
                    slot += 1;
                }
                (0, 0, Some(inline), slot as u32)
            } else {
                let mut entries = Vec::with_capacity(entry_count);
                entries.extend(
                    self.overlay
                        .iter()
                        .map(|(page_id, _)| CommitEntry::Page(*page_id)),
                );
                entries.extend(
                    self.dirty_pages
                        .iter()
                        .map(|page_id| CommitEntry::Page(*page_id)),
                );
                entries.extend(self.extents.iter().map(|(start_page, bytes)| {
                    CommitEntry::Extent {
                        start_page: *start_page,
                        page_count: (bytes.len() / PAGE_SIZE) as u64,
                    }
                }));
                entries.extend(
                    self.direct_extent_pages
                        .iter()
                        .map(|(start_page, page_count)| CommitEntry::Extent {
                            start_page: *start_page,
                            page_count: *page_count,
                        }),
                );
                match commit::build(&mut self, txn_id, &entries)? {
                    Descriptor::None => (0, 0, None, 0),
                    Descriptor::Page(page_id) => (0, page_id, None, 0),
                    Descriptor::Inline(entries) => {
                        let mut inline = [InlineCommit::default(); meta::INLINE_COMMIT_CAPACITY];
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
                        (0, 0, Some(inline), entries.len() as u32)
                    }
                    Descriptor::Chain(page_id) => {
                        let pages = commit::chain_page_count(entries.len())?;
                        self.deferred_free.push(FreeExtent {
                            start_page: page_id,
                            pages,
                            // The new meta references this chain. It cannot be reused
                            // until the following two commits have replaced both
                            // recoverable meta snapshots.
                            freed_txn: self.txn_id.saturating_add(1),
                        });
                        (page_id, 0, None, 0)
                    }
                }
            };

        let new_meta = Meta {
            txn_id,
            unnamed_root: root,
            catalog_root: self.catalog_root,
            page_count: self.next_page,
            database_count: self.database_count,
            flags: 0,
            commit_record,
            commit_page,
            inline_count,
            inline: inline.unwrap_or_default(),
        };
        new_meta.validate()?;

        if let Err(error) = self.write_and_sync(&new_meta) {
            self.env.inner.poison();
            return Err(error);
        }

        // These pages were just written and synced with checksums computed by
        // the page builder or reseal path. Mark them verified so the next
        // writer does not re-parse and checksum them on its first read.
        for (page_id, _) in self.overlay.iter() {
            self.state.mark_verified(*page_id);
        }
        for page_id in &self.dirty_pages {
            self.state.mark_verified(*page_id);
        }
        for start_page in self.extents.keys() {
            self.state.mark_verified(*start_page);
        }
        for start_page in self.direct_extent_pages.keys() {
            self.state.mark_verified(*start_page);
        }

        self.recycle_pages();
        Ok(())
    }

    fn recycle_pages(&mut self) {
        if !self.env.inner.page_reuse {
            return;
        }

        // The free pool uses commit generations to avoid overwriting pages
        // referenced by either of the two recoverable meta snapshots. A page
        // freed by commit N is referenced by the snapshot at N - 1, so it may
        // only be reused once commit N + 1 has overwritten that snapshot.
        let mut extents = std::mem::take(&mut self.cross_free);
        extents.append(&mut self.base_freed);
        extents.append(&mut self.deferred_free);
        self.cross_allocated.clear();
        for page_id in std::mem::take(&mut self.free_single) {
            extents.push(FreeExtent {
                start_page: page_id,
                pages: 1,
                freed_txn: self.txn_id,
            });
        }
        for (start_page, pages) in std::mem::take(&mut self.free_runs) {
            extents.push(FreeExtent {
                start_page,
                pages,
                freed_txn: self.txn_id,
            });
        }
        self.env.inner.return_free(extents);
    }

    fn apply_pending(&mut self, root: &mut u64) -> Result<()> {
        let unnamed = std::mem::replace(&mut self.unnamed_pending, Pending::Empty);
        self.apply_pending_to(root, unnamed)?;

        let mut named = std::mem::take(&mut self.named_pending);
        let cleared = std::mem::take(&mut self.cleared_named);

        for name in cleared {
            let mut named_root = 0;
            if let Some(pending) = named.remove(&name) {
                self.apply_pending_to(&mut named_root, pending)?;
            }
            self.catalog_put(&name, named_root)?;
        }

        for (name, pending) in named {
            let mut named_root = self
                .catalog_lookup(&name)?
                .with_context(|| format!("catalog entry for `{name}` disappeared during commit"))?;
            self.apply_pending_to(&mut named_root, pending)?;
            self.catalog_put(&name, named_root)?;
        }

        Ok(())
    }

    fn apply_pending_to(&mut self, root: &mut u64, pending: Pending) -> Result<()> {
        let comparator = self.env.inner.comparator;
        match pending {
            Pending::Empty => Ok(()),
            Pending::One(key, value) => self.apply_one(root, key, value, comparator),
            Pending::Unordered(mut pending) => {
                sort_unordered_entries(&mut pending.entries, comparator);
                let mut entries = deduplicate_unordered(pending.entries);
                if *root != 0
                    && entries.len() >= BULK_LOAD_THRESHOLD
                    && entries.iter().all(|(_, value)| value.is_none())
                {
                    let keys: Vec<PendingKey> = entries.into_iter().map(|(key, _)| key).collect();
                    *root = btree::bulk_delete_with(self, *root, &keys, self.txn_id, &comparator)?;
                    return Ok(());
                }
                if *root == 0 && entries.len() >= BULK_LOAD_THRESHOLD {
                    let entries: Vec<(PendingKey, PendingValue)> = entries
                        .drain(..)
                        .filter_map(|(key, value)| value.map(|value| (key, value)))
                        .collect();
                    if !entries.is_empty() {
                        *root = btree::bulk_load_with(self, &entries, self.txn_id, &comparator)?;
                    }
                    return Ok(());
                }
                for (key, value) in entries {
                    self.apply_one(root, key, value, comparator)?;
                }
                Ok(())
            }
            Pending::Map(pending) => {
                if *root != 0
                    && pending.len() >= BULK_LOAD_THRESHOLD
                    && pending.values().all(Option::is_none)
                {
                    let keys: Vec<PendingKey> = pending.into_keys().collect();
                    *root = btree::bulk_delete_with(self, *root, &keys, self.txn_id, &comparator)?;
                    return Ok(());
                }
                if *root == 0 && pending.len() >= BULK_LOAD_THRESHOLD {
                    let mut entries: Vec<(PendingKey, PendingValue)> = pending
                        .into_iter()
                        .filter_map(|(key, value)| value.map(|value| (key, value)))
                        .collect();
                    if !comparator.is_lexicographic() {
                        entries.sort_by(|left, right| {
                            comparator.compare(left.0.as_ref(), right.0.as_ref())
                        });
                    }
                    if !entries.is_empty() {
                        *root = btree::bulk_load_with(self, &entries, self.txn_id, &comparator)?;
                    }
                    return Ok(());
                }

                for (key, value) in pending {
                    self.apply_one(root, key, value, comparator)?;
                }
                Ok(())
            }
        }
    }

    fn apply_one(
        &mut self,
        root: &mut u64,
        key: PendingKey,
        value: Option<PendingValue>,
        comparator: KeyComparator,
    ) -> Result<()> {
        *root = match value {
            Some(value) => btree::insert_value_with(
                self,
                *root,
                key.as_slice(),
                value.value_ref(),
                self.txn_id,
                &comparator,
            )?,
            None => btree::delete_with(self, *root, key.as_slice(), self.txn_id, &comparator)?,
        };
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

    fn seal_pages(&mut self) -> Result<()> {
        let pages = std::mem::take(&mut self.unsealed_pages);
        for page_id in pages {
            if let Some(page) = self.overlay.get_mut(page_id) {
                page::seal_page(page);
            } else if self.state.has_write_mmap() {
                self.state.seal_page_direct(page_id)?;
            } else {
                bail!("page {page_id} has no writable storage for sealing");
            }
        }
        Ok(())
    }

    fn write_and_sync(&mut self, new_meta: &Meta) -> Result<()> {
        self.seal_pages()?;
        let data = &self.state.data;

        let mut pages = self.overlay.iter().peekable();
        while let Some((&start_page, first)) = pages.next() {
            ensure!(
                start_page >= 2 && start_page < self.next_page,
                "page {start_page} is outside the transaction allocation range"
            );

            let offset = start_page
                .checked_mul(PAGE_SIZE as u64)
                .context("page offset overflow")?;
            let mut end_page = start_page;
            let mut batch = None;
            while end_page - start_page + 1 < WRITE_BATCH_PAGES {
                let Some(&(next_page, next)) = pages.peek() else {
                    break;
                };
                if *next_page != end_page + 1 {
                    break;
                }
                let buffer = batch.get_or_insert_with(|| {
                    let mut buffer = Vec::with_capacity(PAGE_SIZE * 2);
                    buffer.extend_from_slice(first);
                    buffer
                });
                buffer.extend_from_slice(next);
                pages.next();
                end_page = *next_page;
            }

            if let Some(batch) = batch {
                io::write_all_at(data, &batch, offset)
                    .with_context(|| format!("failed to write pages {start_page}..={end_page}"))?;
            } else {
                io::write_all_at(data, first, offset)
                    .with_context(|| format!("failed to write page {start_page}"))?;
            }
        }

        for (start_page, bytes) in &self.extents {
            ensure!(
                bytes.len().is_multiple_of(PAGE_SIZE),
                "overflow extent is not page-aligned"
            );
            let offset = start_page
                .checked_mul(PAGE_SIZE as u64)
                .context("extent offset overflow")?;
            io::write_all_at(data, bytes, offset)
                .with_context(|| format!("failed to write overflow extent at page {start_page}"))?;
        }
        // POSIX `fdatasync` below writes back dirty shared mappings and the
        // meta page together. On Windows, mapped views need an explicit flush
        // before the meta page is published. Either way, unflushed page writes
        // remain unreachable from the old meta page and are rejected by the
        // commit-intent validation if the new meta page survives alone.
        #[cfg(windows)]
        self.state.flush_write_pages(&self.dirty_pages)?;
        #[cfg(windows)]
        self.state.flush_write_extents(&self.direct_extent_pages)?;

        let slot = 1 - self.base_slot;
        {
            let mut page = self
                .env
                .inner
                .meta_buffer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            new_meta.encode_into(&mut page);
            if !self.state.write_meta_direct(slot as usize, &page)? {
                meta::write_slot_page(data, slot as usize, &page)?;
            }
            #[cfg(windows)]
            self.state.flush_meta_page(slot as usize)?;
        }
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

impl Drop for RwTxn<'_> {
    fn drop(&mut self) {
        // Aborted transactions must not lose pages that were taken from the
        // shared free pool. Returning them with their original generation is
        // safe because uncommitted page contents are never written to disk.
        let mut extents = std::mem::take(&mut self.cross_free);
        extents.append(&mut self.cross_allocated);
        if !extents.is_empty() {
            self.env.inner.return_free(extents);
        }
    }
}

impl PageSource for RwTxn<'_> {
    fn page(&self, page_id: u64) -> Result<Page<'_>> {
        if let Some(page) = self.overlay.get(page_id) {
            Page::parse_trusted(page, Some(page_id))
        } else if self.dirty_pages.contains(&page_id) {
            self.state.read_written_page(page_id)
        } else {
            self.state.read_page(page_id, &self.base)
        }
    }

    fn overflow(&self, reference: OverflowRef) -> Result<&[u8]> {
        if let Some(bytes) = self.extents.get(&reference.start_page) {
            overflow::validate_extent(bytes, reference)
        } else if self.direct_extent_pages.contains_key(&reference.start_page) {
            self.state.read_overflow_written(reference)
        } else {
            self.state.read_overflow(reference, &self.base)
        }
    }

    fn comparator(&self) -> &crate::comparator::KeyComparator {
        &self.env.inner.comparator
    }
}

impl PageStore for RwTxn<'_> {
    fn with_page<R>(&self, page_id: u64, visit: impl FnOnce(&Page<'_>) -> Result<R>) -> Result<R> {
        let page = <Self as PageSource>::page(self, page_id)?;
        visit(&page)
    }

    fn update_page<F>(&mut self, old_page_id: u64, new_page_id: u64, edit: F) -> Result<()>
    where
        F: FnOnce(&mut [u8; PAGE_SIZE]) -> Result<()>,
    {
        ensure!(
            new_page_id >= 2 && new_page_id < self.next_page,
            "page {new_page_id} is outside the transaction allocation range"
        );
        if self.state.has_write_mmap() {
            let defer_checksum = new_page_id == old_page_id;
            self.state
                .update_page_direct(old_page_id, new_page_id, |page| {
                    edit(page)?;
                    if !defer_checksum {
                        page::seal_page(page);
                    }
                    Ok(())
                })?;
            self.dirty_pages.insert(new_page_id);
            self.state.mark_verified(new_page_id);
            if defer_checksum {
                self.unsealed_pages.insert(new_page_id);
            }
        } else {
            let mut page = self.read_page(old_page_id)?;
            edit(&mut page)?;
            self.write_page(new_page_id, page)?;
            self.unsealed_pages.insert(new_page_id);
        }
        Ok(())
    }

    fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
        if let Some(page) = self.overlay.get(page_id) {
            return Ok(*page);
        }

        let page = if self.dirty_pages.contains(&page_id) {
            self.state.read_written_page(page_id)?
        } else {
            self.state.read_page(page_id, &self.base)?
        };
        let bytes: &[u8; PAGE_SIZE] = page
            .bytes()
            .try_into()
            .context("page has an unexpected size")?;
        Ok(*bytes)
    }

    fn allocate_pages(&mut self, pages: u64) -> Result<u64> {
        ensure!(pages > 0, "allocation must contain at least one page");

        // Prefer exact-size reusable runs. If none exists, split the smallest
        // larger run. This reclaims small leaves and branches from runs left by
        // larger values without destroying every contiguous extent.
        if pages == 1
            && let Some(page) = self.free_single.pop_first()
        {
            self.owned_pages.insert(page);
            return Ok(page);
        }
        if pages > 1 {
            let run = self
                .free_runs
                .iter()
                .find(|(_, len)| **len == pages)
                .map(|(&start, _)| start);
            if let Some(start) = run {
                self.free_runs.remove(&start);
                self.owned_pages.extend(start..start + pages);
                return Ok(start);
            }
        }

        let cross = self
            .cross_free
            .iter()
            .enumerate()
            .find(|(_, extent)| extent.pages == pages)
            .map(|(index, extent)| (index, extent.start_page, extent.freed_txn));
        if let Some((index, start, freed_txn)) = cross {
            self.cross_free.remove(index);
            self.cross_allocated.push(FreeExtent {
                start_page: start,
                pages,
                freed_txn,
            });
            self.owned_pages.extend(start..start + pages);
            return Ok(start);
        }

        let run = self
            .free_runs
            .iter()
            .filter(|(_, len)| **len >= pages)
            .min_by_key(|(_, len)| **len)
            .map(|(&start, &len)| (start, len));
        if let Some((start, len)) = run {
            self.free_runs.remove(&start);
            if len > pages {
                self.free_runs.insert(start + pages, len - pages);
            }
            self.owned_pages.extend(start..start + pages);
            return Ok(start);
        }

        let cross = self
            .cross_free
            .iter()
            .enumerate()
            .filter(|(_, extent)| extent.pages >= pages)
            .min_by_key(|(_, extent)| extent.pages)
            .map(|(index, extent)| (index, extent.start_page, extent.pages, extent.freed_txn));
        if let Some((index, start, len, freed_txn)) = cross {
            self.cross_free.remove(index);
            if len > pages {
                self.cross_free.push(FreeExtent {
                    start_page: start + pages,
                    pages: len - pages,
                    freed_txn,
                });
            }
            self.cross_allocated.push(FreeExtent {
                start_page: start,
                pages,
                freed_txn,
            });
            self.owned_pages.extend(start..start + pages);
            return Ok(start);
        }

        let start = self.next_page;
        self.next_page = self
            .next_page
            .checked_add(pages)
            .context("page ID overflow")?;
        let max_pages =
            u64::try_from(self.state.map_size / PAGE_SIZE).context("map size is too large")?;
        ensure!(
            self.next_page <= max_pages,
            "environment map is full at {} pages; reopen it with a larger map_size",
            max_pages
        );
        self.owned_pages.extend(start..start + pages);

        Ok(start)
    }

    fn owns_page(&self, page_id: u64) -> bool {
        self.owned_pages.contains(&page_id)
    }

    fn write_page(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) -> Result<()> {
        ensure!(
            page_id >= 2 && page_id < self.next_page,
            "page {page_id} is outside the transaction allocation range"
        );
        if self.state.write_page_direct(page_id, &page)? {
            self.dirty_pages.insert(page_id);
            self.unsealed_pages.remove(&page_id);
            self.state.mark_verified(page_id);
        } else {
            self.overlay.insert(page_id, page);
            self.unsealed_pages.remove(&page_id);
        }
        Ok(())
    }

    fn write_extent(&mut self, start_page: u64, bytes: &[u8]) -> Result<()> {
        ensure!(
            start_page >= 2 && start_page < self.next_page,
            "extent start {start_page} is outside the transaction allocation range"
        );
        ensure!(
            bytes.len().is_multiple_of(PAGE_SIZE),
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

    fn write_overflow(&mut self, reference: OverflowRef, value: &[u8], txn_id: u64) -> Result<()> {
        if self.state.write_overflow_direct(reference, value, txn_id)? {
            self.direct_extent_pages
                .insert(reference.start_page, reference.page_count);
        } else {
            let bytes = overflow::encode_extent(reference, value, txn_id)?;
            self.write_extent(reference.start_page, &bytes)?;
        }
        Ok(())
    }

    fn free_pages(&mut self, start_page: u64, pages: u64) -> Result<()> {
        ensure!(pages > 0, "free must contain at least one page");
        let end = start_page
            .checked_add(pages)
            .context("free range overflow")?;
        let base = self.base.page_count;
        let owned = start_page < self.next_page
            && (start_page..end).all(|page| self.owned_pages.contains(&page));

        if start_page >= base || owned {
            ensure!(owned, "page {start_page} is not owned by this transaction");
            self.free_owned_pages(start_page, pages)?;
        } else {
            let base_end = end.min(base);
            if base_end > start_page {
                ensure!(
                    (start_page..base_end).all(|page| !self.owned_pages.contains(&page)),
                    "free range mixes base and transaction pages"
                );
                self.base_freed.push(FreeExtent {
                    start_page,
                    pages: base_end - start_page,
                    freed_txn: self.txn_id,
                });
            }
            if end > base {
                ensure!(
                    (base..end).all(|page| self.owned_pages.contains(&page)),
                    "page {base} is not owned by this transaction"
                );
                self.free_owned_pages(base, end - base)?;
            }
        }

        Ok(())
    }
}

impl RwTxn<'_> {
    fn free_owned_pages(&mut self, start_page: u64, pages: u64) -> Result<()> {
        let end = start_page
            .checked_add(pages)
            .context("free range overflow")?;
        ensure!(
            end <= self.next_page,
            "free range {start_page}..{end} exceeds the transaction high-water mark"
        );

        for page_id in start_page..end {
            self.dirty_pages.remove(&page_id);
            self.unsealed_pages.remove(&page_id);
        }
        self.extents.remove(&start_page);
        self.direct_extent_pages.remove(&start_page);
        if pages == 1 {
            self.overlay.remove(start_page);
            self.free_single.insert(start_page);
        } else {
            self.free_runs.insert(start_page, pages);
        }

        Ok(())
    }
}
