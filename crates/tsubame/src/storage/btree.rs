//! B+tree traversal and copy-on-write mutation.
//!
//! The write path is wired into transactions in the next milestone; until then
//! some entry points are only exercised by the storage tests.

#![allow(dead_code)]

use std::collections::BTreeSet;

use std::cmp::Ordering;

use anyhow::{Context, Result, bail, ensure};

use super::PAGE_SIZE;
use super::overflow::{self, OverflowRef};
use super::page::{
    self, HEADER_SIZE, INLINE_BUDGET, LeafValue, MAX_KEY_SIZE, Page, PageType, SLOT_SIZE,
};
use super::store::PageStore;
use crate::comparator::KeyComparator;

/// The maximum number of branch levels followed by a lookup.
///
/// This is a safety bound against corrupt or cyclic pages, not a format limit.
const MAX_DEPTH: usize = 64;

/// A source of validated pages addressed by page ID.
pub(crate) trait PageSource {
    /// Returns the page with the given ID.
    fn page(&self, page_id: u64) -> Result<Page<'_>>;

    /// Returns the payload of an overflow extent.
    fn overflow(&self, reference: OverflowRef) -> Result<&[u8]>;

    /// Returns the key order used by the source.
    fn comparator(&self) -> &KeyComparator;
}

/// Looks up `key` in the tree rooted at `root`.
///
/// A root of 0 represents an empty tree. Values are returned as a slice into
/// the page source, so they keep the source's lifetime.
#[inline]
pub(crate) fn get<'a, S: PageSource>(
    source: &'a S,
    root: u64,
    key: &[u8],
) -> Result<Option<&'a [u8]>> {
    get_with(source, root, key, &KeyComparator::lexicographic())
}

pub(crate) fn get_with<'a, S: PageSource>(
    source: &'a S,
    root: u64,
    key: &[u8],
    comparator: &KeyComparator,
) -> Result<Option<&'a [u8]>> {
    if root == 0 {
        return Ok(None);
    }

    let mut page_id = root;
    for _ in 0..MAX_DEPTH {
        let page = source.page(page_id)?;
        match page.page_type() {
            PageType::Branch => {
                page_id = page.branch_child_with(key, comparator)?;
                ensure!(page_id >= 2, "branch points at reserved page ID {page_id}");
            }
            PageType::Leaf => {
                let Some(index) = page.leaf_index_with(key, comparator)? else {
                    return Ok(None);
                };
                return match page.leaf_value(index)? {
                    LeafValue::Inline(value) => Ok(Some(value)),
                    LeafValue::Overflow(reference) => Ok(Some(source.overflow(reference)?)),
                };
            }
        }
    }

    bail!("B+tree depth exceeded {MAX_DEPTH}; the page chain may contain a cycle")
}

/// Frees every page and overflow extent reachable from `root`.
///
/// The tree is only read; the caller decides whether the transaction commits.
/// Aborting a transaction that called this function discards the recorded frees,
/// leaving the committed tree intact.
pub(crate) fn free_tree<S: PageStore>(store: &mut S, root: u64) -> Result<()> {
    if root == 0 {
        return Ok(());
    }

    let mut visited = BTreeSet::new();
    free_tree_inner(store, root, 0, &mut visited)
}

fn free_tree_inner<S: PageStore>(
    store: &mut S,
    page_id: u64,
    depth: usize,
    visited: &mut BTreeSet<u64>,
) -> Result<()> {
    ensure!(depth < MAX_DEPTH, "tree exceeds the maximum depth");
    ensure!(
        page_id >= 2,
        "cannot free reserved page {page_id} as part of a tree"
    );
    ensure!(
        visited.insert(page_id),
        "tree references page {page_id} more than once"
    );

    let bytes = store.read_page(page_id)?;
    let page = Page::parse(&bytes, Some(page_id))?;

    match page.page_type() {
        PageType::Branch => {
            for index in 0..page.slot_count() {
                let child = page.child(index)?;
                free_tree_inner(store, child, depth + 1, visited)?;
            }
        }
        PageType::Leaf => {
            for index in 0..page.slot_count() {
                if let LeafValue::Overflow(reference) = page.leaf_value(index)? {
                    store.free_pages(reference.start_page, reference.page_count)?;
                }
            }
        }
    }

    store.free_pages(page_id, 1)
}

/// Inserts or replaces `key` and returns the new root page ID.
pub(crate) fn insert<S: PageStore>(
    store: &mut S,
    root: u64,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
) -> Result<u64> {
    insert_with(
        store,
        root,
        key,
        value,
        txn_id,
        &KeyComparator::lexicographic(),
    )
}

pub(crate) fn insert_with<S: PageStore>(
    store: &mut S,
    root: u64,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
    comparator: &KeyComparator,
) -> Result<u64> {
    ensure!(
        key.len() <= MAX_KEY_SIZE,
        "key is {} bytes, maximum is {MAX_KEY_SIZE}",
        key.len()
    );

    if root == 0 {
        let value = make_value(store, key.len(), value, txn_id)?;
        let entry = OwnedLeafEntry {
            key: key.to_vec(),
            value,
        };
        return allocate_leaf(store, &[entry], txn_id);
    }

    let result = insert_into(store, root, key, value, txn_id, 0, comparator)?;
    if let Some(split) = result.split {
        let entries = vec![(Vec::new(), result.page_id), (split.separator, split.right)];
        allocate_branch(store, &entries, txn_id)
    } else {
        Ok(result.page_id)
    }
}

/// Deletes `key` and returns the new root page ID.
pub(crate) fn delete<S: PageStore>(
    store: &mut S,
    root: u64,
    key: &[u8],
    txn_id: u64,
) -> Result<u64> {
    delete_with(store, root, key, txn_id, &KeyComparator::lexicographic())
}

pub(crate) fn delete_with<S: PageStore>(
    store: &mut S,
    root: u64,
    key: &[u8],
    txn_id: u64,
    comparator: &KeyComparator,
) -> Result<u64> {
    if root == 0 {
        return Ok(0);
    }

    let result = delete_into(store, root, key, txn_id, 0, comparator)?;
    Ok(result.page_id)
}

struct Split {
    separator: Vec<u8>,
    right: u64,
}

struct InsertResult {
    page_id: u64,
    split: Option<Split>,
}

#[derive(Debug)]
struct OwnedLeafEntry {
    key: Vec<u8>,
    value: OwnedValue,
}

#[derive(Debug, Clone)]
enum OwnedValue {
    Inline(Vec<u8>),
    Overflow(OverflowRef),
}

impl OwnedValue {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Inline(value) => value.len(),
            Self::Overflow(_) => overflow::OVERFLOW_REF_SIZE,
        }
    }

    fn as_leaf_value(&self) -> LeafValue<'_> {
        match self {
            Self::Inline(value) => LeafValue::Inline(value),
            Self::Overflow(reference) => LeafValue::Overflow(*reference),
        }
    }
}

fn insert_into<S: PageStore>(
    store: &mut S,
    page_id: u64,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
    depth: usize,
    comparator: &KeyComparator,
) -> Result<InsertResult> {
    ensure!(
        depth < MAX_DEPTH,
        "B+tree depth exceeded {MAX_DEPTH} during insert"
    );

    let page_type = store.with_page(page_id, |page| Ok(page.page_type()))?;

    match page_type {
        PageType::Leaf => insert_leaf(store, page_id, key, value, txn_id, comparator),
        PageType::Branch => insert_branch(store, page_id, key, value, txn_id, depth, comparator),
    }
}

enum LeafEdit {
    Same(usize),
    Resized(usize, Option<OverflowRef>),
    Append,
}

fn insert_leaf<S: PageStore>(
    store: &mut S,
    page_id: u64,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
    comparator: &KeyComparator,
) -> Result<InsertResult> {
    let edit = store.with_page(page_id, |page| {
        let position = page.lower_bound_with(key, comparator)?;
        let existing = position < page.slot_count() && page.key(position)? == key;
        let inline = SLOT_SIZE + key.len() + value.len() <= INLINE_BUDGET;
        let mut edit = None;

        if inline {
            if existing {
                let old_value = page.leaf_value(position)?;
                if let LeafValue::Inline(old) = old_value
                    && old.len() == value.len()
                {
                    edit = Some(LeafEdit::Same(position));
                } else {
                    let old_value_len = match old_value {
                        LeafValue::Inline(value) => value.len(),
                        LeafValue::Overflow(_) => overflow::OVERFLOW_REF_SIZE,
                    };
                    let used = HEADER_SIZE
                        .checked_add(page.slot_count() * SLOT_SIZE)
                        .and_then(|size| size.checked_add(PAGE_SIZE - page.free_end()))
                        .context("leaf size overflow")?;
                    let size = used
                        .checked_sub(old_value_len)
                        .and_then(|size| size.checked_add(value.len()))
                        .context("leaf size overflow")?;
                    if size <= PAGE_SIZE {
                        let overflow = match old_value {
                            LeafValue::Overflow(reference) => Some(reference),
                            LeafValue::Inline(_) => None,
                        };
                        edit = Some(LeafEdit::Resized(position, overflow));
                    }
                }
            } else if position == page.slot_count() && page.can_append(key.len(), value.len()) {
                edit = Some(LeafEdit::Append);
            }
        }
        Ok(edit)
    })?;

    if let Some(edit) = edit {
        let old_overflow = match edit {
            LeafEdit::Resized(_, overflow) => overflow,
            LeafEdit::Same(_) | LeafEdit::Append => None,
        };
        let new_page_id = store.allocate_pages(1)?;
        store.update_page(page_id, new_page_id, |bytes| match edit {
            LeafEdit::Same(position) => {
                page::replace_value_in_place(bytes, new_page_id, txn_id, position, value, 0)
            }
            LeafEdit::Resized(position, _) => {
                page::replace_value_resized_in_place(bytes, new_page_id, txn_id, position, value, 0)
            }
            LeafEdit::Append => {
                page::append_entry_in_place(bytes, new_page_id, txn_id, key, value, 0)
            }
        })?;
        if let Some(reference) = old_overflow {
            store.free_pages(reference.start_page, reference.page_count)?;
        }
        store.free_pages(page_id, 1)?;
        return Ok(InsertResult {
            page_id: new_page_id,
            split: None,
        });
    }

    let mut entries = store.with_page(page_id, collect_leaf)?;
    let new_value = make_value(store, key.len(), value, txn_id)?;
    let replaced =
        match entries.binary_search_by(|entry| comparator.compare(entry.key.as_slice(), key)) {
            Ok(index) => Some(std::mem::replace(&mut entries[index].value, new_value)),
            Err(index) => {
                entries.insert(
                    index,
                    OwnedLeafEntry {
                        key: key.to_vec(),
                        value: new_value,
                    },
                );
                None
            }
        };

    let result = store_leaf(store, page_id, entries, txn_id)?;
    if let Some(old_value) = replaced {
        free_value(store, &old_value)?;
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn insert_branch<S: PageStore>(
    store: &mut S,
    page_id: u64,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
    depth: usize,
    comparator: &KeyComparator,
) -> Result<InsertResult> {
    let (index, child) = store.with_page(page_id, |page| {
        let index = page.branch_child_index_with(key, comparator)?;
        Ok((index, page.child(index)?))
    })?;
    let result = insert_into(store, child, key, value, txn_id, depth + 1, comparator)?;

    if result.split.is_none() {
        let new_page_id = store.allocate_pages(1)?;
        store.update_page(page_id, new_page_id, |bytes| {
            page::replace_child_in_place(bytes, new_page_id, txn_id, index, result.page_id)
        })?;
        store.free_pages(page_id, 1)?;
        return Ok(InsertResult {
            page_id: new_page_id,
            split: None,
        });
    }

    let mut entries = store.with_page(page_id, collect_branch)?;
    entries[index].1 = result.page_id;
    if let Some(split) = result.split {
        entries.insert(index + 1, (split.separator, split.right));
    }

    store_branch(store, page_id, entries, txn_id)
}

fn delete_into<S: PageStore>(
    store: &mut S,
    page_id: u64,
    key: &[u8],
    txn_id: u64,
    depth: usize,
    comparator: &KeyComparator,
) -> Result<DeleteResult> {
    ensure!(
        depth < MAX_DEPTH,
        "B+tree depth exceeded {MAX_DEPTH} during delete"
    );

    let bytes = store.read_page(page_id)?;
    let page = Page::parse_trusted(&bytes, Some(page_id))?;

    match page.page_type() {
        PageType::Leaf => delete_leaf(store, page_id, &page, key, txn_id, comparator),
        PageType::Branch => delete_branch(store, page_id, &page, key, txn_id, depth, comparator),
    }
}

struct DeleteResult {
    page_id: u64,
    changed: bool,
}

fn delete_leaf<S: PageStore>(
    store: &mut S,
    page_id: u64,
    page: &Page<'_>,
    key: &[u8],
    txn_id: u64,
    comparator: &KeyComparator,
) -> Result<DeleteResult> {
    let mut entries = collect_leaf(page)?;
    let Ok(index) = entries.binary_search_by(|entry| comparator.compare(entry.key.as_slice(), key))
    else {
        return Ok(DeleteResult {
            page_id,
            changed: false,
        });
    };

    let removed = entries.remove(index);
    if entries.is_empty() {
        store.free_pages(page_id, 1)?;
        free_value(store, &removed.value)?;
        return Ok(DeleteResult {
            page_id: 0,
            changed: true,
        });
    }

    let new_page = allocate_leaf(store, &entries, txn_id)?;
    store.free_pages(page_id, 1)?;
    free_value(store, &removed.value)?;

    Ok(DeleteResult {
        page_id: new_page,
        changed: true,
    })
}

fn delete_branch<S: PageStore>(
    store: &mut S,
    page_id: u64,
    page: &Page<'_>,
    key: &[u8],
    txn_id: u64,
    depth: usize,
    comparator: &KeyComparator,
) -> Result<DeleteResult> {
    let index = page.branch_child_index_with(key, comparator)?;
    let child = page.child(index)?;
    let result = delete_into(store, child, key, txn_id, depth + 1, comparator)?;

    if !result.changed {
        return Ok(DeleteResult {
            page_id,
            changed: false,
        });
    }

    if result.page_id != 0 {
        if page.slot_count() > 1 {
            let new_page_id = store.allocate_pages(1)?;
            let new_page = page.replace_child(new_page_id, txn_id, index, result.page_id)?;
            store.write_page(new_page_id, new_page)?;
            store.free_pages(page_id, 1)?;
            return Ok(DeleteResult {
                page_id: new_page_id,
                changed: true,
            });
        }

        store.free_pages(page_id, 1)?;
        return Ok(DeleteResult {
            page_id: result.page_id,
            changed: true,
        });
    }

    let mut entries = collect_branch(page)?;
    entries.remove(index);

    if entries.is_empty() {
        store.free_pages(page_id, 1)?;
        return Ok(DeleteResult {
            page_id: 0,
            changed: true,
        });
    }

    entries[0].0.clear();

    if entries.len() == 1 {
        let child = entries[0].1;
        store.free_pages(page_id, 1)?;
        return Ok(DeleteResult {
            page_id: child,
            changed: true,
        });
    }

    let new_page = allocate_branch(store, &entries, txn_id)?;
    store.free_pages(page_id, 1)?;

    Ok(DeleteResult {
        page_id: new_page,
        changed: true,
    })
}

fn collect_leaf(page: &Page<'_>) -> Result<Vec<OwnedLeafEntry>> {
    let mut entries = Vec::with_capacity(page.slot_count());
    for index in 0..page.slot_count() {
        let (key, value) = page.leaf_entry(index)?;
        let value = match value {
            LeafValue::Inline(value) => OwnedValue::Inline(value.to_vec()),
            LeafValue::Overflow(reference) => OwnedValue::Overflow(reference),
        };
        entries.push(OwnedLeafEntry {
            key: key.to_vec(),
            value,
        });
    }
    Ok(entries)
}

fn collect_branch(page: &Page<'_>) -> Result<Vec<(Vec<u8>, u64)>> {
    let mut entries = Vec::with_capacity(page.slot_count());
    for index in 0..page.slot_count() {
        entries.push((page.key(index)?.to_vec(), page.child(index)?));
    }
    Ok(entries)
}

fn store_leaf<S: PageStore>(
    store: &mut S,
    old_page: u64,
    mut entries: Vec<OwnedLeafEntry>,
    txn_id: u64,
) -> Result<InsertResult> {
    if leaf_page_size(&entries) <= PAGE_SIZE {
        let page_id = allocate_leaf(store, &entries, txn_id)?;
        store.free_pages(old_page, 1)?;
        return Ok(InsertResult {
            page_id,
            split: None,
        });
    }

    let sizes: Vec<usize> = entries
        .iter()
        .map(|entry| entry.key.len() + entry.value.encoded_len())
        .collect();
    let split = choose_split(&sizes)?;
    let right = entries.split_off(split);
    let separator = right[0].key.clone();

    let left_page = allocate_leaf(store, &entries, txn_id)?;
    let right_page = allocate_leaf(store, &right, txn_id)?;
    store.free_pages(old_page, 1)?;

    Ok(InsertResult {
        page_id: left_page,
        split: Some(Split {
            separator,
            right: right_page,
        }),
    })
}

fn store_branch<S: PageStore>(
    store: &mut S,
    old_page: u64,
    mut entries: Vec<(Vec<u8>, u64)>,
    txn_id: u64,
) -> Result<InsertResult> {
    ensure!(
        entries.first().is_some_and(|(key, _)| key.is_empty()),
        "branch page must start with an empty key"
    );

    if branch_page_size(&entries) <= PAGE_SIZE {
        let page_id = allocate_branch(store, &entries, txn_id)?;
        store.free_pages(old_page, 1)?;
        return Ok(InsertResult {
            page_id,
            split: None,
        });
    }

    let sizes: Vec<usize> = entries
        .iter()
        .map(|(key, _)| key.len() + std::mem::size_of::<u64>())
        .collect();
    let split = choose_split(&sizes)?;
    let mut right = entries.split_off(split);
    let separator = right[0].0.clone();
    right[0].0.clear();

    let left_page = allocate_branch(store, &entries, txn_id)?;
    let right_page = allocate_branch(store, &right, txn_id)?;
    store.free_pages(old_page, 1)?;

    Ok(InsertResult {
        page_id: left_page,
        split: Some(Split {
            separator,
            right: right_page,
        }),
    })
}

/// Bulk-loads sorted entries into a new tree and returns its root.
///
/// This is used when a transaction writes a large number of entries into an
/// empty database; building the tree bottom-up avoids rewriting the same
/// pages for every insert.
pub(crate) fn bulk_load<S: PageStore>(
    store: &mut S,
    entries: &[(Vec<u8>, Vec<u8>)],
    txn_id: u64,
) -> Result<u64> {
    bulk_load_with(store, entries, txn_id, &KeyComparator::lexicographic())
}

pub(crate) fn bulk_load_with<S: PageStore, K: AsRef<[u8]>, V: AsRef<[u8]>>(
    store: &mut S,
    entries: &[(K, V)],
    txn_id: u64,
    comparator: &KeyComparator,
) -> Result<u64> {
    if entries.is_empty() {
        return Ok(0);
    }
    for pair in entries.windows(2) {
        let left = pair[0].0.as_ref();
        let right = pair[1].0.as_ref();
        let ordered = if comparator.is_lexicographic() {
            left < right
        } else {
            comparator.compare(left, right) == Ordering::Less
        };
        ensure!(ordered, "bulk-load keys must be strictly ascending");
    }

    let mut leaves: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut page_start = 0;
    let mut leaf_size = HEADER_SIZE;
    let mut page_values: Vec<LeafValue<'_>> = Vec::new();

    for (index, (key, value)) in entries.iter().enumerate() {
        let key = key.as_ref();
        ensure!(
            key.len() <= MAX_KEY_SIZE,
            "key is {} bytes, maximum is {MAX_KEY_SIZE}",
            key.len()
        );
        let value = value.as_ref();
        let inline = SLOT_SIZE + key.len() + value.len() <= INLINE_BUDGET;
        let value_len = if inline {
            value.len()
        } else {
            overflow::OVERFLOW_REF_SIZE
        };
        let entry_size = SLOT_SIZE + key.len() + value_len;
        ensure!(
            HEADER_SIZE + entry_size <= PAGE_SIZE,
            "entry is too large for a leaf page"
        );

        if !page_values.is_empty() && leaf_size + entry_size > PAGE_SIZE {
            flush_bulk_leaf(
                store,
                &entries[page_start..index],
                &page_values,
                &mut leaves,
                txn_id,
            )?;
            page_start = index;
            leaf_size = HEADER_SIZE;
            page_values.clear();
        }

        leaf_size += entry_size;
        page_values.push(if inline {
            LeafValue::Inline(value)
        } else {
            LeafValue::Overflow(make_overflow(store, value, txn_id)?)
        });
    }
    flush_bulk_leaf(
        store,
        &entries[page_start..],
        &page_values,
        &mut leaves,
        txn_id,
    )?;

    let mut level = leaves;
    while level.len() > 1 {
        let mut next = Vec::new();
        let mut group: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut group_size = HEADER_SIZE;
        let mut group_separator = Vec::new();

        for (separator, child) in level {
            let entry_separator = separator;
            if !group.is_empty() {
                let candidate_size = SLOT_SIZE + entry_separator.len() + std::mem::size_of::<u64>();
                if group_size + candidate_size > PAGE_SIZE {
                    let page_id = allocate_branch(store, &group, txn_id)?;
                    let separator = std::mem::take(&mut group_separator);
                    next.push((separator, page_id));
                    group.clear();
                    group_size = HEADER_SIZE;
                }
            }

            if group.is_empty() {
                group_separator = entry_separator.clone();
            }
            let key = if group.is_empty() {
                Vec::new()
            } else {
                entry_separator
            };
            let entry_size = SLOT_SIZE + key.len() + std::mem::size_of::<u64>();
            ensure!(
                HEADER_SIZE + entry_size <= PAGE_SIZE,
                "entry is too large for a branch page"
            );

            group_size += entry_size;
            group.push((key, child));
        }

        if !group.is_empty() {
            let page_id = allocate_branch(store, &group, txn_id)?;
            next.push((group_separator, page_id));
        }

        level = next;
    }

    Ok(level[0].1)
}

fn flush_leaf<S: PageStore>(
    store: &mut S,
    leaves: &mut Vec<(Vec<u8>, u64)>,
    entries: &mut Vec<OwnedLeafEntry>,
    txn_id: u64,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let separator = if leaves.is_empty() {
        Vec::new()
    } else {
        entries[0].key.clone()
    };
    let page_id = allocate_leaf(store, entries, txn_id)?;
    leaves.push((separator, page_id));
    entries.clear();

    Ok(())
}

fn flush_bulk_leaf<S: PageStore, K: AsRef<[u8]>, V: AsRef<[u8]>>(
    store: &mut S,
    entries: &[(K, V)],
    values: &[LeafValue<'_>],
    leaves: &mut Vec<(Vec<u8>, u64)>,
    txn_id: u64,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    ensure!(
        entries.len() == values.len(),
        "bulk-load page has mismatched entry and value counts"
    );

    let separator = if leaves.is_empty() {
        Vec::new()
    } else {
        entries[0].0.as_ref().to_vec()
    };
    let page_id = store.allocate_pages(1)?;
    let page = page::build_leaf_entries_from_iter(
        page_id,
        txn_id,
        entries
            .iter()
            .zip(values)
            .map(|((key, _), value)| (key.as_ref(), *value)),
    )?;
    store.write_page(page_id, page)?;
    leaves.push((separator, page_id));
    Ok(())
}

fn allocate_leaf<S: PageStore>(
    store: &mut S,
    entries: &[OwnedLeafEntry],
    txn_id: u64,
) -> Result<u64> {
    ensure!(
        leaf_page_size(entries) <= PAGE_SIZE,
        "leaf entries do not fit in a page"
    );
    let page_id = store.allocate_pages(1)?;
    let page = page::build_leaf_entries_from_iter(
        page_id,
        txn_id,
        entries
            .iter()
            .map(|entry| (entry.key.as_slice(), entry.value.as_leaf_value())),
    )?;
    store.write_page(page_id, page)?;
    Ok(page_id)
}

fn allocate_branch<S: PageStore>(
    store: &mut S,
    entries: &[(Vec<u8>, u64)],
    txn_id: u64,
) -> Result<u64> {
    ensure!(
        branch_page_size(entries) <= PAGE_SIZE,
        "branch entries do not fit in a page"
    );
    let page_id = store.allocate_pages(1)?;
    let refs: Vec<(&[u8], u64)> = entries
        .iter()
        .map(|(key, child)| (key.as_slice(), *child))
        .collect();
    let page = page::build_branch_sorted(page_id, txn_id, &refs)?;
    store.write_page(page_id, page)?;
    Ok(page_id)
}

fn make_value<S: PageStore>(
    store: &mut S,
    key_len: usize,
    value: &[u8],
    txn_id: u64,
) -> Result<OwnedValue> {
    if SLOT_SIZE + key_len + value.len() <= INLINE_BUDGET {
        return Ok(OwnedValue::Inline(value.to_vec()));
    }

    Ok(OwnedValue::Overflow(make_overflow(store, value, txn_id)?))
}

fn make_overflow<S: PageStore>(store: &mut S, value: &[u8], txn_id: u64) -> Result<OverflowRef> {
    let total_len = u32::try_from(value.len()).context("value is larger than 4 GiB")?;
    let pages = overflow::pages_for(value.len())?;
    let start_page = store.allocate_pages(pages)?;
    let reference = OverflowRef {
        total_len,
        start_page,
        page_count: pages,
    };
    let bytes = overflow::encode_extent(reference, value, txn_id)?;
    store.write_extent(start_page, &bytes)?;
    Ok(reference)
}

fn free_value<S: PageStore>(store: &mut S, value: &OwnedValue) -> Result<()> {
    if let OwnedValue::Overflow(reference) = value {
        store.free_pages(reference.start_page, reference.page_count)?;
    }
    Ok(())
}

fn leaf_page_size(entries: &[OwnedLeafEntry]) -> usize {
    HEADER_SIZE
        + entries.len() * SLOT_SIZE
        + entries
            .iter()
            .map(|entry| entry.key.len() + entry.value.encoded_len())
            .sum::<usize>()
}

fn branch_page_size(entries: &[(Vec<u8>, u64)]) -> usize {
    HEADER_SIZE
        + entries.len() * SLOT_SIZE
        + entries
            .iter()
            .map(|(key, _)| key.len() + std::mem::size_of::<u64>())
            .sum::<usize>()
}

fn choose_split(sizes: &[usize]) -> Result<usize> {
    ensure!(
        sizes.len() >= 2,
        "cannot split a page with fewer than two entries"
    );

    let total: usize = sizes.iter().sum();
    let mut left_payload = 0;
    let mut best: Option<(usize, usize)> = None;

    for split in 1..sizes.len() {
        left_payload += sizes[split - 1];
        let left_size = HEADER_SIZE + split * SLOT_SIZE + left_payload;
        let right_size = HEADER_SIZE + (sizes.len() - split) * SLOT_SIZE + (total - left_payload);

        if left_size <= PAGE_SIZE && right_size <= PAGE_SIZE {
            let difference = left_size.abs_diff(right_size);
            if best.is_none_or(|(_, best_difference)| difference < best_difference) {
                best = Some((split, difference));
            }
        }
    }

    best.map(|(split, _)| split)
        .context("no split point keeps both pages within the page size")
}

/// Compares two encoded keys in lexicographic order.
#[cfg(test)]
fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::storage::store::memory::MemoryStore;

    fn insert_map(
        store: &mut MemoryStore,
        root: &mut u64,
        map: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        *root = insert(store, *root, &key, &value, 1).unwrap();
        map.insert(key, value);
    }

    fn check_map(store: &MemoryStore, root: u64, map: &BTreeMap<Vec<u8>, Vec<u8>>) {
        for (key, value) in map {
            assert_eq!(get(store, root, key).unwrap(), Some(value.as_slice()));
        }
    }

    #[test]
    fn inserts_a_smaller_key_into_one_leaf() {
        let mut store = MemoryStore::new();
        let mut root = insert(&mut store, 0, b"key-012", b"base", 1).unwrap();
        root = insert(&mut store, root, b"key-007", b"v", 2).unwrap();

        assert_eq!(get(&store, root, b"key-007").unwrap(), Some(&b"v"[..]));
        assert_eq!(get(&store, root, b"key-012").unwrap(), Some(&b"base"[..]));
    }

    #[test]
    fn bulk_load_handles_inline_and_overflow_values() {
        let mut store = MemoryStore::new();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1_500u32)
            .map(|index| {
                let value = if index % 100 == 0 {
                    vec![b'x'; 10_000]
                } else {
                    format!("value-{index}").into_bytes()
                };
                (index.to_be_bytes().to_vec(), value)
            })
            .collect();

        let root = bulk_load(&mut store, &entries, 1).unwrap();
        for (key, value) in &entries {
            assert_eq!(get(&store, root, key).unwrap(), Some(value.as_slice()));
        }
    }

    #[test]
    fn updates_leaf_values_with_changed_lengths() {
        let mut store = MemoryStore::new();
        let mut root = 0;
        for index in 0..100u32 {
            root = insert(
                &mut store,
                root,
                &index.to_be_bytes(),
                format!("value-{index}").as_bytes(),
                1,
            )
            .unwrap();
        }

        root = insert(&mut store, root, &50u32.to_be_bytes(), b"short", 2).unwrap();
        for index in 0..100u32 {
            let expected = if index == 50 {
                "short".to_owned()
            } else {
                format!("value-{index}")
            };
            assert_eq!(
                get(&store, root, &index.to_be_bytes()).unwrap(),
                Some(expected.as_bytes())
            );
        }

        let large = vec![b'y'; 10_000];
        root = insert(&mut store, root, &10u32.to_be_bytes(), &large, 3).unwrap();
        assert_eq!(
            get(&store, root, &10u32.to_be_bytes()).unwrap(),
            Some(large.as_slice())
        );
        root = insert(&mut store, root, &10u32.to_be_bytes(), b"small", 4).unwrap();
        assert_eq!(
            get(&store, root, &10u32.to_be_bytes()).unwrap(),
            Some(&b"small"[..])
        );
    }

    #[test]
    fn empty_tree_has_no_values() {
        let store = MemoryStore::new();

        assert_eq!(get(&store, 0, b"key").unwrap(), None);
    }

    #[test]
    fn finds_values_in_a_single_leaf() {
        let mut store = MemoryStore::new();
        let root = insert(&mut store, 0, b"a", b"1", 1).unwrap();

        assert_eq!(get(&store, root, b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(get(&store, root, b"b").unwrap(), None);
    }

    #[test]
    fn inserts_update_and_delete_into_one_leaf() {
        let mut store = MemoryStore::new();
        let mut root = 0;
        for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
            root = insert(&mut store, root, key.as_bytes(), value.as_bytes(), 1).unwrap();
        }

        assert_eq!(get(&store, root, b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(get(&store, root, b"b").unwrap(), Some(&b"2"[..]));
        assert_eq!(get(&store, root, b"c").unwrap(), Some(&b"3"[..]));

        root = insert(&mut store, root, b"b", b"20", 2).unwrap();
        assert_eq!(get(&store, root, b"b").unwrap(), Some(&b"20"[..]));

        root = delete(&mut store, root, b"b", 3).unwrap();
        assert_eq!(get(&store, root, b"b").unwrap(), None);
        assert_eq!(get(&store, root, b"a").unwrap(), Some(&b"1"[..]));
    }

    #[test]
    fn traverses_branch_pages() {
        let mut store = MemoryStore::new();
        let mut root = 0;
        for key in ["a", "b", "c", "d", "e", "f", "g", "h", "i"] {
            root = insert(&mut store, root, key.as_bytes(), key.as_bytes(), 1).unwrap();
        }

        for key in ["a", "b", "c", "d", "e", "f", "g", "h", "i"] {
            assert_eq!(
                get(&store, root, key.as_bytes()).unwrap(),
                Some(key.as_bytes())
            );
        }
        assert_eq!(get(&store, root, b"z").unwrap(), None);
    }

    #[test]
    fn matches_btreemap_under_many_operations() {
        let mut store = MemoryStore::new();
        let mut map = BTreeMap::new();
        let mut root = 0;

        for i in 0..1_500u32 {
            let key = i.to_be_bytes().to_vec();
            let value = format!("value-{i}").into_bytes();
            insert_map(&mut store, &mut root, &mut map, key, value);
        }
        check_map(&store, root, &map);

        for i in (0..1_500u32).step_by(3) {
            let key = i.to_be_bytes().to_vec();
            let value = format!("updated-{i}").into_bytes();
            insert_map(&mut store, &mut root, &mut map, key, value);
        }
        check_map(&store, root, &map);

        for i in (0..1_500u32).step_by(2) {
            let key = i.to_be_bytes().to_vec();
            root = delete(&mut store, root, &key, 2).unwrap();
            map.remove(&key);
        }
        check_map(&store, root, &map);

        for i in (0..1_500u32).step_by(2) {
            assert_eq!(get(&store, root, &i.to_be_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn splits_internal_branches() {
        let mut store = MemoryStore::new();
        let mut map = BTreeMap::new();
        let mut root = 0;
        let value = vec![0x42; 700];

        for i in 0..3_000u32 {
            insert_map(
                &mut store,
                &mut root,
                &mut map,
                i.to_be_bytes().to_vec(),
                value.clone(),
            );
        }
        check_map(&store, root, &map);

        for i in (0..3_000u32).step_by(2) {
            let key = i.to_be_bytes().to_vec();
            root = delete(&mut store, root, &key, 2).unwrap();
            map.remove(&key);
        }
        check_map(&store, root, &map);
    }

    #[test]
    fn deletes_everything_and_collapses_the_root() {
        let mut store = MemoryStore::new();
        let mut root = 0;
        for i in 0..100u32 {
            root = insert(&mut store, root, &i.to_be_bytes(), b"value", 1).unwrap();
        }

        for i in 0..100u32 {
            root = delete(&mut store, root, &i.to_be_bytes(), 2).unwrap();
        }

        assert_eq!(root, 0);
        assert_eq!(get(&store, 0, &0u32.to_be_bytes()).unwrap(), None);
    }

    #[test]
    fn overflow_values_round_trip_and_are_freed() {
        let mut store = MemoryStore::new();
        let key = b"large";
        let value = vec![0x5a; 10_000];

        let root = insert(&mut store, 0, key, &value, 1).unwrap();
        assert_eq!(get(&store, root, key).unwrap(), Some(value.as_slice()));

        let replacement = vec![0x33; 20_000];
        let root = insert(&mut store, root, key, &replacement, 2).unwrap();
        assert_eq!(
            get(&store, root, key).unwrap(),
            Some(replacement.as_slice())
        );
        assert!(store.freed.len() >= 2);

        let root = delete(&mut store, root, key, 3).unwrap();
        assert_eq!(get(&store, root, key).unwrap(), None);
    }

    #[test]
    fn missing_delete_is_a_noop() {
        let mut store = MemoryStore::new();
        let root = insert(&mut store, 0, b"a", b"1", 1).unwrap();

        assert_eq!(delete(&mut store, root, b"missing", 2).unwrap(), root);
    }
}
