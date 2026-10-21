//! Ordered iteration over database snapshots.

use std::cmp::Ordering;
use std::iter::Peekable;
use std::marker::PhantomData;
use std::ops::Bound;

use anyhow::{Result, bail, ensure};

use crate::storage::btree::PageSource;
use crate::storage::page::{LeafValue, Page, PageType};
use crate::traits::BytesDecode;
use crate::txn::{PendingIter, RoTxn, RwTxn};

/// The maximum branch depth followed by a cursor.
const MAX_DEPTH: usize = 64;

struct Cursor<'a, S: PageSource> {
    source: &'a S,
    stack: Vec<(u64, usize)>,
    leaf: Option<u64>,
    slot: usize,
}

impl<'a, S: PageSource> Cursor<'a, S> {
    fn new(source: &'a S) -> Self {
        Self {
            source,
            stack: Vec::new(),
            leaf: None,
            slot: 0,
        }
    }

    fn page(&self, page_id: u64) -> Result<Page<'a>> {
        self.source.page(page_id)
    }

    fn seek_first(&mut self, root: u64) -> Result<()> {
        self.stack.clear();
        self.leaf = None;
        if root != 0 {
            self.descend_leftmost(root)?;
        }
        Ok(())
    }

    fn seek_last(&mut self, root: u64) -> Result<()> {
        self.stack.clear();
        self.leaf = None;
        if root != 0 {
            self.descend_rightmost(root)?;
        }
        Ok(())
    }

    fn seek_forward(&mut self, root: u64, key: &[u8]) -> Result<()> {
        self.stack.clear();
        self.leaf = None;
        if root == 0 {
            return Ok(());
        }

        let mut page_id = root;
        for _ in 0..MAX_DEPTH {
            let page = self.page(page_id)?;
            match page.page_type() {
                PageType::Branch => {
                    ensure!(
                        page.slot_count() > 0,
                        "branch page {page_id} has no children"
                    );
                    let index = page.branch_child_index(key)?;
                    self.stack.push((page_id, index));
                    page_id = page.child(index)?;
                }
                PageType::Leaf => {
                    let slot = page.lower_bound(key)?;
                    self.leaf = Some(page_id);
                    self.slot = slot;
                    if slot >= page.slot_count() {
                        self.advance_leaf()?;
                    }
                    return Ok(());
                }
            }
        }

        bail!("B+tree depth exceeded {MAX_DEPTH} during range seek")
    }

    fn seek_reverse(&mut self, root: u64, key: &[u8]) -> Result<()> {
        self.stack.clear();
        self.leaf = None;
        if root == 0 {
            return Ok(());
        }

        let mut page_id = root;
        for _ in 0..MAX_DEPTH {
            let page = self.page(page_id)?;
            match page.page_type() {
                PageType::Branch => {
                    ensure!(
                        page.slot_count() > 0,
                        "branch page {page_id} has no children"
                    );
                    let index = page.branch_child_index(key)?;
                    self.stack.push((page_id, index));
                    page_id = page.child(index)?;
                }
                PageType::Leaf => {
                    let slot = page.upper_bound(key)?;
                    self.leaf = Some(page_id);
                    if slot > 0 {
                        self.slot = slot - 1;
                    } else {
                        self.retreat_leaf()?;
                    }
                    return Ok(());
                }
            }
        }

        bail!("B+tree depth exceeded {MAX_DEPTH} during reverse seek")
    }

    fn descend_leftmost(&mut self, mut page_id: u64) -> Result<()> {
        for _ in 0..MAX_DEPTH {
            let page = self.page(page_id)?;
            match page.page_type() {
                PageType::Branch => {
                    ensure!(
                        page.slot_count() > 0,
                        "branch page {page_id} has no children"
                    );
                    self.stack.push((page_id, 0));
                    page_id = page.child(0)?;
                }
                PageType::Leaf => {
                    self.leaf = Some(page_id);
                    self.slot = 0;
                    return Ok(());
                }
            }
        }

        bail!("B+tree depth exceeded {MAX_DEPTH} during iteration")
    }

    fn descend_rightmost(&mut self, mut page_id: u64) -> Result<()> {
        for _ in 0..MAX_DEPTH {
            let page = self.page(page_id)?;
            match page.page_type() {
                PageType::Branch => {
                    ensure!(
                        page.slot_count() > 0,
                        "branch page {page_id} has no children"
                    );
                    let index = page.slot_count() - 1;
                    self.stack.push((page_id, index));
                    page_id = page.child(index)?;
                }
                PageType::Leaf => {
                    ensure!(page.slot_count() > 0, "leaf page {page_id} is empty");
                    self.leaf = Some(page_id);
                    self.slot = page.slot_count() - 1;
                    return Ok(());
                }
            }
        }

        bail!("B+tree depth exceeded {MAX_DEPTH} during reverse iteration")
    }

    fn advance_leaf(&mut self) -> Result<()> {
        self.leaf = None;

        loop {
            let depth = self.stack.len();
            if depth == 0 {
                return Ok(());
            }

            let (branch_page, index) = self.stack[depth - 1];
            let page = self.page(branch_page)?;
            let next_index = index + 1;

            if next_index < page.slot_count() {
                self.stack[depth - 1].1 = next_index;
                let child = page.child(next_index)?;
                return self.descend_leftmost(child);
            }

            self.stack.pop();
        }
    }

    fn retreat_leaf(&mut self) -> Result<()> {
        self.leaf = None;

        loop {
            let depth = self.stack.len();
            if depth == 0 {
                return Ok(());
            }

            let (branch_page, index) = self.stack[depth - 1];
            if index > 0 {
                let page = self.page(branch_page)?;
                self.stack[depth - 1].1 = index - 1;
                let child = page.child(index - 1)?;
                return self.descend_rightmost(child);
            }

            self.stack.pop();
        }
    }

    fn current(&self) -> Result<Option<(&'a [u8], &'a [u8])>> {
        let Some(leaf_id) = self.leaf else {
            return Ok(None);
        };
        let page = self.page(leaf_id)?;
        if self.slot >= page.slot_count() {
            return Ok(None);
        }

        let key = page.key(self.slot)?;
        let value = match page.leaf_value(self.slot)? {
            LeafValue::Inline(value) => value,
            LeafValue::Overflow(reference) => self.source.overflow(reference)?,
        };

        Ok(Some((key, value)))
    }

    fn current_key(&self) -> Result<Option<&'a [u8]>> {
        let Some(leaf_id) = self.leaf else {
            return Ok(None);
        };
        let page = self.page(leaf_id)?;
        if self.slot >= page.slot_count() {
            return Ok(None);
        }
        Ok(Some(page.key(self.slot)?))
    }

    fn next(&mut self) -> Result<()> {
        let Some(leaf_id) = self.leaf else {
            return Ok(());
        };
        let page = self.page(leaf_id)?;
        if self.slot + 1 < page.slot_count() {
            self.slot += 1;
            Ok(())
        } else {
            self.advance_leaf()
        }
    }

    fn prev(&mut self) -> Result<()> {
        if self.leaf.is_none() {
            return Ok(());
        }
        if self.slot > 0 {
            self.slot -= 1;
            Ok(())
        } else {
            self.retreat_leaf()
        }
    }
}

fn decode<'txn, KC, DC>(key: &'txn [u8], value: &'txn [u8]) -> Result<(KC::DItem, DC::DItem)>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    let key =
        KC::bytes_decode(key).map_err(|error| anyhow::anyhow!("key decoding failed: {error}"))?;
    let value = DC::bytes_decode(value)
        .map_err(|error| anyhow::anyhow!("value decoding failed: {error}"))?;
    Ok((key, value))
}

/// An ordered iterator over a database snapshot.
///
/// Yields key/value pairs in ascending key order.
pub struct RoIter<'txn, 'env, KC, DC> {
    cursor: Cursor<'txn, RoTxn<'env>>,
    _codecs: PhantomData<(KC, DC)>,
}

impl<'txn, 'env, KC, DC> RoIter<'txn, 'env, KC, DC> {
    pub(crate) fn new(source: &'txn RoTxn<'env>, root: u64) -> Result<Self> {
        let mut cursor = Cursor::new(source);
        cursor.seek_first(root)?;
        Ok(Self {
            cursor,
            _codecs: PhantomData,
        })
    }
}

impl<'txn, 'env, KC, DC> Iterator for RoIter<'txn, 'env, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = match self.cursor.current() {
            Ok(Some(current)) => current,
            Ok(None) => return None,
            Err(error) => {
                self.cursor.leaf = None;
                return Some(Err(error));
            }
        };

        let item = decode::<KC, DC>(current.0, current.1);
        if let Err(error) = self.cursor.next() {
            self.cursor.leaf = None;
            return Some(Err(error));
        }
        Some(item)
    }
}

/// An ordered reverse iterator over a database snapshot.
///
/// Yields key/value pairs in descending key order.
pub struct RoRevIter<'txn, 'env, KC, DC> {
    cursor: Cursor<'txn, RoTxn<'env>>,
    _codecs: PhantomData<(KC, DC)>,
}

impl<'txn, 'env, KC, DC> RoRevIter<'txn, 'env, KC, DC> {
    pub(crate) fn new(source: &'txn RoTxn<'env>, root: u64) -> Result<Self> {
        let mut cursor = Cursor::new(source);
        cursor.seek_last(root)?;
        Ok(Self {
            cursor,
            _codecs: PhantomData,
        })
    }
}

impl<'txn, 'env, KC, DC> Iterator for RoRevIter<'txn, 'env, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = match self.cursor.current() {
            Ok(Some(current)) => current,
            Ok(None) => return None,
            Err(error) => {
                self.cursor.leaf = None;
                return Some(Err(error));
            }
        };

        let item = decode::<KC, DC>(current.0, current.1);
        if let Err(error) = self.cursor.prev() {
            self.cursor.leaf = None;
            return Some(Err(error));
        }
        Some(item)
    }
}

/// An ordered iterator over a write transaction, including staged writes.
///
/// The iterator merges the committed snapshot with the transaction's pending
/// put/delete operations. It borrows the transaction immutably.
pub struct RwIter<'txn, 'env, KC, DC> {
    cursor: Cursor<'txn, RwTxn<'env>>,
    pending: Peekable<PendingIter<'txn>>,
    done: bool,
    _codecs: PhantomData<(KC, DC)>,
}

impl<'txn, 'env, KC, DC> RwIter<'txn, 'env, KC, DC> {
    pub(crate) fn new(
        source: &'txn RwTxn<'env>,
        root: u64,
        pending: PendingIter<'txn>,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(source);
        cursor.seek_first(root)?;
        Ok(Self {
            cursor,
            pending: pending.peekable(),
            done: false,
            _codecs: PhantomData,
        })
    }
}

impl<'txn, 'env, KC, DC> Iterator for RwIter<'txn, 'env, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let base = match self.cursor.current() {
                Ok(current) => current,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            };
            let pending = self.pending.peek().copied();

            match (base, pending) {
                (None, None) => {
                    self.done = true;
                    return None;
                }
                (Some((key, value)), None) => {
                    let item = decode::<KC, DC>(key, value);
                    if let Err(error) = self.cursor.next() {
                        self.done = true;
                        return Some(Err(error));
                    }
                    return Some(item);
                }
                (None, Some((key, value))) => {
                    self.pending.next();
                    if let Some(value) = value {
                        return Some(decode::<KC, DC>(key, value));
                    }
                }
                (Some((base_key, base_value)), Some((pending_key, pending_value))) => {
                    match pending_key.cmp(base_key) {
                        Ordering::Less => {
                            self.pending.next();
                            if let Some(value) = pending_value {
                                return Some(decode::<KC, DC>(pending_key, value));
                            }
                        }
                        Ordering::Equal => {
                            self.pending.next();
                            if let Err(error) = self.cursor.next() {
                                self.done = true;
                                return Some(Err(error));
                            }
                            if let Some(value) = pending_value {
                                return Some(decode::<KC, DC>(pending_key, value));
                            }
                        }
                        Ordering::Greater => {
                            let item = decode::<KC, DC>(base_key, base_value);
                            if let Err(error) = self.cursor.next() {
                                self.done = true;
                                return Some(Err(error));
                            }
                            return Some(item);
                        }
                    }
                }
            }
        }
    }
}

/// A forward range iterator.
pub struct RoRange<'txn, 'env, KC, DC> {
    cursor: Cursor<'txn, RoTxn<'env>>,
    end: Bound<Vec<u8>>,
    done: bool,
    _codecs: PhantomData<(KC, DC)>,
}

impl<'txn, 'env, KC, DC> RoRange<'txn, 'env, KC, DC> {
    pub(crate) fn new(
        source: &'txn RoTxn<'env>,
        root: u64,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(source);
        match start {
            Bound::Unbounded => cursor.seek_first(root)?,
            Bound::Included(key) => cursor.seek_forward(root, &key)?,
            Bound::Excluded(key) => {
                cursor.seek_forward(root, &key)?;
                if cursor.current_key()?.is_some_and(|current| current == key) {
                    cursor.next()?;
                }
            }
        }

        Ok(Self {
            cursor,
            end,
            done: false,
            _codecs: PhantomData,
        })
    }

    fn past_end(&self, key: &[u8]) -> bool {
        match &self.end {
            Bound::Unbounded => false,
            Bound::Included(end) => key > end.as_slice(),
            Bound::Excluded(end) => key >= end.as_slice(),
        }
    }
}

impl<'txn, 'env, KC, DC> Iterator for RoRange<'txn, 'env, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let key = match self.cursor.current_key() {
            Ok(Some(key)) => key,
            Ok(None) => return None,
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        };

        if self.past_end(key) {
            self.done = true;
            return None;
        }

        let Some((key, value)) = (match self.cursor.current() {
            Ok(current) => current,
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        }) else {
            self.done = true;
            return None;
        };

        let item = decode::<KC, DC>(key, value);
        if let Err(error) = self.cursor.next() {
            self.done = true;
            return Some(Err(error));
        }
        Some(item)
    }
}

/// A reverse range iterator.
pub struct RoRevRange<'txn, 'env, KC, DC> {
    cursor: Cursor<'txn, RoTxn<'env>>,
    start: Bound<Vec<u8>>,
    done: bool,
    _codecs: PhantomData<(KC, DC)>,
}

impl<'txn, 'env, KC, DC> RoRevRange<'txn, 'env, KC, DC> {
    pub(crate) fn new(
        source: &'txn RoTxn<'env>,
        root: u64,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(source);
        match end {
            Bound::Unbounded => cursor.seek_last(root)?,
            Bound::Included(key) => cursor.seek_reverse(root, &key)?,
            Bound::Excluded(key) => {
                cursor.seek_reverse(root, &key)?;
                if cursor.current_key()?.is_some_and(|current| current == key) {
                    cursor.prev()?;
                }
            }
        }

        Ok(Self {
            cursor,
            start,
            done: false,
            _codecs: PhantomData,
        })
    }

    fn before_start(&self, key: &[u8]) -> bool {
        match &self.start {
            Bound::Unbounded => false,
            Bound::Included(start) => key < start.as_slice(),
            Bound::Excluded(start) => key <= start.as_slice(),
        }
    }
}

impl<'txn, 'env, KC, DC> Iterator for RoRevRange<'txn, 'env, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let key = match self.cursor.current_key() {
            Ok(Some(key)) => key,
            Ok(None) => return None,
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        };

        if self.before_start(key) {
            self.done = true;
            return None;
        }

        let Some((key, value)) = (match self.cursor.current() {
            Ok(current) => current,
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        }) else {
            self.done = true;
            return None;
        };

        let item = decode::<KC, DC>(key, value);
        if let Err(error) = self.cursor.prev() {
            self.done = true;
            return Some(Err(error));
        }
        Some(item)
    }
}

/// A prefix iterator, implemented as a forward range.
pub type RoPrefix<'txn, 'env, KC, DC> = RoRange<'txn, 'env, KC, DC>;

/// A reverse prefix iterator, implemented as a reverse range.
pub type RoRevPrefix<'txn, 'env, KC, DC> = RoRevRange<'txn, 'env, KC, DC>;

/// Returns the exclusive upper bound for a byte prefix, if one exists.
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xFF {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}
