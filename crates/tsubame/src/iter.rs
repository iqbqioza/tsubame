//! Ordered iteration over databases.

use std::marker::PhantomData;

use anyhow::{Result, bail, ensure};

use crate::storage::btree::PageSource;
use crate::storage::page::{LeafValue, Page, PageType};
use crate::traits::BytesDecode;
use crate::txn::RoTxn;

/// The maximum branch depth followed by the iterator.
const MAX_DEPTH: usize = 64;

/// An ordered iterator over a database snapshot.
///
/// Yields key/value pairs in ascending key order. Keys and values borrow from
/// the read transaction's snapshot, so the iterator must be dropped before the
/// transaction.
pub struct RoIter<'txn, 'env, KC, DC> {
    source: &'txn RoTxn<'env>,
    stack: Vec<(u64, usize)>,
    leaf: Option<u64>,
    slot: usize,
    done: bool,
    _codecs: PhantomData<(KC, DC)>,
}

impl<'txn, 'env, KC, DC> RoIter<'txn, 'env, KC, DC> {
    pub(crate) fn new(source: &'txn RoTxn<'env>, root: u64) -> Result<Self> {
        let mut iterator = Self {
            source,
            stack: Vec::new(),
            leaf: None,
            slot: 0,
            done: root == 0,
            _codecs: PhantomData,
        };

        if root != 0 {
            iterator.descend_leftmost(root)?;
        }

        Ok(iterator)
    }

    fn page(&self, page_id: u64) -> Result<Page<'txn>> {
        self.source.page(page_id)
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

    fn advance_leaf(&mut self) -> Result<()> {
        self.leaf = None;

        loop {
            let depth = self.stack.len();
            if depth == 0 {
                self.done = true;
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

    fn next_inner(&mut self) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        loop {
            if self.done {
                return Ok(None);
            }

            let Some(leaf_id) = self.leaf else {
                self.done = true;
                return Ok(None);
            };

            let page = self.page(leaf_id)?;
            if self.slot < page.slot_count() {
                let index = self.slot;
                self.slot += 1;

                let key = page.key(index)?;
                let value = match page.leaf_value(index)? {
                    LeafValue::Inline(value) => value,
                    LeafValue::Overflow(reference) => self.source.overflow(reference)?,
                };

                let key = KC::bytes_decode(key)
                    .map_err(|error| anyhow::anyhow!("key decoding failed: {error}"))?;
                let value = DC::bytes_decode(value)
                    .map_err(|error| anyhow::anyhow!("value decoding failed: {error}"))?;

                return Ok(Some((key, value)));
            }

            self.advance_leaf()?;
        }
    }
}

impl<'txn, 'env, KC, DC> Iterator for RoIter<'txn, 'env, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_inner() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}
