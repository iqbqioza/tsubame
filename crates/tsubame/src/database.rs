//! Typed databases.
//!
//! A [`Database`] is parameterized by a key codec and a data codec. The codecs
//! determine how keys and values are stored and in which type they are
//! returned. Database handles are lightweight and can be cloned freely.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::{Context, ensure};

use crate::Result;
use crate::env::EnvInner;
use crate::iter::{
    RoIter, RoPrefix, RoRange, RoRevIter, RoRevPrefix, RoRevRange, RwIter, RwPrefix, RwRange,
    RwRevIter, RwRevPrefix, RwRevRange, prefix_successor,
};
use crate::storage::btree;
use crate::storage::page::MAX_KEY_SIZE;
use crate::traits::{BoxedError, BytesDecode, BytesEncode};
use crate::txn::{RoTxn, RwTxn};

/// Converts a codec error into an `anyhow` error with context.
fn codec_error(context: &str, error: BoxedError) -> anyhow::Error {
    anyhow::anyhow!("{context}: {error}")
}

/// The identity of a database inside an environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DatabaseKind {
    /// The unnamed database whose root lives in the meta page.
    Unnamed,
    /// A named database whose root lives in the catalog.
    Named(Arc<str>),
}

/// A typed database.
///
/// `KC` is the key codec and `DC` is the data codec. Both codecs are marker
/// types such as [`crate::types::Str`] or [`crate::types::U32`].
pub struct Database<KC, DC> {
    pub(crate) env: Arc<EnvInner>,
    pub(crate) kind: DatabaseKind,
    _codecs: PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(env: Arc<EnvInner>, kind: DatabaseKind) -> Self {
        Self {
            env,
            kind,
            _codecs: PhantomData,
        }
    }

    fn resolve_root(&self, txn: &RoTxn<'_>) -> Result<u64> {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        txn.root_for(&self.kind)
    }

    /// Returns the value associated with `key`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be encoded or the stored value
    /// cannot be decoded.
    #[inline]
    pub fn get<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );

        let mut key_buffer = [0u8; KEY_ENCODE_BUFFER];
        let key = encode_key::<KC>(key, &mut key_buffer)?;
        let root = txn.root_for(&self.kind)?;
        let Some(value) = btree::get_with(txn, root, key.as_slice(), &txn.comparator())? else {
            return Ok(None);
        };

        Ok(Some(DC::bytes_decode(value).map_err(|error| {
            codec_error("value decoding failed", error)
        })?))
    }

    /// Returns the write transaction's value for `key`, including staged
    /// writes and deletes.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be encoded or the stored value
    /// cannot be decoded.
    pub fn get_rw<'a, 'txn>(
        &self,
        txn: &'txn RwTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );

        let mut key_buffer = [0u8; KEY_ENCODE_BUFFER];
        let key = encode_key::<KC>(key, &mut key_buffer)?;
        let Some(value) = txn.get_raw(&self.kind, key.as_slice())? else {
            return Ok(None);
        };

        Ok(Some(DC::bytes_decode(value).map_err(|error| {
            codec_error("value decoding failed", error)
        })?))
    }

    /// Returns the first entry whose key is strictly greater than `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the bound cannot be encoded or the entry cannot be
    /// decoded.
    pub fn get_greater_than<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.range(txn, Bound::Excluded(key), Bound::Unbounded)?
            .next()
            .transpose()
    }

    /// Returns the first entry whose key is greater than or equal to `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the bound cannot be encoded or the entry cannot be
    /// decoded.
    pub fn get_greater_than_or_equal_to<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.range(txn, Bound::Included(key), Bound::Unbounded)?
            .next()
            .transpose()
    }

    /// Returns the last entry whose key is strictly less than `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the bound cannot be encoded or the entry cannot be
    /// decoded.
    pub fn get_lower_than<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.rev_range(txn, Bound::Unbounded, Bound::Excluded(key))?
            .next()
            .transpose()
    }

    /// Returns the last entry whose key is less than or equal to `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the bound cannot be encoded or the entry cannot be
    /// decoded.
    pub fn get_lower_than_or_equal_to<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.rev_range(txn, Bound::Unbounded, Bound::Included(key))?
            .next()
            .transpose()
    }

    /// Returns the first entry in ascending key order.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn first<'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.iter(txn)?.next().transpose()
    }

    /// Returns the last entry in ascending key order.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn last<'txn, 'env>(&self, txn: &'txn RoTxn<'env>) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.rev_iter(txn)?.next().transpose()
    }

    /// Returns the number of entries in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn len(&self, txn: &RoTxn<'_>) -> Result<u64>
    where
        KC: for<'a> BytesDecode<'a>,
        DC: for<'a> BytesDecode<'a>,
    {
        let mut len = 0u64;
        for entry in self.iter(txn)? {
            entry?;
            len = len.checked_add(1).context("database length overflow")?;
        }
        Ok(len)
    }

    /// Returns whether the database contains no entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn is_empty(&self, txn: &RoTxn<'_>) -> Result<bool>
    where
        KC: for<'a> BytesDecode<'a>,
        DC: for<'a> BytesDecode<'a>,
    {
        Ok(self.first(txn)?.is_none())
    }

    /// Returns the first entry in the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn first_rw<'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.iter_rw(txn)?.next().transpose()
    }

    /// Returns the last entry in the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn last_rw<'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.rev_iter_rw(txn)?.next().transpose()
    }

    /// Returns the number of entries in the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn len_rw(&self, txn: &RwTxn<'_>) -> Result<u64>
    where
        KC: for<'a> BytesDecode<'a>,
        DC: for<'a> BytesDecode<'a>,
    {
        let mut len = 0u64;
        for entry in self.iter_rw(txn)? {
            entry?;
            len = len.checked_add(1).context("database length overflow")?;
        }
        Ok(len)
    }

    /// Returns whether the write transaction's view is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved or a key/value
    /// cannot be decoded.
    pub fn is_empty_rw(&self, txn: &RwTxn<'_>) -> Result<bool>
    where
        KC: for<'a> BytesDecode<'a>,
        DC: for<'a> BytesDecode<'a>,
    {
        Ok(self.first_rw(txn)?.is_none())
    }

    /// Returns an iterator over all entries in ascending key order.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved in the transaction
    /// snapshot.
    pub fn iter<'txn, 'env>(&self, txn: &'txn RoTxn<'env>) -> Result<RoIter<'txn, 'env, KC, DC>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let root = self.resolve_root(txn)?;
        RoIter::new(txn, root)
    }

    /// Returns an iterator over the write transaction's view of the database.
    ///
    /// The iterator merges the committed snapshot with staged puts and deletes.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved in the transaction.
    pub fn iter_rw<'txn, 'env>(&self, txn: &'txn RwTxn<'env>) -> Result<RwIter<'txn, 'env, KC, DC>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        let root = txn.read_root(&self.kind)?;
        let comparator = txn.comparator();
        let pending = txn.pending_for(&self.kind, comparator);
        RwIter::new(txn, root, pending)
    }

    /// Returns a forward range iterator over the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if a bound cannot be encoded, or if the database cannot
    /// be resolved in the transaction.
    pub fn range_rw<'a, 'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
        start: Bound<&'a KC::EItem>,
        end: Bound<&'a KC::EItem>,
    ) -> Result<RwRange<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        let root = txn.read_root(&self.kind)?;
        let comparator = txn.comparator();
        let pending = txn.pending_for(&self.kind, comparator);
        let start = encode_bound::<KC>(start)?;
        let end = encode_bound::<KC>(end)?;
        RwIter::range(txn, root, pending, start, end)
    }

    /// Returns a reverse iterator over the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved in the transaction.
    pub fn rev_iter_rw<'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
    ) -> Result<RwRevIter<'txn, 'env, KC, DC>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        let root = txn.read_root(&self.kind)?;
        let comparator = txn.comparator();
        let pending = txn.pending_for_rev(&self.kind, comparator);
        RwRevIter::new(txn, root, pending, Bound::Unbounded, Bound::Unbounded)
    }

    /// Returns a reverse range iterator over the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if a bound cannot be encoded, or if the database cannot
    /// be resolved in the transaction.
    pub fn rev_range_rw<'a, 'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
        start: Bound<&'a KC::EItem>,
        end: Bound<&'a KC::EItem>,
    ) -> Result<RwRevRange<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        let root = txn.read_root(&self.kind)?;
        let comparator = txn.comparator();
        let pending = txn.pending_for_rev(&self.kind, comparator);
        let start = encode_bound::<KC>(start)?;
        let end = encode_bound::<KC>(end)?;
        RwRevIter::new(txn, root, pending, start, end)
    }

    /// Returns a forward prefix iterator over the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix cannot be encoded, or if the database
    /// cannot be resolved in the transaction.
    pub fn prefix_rw<'a, 'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
        prefix: &'a KC::EItem,
    ) -> Result<RwPrefix<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        ensure!(
            txn.comparator().is_lexicographic(),
            "prefix iteration requires a lexicographic comparator"
        );
        let root = txn.read_root(&self.kind)?;
        let comparator = txn.comparator();
        let pending = txn.pending_for(&self.kind, comparator);
        let start = KC::bytes_encode(prefix)
            .map_err(|error| codec_error("prefix encoding failed", error))?
            .into_owned();
        let end = prefix_successor(&start)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        RwIter::range(txn, root, pending, Bound::Included(start), end)
    }

    /// Returns a reverse prefix iterator over the write transaction's view.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix cannot be encoded, or if the database
    /// cannot be resolved in the transaction.
    pub fn rev_prefix_rw<'a, 'txn, 'env>(
        &self,
        txn: &'txn RwTxn<'env>,
        prefix: &'a KC::EItem,
    ) -> Result<RwRevPrefix<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        ensure!(
            txn.comparator().is_lexicographic(),
            "prefix iteration requires a lexicographic comparator"
        );
        let root = txn.read_root(&self.kind)?;
        let comparator = txn.comparator();
        let pending = txn.pending_for_rev(&self.kind, comparator);
        let start = KC::bytes_encode(prefix)
            .map_err(|error| codec_error("prefix encoding failed", error))?
            .into_owned();
        let end = prefix_successor(&start)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        RwRevIter::new(txn, root, pending, Bound::Included(start), end)
    }

    /// Returns a reverse iterator over all entries in descending key order.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be resolved in the transaction
    /// snapshot.
    pub fn rev_iter<'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
    ) -> Result<RoRevIter<'txn, 'env, KC, DC>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let root = self.resolve_root(txn)?;
        RoRevIter::new(txn, root)
    }

    /// Returns a forward iterator over the requested key range.
    ///
    /// # Errors
    ///
    /// Returns an error if a bound cannot be encoded, or if the database cannot
    /// be resolved in the transaction snapshot.
    pub fn range<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        start: Bound<&'a KC::EItem>,
        end: Bound<&'a KC::EItem>,
    ) -> Result<RoRange<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let root = self.resolve_root(txn)?;
        let start = encode_bound::<KC>(start)?;
        let end = encode_bound::<KC>(end)?;
        RoRange::new(txn, root, start, end)
    }

    /// Returns a reverse iterator over the requested key range.
    ///
    /// # Errors
    ///
    /// Returns an error if a bound cannot be encoded, or if the database cannot
    /// be resolved in the transaction snapshot.
    pub fn rev_range<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        start: Bound<&'a KC::EItem>,
        end: Bound<&'a KC::EItem>,
    ) -> Result<RoRevRange<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let root = self.resolve_root(txn)?;
        let start = encode_bound::<KC>(start)?;
        let end = encode_bound::<KC>(end)?;
        RoRevRange::new(txn, root, start, end)
    }

    /// Returns a forward iterator over all keys with the given prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix cannot be encoded, or if the database
    /// cannot be resolved in the transaction snapshot.
    pub fn prefix_iter<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        prefix: &'a KC::EItem,
    ) -> Result<RoPrefix<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.comparator().is_lexicographic(),
            "prefix iteration requires a lexicographic comparator"
        );
        let root = self.resolve_root(txn)?;
        let start = KC::bytes_encode(prefix)
            .map_err(|error| codec_error("prefix encoding failed", error))?
            .into_owned();
        let end = prefix_successor(&start)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        RoRange::new(txn, root, Bound::Included(start), end)
    }

    /// Returns a reverse iterator over all keys with the given prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix cannot be encoded, or if the database
    /// cannot be resolved in the transaction snapshot.
    pub fn rev_prefix_iter<'a, 'txn, 'env>(
        &self,
        txn: &'txn RoTxn<'env>,
        prefix: &'a KC::EItem,
    ) -> Result<RoRevPrefix<'txn, 'env, KC, DC>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        ensure!(
            txn.comparator().is_lexicographic(),
            "prefix iteration requires a lexicographic comparator"
        );
        let root = self.resolve_root(txn)?;
        let start = KC::bytes_encode(prefix)
            .map_err(|error| codec_error("prefix encoding failed", error))?
            .into_owned();
        let end = prefix_successor(&start)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        RoRevRange::new(txn, root, Bound::Included(start), end)
    }

    /// Stores `data` under `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or value cannot be encoded, or if the
    /// transaction cannot stage the write. A failed write marks the
    /// transaction as failed; it can only be aborted.
    pub fn put<'a>(
        &self,
        txn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );

        let result = (|| {
            let mut key_buffer = [0u8; KEY_ENCODE_BUFFER];
            let key = encode_key::<KC>(key, &mut key_buffer)?;
            let value = DC::bytes_encode(data)
                .map_err(|error| codec_error("value encoding failed", error))?;
            ensure!(
                key.as_slice().len() <= MAX_KEY_SIZE,
                "key is {} bytes, maximum is {MAX_KEY_SIZE}",
                key.as_slice().len()
            );
            txn.put_raw(self.kind.clone(), key.as_slice(), value.as_ref())?;
            Ok(())
        })();

        if result.is_err() {
            txn.mark_failed();
        }
        result
    }

    /// Deletes the entry stored under `key`.
    ///
    /// Deleting a missing key is not an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be encoded, or if the transaction
    /// cannot stage the write. A failed delete marks the transaction as
    /// failed; it can only be aborted.
    pub fn delete<'a>(&self, txn: &mut RwTxn<'_>, key: &'a KC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
    {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );

        let result = (|| {
            let mut key_buffer = [0u8; KEY_ENCODE_BUFFER];
            let key = encode_key::<KC>(key, &mut key_buffer)?;
            ensure!(
                key.as_slice().len() <= MAX_KEY_SIZE,
                "key is {} bytes, maximum is {MAX_KEY_SIZE}",
                key.as_slice().len()
            );
            txn.delete_raw(self.kind.clone(), key.as_slice())?;
            Ok(())
        })();

        if result.is_err() {
            txn.mark_failed();
        }
        result
    }

    /// Removes every entry from the database.
    ///
    /// Pages reachable from the old tree become reusable after the same
    /// generation checks as any other freed page.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction belongs to a different environment
    /// or the old tree cannot be traversed. A failed clear marks the
    /// transaction as failed; it can only be aborted.
    pub fn clear(&self, txn: &mut RwTxn<'_>) -> Result<()> {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        let result = txn.clear_raw(self.kind.clone());
        if result.is_err() {
            txn.mark_failed();
        }
        result
    }
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Self {
        Self {
            env: Arc::clone(&self.env),
            kind: self.kind.clone(),
            _codecs: PhantomData,
        }
    }
}

impl<KC, DC> fmt::Debug for Database<KC, DC> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("path", &self.env.path)
            .field("kind", &self.kind)
            .field("key_codec", &std::any::type_name::<KC>())
            .field("data_codec", &std::any::type_name::<DC>())
            .finish()
    }
}

const KEY_ENCODE_BUFFER: usize = 16;

enum EncodedKey<'item, 'buffer> {
    Buffer(&'buffer [u8]),
    Stored(Cow<'item, [u8]>),
}

impl EncodedKey<'_, '_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Buffer(bytes) => bytes,
            Self::Stored(bytes) => bytes.as_ref(),
        }
    }
}

fn encode_key<'item, 'buffer, KC>(
    key: &'item KC::EItem,
    buffer: &'buffer mut [u8],
) -> Result<EncodedKey<'item, 'buffer>>
where
    KC: BytesEncode<'item>,
{
    if let Some(bytes) = KC::bytes_encode_into(key, buffer)
        .map_err(|error| codec_error("key encoding failed", error))?
    {
        return Ok(EncodedKey::Buffer(bytes));
    }
    let bytes = KC::bytes_encode(key).map_err(|error| codec_error("key encoding failed", error))?;
    Ok(EncodedKey::Stored(bytes))
}

fn encode_bound<'a, KC>(bound: Bound<&'a KC::EItem>) -> Result<Bound<Vec<u8>>>
where
    KC: BytesEncode<'a>,
{
    match bound {
        Bound::Included(item) => Ok(Bound::Included(
            KC::bytes_encode(item)
                .map_err(|error| codec_error("range bound encoding failed", error))?
                .into_owned(),
        )),
        Bound::Excluded(item) => Ok(Bound::Excluded(
            KC::bytes_encode(item)
                .map_err(|error| codec_error("range bound encoding failed", error))?
                .into_owned(),
        )),
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}
