//! Typed databases.
//!
//! A [`Database`] is parameterized by a key codec and a data codec. The codecs
//! determine how keys and values are stored and in which type they are
//! returned. Database handles are lightweight and can be cloned freely.

use std::fmt;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::ensure;

use crate::Result;
use crate::env::EnvInner;
use crate::iter::{
    RoIter, RoPrefix, RoRange, RoRevIter, RoRevPrefix, RoRevRange, RwIter, prefix_successor,
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

        let key =
            KC::bytes_encode(key).map_err(|error| codec_error("key encoding failed", error))?;
        let root = txn.root_for(&self.kind)?;
        let Some(value) = btree::get(txn, root, &key)? else {
            return Ok(None);
        };

        Ok(Some(DC::bytes_decode(value).map_err(|error| {
            codec_error("value decoding failed", error)
        })?))
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
        let pending = txn.pending_for(&self.kind);
        RwIter::new(txn, root, pending)
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
            let key =
                KC::bytes_encode(key).map_err(|error| codec_error("key encoding failed", error))?;
            let value = DC::bytes_encode(data)
                .map_err(|error| codec_error("value encoding failed", error))?;
            ensure!(
                key.len() <= MAX_KEY_SIZE,
                "key is {} bytes, maximum is {MAX_KEY_SIZE}",
                key.len()
            );
            txn.put_raw(self.kind.clone(), key.into_owned(), value.into_owned());
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
            let key =
                KC::bytes_encode(key).map_err(|error| codec_error("key encoding failed", error))?;
            ensure!(
                key.len() <= MAX_KEY_SIZE,
                "key is {} bytes, maximum is {MAX_KEY_SIZE}",
                key.len()
            );
            txn.delete_raw(self.kind.clone(), key.into_owned());
            Ok(())
        })();

        if result.is_err() {
            txn.mark_failed();
        }
        result
    }

    /// Removes every entry from the database.
    ///
    /// The old tree becomes unreachable and is reclaimed by compaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction belongs to a different environment.
    pub fn clear(&self, txn: &mut RwTxn<'_>) -> Result<()> {
        ensure!(
            txn.belongs_to(&self.env),
            "transaction belongs to a different environment"
        );
        txn.clear_raw(self.kind.clone())?;
        Ok(())
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
