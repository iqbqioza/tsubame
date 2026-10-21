//! Typed databases.
//!
//! A [`Database`] is parameterized by a key codec and a data codec. The codecs
//! determine how keys and values are stored and in which type they are
//! returned. Database handles are lightweight and can be copied freely.

use std::fmt;
use std::marker::PhantomData;

use anyhow::bail;

use crate::Result;
use crate::traits::{BytesDecode, BytesEncode};
use crate::txn::{RoTxn, RwTxn};

/// A typed database.
///
/// `KC` is the key codec and `DC` is the data codec. Both codecs are marker
/// types such as [`crate::types::Str`] or
/// [`crate::types::U32`]. Creating a database with the wrong codecs does not
/// corrupt data, but reads and writes will fail to decode.
pub struct Database<KC, DC> {
    _codecs: PhantomData<(KC, DC)>,
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}

impl<KC, DC> fmt::Debug for Database<KC, DC> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("key_codec", &std::any::type_name::<KC>())
            .field("data_codec", &std::any::type_name::<DC>())
            .finish()
    }
}

impl<KC, DC> Database<KC, DC> {
    /// Stores `data` under `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or value cannot be encoded, or if the
    /// transaction is no longer usable. The storage engine is not implemented
    /// yet, so this currently always returns an error.
    pub fn put<'a>(
        &self,
        _txn: &mut RwTxn<'_>,
        _key: &'a KC::EItem,
        _data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        bail!("database writes are not implemented yet")
    }

    /// Returns the value associated with `key`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be encoded or the stored value
    /// cannot be decoded. The storage engine is not implemented yet, so this
    /// currently always returns an error.
    pub fn get<'a, 'txn>(
        &self,
        _txn: &'txn RoTxn<'_>,
        _key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        bail!("database reads are not implemented yet")
    }

    /// Deletes the entry stored under `key`.
    ///
    /// Deleting a missing key is not an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be encoded. The storage engine is not
    /// implemented yet, so this currently always returns an error.
    pub fn delete<'a>(&self, _txn: &mut RwTxn<'_>, _key: &'a KC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
    {
        bail!("database deletes are not implemented yet")
    }

    /// Removes every entry in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is no longer usable. The storage
    /// engine is not implemented yet, so this currently always returns an
    /// error.
    pub fn clear(&self, _txn: &mut RwTxn<'_>) -> Result<()> {
        bail!("database clears are not implemented yet")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::byteorder::NativeEndian;
    use crate::types::{Str, U32};

    fn database() -> Database<Str, U32<NativeEndian>> {
        Database {
            _codecs: PhantomData,
        }
    }

    #[test]
    fn database_handles_are_copy() {
        let database = database();
        let copy = database;

        assert_eq!(format!("{database:?}"), format!("{copy:?}"));
    }
}
