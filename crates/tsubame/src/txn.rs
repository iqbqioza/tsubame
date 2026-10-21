//! Read-only and read-write transactions.
//!
//! Every database operation runs inside a transaction. Read-only transactions
//! provide a consistent snapshot and can coexist; a read-write transaction has
//! exclusive access to the environment.

use std::marker::PhantomData;

use anyhow::bail;

use crate::Result;
use crate::env::Env;

/// A read-write transaction.
///
/// Writes are made through a [`RwTxn`] and become visible to new read
/// transactions only after [`RwTxn::commit`] succeeds.
pub struct RwTxn<'env> {
    _env: PhantomData<&'env Env>,
}

impl RwTxn<'_> {
    /// Commits the transaction and publishes its writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be committed. The storage
    /// engine is not implemented yet, so this currently always returns an
    /// error.
    pub fn commit(self) -> Result<()> {
        bail!("committing transactions is not implemented yet")
    }

    /// Aborts the transaction and discards its writes.
    ///
    /// Dropping a [`RwTxn`] without committing it aborts it as well.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be aborted cleanly. The
    /// storage engine is not implemented yet, so this currently always returns
    /// an error.
    pub fn abort(self) -> Result<()> {
        bail!("aborting transactions is not implemented yet")
    }
}

/// A read-only transaction.
///
/// Read-only transactions see a stable snapshot of the environment until they
/// are dropped. They are created with [`crate::Env::read_txn`].
pub struct RoTxn<'env> {
    _env: PhantomData<&'env Env>,
}
