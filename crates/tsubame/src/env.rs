//! Environments and environment configuration.
//!
//! An environment is a directory that holds one or more databases. Every
//! database belongs to exactly one environment, and transactions are created
//! from the environment.

use std::path::{Path, PathBuf};

use anyhow::bail;

use crate::Result;
use crate::database::Database;
use crate::txn::{RoTxn, RwTxn};

/// The default maximum size of an environment's memory map: 10 MiB.
pub const DEFAULT_MAP_SIZE: usize = 10 * 1024 * 1024;

/// The default maximum number of named databases in an environment.
pub const DEFAULT_MAX_DBS: usize = 10;

/// An open environment.
///
/// Environments are created with [`EnvOpenOptions::open`]. All databases in an
/// environment share the same memory map and transaction namespace.
#[derive(Debug)]
pub struct Env {
    path: PathBuf,
}

impl Env {
    /// Returns the directory that backs this environment.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Starts a read-only transaction.
    ///
    /// Read-only transactions can run concurrently with each other and see a
    /// consistent snapshot of the environment.
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        bail!("read transactions are not implemented yet")
    }

    /// Starts a read-write transaction.
    ///
    /// Only one read-write transaction can be active at a time.
    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        bail!("write transactions are not implemented yet")
    }

    /// Opens an existing database, or the unnamed default database.
    ///
    /// Opening a database that does not exist is an error. Use
    /// [`Env::create_database`] to create it.
    pub fn open_database<KC, DC>(
        &self,
        _txn: &RoTxn<'_>,
        _name: Option<&str>,
    ) -> Result<Database<KC, DC>> {
        bail!("opening databases is not implemented yet")
    }

    /// Creates a named database, or opens it if it already exists.
    ///
    /// Pass `None` to use the unnamed default database.
    pub fn create_database<KC, DC>(
        &self,
        _txn: &mut RwTxn<'_>,
        _name: Option<&str>,
    ) -> Result<Database<KC, DC>> {
        bail!("creating databases is not implemented yet")
    }
}

/// Options used to open an [`Env`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnvOpenOptions {
    /// The maximum size of the environment's memory map, in bytes.
    pub map_size: usize,
    /// The maximum number of named databases that can be created.
    pub max_dbs: usize,
}

impl Default for EnvOpenOptions {
    fn default() -> Self {
        Self {
            map_size: DEFAULT_MAP_SIZE,
            max_dbs: DEFAULT_MAX_DBS,
        }
    }
}

impl EnvOpenOptions {
    /// Creates options with the default map size and database limit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum size of the environment's memory map, in bytes.
    pub fn map_size(&mut self, map_size: usize) -> &mut Self {
        self.map_size = map_size;
        self
    }

    /// Sets the maximum number of named databases.
    pub fn max_dbs(&mut self, max_dbs: usize) -> &mut Self {
        self.max_dbs = max_dbs;
        self
    }

    /// Opens the environment in `path`, creating the directory if needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be opened as a Tsubame
    /// environment. The storage engine is not implemented yet, so this
    /// currently always returns an error.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Env> {
        let path = path.as_ref();

        bail!(
            "the Tsubame storage engine is not implemented yet \
             (path `{}`, map_size {}, max_dbs {})",
            path.display(),
            self.map_size,
            self.max_dbs
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_applied() {
        let options = EnvOpenOptions::new();

        assert_eq!(options.map_size, DEFAULT_MAP_SIZE);
        assert_eq!(options.max_dbs, DEFAULT_MAX_DBS);
    }

    #[test]
    fn builder_updates_options() {
        let mut options = EnvOpenOptions::new();
        options.map_size(4096).max_dbs(2);

        assert_eq!(options.map_size, 4096);
        assert_eq!(options.max_dbs, 2);
    }
}
