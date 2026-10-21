//! Environments and environment configuration.
//!
//! An environment is a directory that holds one or more databases. Every
//! database belongs to exactly one environment, and transactions are created
//! from the environment.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail, ensure};
use fs4::TryLockError;
use memmap2::Mmap;

use crate::Result;
use crate::database::Database;
use crate::storage::meta::{self, Meta};
use crate::storage::{PAGE_SIZE, io, mmap};
use crate::txn::{RoTxn, RwTxn};

/// The default maximum size of an environment: 1 GiB.
pub const DEFAULT_MAP_SIZE: usize = 1024 * 1024 * 1024;

/// The default maximum number of named databases in an environment.
pub const DEFAULT_MAX_DBS: usize = 10;

const DATA_FILE: &str = "data.tsubame";
const LOCK_FILE: &str = "lock.tsubame";
const MIN_MAP_SIZE: usize = 3 * PAGE_SIZE;

/// An open environment.
///
/// Environments are created with [`EnvOpenOptions::open`]. All databases in an
/// environment share the same data file and transaction namespace.
pub struct Env {
    inner: Arc<EnvInner>,
}

impl Env {
    /// Returns the directory that backs this environment.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Returns a consistent snapshot of the environment metadata.
    pub fn info(&self) -> EnvInfo {
        EnvInfo {
            map_size: self.inner.map_size,
            page_size: PAGE_SIZE,
            page_count: self.inner.meta.page_count,
            txn_id: self.inner.meta.txn_id,
            database_count: self.inner.meta.database_count,
            max_dbs: self.inner.max_dbs,
            format_version: meta::FORMAT_VERSION,
        }
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

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Env")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

/// Metadata about an open [`Env`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EnvInfo {
    /// The effective maximum size of the data file, in bytes.
    pub map_size: usize,
    /// The page size used by the on-disk format.
    pub page_size: usize,
    /// The number of pages reachable from the current commit.
    pub page_count: u64,
    /// The transaction ID of the current commit.
    pub txn_id: u64,
    /// The number of named databases.
    pub database_count: u32,
    /// The configured maximum number of named databases.
    pub max_dbs: usize,
    /// The on-disk format version.
    pub format_version: u32,
}

/// The in-memory state shared by an environment and its handles.
struct EnvInner {
    path: PathBuf,
    map_size: usize,
    max_dbs: usize,
    meta: Meta,
    _data: File,
    _lock: File,
    _mmap: Mmap,
}

/// Options used to open an [`Env`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnvOpenOptions {
    /// The maximum size of the environment, in bytes.
    ///
    /// The data file is extended sparsely to this size. When the logical page
    /// range reaches the end of the map, writes fail with a clear error.
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

    /// Sets the maximum size of the environment, in bytes.
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
    /// Returns an error if the directory cannot be created, the environment is
    /// already locked by another process, the on-disk format is invalid, or the
    /// data file cannot be mapped.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Env> {
        let path = path.as_ref();
        fs::create_dir_all(path).with_context(|| {
            format!(
                "failed to create the environment directory `{}`",
                path.display()
            )
        })?;

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join(LOCK_FILE))
            .with_context(|| {
                format!(
                    "failed to open the environment lock `{}`",
                    path.join(LOCK_FILE).display()
                )
            })?;

        match fs4::FileExt::try_lock(&lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                bail!("environment `{}` is already locked", path.display())
            }
            Err(TryLockError::Error(error)) => {
                return Err(error).with_context(|| {
                    format!("failed to lock the environment `{}`", path.display())
                });
            }
        }

        let data_path = path.join(DATA_FILE);
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)
            .with_context(|| format!("failed to open `{}`", data_path.display()))?;

        let existing_len = data
            .metadata()
            .with_context(|| format!("failed to inspect `{}`", data_path.display()))?
            .len();
        let requested_map_size = normalize_map_size(self.map_size)?;

        let (meta, map_size) = if existing_len == 0 {
            data.set_len(requested_map_size as u64)
                .with_context(|| format!("failed to size `{}`", data_path.display()))?;
            let meta = meta::initialize(&data).context("failed to initialize the environment")?;
            data.sync_all()
                .with_context(|| format!("failed to sync `{}`", data_path.display()))?;
            io::sync_dir(path).with_context(|| format!("failed to sync `{}`", path.display()))?;
            (meta, requested_map_size)
        } else {
            let meta = meta::load(&data, &data_path)?;
            let minimum = pages_to_bytes(meta.page_count)?;
            ensure!(
                existing_len >= minimum as u64,
                "data file `{}` is truncated: {} bytes cannot hold {} pages",
                data_path.display(),
                existing_len,
                meta.page_count
            );

            let map_size = requested_map_size.max(minimum);
            if existing_len < map_size as u64 {
                data.set_len(map_size as u64)
                    .with_context(|| format!("failed to size `{}`", data_path.display()))?;
                data.sync_all()
                    .with_context(|| format!("failed to sync `{}`", data_path.display()))?;
            }
            (meta, map_size)
        };

        let data_offset = (2 * PAGE_SIZE) as u64;
        let mapped_len = map_size - 2 * PAGE_SIZE;
        let mmap = mmap::map_data(&data, data_offset, mapped_len)
            .with_context(|| format!("failed to map `{}`", data_path.display()))?;
        debug_assert_eq!(mmap.len(), mapped_len);

        let inner = EnvInner {
            path: path.to_path_buf(),
            map_size,
            max_dbs: self.max_dbs,
            meta,
            _data: data,
            _lock: lock,
            _mmap: mmap,
        };

        Ok(Env {
            inner: Arc::new(inner),
        })
    }
}

/// Rounds `map_size` up to a page boundary and enforces the minimum size.
fn normalize_map_size(requested: usize) -> Result<usize> {
    ensure!(
        requested >= MIN_MAP_SIZE,
        "map_size must be at least {MIN_MAP_SIZE} bytes, got {requested}"
    );

    requested
        .checked_add(PAGE_SIZE - 1)
        .map(|size| size / PAGE_SIZE * PAGE_SIZE)
        .context("map_size is too large")
}

/// Converts a page count to a byte size, checking for overflow.
fn pages_to_bytes(page_count: u64) -> Result<usize> {
    let pages = usize::try_from(page_count).context("page count does not fit in usize")?;
    pages
        .checked_mul(PAGE_SIZE)
        .context("environment size overflows usize")
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};

    use tempfile::tempdir;

    use super::*;

    fn options() -> EnvOpenOptions {
        let mut options = EnvOpenOptions::new();
        options.map_size(16 * 1024 * 1024);
        options
    }

    #[test]
    fn defaults_are_applied() {
        let options = EnvOpenOptions::new();

        assert_eq!(options.map_size, DEFAULT_MAP_SIZE);
        assert_eq!(options.max_dbs, DEFAULT_MAX_DBS);
    }

    #[test]
    fn builder_updates_options() {
        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 4).max_dbs(2);

        assert_eq!(options.map_size, 4096 * 4);
        assert_eq!(options.max_dbs, 2);
    }

    #[test]
    fn open_creates_environment_and_reports_info() {
        let dir = tempdir().unwrap();
        let env = options().open(dir.path()).unwrap();

        assert_eq!(env.path(), dir.path());
        assert!(dir.path().join(DATA_FILE).is_file());
        assert!(dir.path().join(LOCK_FILE).is_file());

        let info = env.info();
        assert_eq!(info.map_size, 16 * 1024 * 1024);
        assert_eq!(info.page_size, PAGE_SIZE);
        assert_eq!(info.page_count, 2);
        assert_eq!(info.txn_id, 1);
        assert_eq!(info.database_count, 0);
        assert_eq!(info.max_dbs, DEFAULT_MAX_DBS);
        assert_eq!(info.format_version, meta::FORMAT_VERSION);
    }

    #[test]
    fn reopen_reads_the_same_meta() {
        let dir = tempdir().unwrap();
        let info = {
            let env = options().open(dir.path()).unwrap();
            env.info()
        };

        let env = options().open(dir.path()).unwrap();
        assert_eq!(env.info(), info);
    }

    #[test]
    fn map_size_is_rounded_up_to_a_page() {
        let dir = tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(MIN_MAP_SIZE + 1);

        let env = options.open(dir.path()).unwrap();
        assert_eq!(env.info().map_size, 4 * PAGE_SIZE);
    }

    #[test]
    fn map_size_below_the_meta_area_is_rejected() {
        let dir = tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(MIN_MAP_SIZE - 1);

        assert!(options.open(dir.path()).is_err());
    }

    #[test]
    fn second_open_is_rejected_while_locked() {
        let dir = tempdir().unwrap();
        let _env = options().open(dir.path()).unwrap();

        let error = options().open(dir.path()).unwrap_err();
        assert!(error.to_string().contains("already locked"));
    }

    #[test]
    fn lock_is_released_on_drop() {
        let dir = tempdir().unwrap();
        drop(options().open(dir.path()).unwrap());

        assert!(options().open(dir.path()).is_ok());
    }

    #[test]
    fn truncated_data_file_is_rejected() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(DATA_FILE), b"x").unwrap();

        assert!(options().open(dir.path()).is_err());
    }

    #[test]
    fn corrupt_meta_is_rejected() {
        let dir = tempdir().unwrap();
        drop(options().open(dir.path()).unwrap());

        let path = dir.path().join(DATA_FILE);
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(std::io::SeekFrom::Start(100)).unwrap();
        file.write_all(&[0xff]).unwrap();

        assert!(options().open(dir.path()).is_err());
    }

    #[test]
    fn corrupt_inactive_slot_falls_back_to_the_active_slot() {
        let dir = tempdir().unwrap();
        drop(options().open(dir.path()).unwrap());

        let path = dir.path().join(DATA_FILE);
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(std::io::SeekFrom::Start(PAGE_SIZE as u64 + 100))
            .unwrap();
        file.write_all(&[0xff]).unwrap();

        let env = options().open(dir.path()).unwrap();
        assert_eq!(env.info().txn_id, 1);
    }
}
