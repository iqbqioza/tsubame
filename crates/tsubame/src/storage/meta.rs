use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use byteorder::{ByteOrder, LittleEndian};

use super::PAGE_SIZE;
use super::io;

/// The magic bytes at the start of every meta page.
const MAGIC: [u8; 8] = *b"TSUBAME\0";

/// The on-disk format version implemented by this crate.
pub(crate) const FORMAT_VERSION: u32 = 2;

/// The number of meta slots stored at the start of the data file.
const META_SLOTS: usize = 2;

const CHECKSUM_OFFSET: usize = 64;

/// Maximum number of commit descriptors stored directly in a meta page.
pub(crate) const INLINE_COMMIT_CAPACITY: usize = 16;

/// One inline commit descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct InlineCommit {
    /// Page ID, or the first page of an overflow extent.
    pub(crate) page_id: u64,
    /// Zero for a normal page, or the number of pages in an extent.
    pub(crate) page_count: u64,
}

/// The committed state of an environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Meta {
    /// Monotonically increasing transaction ID.
    pub(crate) txn_id: u64,
    /// Root page of the unnamed database, or 0 when empty.
    pub(crate) unnamed_root: u64,
    /// Root page of the named-database catalog, or 0 when empty.
    pub(crate) catalog_root: u64,
    /// Number of pages reachable from this commit, including meta pages.
    pub(crate) page_count: u64,
    /// Number of named databases.
    pub(crate) database_count: u32,
    /// Reserved flags. Must be zero in format version 2.
    pub(crate) flags: u32,
    /// First page of the commit record chain, or 0 when there are no new pages.
    pub(crate) commit_record: u64,
    /// Single committed page when the commit wrote exactly one page.
    pub(crate) commit_page: u64,
    /// Number of inline commit descriptors.
    pub(crate) inline_count: u32,
    /// Inline commit descriptors for small commits.
    pub(crate) inline: [InlineCommit; INLINE_COMMIT_CAPACITY],
}

impl Meta {
    /// Returns the initial meta state for a newly created environment.
    pub(crate) fn initial() -> Self {
        Self {
            txn_id: 1,
            unnamed_root: 0,
            catalog_root: 0,
            page_count: 2,
            database_count: 0,
            flags: 0,
            commit_record: 0,
            commit_page: 0,
            inline_count: 0,
            inline: [InlineCommit::default(); INLINE_COMMIT_CAPACITY],
        }
    }

    /// Encodes this meta state into a complete page.
    pub(crate) fn encode(&self) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];

        page[0..8].copy_from_slice(&MAGIC);
        LittleEndian::write_u32(&mut page[8..12], FORMAT_VERSION);
        LittleEndian::write_u32(&mut page[12..16], PAGE_SIZE as u32);
        LittleEndian::write_u64(&mut page[16..24], self.txn_id);
        LittleEndian::write_u64(&mut page[24..32], self.unnamed_root);
        LittleEndian::write_u64(&mut page[32..40], self.catalog_root);
        LittleEndian::write_u64(&mut page[40..48], self.page_count);
        LittleEndian::write_u32(&mut page[48..52], self.database_count);
        LittleEndian::write_u32(&mut page[52..56], self.flags);
        LittleEndian::write_u64(&mut page[56..64], self.commit_record);
        LittleEndian::write_u64(&mut page[68..76], self.commit_page);
        LittleEndian::write_u32(&mut page[76..80], self.inline_count);
        for (index, entry) in self.inline.iter().enumerate() {
            let offset = 80 + index * 16;
            LittleEndian::write_u64(&mut page[offset..offset + 8], entry.page_id);
            LittleEndian::write_u64(&mut page[offset + 8..offset + 16], entry.page_count);
        }

        let checksum = checksum(&page);
        LittleEndian::write_u32(&mut page[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4], checksum);

        page
    }

    /// Decodes and validates a meta page.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == PAGE_SIZE,
            "meta page has {} bytes, expected {PAGE_SIZE}",
            bytes.len()
        );
        ensure!(&bytes[0..8] == MAGIC.as_slice(), "invalid meta magic");

        let stored = LittleEndian::read_u32(&bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]);
        let mut copy = [0u8; PAGE_SIZE];
        copy.copy_from_slice(bytes);
        copy[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
        let actual = checksum(&copy);
        ensure!(
            stored == actual,
            "meta checksum mismatch (stored {stored:#010x}, computed {actual:#010x})"
        );

        let version = LittleEndian::read_u32(&bytes[8..12]);
        ensure!(
            version == FORMAT_VERSION,
            "unsupported format version {version}, expected {FORMAT_VERSION}"
        );

        let page_size = LittleEndian::read_u32(&bytes[12..16]);
        ensure!(
            page_size as usize == PAGE_SIZE,
            "unsupported page size {page_size}, expected {PAGE_SIZE}"
        );

        let meta = Self {
            txn_id: LittleEndian::read_u64(&bytes[16..24]),
            unnamed_root: LittleEndian::read_u64(&bytes[24..32]),
            catalog_root: LittleEndian::read_u64(&bytes[32..40]),
            page_count: LittleEndian::read_u64(&bytes[40..48]),
            database_count: LittleEndian::read_u32(&bytes[48..52]),
            flags: LittleEndian::read_u32(&bytes[52..56]),
            commit_record: LittleEndian::read_u64(&bytes[56..64]),
            commit_page: LittleEndian::read_u64(&bytes[68..76]),
            inline_count: LittleEndian::read_u32(&bytes[76..80]),
            inline: read_inline(&bytes[80..]),
        };
        meta.validate()?;

        Ok(meta)
    }

    /// Checks invariants that every committed meta page must satisfy.
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.txn_id > 0, "meta transaction ID must be non-zero");
        ensure!(
            self.page_count >= META_SLOTS as u64,
            "meta page count {} is smaller than the meta area",
            self.page_count
        );
        ensure!(
            self.flags == 0,
            "unsupported meta flags {:#010x}",
            self.flags
        );
        ensure!(
            self.commit_record == 0
                || (self.commit_record >= META_SLOTS as u64
                    && self.commit_record < self.page_count),
            "meta commit record page {} is outside the page range",
            self.commit_record
        );
        ensure!(
            self.commit_page == 0
                || (self.commit_page >= META_SLOTS as u64 && self.commit_page < self.page_count),
            "meta commit page {} is outside the page range",
            self.commit_page
        );
        ensure!(
            self.commit_record == 0 || self.commit_page == 0,
            "meta cannot have both a commit record and a single commit page"
        );
        ensure!(
            self.inline_count as usize <= INLINE_COMMIT_CAPACITY,
            "meta has too many inline commit entries"
        );
        if self.inline_count > 0 {
            ensure!(
                self.commit_record == 0 && self.commit_page == 0,
                "meta cannot mix inline commit entries with other commit records"
            );
            for entry in &self.inline[..self.inline_count as usize] {
                ensure!(
                    entry.page_id >= META_SLOTS as u64 && entry.page_id < self.page_count,
                    "inline commit page {} is outside the page range",
                    entry.page_id
                );
                if entry.page_count > 0 {
                    let end = entry
                        .page_id
                        .checked_add(entry.page_count)
                        .context("inline commit extent overflow")?;
                    ensure!(
                        end <= self.page_count,
                        "inline commit extent {}..{end} is outside the page range",
                        entry.page_id
                    );
                }
            }
        }

        validate_root("unnamed database", self.unnamed_root, self.page_count)?;
        validate_root("database catalog", self.catalog_root, self.page_count)?;

        if self.database_count == 0 {
            ensure!(
                self.catalog_root == 0,
                "meta has a catalog root but no databases"
            );
        } else {
            ensure!(
                self.catalog_root != 0,
                "meta has named databases but no catalog root"
            );
        }

        Ok(())
    }
}

/// Reads inline commit descriptors from the meta page tail.
fn read_inline(bytes: &[u8]) -> [InlineCommit; INLINE_COMMIT_CAPACITY] {
    let mut inline = [InlineCommit::default(); INLINE_COMMIT_CAPACITY];
    for (index, entry) in inline.iter_mut().enumerate() {
        let offset = index * 16;
        if offset + 16 <= bytes.len() {
            entry.page_id = LittleEndian::read_u64(&bytes[offset..offset + 8]);
            entry.page_count = LittleEndian::read_u64(&bytes[offset + 8..offset + 16]);
        }
    }
    inline
}

/// Validates that a root page is either empty or inside the page range.
fn validate_root(name: &str, root: u64, page_count: u64) -> Result<()> {
    ensure!(
        root == 0 || (root >= META_SLOTS as u64 && root < page_count),
        "{name} root page {root} is outside the page range 2..{page_count}"
    );

    Ok(())
}

/// Computes the CRC32C of a page with its checksum field zeroed.
fn checksum(page: &[u8; PAGE_SIZE]) -> u32 {
    let mut copy = *page;
    copy[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    crc32c::crc32c(&copy)
}

/// Returns the byte offset of a meta slot.
fn slot_offset(slot: usize) -> u64 {
    debug_assert!(slot < META_SLOTS);
    (slot * PAGE_SIZE) as u64
}

/// Writes a meta state into a slot.
pub(crate) fn write_slot(file: &File, slot: usize, meta: &Meta) -> Result<()> {
    ensure!(slot < META_SLOTS, "invalid meta slot {slot}");

    io::write_all_at(file, &meta.encode(), slot_offset(slot))
        .with_context(|| format!("failed to write meta slot {slot}"))?;

    Ok(())
}

/// Initializes a new data file and returns its first meta state.
pub(crate) fn initialize(file: &File) -> Result<Meta> {
    let zero = [0u8; PAGE_SIZE];
    io::write_all_at(file, &zero, slot_offset(1))
        .context("failed to initialize the inactive meta slot")?;

    let meta = Meta::initial();
    write_slot(file, 0, &meta)?;

    Ok(meta)
}

/// A loaded meta page and the slot it came from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LoadedMeta {
    pub(crate) meta: Meta,
    pub(crate) slot: u8,
}

/// Reads all valid meta slots, newest first.
pub(crate) fn load_all(file: &File, path: &Path) -> Result<Vec<LoadedMeta>> {
    let len = file
        .metadata()
        .with_context(|| format!("failed to inspect `{}`", path.display()))?
        .len();
    ensure!(
        len >= (META_SLOTS * PAGE_SIZE) as u64,
        "data file `{}` is truncated: {} bytes is smaller than the {} byte meta area",
        path.display(),
        len,
        META_SLOTS * PAGE_SIZE
    );

    let mut valid = Vec::with_capacity(META_SLOTS);
    let mut errors = Vec::with_capacity(META_SLOTS);

    for slot in 0..META_SLOTS {
        let mut page = [0u8; PAGE_SIZE];
        match io::read_exact_at(file, &mut page, slot_offset(slot)) {
            Ok(()) => match Meta::decode(&page) {
                Ok(meta) => valid.push(LoadedMeta {
                    meta,
                    slot: slot as u8,
                }),
                Err(error) => errors.push(format!("slot {slot}: {error:#}")),
            },
            Err(error) => errors.push(format!("slot {slot}: {error}")),
        }
    }

    valid.sort_by(|left, right| {
        right
            .meta
            .txn_id
            .cmp(&left.meta.txn_id)
            .then_with(|| right.slot.cmp(&left.slot))
    });

    if valid.is_empty() {
        bail!(
            "no valid meta page in `{}`: {}",
            path.display(),
            errors.join("; ")
        );
    }

    Ok(valid)
}

/// Reads both meta slots and returns the newest valid state.
#[cfg(test)]
pub(crate) fn load(file: &File, path: &Path) -> Result<LoadedMeta> {
    load_all(file, path)?
        .into_iter()
        .next()
        .context("no valid meta page")
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::tempdir;

    use super::*;

    fn meta(txn_id: u64, page_count: u64) -> Meta {
        Meta {
            txn_id,
            unnamed_root: 0,
            catalog_root: 0,
            page_count,
            database_count: 0,
            flags: 0,
            commit_record: 0,
            commit_page: 0,
            inline_count: 0,
            inline: [InlineCommit::default(); INLINE_COMMIT_CAPACITY],
        }
    }

    fn create_file() -> (tempfile::TempDir, File) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.tsubame");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len((META_SLOTS * PAGE_SIZE) as u64).unwrap();
        (dir, file)
    }

    #[test]
    fn meta_round_trip() {
        let meta = Meta {
            txn_id: 42,
            unnamed_root: 3,
            catalog_root: 7,
            page_count: 9,
            database_count: 2,
            flags: 0,
            commit_record: 0,
            commit_page: 0,
            inline_count: 0,
            inline: [InlineCommit::default(); INLINE_COMMIT_CAPACITY],
        };

        assert_eq!(Meta::decode(&meta.encode()).unwrap(), meta);
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut page = meta(1, 2).encode();
        page[100] ^= 0xff;

        assert!(Meta::decode(&page).is_err());
    }

    #[test]
    fn rejects_invalid_page_count() {
        let page = meta(1, 1).encode();

        assert!(Meta::decode(&page).is_err());
    }

    #[test]
    fn rejects_out_of_range_root() {
        let page = Meta {
            unnamed_root: 2,
            page_count: 2,
            ..meta(1, 2)
        }
        .encode();

        assert!(Meta::decode(&page).is_err());
    }

    #[test]
    fn loads_newest_valid_slot() {
        let (_dir, file) = create_file();
        write_slot(&file, 0, &meta(1, 2)).unwrap();
        write_slot(&file, 1, &meta(2, 3)).unwrap();

        let loaded = load(&file, Path::new("test")).unwrap();
        assert_eq!(loaded.meta.txn_id, 2);
        assert_eq!(loaded.slot, 1);
    }

    #[test]
    fn falls_back_to_older_slot() {
        let (_dir, file) = create_file();
        write_slot(&file, 0, &meta(1, 2)).unwrap();
        write_slot(&file, 1, &meta(2, 3)).unwrap();

        let mut byte = [0u8; 1];
        io::read_exact_at(&file, &mut byte, PAGE_SIZE as u64 + 100).unwrap();
        byte[0] ^= 0xff;
        io::write_all_at(&file, &byte, PAGE_SIZE as u64 + 100).unwrap();

        let loaded = load(&file, Path::new("test")).unwrap();
        assert_eq!(loaded.meta.txn_id, 1);
    }

    #[test]
    fn rejects_both_invalid_slots() {
        let (_dir, file) = create_file();

        assert!(load(&file, Path::new("test")).is_err());
    }

    #[test]
    fn initialize_writes_the_first_slot() {
        let (_dir, file) = create_file();
        let initialized = initialize(&file).unwrap();

        let loaded = load(&file, Path::new("test")).unwrap();
        assert_eq!(loaded.meta, initialized);
    }
}
