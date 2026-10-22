use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{Mmap, MmapMut, MmapOptions};

/// Maps the data region of an environment read-only.
///
/// `offset` and `len` must describe a region inside the file. The caller keeps
/// the file open, keeps the environment exclusively locked, and never shrinks
/// the file while the mapping is alive.
///
/// # Safety
///
/// Creating a memory map is unsafe because the mapping is invalidated if the
/// underlying file is truncated or modified in a way that violates Rust's
/// aliasing rules. The storage engine upholds these invariants:
///
/// - `EnvInner` keeps the mapped file open for as long as the map exists.
/// - The environment holds an advisory lock for as long as the map exists:
///   exclusive for writable environments and shared for read-only ones, so
///   another process cannot truncate the file.
/// - The file is preallocated to `map_size` at open and is never shrunk while
///   the environment is alive.
/// - The map is read-only. Pages are written with `pwrite` only after they
///   are allocated, and a page is never rewritten while a snapshot that
///   references it can still be read.
#[allow(unsafe_code)]
pub(crate) fn map_data(file: &File, offset: u64, len: usize) -> io::Result<Mmap> {
    // SAFETY: see the function-level contract above.
    unsafe { MmapOptions::new().offset(offset).len(len).map(file) }
}

/// Returns a small read-only anonymous mapping for an environment whose data
/// region is empty (only the two meta pages exist).
pub(crate) fn map_empty() -> io::Result<Mmap> {
    MmapOptions::new().len(1).map_anon()?.make_read_only()
}

/// A shared array of atomic u64 slots backed by a file.
///
/// The first slot is reserved for a count; the remaining slots hold reader
/// snapshots. All accesses are atomic, so separate mappings in different
/// processes can coordinate without a registry file lock.
pub(crate) struct AtomicSlots {
    map: MmapMut,
    _file: File,
}

impl AtomicSlots {
    #[allow(unsafe_code)]
    pub(crate) fn map(file: File, len: usize) -> io::Result<Self> {
        if len == 0 || !len.is_multiple_of(8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "atomic slot mapping must contain whole u64 values",
            ));
        }
        // SAFETY: the caller keeps the file open in this struct, the file is
        // never shrunk while the mapping is alive, and all accesses go through
        // the atomic references returned by `slot`.
        let map = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        Ok(Self { map, _file: file })
    }

    #[allow(unsafe_code)]
    fn slot(&self, index: usize) -> &AtomicU64 {
        debug_assert!(index * 8 < self.map.len());
        // SAFETY: the mapping starts page-aligned and each slot is eight bytes
        // apart, so the cast is aligned. The mapping outlives `self`, and only
        // atomic operations access the slot. The file is shared, so separate
        // mappings observe each other's atomic writes.
        unsafe { &*(self.map.as_ptr().add(index * 8) as *const AtomicU64) }
    }

    pub(crate) fn load(&self, index: usize, ordering: Ordering) -> u64 {
        self.slot(index).load(ordering)
    }

    pub(crate) fn store(&self, index: usize, value: u64, ordering: Ordering) {
        self.slot(index).store(value, ordering);
    }

    pub(crate) fn try_claim(&self, index: usize, value: u64) -> bool {
        self.slot(index)
            .compare_exchange(0, value, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn fetch_add(&self, index: usize, value: u64, ordering: Ordering) -> u64 {
        self.slot(index).fetch_add(value, ordering)
    }

    pub(crate) fn fetch_sub(&self, index: usize, value: u64, ordering: Ordering) -> u64 {
        self.slot(index).fetch_sub(value, ordering)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn atomic_slots_share_updates_between_mappings() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slots");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(24).unwrap();
        let first = AtomicSlots::map(file, 24).unwrap();
        let second = AtomicSlots::map(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
            24,
        )
        .unwrap();

        assert!(first.try_claim(1, u64::MAX));
        assert_eq!(second.load(1, Ordering::Acquire), u64::MAX);
        first.store(1, 42, Ordering::Release);
        assert_eq!(second.load(1, Ordering::Acquire), 42);
        assert!(!second.try_claim(1, u64::MAX));
        first.fetch_add(0, 1, Ordering::AcqRel);
        assert_eq!(second.load(0, Ordering::Acquire), 1);
        first.fetch_sub(0, 1, Ordering::AcqRel);
        assert_eq!(second.load(0, Ordering::Acquire), 0);
    }

    #[test]
    fn maps_a_file_region() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.write_all(b"prefix-payload").unwrap();

        let map = map_data(&file, 7, 7).unwrap();
        assert_eq!(&map[..], b"payload");
    }
}
