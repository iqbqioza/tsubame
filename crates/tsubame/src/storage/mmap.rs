use std::fs::File;
use std::io;

use memmap2::{Mmap, MmapOptions};

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

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

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
