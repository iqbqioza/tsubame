use std::fs::File;
use std::io;
use std::path::Path;

/// Reads exactly `buf.len()` bytes at `offset`.
#[cfg(unix)]
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(buf, offset)
}

/// Reads exactly `buf.len()` bytes at `offset`.
#[cfg(windows)]
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    let mut read = 0;
    while read < buf.len() {
        let written = file.seek_read(&mut buf[read..], offset + read as u64)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill the whole buffer",
            ));
        }
        read += written;
    }

    Ok(())
}

/// Writes all bytes at `offset`.
#[cfg(unix)]
pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.write_all_at(buf, offset)
}

/// Writes all bytes at `offset`.
#[cfg(windows)]
pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    let mut written = 0;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write the whole buffer",
            ));
        }
        written += n;
    }

    Ok(())
}

/// Syncs a directory entry so that newly created files survive a crash.
#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Syncs a directory entry so that newly created files survive a crash.
#[cfg(not(unix))]
pub(crate) fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn positional_read_write_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();

        write_all_at(&file, b"hello", 7).unwrap();

        let mut buf = [0u8; 5];
        read_exact_at(&file, &mut buf, 7).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn read_exact_at_reports_eof() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(4).unwrap();

        let mut buf = [0u8; 8];
        assert_eq!(
            read_exact_at(&file, &mut buf, 0).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
}
