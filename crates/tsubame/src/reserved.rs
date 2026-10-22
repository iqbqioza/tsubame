//! Buffers for reserved-value writes.

use std::fmt;
use std::io::{self, Seek, SeekFrom, Write};

/// A fixed-size buffer supplied to
/// [`Database::put_reserved`](crate::Database::put_reserved) closures.
///
/// The closure must fill the entire buffer. Writes past the buffer fail with
/// [`io::ErrorKind::WriteZero`], and leaving bytes unwritten makes the put
/// return [`io::ErrorKind::UnexpectedEof`].
pub struct ReservedSpace<'a> {
    bytes: &'a mut [u8],
    written: usize,
    write_head: usize,
}

impl<'a> ReservedSpace<'a> {
    pub(crate) fn new(bytes: &'a mut [u8]) -> Self {
        Self {
            bytes,
            written: 0,
            write_head: 0,
        }
    }

    /// Returns the total number of bytes in this buffer.
    #[must_use]
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the number of bytes remaining after the write head.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.write_head
    }

    /// Returns the bytes written so far.
    pub fn written_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.written]
    }

    /// Marks the entire buffer as written and fills unwritten bytes with zero.
    pub fn fill_zeroes(&mut self) {
        self.bytes[self.write_head..].fill(0);
        self.written = self.bytes.len();
        self.write_head = self.bytes.len();
    }
}

impl Write for ReservedSpace<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let remaining = &mut self.bytes[self.write_head..];
        if buf.len() > remaining.len() {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        remaining[..buf.len()].copy_from_slice(buf);
        self.write_head += buf.len();
        self.written = self.written.max(self.write_head);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for ReservedSpace<'_> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let (base, offset) = match pos {
            SeekFrom::Start(start) => (start, 0),
            SeekFrom::End(offset) => (self.written as u64, offset),
            SeekFrom::Current(offset) => (self.write_head as u64, offset),
        };
        let Some(new_pos) = base.checked_add_signed(offset) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot seek before the start of reserved space",
            ));
        };
        if new_pos > self.written as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot seek past the written reserved space",
            ));
        }
        self.write_head = new_pos as usize;
        Ok(new_pos)
    }

    fn rewind(&mut self) -> io::Result<()> {
        self.write_head = 0;
        Ok(())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.write_head as u64)
    }
}

impl fmt::Debug for ReservedSpace<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservedSpace")
            .field("size", &self.size())
            .field("written", &self.written)
            .field("write_head", &self.write_head)
            .finish()
    }
}
