//! Flags controlling conditional writes.

/// Flags accepted by [`Database::put_with_flags`](crate::Database::put_with_flags).
///
/// Tsubame does not support duplicate data items. Only
/// [`PutFlags::NO_OVERWRITE`] is currently implemented; the other flags are
/// defined for source compatibility with heed and return an error when used.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PutFlags(u32);

impl PutFlags {
    /// Do not replace an existing value. The write fails if the key exists.
    pub const NO_OVERWRITE: Self = Self(1 << 0);
    /// Hint that keys are appended in order.
    ///
    /// This flag is reserved for compatibility and is not implemented yet.
    pub const APPEND: Self = Self(1 << 1);
    /// Append a duplicate value in key order.
    ///
    /// This flag is reserved for compatibility and is not implemented yet.
    pub const APPEND_DUP: Self = Self(1 << 2);
    /// Do not insert a duplicate key/value pair.
    ///
    /// This flag is reserved for compatibility and is not implemented yet.
    pub const NO_DUP_DATA: Self = Self(1 << 3);

    /// Returns an empty flag set.
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Returns the raw flag bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Returns whether every flag in `other` is present.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns whether no flags are set.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl std::ops::BitOr for PutFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for PutFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}
