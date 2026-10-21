//! Key ordering for an environment.
//!
//! By default Tsubame orders encoded keys lexicographically. Applications can
//! supply a deterministic comparator to use a different order. The comparator
//! implementation is not stored, but a stable identifier is persisted in
//! `comparator.tsubame`; every process that opens the environment, and every
//! offline compaction, must use the same comparator and identifier.

use std::cmp::Ordering;

/// A function that compares two encoded keys.
pub type CompareFn = fn(&[u8], &[u8]) -> Ordering;

/// The key order used by a database.
#[derive(Clone, Copy, Debug)]
pub struct KeyComparator {
    compare: CompareFn,
    lexicographic: bool,
}

pub(crate) const LEXICOGRAPHIC: KeyComparator = KeyComparator {
    compare: <[u8]>::cmp,
    lexicographic: true,
};

impl KeyComparator {
    /// Returns the default lexicographic byte order.
    #[must_use]
    pub fn lexicographic() -> Self {
        LEXICOGRAPHIC
    }

    /// Creates a custom key order.
    ///
    /// The function must be deterministic and consistent with the key encoding.
    /// It must also be supplied again, identically, whenever the environment is
    /// opened or compacted. Prefix iteration is only available for the default
    /// lexicographic comparator.
    #[must_use]
    pub fn custom(compare: CompareFn) -> Self {
        Self {
            compare,
            lexicographic: false,
        }
    }

    /// Returns whether this comparator uses lexicographic byte order.
    #[must_use]
    pub fn is_lexicographic(self) -> bool {
        self.lexicographic
    }

    #[inline]
    pub(crate) fn compare(self, a: &[u8], b: &[u8]) -> Ordering {
        if self.lexicographic {
            a.cmp(b)
        } else {
            (self.compare)(a, b)
        }
    }
}

impl Default for KeyComparator {
    fn default() -> Self {
        Self::lexicographic()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_order_is_lexicographic() {
        let comparator = KeyComparator::lexicographic();
        assert!(comparator.is_lexicographic());
        assert_eq!(comparator.compare(b"a", b"b"), Ordering::Less);
    }

    #[test]
    fn custom_order_is_used() {
        let comparator = KeyComparator::custom(|a, b| b.cmp(a));
        assert!(!comparator.is_lexicographic());
        assert_eq!(comparator.compare(b"a", b"b"), Ordering::Greater);
    }
}
