//! Encoding, decoding, and key comparison traits used by `tsubame`.
//!
//! The traits in this crate are deliberately small so they can be implemented
//! by applications and by codec crates without pulling in the storage engine.
//!
//! - [`BytesEncode`] and [`BytesDecode`] turn typed keys and values into bytes.
//! - [`Comparator`] defines the order of stored keys.
//! - [`LexicographicComparator`] is a convenience trait for comparators that
//!   order keys byte by byte.

use std::borrow::Cow;
use std::cmp::{Ord, Ordering};
use std::error::Error as StdError;

/// A boxed error returned by codec implementations.
pub type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// Encodes a typed item as a byte slice.
///
/// The returned [`Cow`] lets borrowed encodings avoid an allocation while
/// still allowing codecs that must build a buffer.
pub trait BytesEncode<'a> {
    /// The type that is encoded.
    type EItem: ?Sized + 'a;

    /// Encodes `item` as bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the item cannot be represented as bytes.
    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError>;

    /// Encodes `item` into a caller-provided buffer when possible.
    ///
    /// Codecs that can write into `buffer` should override this method to
    /// avoid an allocation on hot lookup paths. Return `None` when the codec
    /// cannot use the buffer (for example, when it needs more space); callers
    /// then fall back to [`Self::bytes_encode`].
    ///
    /// # Errors
    ///
    /// Returns an error when the item cannot be represented as bytes.
    fn bytes_encode_into<'b>(
        _item: &'a Self::EItem,
        _buffer: &'b mut [u8],
    ) -> Result<Option<&'b [u8]>, BoxedError> {
        Ok(None)
    }
}

/// Decodes a typed item from a byte slice.
///
/// The decoded item may borrow from the input bytes. This is what makes
/// zero-copy reads possible for codecs such as `Str` in `tsubame-types`.
pub trait BytesDecode<'a> {
    /// The type that is decoded.
    type DItem: 'a;

    /// Decodes `bytes` as `DItem`.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not a valid encoding.
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError>;
}

/// Defines the order of raw keys in a database.
///
/// Implementing this trait lets a database use an order other than plain
/// lexicographic byte order. A custom comparator must be deterministic and
/// consistent with the key encoding.
pub trait Comparator {
    /// Compares two encoded keys.
    fn compare(a: &[u8], b: &[u8]) -> Ordering;
}

/// A [`Comparator`] that orders keys lexicographically.
///
/// Every type implementing this trait automatically implements
/// [`Comparator`]. The extra methods describe how single bytes are ordered so
/// that prefix iteration can compute range boundaries.
pub trait LexicographicComparator: Comparator {
    /// Compares two individual bytes.
    fn compare_elem(a: u8, b: u8) -> Ordering;

    /// Returns the byte immediately after `elem`, if one exists.
    fn successor(elem: u8) -> Option<u8>;

    /// Returns the byte immediately before `elem`, if one exists.
    fn predecessor(elem: u8) -> Option<u8>;

    /// Returns the largest byte in this comparator's order.
    fn max_elem() -> u8;

    /// Returns the smallest byte in this comparator's order.
    fn min_elem() -> u8;
}

impl<C: LexicographicComparator> Comparator for C {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        for (left, right) in a.iter().zip(b) {
            if left != right {
                return C::compare_elem(*left, *right);
            }
        }

        Ord::cmp(&a.len(), &b.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Lex;

    impl LexicographicComparator for Lex {
        fn compare_elem(a: u8, b: u8) -> Ordering {
            a.cmp(&b)
        }

        fn successor(elem: u8) -> Option<u8> {
            elem.checked_add(1)
        }

        fn predecessor(elem: u8) -> Option<u8> {
            elem.checked_sub(1)
        }

        fn max_elem() -> u8 {
            u8::MAX
        }

        fn min_elem() -> u8 {
            u8::MIN
        }
    }

    #[test]
    fn compares_lexicographically() {
        assert_eq!(Lex::compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(Lex::compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(Lex::compare(b"abd", b"abc"), Ordering::Greater);
    }

    #[test]
    fn shorter_prefix_sorts_first() {
        assert_eq!(Lex::compare(b"", b"a"), Ordering::Less);
        assert_eq!(Lex::compare(b"ab", b"abc"), Ordering::Less);
    }

    #[test]
    fn reports_byte_neighbours() {
        assert_eq!(Lex::successor(0), Some(1));
        assert_eq!(Lex::successor(u8::MAX), None);
        assert_eq!(Lex::predecessor(1), Some(0));
        assert_eq!(Lex::predecessor(u8::MIN), None);
        assert_eq!(Lex::min_elem(), 0);
        assert_eq!(Lex::max_elem(), u8::MAX);
    }
}
