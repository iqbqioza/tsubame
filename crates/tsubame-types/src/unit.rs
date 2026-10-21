use std::borrow::Cow;
use std::error;
use std::fmt;

use tsubame_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes the unit type `()`.
///
/// The unit type is encoded as an empty byte slice.
pub enum Unit {}

impl BytesEncode<'_> for Unit {
    type EItem = ();

    fn bytes_encode(_item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Unit {
    type DItem = ();

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(NonEmptyError.into())
        }
    }
}

/// Returned when decoding a non-empty byte slice as `()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NonEmptyError;

impl fmt::Display for NonEmptyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the slice of bytes is non-empty and therefore is not a unit `()` type")
    }
}

impl error::Error for NonEmptyError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_unit() {
        let encoded = Unit::bytes_encode(&()).unwrap();
        assert!(encoded.is_empty());
        assert_eq!(Unit::bytes_decode(&encoded).unwrap(), ());
    }

    #[test]
    fn rejects_non_empty_bytes() {
        let error = Unit::bytes_decode(&[0]).unwrap_err();
        assert_eq!(error.to_string(), NonEmptyError.to_string());
    }
}
