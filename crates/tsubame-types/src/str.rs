use std::borrow::Cow;
use std::str;

use tsubame_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes a UTF-8 string slice ([`prim@str`]).
///
/// Encoding and decoding are both zero-copy. Decoding validates UTF-8.
pub enum Str {}

impl BytesEncode<'_> for Str {
    type EItem = str;

    #[inline]
    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    #[inline]
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        str::from_utf8(bytes).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_valid_utf8() {
        let encoded = Str::bytes_encode("héllo").unwrap();
        let decoded = Str::bytes_decode(&encoded).unwrap();

        assert_eq!(decoded, "héllo");
    }

    #[test]
    fn rejects_invalid_utf8() {
        assert!(Str::bytes_decode(&[0xff]).is_err());
    }
}
