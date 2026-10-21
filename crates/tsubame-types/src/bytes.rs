use std::borrow::Cow;

use tsubame_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes a borrowed byte slice (`[u8]`).
///
/// Encoding and decoding are both zero-copy: the returned bytes borrow from
/// the input.
pub enum Bytes {}

impl<'a> BytesEncode<'a> for Bytes {
    type EItem = [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for Bytes {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_without_copying() {
        let input = b"hello".as_slice();
        let encoded = Bytes::bytes_encode(input).unwrap();
        let decoded = Bytes::bytes_decode(&encoded).unwrap();

        assert!(matches!(encoded, Cow::Borrowed(_)));
        assert_eq!(decoded, input);
    }
}
