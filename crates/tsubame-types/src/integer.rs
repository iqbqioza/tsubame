use std::borrow::Cow;
use std::io;
use std::marker::PhantomData;

use byteorder::ByteOrder;
use tsubame_traits::{BoxedError, BytesDecode, BytesEncode};

/// Creates an error for a byte slice whose length is not the expected one.
fn invalid_length(expected: usize, actual: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("expected {expected} bytes, got {actual}"),
    )
}

/// Encodable version of [`u8`].
pub struct U8;

impl BytesEncode<'_> for U8 {
    type EItem = u8;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(std::slice::from_ref(item)))
    }
}

impl BytesDecode<'_> for U8 {
    type DItem = u8;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        match bytes {
            [byte] => Ok(*byte),
            _ => Err(invalid_length(1, bytes.len()).into()),
        }
    }
}

/// Encodable version of [`i8`].
pub struct I8;

impl BytesEncode<'_> for I8 {
    type EItem = i8;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        Ok(Cow::Owned(vec![*item as u8]))
    }
}

impl BytesDecode<'_> for I8 {
    type DItem = i8;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        match bytes {
            [byte] => Ok(*byte as i8),
            _ => Err(invalid_length(1, bytes.len()).into()),
        }
    }
}

macro_rules! define_integer {
    ($name:ident, $native:ty, $size:literal, $read:ident, $write:ident, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<O>(PhantomData<O>);

        impl<O: ByteOrder> BytesEncode<'_> for $name<O> {
            type EItem = $native;

            #[inline]
            fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
                let mut buf = [0u8; $size];
                O::$write(&mut buf, *item);
                Ok(Cow::Owned(buf.to_vec()))
            }

            #[inline]
            fn bytes_encode_into<'b>(
                item: &Self::EItem,
                buffer: &'b mut [u8],
            ) -> Result<Option<&'b [u8]>, BoxedError> {
                if buffer.len() < $size {
                    return Ok(None);
                }
                let buf = &mut buffer[..$size];
                O::$write(buf, *item);
                Ok(Some(buf))
            }
        }

        impl<O: ByteOrder> BytesDecode<'_> for $name<O> {
            type DItem = $native;

            #[inline]
            fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
                if bytes.len() != $size {
                    return Err(invalid_length($size, bytes.len()).into());
                }

                Ok(O::$read(bytes))
            }
        }
    };
}

define_integer!(
    U16,
    u16,
    2,
    read_u16,
    write_u16,
    "Encodable version of [`u16`] using the byte order `O`."
);
define_integer!(
    U32,
    u32,
    4,
    read_u32,
    write_u32,
    "Encodable version of [`u32`] using the byte order `O`."
);
define_integer!(
    U64,
    u64,
    8,
    read_u64,
    write_u64,
    "Encodable version of [`u64`] using the byte order `O`."
);
define_integer!(
    U128,
    u128,
    16,
    read_u128,
    write_u128,
    "Encodable version of [`u128`] using the byte order `O`."
);
define_integer!(
    I16,
    i16,
    2,
    read_i16,
    write_i16,
    "Encodable version of [`i16`] using the byte order `O`."
);
define_integer!(
    I32,
    i32,
    4,
    read_i32,
    write_i32,
    "Encodable version of [`i32`] using the byte order `O`."
);
define_integer!(
    I64,
    i64,
    8,
    read_i64,
    write_i64,
    "Encodable version of [`i64`] using the byte order `O`."
);
define_integer!(
    I128,
    i128,
    16,
    read_i128,
    write_i128,
    "Encodable version of [`i128`] using the byte order `O`."
);

#[cfg(test)]
mod tests {
    use byteorder::BigEndian;

    use super::*;

    #[test]
    fn encodes_u32_in_requested_byte_order() {
        let encoded = U32::<BigEndian>::bytes_encode(&42).unwrap();

        assert_eq!(&encoded[..], &[0, 0, 0, 42]);
        assert_eq!(U32::<BigEndian>::bytes_decode(&encoded).unwrap(), 42);
    }

    #[test]
    fn round_trips_signed_integer() {
        let encoded = I16::<BigEndian>::bytes_encode(&-2).unwrap();

        assert_eq!(I16::<BigEndian>::bytes_decode(&encoded).unwrap(), -2);
    }

    #[test]
    fn round_trips_single_byte_integers() {
        assert_eq!(&U8::bytes_encode(&7).unwrap()[..], &[7]);
        assert_eq!(U8::bytes_decode(&[7]).unwrap(), 7);
        assert_eq!(I8::bytes_decode(&[0xff]).unwrap(), -1);
    }

    #[test]
    fn encodes_into_caller_buffer() {
        let mut buffer = [0u8; 8];
        let encoded = U64::<BigEndian>::bytes_encode_into(&0x0102, &mut buffer)
            .unwrap()
            .unwrap();
        assert_eq!(encoded, &[0, 0, 0, 0, 0, 0, 1, 2]);

        let mut too_small = [0u8; 3];
        assert!(
            U32::<BigEndian>::bytes_encode_into(&42, &mut too_small)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_wrong_lengths() {
        assert!(U8::bytes_decode(&[]).is_err());
        assert!(I8::bytes_decode(&[0, 1]).is_err());
        assert!(U32::<BigEndian>::bytes_decode(&[0, 0, 42]).is_err());
        assert!(U32::<BigEndian>::bytes_decode(&[0, 0, 0, 0, 42]).is_err());
    }
}
