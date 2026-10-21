use std::borrow::Cow;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tsubame_traits::{BoxedError, BytesDecode, BytesEncode};

/// A codec that stores values as JSON using `serde_json`.
///
/// Encoding always allocates a buffer. Decoding deserializes into an owned
/// value.
pub struct SerdeJson<T>(PhantomData<T>);

impl<'a, T> BytesEncode<'a> for SerdeJson<T>
where
    T: Serialize + 'a,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Owned(serde_json::to_vec(item)?))
    }
}

impl<'a, T> BytesDecode<'a> for SerdeJson<T>
where
    T: DeserializeOwned + 'a,
{
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        name: String,
        count: u32,
    }

    #[test]
    fn round_trips_json() {
        let item = Item {
            name: "tsubame".to_owned(),
            count: 3,
        };
        let encoded = SerdeJson::<Item>::bytes_encode(&item).unwrap();
        let decoded = SerdeJson::<Item>::bytes_decode(&encoded).unwrap();

        assert_eq!(decoded, item);
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(SerdeJson::<Item>::bytes_decode(b"{").is_err());
    }
}
