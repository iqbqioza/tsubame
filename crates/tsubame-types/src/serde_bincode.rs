use std::borrow::Cow;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tsubame_traits::{BoxedError, BytesDecode, BytesEncode};

/// A codec that stores values with `bincode`.
///
/// Bincode is compact but not self-describing; schema changes require care.
pub struct SerdeBincode<T>(PhantomData<T>);

impl<'a, T> BytesEncode<'a> for SerdeBincode<T>
where
    T: Serialize + 'a,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Owned(bincode::serialize(item)?))
    }
}

impl<'a, T> BytesDecode<'a> for SerdeBincode<T>
where
    T: DeserializeOwned + 'a,
{
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Ok(bincode::deserialize(bytes)?)
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
    fn round_trips_bincode() {
        let item = Item {
            name: "tsubame".to_owned(),
            count: 3,
        };
        let encoded = SerdeBincode::<Item>::bytes_encode(&item).unwrap();
        let decoded = SerdeBincode::<Item>::bytes_decode(&encoded).unwrap();

        assert_eq!(decoded, item);
    }

    #[test]
    fn rejects_invalid_bincode() {
        assert!(SerdeBincode::<Item>::bytes_decode(&[]).is_err());
    }
}
