use super::{DeserializeFn, DeserializeMessage, SerializeFn, SerializeMessage};

pub struct JsonDeserializer;

impl<T> DeserializeMessage<T, serde_json::Value> for JsonDeserializer
where
    T: Send + Sync + 'static + serde::de::DeserializeOwned,
{
    fn deserialize_fn() -> Option<DeserializeFn<serde_json::Value>> {
        Some(|output, builder| {
            Ok(output
                .chain(builder)
                .map_block(|msg| serde_json::from_value::<T>(msg))
                .cancel_on_err()
                .output()
                .into())
        })
    }
}

pub struct JsonSerializer;

impl<T> SerializeMessage<T, serde_json::Value> for JsonSerializer
where
    T: Send + Sync + 'static + serde::Serialize,
{
    fn serialize_fn() -> Option<SerializeFn<serde_json::Value>> {
        Some(|output, builder| {
            Ok(output
                .into_output::<T>()?
                .chain(builder)
                .map_block(|msg| serde_json::to_value(msg))
                .cancel_on_err()
                .output())
        })
    }
}
