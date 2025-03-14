use std::marker::PhantomData;

use bevy_impulse::{Builder, InputSlot, Output};

trait DeserializeMessage<Serialized>: Sized {
    fn deserialize_output(output: Output<Serialized>, builder: &mut Builder) -> Output<Self>;
}

impl<T> DeserializeMessage<serde_json::Value> for T
where
    T: Send + Sync + 'static + serde::de::DeserializeOwned,
{
    fn deserialize_output(
        output: Output<serde_json::Value>,
        builder: &mut Builder,
    ) -> Output<Self> {
        output
            .chain(builder)
            .map_block(|msg| serde_json::from_value::<T>(msg))
            .cancel_on_err()
            .output()
    }
}

trait SerializationOptions {
    type Serialized: Send + Sync + 'static;
}

struct JsonSerialization;

impl SerializationOptions for JsonSerialization {
    type Serialized = serde_json::Value;
}

struct DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    _unused: PhantomData<SerializationOptionsT>,
}

impl<SerializationOptionsT> DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    fn register_message<Message>(&mut self)
    where
        Message: DeserializeMessage<SerializationOptionsT::Serialized>,
    {
    }
}

trait Section<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    fn on_register(registry: &mut DiagramElementRegistry<SerializationOptionsT>);
}

struct TestSection {
    foo: InputSlot<i64>,
}

impl<SerializationOptionsT> Section<SerializationOptionsT> for TestSection
where
    SerializationOptionsT: SerializationOptions,
    i64: DeserializeMessage<SerializationOptionsT::Serialized>,
{
    fn on_register(registry: &mut DiagramElementRegistry<SerializationOptionsT>) {
        registry.register_message::<i64>();
    }
}
