use std::marker::PhantomData;

use bevy_impulse::{Builder, DynOutput, InputSlot};

trait DeserializeMessage: Sized {
    fn deserialize_output(output: DynOutput, builder: &mut Builder) -> DynOutput;
}

impl<T> DeserializeMessage for T
where
    T: Send + Sync + 'static + serde::de::DeserializeOwned,
{
    fn deserialize_output(output: DynOutput, builder: &mut Builder) -> DynOutput {
        output
            .into_output()
            .unwrap()
            .chain(builder)
            .map_block(|msg| serde_json::from_value::<T>(msg))
            .cancel_on_err()
            .output()
            .into()
    }
}

trait ProstMessage {}
impl<T> DeserializeMessage for T
where
    T: Send + Sync + 'static + ProstMessage,
{
    fn deserialize_output(output: DynOutput, builder: &mut Builder) -> DynOutput {
        panic!()
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
        Message: DeserializeMessage,
    {
    }
}

trait Section {
    fn on_register<SerializationOptionsT>(
        registry: &mut DiagramElementRegistry<SerializationOptionsT>,
    ) where
        SerializationOptionsT: SerializationOptions;
}

struct TestSection {
    foo: InputSlot<i64>,
}

impl Section for TestSection {
    fn on_register<SerializationOptionsT>(
        registry: &mut DiagramElementRegistry<SerializationOptionsT>,
    ) where
        SerializationOptionsT: SerializationOptions,
    {
        registry.register_message::<i64>();
    }
}
