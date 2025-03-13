use std::marker::PhantomData;

use bevy_impulse::{Builder, InputSlot, Output};

trait DeserializeMessage<T, Serialized> {
    fn deserialize_output(output: Output<Serialized>, builder: &mut Builder) -> Output<T>;
}

struct JsonDeserializer;

impl<T> DeserializeMessage<T, serde_json::Value> for JsonDeserializer
where
    T: Send + Sync + 'static + serde::de::DeserializeOwned,
{
    fn deserialize_output(output: Output<serde_json::Value>, builder: &mut Builder) -> Output<T> {
        output
            .chain(builder)
            .map_block(|msg| serde_json::from_value::<T>(msg))
            .cancel_on_err()
            .output()
    }
}

struct NotSupported;

impl<T, Serialized> DeserializeMessage<T, Serialized> for NotSupported
where
    T: Send + Sync + 'static,
    Serialized: Send + Sync + 'static,
{
    fn deserialize_output(output: Output<Serialized>, builder: &mut Builder) -> Output<T> {
        output
            .chain(builder)
            .map_block(|_| Err("not supported"))
            .cancel_on_quiet_err()
            .output()
    }
}

struct MessageRegistrationBuilder<'a, SerializationOptionsT, Message, Deserialize>
where
    SerializationOptionsT: SerializationOptions,
{
    registry: &'a mut DiagramElementRegistry<SerializationOptionsT>,
    _unused: PhantomData<(Message, Deserialize)>,
}

impl<'a, SerializationOptionsT, Message, Deserialize>
    MessageRegistrationBuilder<'a, SerializationOptionsT, Message, Deserialize>
where
    SerializationOptionsT: SerializationOptions,
    Deserialize: DeserializeMessage<Message, SerializationOptionsT::Serialized>,
{
    fn no_request_deserializing(
        self,
    ) -> MessageRegistrationBuilder<'a, SerializationOptionsT, Message, NotSupported> {
        MessageRegistrationBuilder {
            registry: self.registry,
            _unused: Default::default(),
        }
    }

    fn register_message(self) {}
}

trait SerializationOptions {
    type Serialized: Send + Sync + 'static;
    type DefaultDeserializer<T>: DeserializeMessage<T, Self::Serialized>;
}

struct JsonSerialization;

impl SerializationOptions for JsonSerialization {
    type Serialized = serde_json::Value;
    type DefaultDeserializer<T> = JsonDeserializer<T>;
}

struct DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    _unused: PhantomData<SerializationOptionsT>,
}

impl<SerializationOptionsT> DiagramElementRegistry<SerializationOptionsT> where
    SerializationOptionsT: SerializationOptions
{
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
        let builder = MessageRegistrationBuilder::<
            SerializationOptionsT,
            i64,
            SerializationOptionsT::DefaultDeserializer,
        > {
            registry,
            _unused: Default::default(),
        };
        builder.register_message();
    }
}
