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

struct CommonOperations<'a, SerializationOptionsT, Deserialize>
where
    SerializationOptionsT: SerializationOptions,
{
    registry: &'a mut DiagramElementRegistry<SerializationOptionsT>,
    _unused: PhantomData<Deserialize>,
}

impl<'a, SerializationOptionsT, Deserialize>
    CommonOperations<'a, SerializationOptionsT, Deserialize>
where
    SerializationOptionsT: SerializationOptions,
{
    fn no_request_deserializing(self) -> CommonOperations<'a, SerializationOptionsT, NotSupported> {
        CommonOperations {
            registry: self.registry,
            _unused: Default::default(),
        }
    }

    fn register_message<Message>(self)
    where
        Deserialize: DeserializeMessage<Message, SerializationOptionsT::Serialized>,
    {
    }
}

trait SerializationOptions {
    type Serialized: Send + Sync + 'static;
    type DefaultDeserializer;
}

struct JsonSerialization;

impl SerializationOptions for JsonSerialization {
    type Serialized = serde_json::Value;
    type DefaultDeserializer = JsonDeserializer;
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
    fn opt_out(
        &mut self,
    ) -> CommonOperations<SerializationOptionsT, SerializationOptionsT::DefaultDeserializer> {
        CommonOperations {
            registry: self,
            _unused: Default::default(),
        }
    }

    fn register_section<SectionT>(&mut self)
    where
        SectionT: Section<SerializationOptionsT>,
    {
    }
}

trait Section<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    fn on_register(registry: &mut DiagramElementRegistry<SerializationOptionsT>);
}

struct OpaqueMessage;

struct TestSection {
    foo: InputSlot<i64>,
    bar: Output<OpaqueMessage>,
}

impl<SerializationOptionsT> Section<SerializationOptionsT> for TestSection
where
    SerializationOptionsT: SerializationOptions,
    // need to add this bound ONLY for messages that does not opt out
    SerializationOptionsT::DefaultDeserializer:
        DeserializeMessage<i64, SerializationOptionsT::Serialized>,
    // SerializationOptionsT::DefaultDeserializer:
    //     DeserializeMessage<OpaqueMessage, SerializationOptionsT::Serialized>,
{
    fn on_register(registry: &mut DiagramElementRegistry<SerializationOptionsT>) {
        registry.opt_out().register_message::<i64>();
        registry
            .opt_out()
            .no_request_deserializing()
            .register_message::<OpaqueMessage>();
    }
}

fn test_register_section() {
    let mut registry = DiagramElementRegistry::<JsonSerialization> {
        _unused: Default::default(),
    };
    registry.register_section::<TestSection>();
}
