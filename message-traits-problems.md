`serialize` and `deserialize` operations need to be generic. These are registered message operations, so `MessageOperation`, `MessageRegistry`, `DiagramElementRegistry` all need to be generic. The serializer and deserializer need to be different as well, assuming a trait is added to support this.

```rust
trait SerializationOptions {
  type Serialized;
  type DefaultDeserializer;
  type DefaultSerializer;
}

struct JsonSerialization;

impl SerializationOptions for JsonSerialization {
  type Serialized = serde_json::Value;
  type DefaultDeserializer = JsonDeserializer;
  type DefaultSerializer = JsonSerializer;
}

struct ProtoSerialization;

impl SerializationOptions for ProtoSerialization {
  type Serialized = prost::Message;
  type DefaultDeserializer = ProtoDeserializer;
  type DefaultSerializer = ProtoSerializer;
}
```

`DiagramElementRegistry::opt_out` can use the serialization options to create a corresponding `CommonOperations`

```rust
    pub fn opt_out(
        &mut self,
    ) -> CommonOperations<
        // need to add another generic param to `CommonOperations` because it contains a reference to `DiagramElementRegistry` (which is now `DiagramElementRegistry<SerializationOptionsT>`).
        SerializationOptionsT,
        SerializationOptionsT::DefaultDeserializer,
        SerializationOptionsT::DefaultSerializer,
        DefaultImpl,
    >;
```

### Problem

There is a problem with sections as one of the generated code is something like

```rust
    fn on_register<SerializationOptionsT>(
        registry: &mut ::bevy_impulse::DiagramElementRegistry<SerializationOptionsT>,
    ) where
        Self: Sized,
        SerializationOptionsT: ::bevy_impulse::SerializationOptions,
    {
        {
            let _opt_out = registry.opt_out();
            let mut _message = _opt_out
                .register_message::<<InputSlot<i64> as ::bevy_impulse::SectionItem>::MessageType>();
        }
        {
            let _opt_out = registry.opt_out();
            let mut _message = _opt_out
                .register_message::<<Output<f64> as ::bevy_impulse::SectionItem>::MessageType>();
        }
    }
```

This fails because `SerializationOptionsT::Deserializer` does not implement `DeserializeMessage<i64>` and `DeserializeMessage<f64>` (and `SerializeMessage<_>`). Trait bounds needs to be added in the `where` clause but it is not possible to add them because it would make the impl stricter than the `Section` trait. It works with single json format support because the `Deserializer` and `Serializer` created in `DiagramElementRegistry::opt_out` is a concrete type, now it is an associated type so bounds are now needed.
