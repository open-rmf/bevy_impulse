use std::{any::Any, fmt::Debug};

use bevy_ecs::entity::Entity;

use crate::{AnyBuffer, AsAnyBuffer, BufferSettings, Builder, InputSlot, Output};

use super::{impls::NotSupported, type_info::TypeInfo, DiagramErrorCode};

/// A type erased [`crate::Output`]
pub struct DynOutput {
    scope: Entity,
    target: Entity,
    pub(super) type_info: TypeInfo,

    into_any_buffer_impl:
        fn(Self, &mut Builder, BufferSettings) -> Result<AnyBuffer, DiagramErrorCode>,
}

impl DynOutput {
    pub(super) fn into_output<T>(self) -> Result<Output<T>, DiagramErrorCode>
    where
        T: Send + Sync + 'static + Any,
    {
        if self.type_info != TypeInfo::of::<T>() {
            Err(DiagramErrorCode::TypeMismatch {
                source_type: self.type_info,
                target_type: TypeInfo::of::<T>(),
            })
        } else {
            Ok(Output::<T>::new(self.scope, self.target))
        }
    }

    pub(super) fn into_any_buffer(
        self,
        builder: &mut Builder,
        buffer_settings: BufferSettings,
    ) -> Result<AnyBuffer, DiagramErrorCode> {
        (self.into_any_buffer_impl)(self, builder, buffer_settings)
    }

    pub(super) fn scope(&self) -> Entity {
        self.scope
    }

    pub(super) fn id(&self) -> Entity {
        self.target
    }
}

impl Debug for DynOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynOutput")
            .field("scope", &self.scope)
            .field("target", &self.target)
            .field("type_info", &self.type_info)
            .finish()
    }
}

impl<T> From<Output<T>> for DynOutput
where
    T: Send + Sync + 'static + Any,
{
    fn from(output: Output<T>) -> Self {
        Self {
            scope: output.scope(),
            target: output.id(),
            type_info: TypeInfo::of::<T>(),
            into_any_buffer_impl: |me, builder, buffer_settings| {
                let buffer = builder.create_buffer::<T>(buffer_settings);
                builder.connect(me.into_output()?, buffer.input_slot());
                Ok(buffer.as_any_buffer())
            },
        }
    }
}

/// A type erased [`crate::InputSlot`]
#[derive(Copy, Clone, Debug)]
pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
    pub(super) type_info: TypeInfo,
}

impl DynInputSlot {
    pub(super) fn scope(&self) -> Entity {
        self.scope
    }

    pub(super) fn id(&self) -> Entity {
        self.source
    }
}

impl<T: Any> From<InputSlot<T>> for DynInputSlot {
    fn from(input: InputSlot<T>) -> Self {
        Self {
            scope: input.scope(),
            source: input.id(),
            type_info: TypeInfo::of::<T>(),
        }
    }
}

pub(super) type DeserializeFn<Serialized> =
    fn(Output<Serialized>, &mut Builder) -> Result<DynOutput, DiagramErrorCode>;

pub trait DeserializeMessage<T, Serialized> {
    fn deserialize_fn() -> Option<DeserializeFn<Serialized>>;
}

pub(super) type SerializeFn<Serialized> =
    fn(DynOutput, &mut Builder) -> Result<Output<Serialized>, DiagramErrorCode>;

pub trait SerializeMessage<T, Serialized> {
    fn serialize_fn() -> Option<SerializeFn<Serialized>>;
}

impl<T, Serialized> DeserializeMessage<T, Serialized> for NotSupported {
    fn deserialize_fn() -> Option<DeserializeFn<Serialized>> {
        None
    }
}

impl<T, Serialized> SerializeMessage<T, Serialized> for NotSupported {
    fn serialize_fn() -> Option<SerializeFn<Serialized>> {
        None
    }
}
