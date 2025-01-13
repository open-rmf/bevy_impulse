use bevy_utils::all_tuples_with_size;

use super::{impls::DefaultImpl, impls::NotSupported, unzip::DynUnzip, SerializeMessage};

pub trait DiagramMessage<Serializer>
where
    Self: Sized,
{
    type DynUnzipImpl: DynUnzip<Self, Serializer>;
}

impl<Serializer> DiagramMessage<Serializer> for () {
    type DynUnzipImpl = NotSupported;
}

macro_rules! dyn_unzip_impl {
    ($len:literal, $(($P:ident, $o:ident)),*) => {
        impl<$($P),*, Serializer> DiagramMessage<Serializer> for ($($P,)*)
        where
            $($P: Send + Sync + 'static + DiagramMessage<Serializer>),*,
            Serializer: $(SerializeMessage<$P> +)* $(SerializeMessage<Vec<$P>> +)*,
        {
            type DynUnzipImpl = DefaultImpl;
        }
    };
}

all_tuples_with_size!(dyn_unzip_impl, 2, 12, R, o);

#[cfg(test)]
mod tests {
    use bevy_impulse_derive::DiagramMessage;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use crate::{diagram::testing::DiagramTestFixture, NodeBuilderOptions};

    use super::*;

    #[derive(DiagramMessage, Clone, JsonSchema, Deserialize, Serialize)]
    struct Foo {}

    #[test]
    fn test_unzip() {
        let mut fixture = DiagramTestFixture::new_empty();

        fixture.registry.register_with_diagram_message(
            NodeBuilderOptions::new("derive_foo_unzip"),
            |builder, _config: ()| builder.create_map_block(|_: ()| (Foo {}, Foo {})),
        );

        assert_eq!(
            fixture
                .registry
                .get_registration("derive_foo_unzip")
                .unwrap()
                .metadata
                .response
                .unzip_slots,
            2
        );
    }
}
