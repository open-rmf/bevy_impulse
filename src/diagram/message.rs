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

macro_rules! diagram_message_impl {
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

all_tuples_with_size!(diagram_message_impl, 1, 12, R, o);

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
            NodeBuilderOptions::new("foo_unzippable"),
            |builder, _config: ()| builder.create_map_block(|_: ()| Foo {}),
        );

        assert_eq!(
            fixture
                .registry
                .get_registration("foo_unzippable")
                .unwrap()
                .metadata
                .response
                .unzip_slots,
            0
        );

        fixture.registry.register_with_diagram_message(
            NodeBuilderOptions::new("foo_1_tuple"),
            |builder, _config: ()| builder.create_map_block(|_: ()| (Foo {},)),
        );

        assert_eq!(
            fixture
                .registry
                .get_registration("foo_1_tuple")
                .unwrap()
                .metadata
                .response
                .unzip_slots,
            1
        );

        fixture.registry.register_with_diagram_message(
            NodeBuilderOptions::new("foo_unzip_2_tuple"),
            |builder, _config: ()| builder.create_map_block(|_: ()| (Foo {}, Foo {})),
        );

        assert_eq!(
            fixture
                .registry
                .get_registration("foo_unzip_2_tuple")
                .unwrap()
                .metadata
                .response
                .unzip_slots,
            2
        );

        fixture.registry.register_with_diagram_message(
            NodeBuilderOptions::new("foo_unzip_3_tuple"),
            |builder, _config: ()| builder.create_map_block(|_: ()| (Foo {}, Foo {}, Foo {})),
        );

        assert_eq!(
            fixture
                .registry
                .get_registration("foo_unzip_3_tuple")
                .unwrap()
                .metadata
                .response
                .unzip_slots,
            3
        );
    }
}
