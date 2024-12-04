use std::any::TypeId;

use bevy_utils::all_tuples_with_size;

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    DiagramError, DynOutput, NodeRegistry, SerializeMessage,
};

pub trait DynUnzip<T, Serializer> {
    const UNZIP_SLOTS: usize;

    fn dyn_unzip(builder: &mut Builder, output: DynOutput) -> Result<Vec<DynOutput>, DiagramError>;

    /// Register serialize functions for all items in the tuple.
    /// For a tuple of (T1, T2, T3), registers serialize for T1, T2 and T3.
    fn register_serialize(registry: &mut NodeRegistry);
}

impl<T, Serializer> DynUnzip<T, Serializer> for NotSupported {
    const UNZIP_SLOTS: usize = 0;

    fn dyn_unzip(
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotUnzippable)
    }

    fn register_serialize(_registry: &mut NodeRegistry) {}
}

fn register_serialize_impl<Response, Serializer>(registry: &mut NodeRegistry)
where
    Serializer: SerializeMessage<Response>,
    Response: Send + Sync + 'static,
{
    registry.serialize_impls.insert(
        TypeId::of::<Response>(),
        Box::new(|builder, output| {
            let n = builder.create_map_block(|resp: Response| Serializer::to_json(&resp));
            builder.connect(output.into_output(), n.input);
            n.output.chain(builder).cancel_on_err().output()
        }),
    );
}

macro_rules! dyn_unzip_impl {
    ($len:literal, $(($P:ident, $o:ident)),*) => {
        impl<$($P),*, Serializer> DynUnzip<($($P,)*), Serializer> for DefaultImpl
        where
            $($P: Send + Sync + 'static),*,
            Serializer: $(SerializeMessage<$P> +)*
        {
            const UNZIP_SLOTS: usize = $len;

            fn dyn_unzip(
                builder: &mut Builder,
                output: DynOutput
            ) -> Result<Vec<DynOutput>, DiagramError> {
                let mut outputs: Vec<DynOutput> = Vec::with_capacity($len);
                let chain = output.into_output::<($($P,)*)>().chain(builder);
                let ($($o,)*) = chain.unzip();

                $({
                    outputs.push($o.into());
                })*

                Ok(outputs)
            }

            fn register_serialize(registry: &mut NodeRegistry)
            {
                $(
                    register_serialize_impl::<$P, Serializer>(registry);
                )*
            }
        }
    };
}

all_tuples_with_size!(dyn_unzip_impl, 1, 12, R, o);
