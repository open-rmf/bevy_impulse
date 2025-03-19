use crate::{Builder, Output};

use super::{
    transform::TransformError, DeserializeFn, DeserializeMessage, DiagramErrorCode, DynOutput,
    SerializeCel, SerializeFn, SerializeMessage, ToCelValueFn,
};

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

    fn from_cel_value_fn() -> Option<super::FromCelValueFn> {
        Some(
            |output: Output<cel_interpreter::Value>,
             builder: &mut Builder|
             -> Result<DynOutput, DiagramErrorCode> {
                let node = builder.create_map_block(
                    |msg: cel_interpreter::Value| -> Result<_, TransformError> {
                        let json_value = msg
                            .json()
                            .map_err(|err| TransformError::Other(err.to_string().into()))?;
                        Ok(serde_json::from_value::<T>(json_value).unwrap())
                    },
                );
                builder.connect(output, node.input);
                Ok(node.output.chain(builder).cancel_on_err().output().into())
            },
        )
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

    fn to_cel_value_fn() -> Option<ToCelValueFn> {
        Some(|output, builder| {
            let node = builder.create_map_block(|msg: T| cel_interpreter::to_value(msg));
            builder.connect(output.into_output()?, node.input);
            Ok(node.output.chain(builder).cancel_on_err().output().into())
        })
    }
}

impl SerializeCel<serde_json::Value> for JsonSerializer {
    fn serialize_cel_output(
        builder: &mut Builder,
        output: Output<cel_interpreter::Value>,
    ) -> Output<serde_json::Value> {
        output
            .chain(builder)
            .map_block(|val| {
                val.json()
                    // the error returned borrows `val`, so we need to break ownership
                    // by converting the error.
                    .map_err(|e| TransformError::Other(e.to_string().into()))
            })
            .cancel_on_err()
            .output()
    }
}
