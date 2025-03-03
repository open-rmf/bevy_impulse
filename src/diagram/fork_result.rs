use std::collections::HashMap;

use schemars::{gen::SchemaGenerator, JsonSchema};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{unknown_diagram_error, Builder};

use super::{
    impls::{DefaultImplMarker, NotSupportedMarker},
    register_serialize,
    type_info::TypeInfo,
    workflow_builder::{Edge, EdgeBuilder},
    DiagramErrorCode, DynOutput, MessageRegistration, MessageRegistry, NextOperation,
    SerializeMessage,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkResultOp {
    pub(super) ok: NextOperation,
    pub(super) err: NextOperation,
}

impl ForkResultOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        builder.add_output_edge(&self.ok, None, None)?;
        builder.add_output_edge(&self.err, None, None)?;
        Ok(())
    }

    pub(super) fn try_connect<'b>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        mut out_edges: Vec<&mut Edge>,
        registry: &MessageRegistry,
    ) -> Result<(), DiagramErrorCode> {
        let (ok, err) = if output.type_info == TypeInfo::of::<serde_json::Value>() {
            Err(DiagramErrorCode::CannotForkResult)
        } else {
            registry.fork_result(builder, output)
        }?;
        {
            let ok_edge = out_edges
                .get_mut(0)
                .ok_or_else(|| unknown_diagram_error!())?;
            ok_edge.output = Some(ok);
        }
        {
            let err_edge = out_edges
                .get_mut(1)
                .ok_or_else(|| unknown_diagram_error!())?;
            err_edge.output = Some(err);
        }

        Ok(())
    }
}

pub trait DynForkResult {
    fn on_register(
        self,
        messages: &mut HashMap<TypeInfo, MessageRegistration>,
        schema_generator: &mut SchemaGenerator,
    ) -> bool;
}

impl<T> DynForkResult for NotSupportedMarker<T> {
    fn on_register(
        self,
        _messages: &mut HashMap<TypeInfo, MessageRegistration>,
        _schema_generator: &mut SchemaGenerator,
    ) -> bool {
        false
    }
}

impl<T, E, S> DynForkResult for DefaultImplMarker<(Result<T, E>, S)>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    S: SerializeMessage<T> + 'static,
{
    fn on_register(
        self,
        messages: &mut HashMap<TypeInfo, MessageRegistration>,
        schema_generator: &mut SchemaGenerator,
    ) -> bool {
        let ops = &mut messages
            .entry(TypeInfo::of::<Result<T, E>>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.fork_result_impl.is_some() {
            return false;
        }

        ops.fork_result_impl = Some(|builder, output| {
            debug!("fork result: {:?}", output);

            let chain = output.into_output::<Result<T, E>>()?.chain(builder);
            let outputs = chain.fork_result(|c| c.output().into(), |c| c.output().into());
            debug!("forked outputs: {:?}", outputs);
            Ok(outputs)
        });

        register_serialize::<T, S>(messages, schema_generator);
        true
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Builder, Diagram, NodeBuilderOptions};

    #[test]
    fn test_fork_result() {
        let mut fixture = DiagramTestFixture::new();

        fn check_even(v: i64) -> Result<String, String> {
            if v % 2 == 0 {
                Ok("even".to_string())
            } else {
                Err("odd".to_string())
            }
        }

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("check_even".to_string()),
                |builder: &mut Builder, _config: ()| builder.create_map_block(&check_even),
            )
            .with_fork_result();

        fn echo(s: String) -> String {
            s
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("echo".to_string()),
            |builder: &mut Builder, _config: ()| builder.create_map_block(&echo),
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "check_even",
                    "next": "fork_result",
                },
                "fork_result": {
                    "type": "fork_result",
                    "ok": "op2",
                    "err": "op3",
                },
                "op2": {
                    "type": "node",
                    "builder": "echo",
                    "next": { "builtin": "terminate" },
                },
                "op3": {
                    "type": "node",
                    "builder": "echo",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, "even");

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(3))
            .unwrap();
        assert_eq!(result, "odd");
    }
}
