use std::collections::HashMap;

use schemars::{gen::SchemaGenerator, JsonSchema};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Builder;

use super::{
    impls::{DefaultImplMarker, NotSupportedMarker},
    register_serialize,
    type_info::TypeInfo,
    validate_single_input, DiagramErrorCode, MessageRegistration, MessageRegistry, NextOperation,
    SerializeMessage, Vertex, WorkflowBuilder,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkResultOp {
    pub(super) ok: NextOperation,
    pub(super) err: NextOperation,
}

impl ForkResultOp {
    pub(super) fn add_vertices<'a>(&'a self, wf_builder: &mut WorkflowBuilder<'a>, op_id: String) {
        let mut edge_builder =
            wf_builder.add_vertex(op_id.clone(), move |vertex, builder, registry, _| {
                self.try_connect(vertex, builder, &registry.messages)
            });
        edge_builder.add_output_edge(self.ok.clone(), None);
        edge_builder.add_output_edge(self.err.clone(), None);
    }

    pub(super) fn try_connect<'b>(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &MessageRegistry,
    ) -> Result<bool, DiagramErrorCode> {
        let output = validate_single_input(vertex)?;
        let (ok, err) = if output.type_info == TypeInfo::of::<serde_json::Value>() {
            Err(DiagramErrorCode::CannotForkResult)
        } else {
            registry.fork_result(builder, output)
        }?;
        {
            let ok_edge = &vertex.out_edges[0];
            ok_edge.set_output(ok);
        }
        {
            let err_edge = &vertex.out_edges[1];
            err_edge.set_output(err);
        }

        Ok(true)
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
