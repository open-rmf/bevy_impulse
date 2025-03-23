/*
 * Copyright (C) 2025 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    impls::DefaultImplMarker, type_info::TypeInfo, BuildDiagramOperation, BuildStatus,
    DiagramContext, DiagramErrorCode, DynInputSlot, DynOutput, MessageRegistration,
    MessageRegistry, NextOperation, OperationId, PerformForkClone, SerializeMessage,
};

pub(super) struct DynForkResult {
    input: DynInputSlot,
    ok: DynOutput,
    err: DynOutput,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkResultSchema {
    pub(super) ok: NextOperation,
    pub(super) err: NextOperation,
}

impl BuildDiagramOperation for ForkResultSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let Some(input_sample) = ctx.get_sample_output_into_target(id) else {
            // There are no outputs ready for this target, so we can't do
            // anything yet. The builder should try again later.
            return Ok(BuildStatus::defer("waiting for an input"));
        };

        let fork = ctx
            .registry
            .messages
            .fork_result(builder, input_sample.message_info())?;
        ctx.set_input_for_target(id, fork.input)?;
        ctx.add_output_into_target(self.ok.clone(), fork.ok);
        ctx.add_output_into_target(self.err.clone(), fork.err);
        Ok(BuildStatus::Finished)
    }
}

pub trait RegisterForkResult {
    fn on_register(registry: &mut MessageRegistry) -> bool;
}

impl<T, E, S, C> RegisterForkResult for DefaultImplMarker<(Result<T, E>, S, C)>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    S: SerializeMessage<T> + SerializeMessage<E>,
    C: PerformForkClone<T> + PerformForkClone<E>,
{
    fn on_register(registry: &mut MessageRegistry) -> bool {
        let ops = &mut registry
            .messages
            .entry(TypeInfo::of::<Result<T, E>>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.fork_result_impl.is_some() {
            return false;
        }

        ops.fork_result_impl = Some(|builder| {
            let (input, outputs) = builder.create_fork_result::<T, E>();
            Ok(DynForkResult {
                input: input.into(),
                ok: outputs.ok.into(),
                err: outputs.err.into(),
            })
        });

        registry.register_serialize::<T, S>();
        registry.register_fork_clone::<T, C>();

        registry.register_serialize::<E, S>();
        registry.register_fork_clone::<E, C>();

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
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, "even");

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(3))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, "odd");
    }
}
