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

use bevy_utils::all_tuples_with_size;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    supported::*, BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode,
    DynInputSlot, DynOutput, MessageRegistry, NextOperation, OperationName, PerformForkClone,
    SerializeMessage, TypeInfo,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UnzipSchema {
    pub(super) next: Vec<NextOperation>,
}

impl BuildDiagramOperation for UnzipSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let Some(inferred_type) = ctx.infer_input_type_into_target(id)? else {
            // There are no outputs ready for this target, so we can't do
            // anything yet. The builder should try again later.
            return Ok(BuildStatus::defer("waiting for an input"));
        };

        let unzip = ctx.registry.messages.unzip(&inferred_type)?;
        let actual_output = unzip.output_types();
        if actual_output.len() != self.next.len() {
            return Err(DiagramErrorCode::UnzipMismatch {
                expected: self.next.len(),
                actual: unzip.output_types().len(),
                elements: actual_output,
            });
        }

        let unzip = unzip.perform_unzip(builder)?;

        ctx.set_input_for_target(id, unzip.input)?;
        for (target, output) in self.next.iter().zip(unzip.outputs) {
            ctx.add_output_into_target(target, output);
        }
        Ok(BuildStatus::Finished)
    }
}

pub struct DynUnzip {
    input: DynInputSlot,
    outputs: Vec<DynOutput>,
}

pub trait PerformUnzip {
    /// Returns a list of type names that this message unzips to.
    fn output_types(&self) -> Vec<TypeInfo>;

    fn perform_unzip(&self, builder: &mut Builder) -> Result<DynUnzip, DiagramErrorCode>;

    /// Called when a node is registered.
    fn on_register(&self, registry: &mut MessageRegistry);
}

macro_rules! dyn_unzip_impl {
    ($len:literal, $(($P:ident, $o:ident)),*) => {
        impl<$($P),*, Serializer, Cloneable> PerformUnzip for Supported<(($($P,)*), Serializer, Cloneable)>
        where
            $($P: Send + Sync + 'static),*,
            Serializer: $(SerializeMessage<$P> +)* $(SerializeMessage<Vec<$P>> +)*,
            Cloneable: $(PerformForkClone<$P> +)* $(PerformForkClone<Vec<$P>> +)*,
        {
            fn output_types(&self) -> Vec<TypeInfo> {
                vec![$(
                    TypeInfo::of::<$P>(),
                )*]
            }

            fn perform_unzip(
                &self,
                builder: &mut Builder,
            ) -> Result<DynUnzip, DiagramErrorCode> {
                let (input, ($($o,)*)) = builder.create_unzip::<($($P,)*)>();

                let mut outputs: Vec<DynOutput> = Vec::with_capacity($len);
                $({
                    outputs.push($o.into());
                })*

                Ok(DynUnzip {
                    input: input.into(),
                    outputs,
                })
            }

            fn on_register(&self, registry: &mut MessageRegistry)
            {
                // Register serialize functions for all items in the tuple.
                // For a tuple of (T1, T2, T3), registers serialize for T1, T2 and T3.
                $(
                    registry.register_serialize::<$P, Serializer>();
                    registry.register_fork_clone::<$P, Cloneable>();
                )*
            }
        }
    };
}

all_tuples_with_size!(dyn_unzip_impl, 1, 12, R, o);

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram, DiagramErrorCode, JsonMessage};

    #[test]
    fn test_unzip_not_unzippable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "unzip"
                },
                "unzip": {
                    "type": "unzip",
                    "next": [{ "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotUnzippable(_)),
            "{}",
            err
        );
    }

    #[test]
    fn test_unzip_to_too_many_slots() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip"
                },
                "unzip": {
                    "type": "unzip",
                    "next": ["op2", "op3", "op4"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
                "op3": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
                "op4": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(matches!(
            err.code,
            DiagramErrorCode::UnzipMismatch {
                expected: 3,
                actual: 2,
                ..
            }
        ));
    }

    #[test]
    fn test_unzip_to_terminate() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip"
                },
                "unzip": {
                    "type": "unzip",
                    "next": [{ "builtin": "dispose" }, { "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 20);
    }

    #[test]
    fn test_unzip() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": [
                        "op2",
                        { "builtin": "dispose" },
                    ],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 36);
    }

    #[test]
    fn test_unzip_with_dispose() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": [{ "builtin": "dispose" }, "op2"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 60);
    }
}
