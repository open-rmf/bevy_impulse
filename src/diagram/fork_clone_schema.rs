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

use crate::{Builder, CloneFromBuffer, ForkCloneOutput};

use super::{
    supported::*, BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode,
    DynInputSlot, DynOutput, NextOperation, OperationName, TraceInfo, TraceSettings, TypeInfo,
    MessageOperation,
};

/// If the request is cloneable, clone it into multiple responses that can
/// each be sent to a different operation. The `next` property is an array.
///
/// This creates multiple simultaneous branches of execution within the
/// workflow. Usually when you have multiple branches you will either
/// * race - connect all branches to `terminate` and the first branch to
///   finish "wins" the race and gets to the be output
/// * join - connect each branch into a buffer and then use the `join`
///   operation to reunite them
/// * collect - TODO(@mxgrey): [add the collect operation](https://github.com/open-rmf/bevy_impulse/issues/59)
///
/// # Examples
/// ```
/// # bevy_impulse::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "start": "begin_race",
///     "ops": {
///         "begin_race": {
///             "type": "fork_clone",
///             "next": [
///                 "ferrari",
///                 "mustang"
///             ]
///         },
///         "ferrari": {
///             "type": "node",
///             "builder": "drive",
///             "config": "ferrari",
///             "next": { "builtin": "terminate" }
///         },
///         "mustang": {
///             "type": "node",
///             "builder": "drive",
///             "config": "mustang",
///             "next": { "builtin": "terminate" }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkCloneSchema {
    pub next: Vec<NextOperation>,
    #[serde(flatten)]
    pub trace_settings: TraceSettings,
}

impl BuildDiagramOperation for ForkCloneSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let inferred_type = 'inferred: {
            match ctx.infer_input_type_into_target(id)? {
                Some(inferred_type) => break 'inferred inferred_type,
                None => {
                    for target in &self.next {
                        if let Some(inferred_type) = ctx.infer_input_type_into_target(target)? {
                            break 'inferred inferred_type;
                        }
                    }

                    // There are no outputs or input slots ready for this target,
                    // so we can't do anything yet. The builder should try again later.
                    return Ok(BuildStatus::defer("waiting for an input"));
                }
            }
        };

        let fork = ctx
            .registry
            .messages
            .fork_clone(&inferred_type, ctx.builder)?;
        let trace = TraceInfo::new(self, self.trace_settings.trace)?;
        ctx.set_input_for_target(id, fork.input, trace)?;
        for target in &self.next {
            let output = fork.outputs.clone_output(ctx.builder);
            ctx.add_output_into_target(target, output);
        }

        Ok(BuildStatus::Finished)
    }
}

pub trait RegisterClone<T> {
    const CLONEABLE: bool;

    fn register_clone(ops: &mut MessageOperation);
}

impl<T: 'static> RegisterClone<T> for NotSupported {
    const CLONEABLE: bool = false;

    fn register_clone(ops: &mut MessageOperation) {
        ops.fork_clone_impl = Some(|_| Err(DiagramErrorCode::NotCloneable(TypeInfo::of::<T>())));
    }
}

impl<T> RegisterClone<T> for Supported
where
    T: Send + Sync + 'static + Clone,
{
    const CLONEABLE: bool = true;

    fn register_clone(ops: &mut MessageOperation) {
        CloneFromBuffer::<T>::register_clone_for_join();
        ops.fork_clone_impl = Some(|builder| {
            let (input, outputs) = builder.create_fork_clone::<T>();

            Ok(DynForkClone {
                input: input.into(),
                outputs: DynForkCloneOutput::new(outputs),
            })
        });
    }
}

pub struct DynForkClone {
    pub input: DynInputSlot,
    pub outputs: DynForkCloneOutput,
}

pub struct DynForkCloneOutput {
    inner: Box<dyn DynamicClone>,
}

impl DynForkCloneOutput {
    pub fn new(inner: impl DynamicClone + 'static) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    pub fn clone_output(&self, builder: &mut Builder) -> DynOutput {
        self.inner.dyn_clone_output(builder)
    }
}

pub trait DynamicClone {
    fn dyn_clone_output(&self, builder: &mut Builder) -> DynOutput;
}

impl<T: 'static + Send + Sync + Clone> DynamicClone for ForkCloneOutput<T> {
    fn dyn_clone_output(&self, builder: &mut Builder) -> DynOutput {
        self.clone_output(builder).into()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram, JsonMessage};

    use super::*;

    #[test]
    fn test_fork_clone_uncloneable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": "fork_clone"
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op2"]
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();
        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotCloneable(_)),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_fork_clone() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "fork_clone"
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op2"]
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
}
