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

use crate::{Builder, ForkCloneOutput};

use super::{
    supported::*, BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode,
    DynInputSlot, DynOutput, NextOperation, OperationId,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkCloneSchema {
    pub(super) next: Vec<NextOperation>,
}

impl BuildDiagramOperation for ForkCloneSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let Some(inferred_type) = ctx.infer_input_type_into_target(id) else {
            // There are no outputs ready for this target, so we can't do
            // anything yet. The builder should try again later.
            return Ok(BuildStatus::defer("waiting for an input"));
        };

        let fork = ctx.registry.messages.fork_clone(inferred_type, builder)?;
        ctx.set_input_for_target(id, fork.input)?;
        for target in &self.next {
            ctx.add_output_into_target(target.clone(), fork.outputs.clone_output(builder));
        }

        Ok(BuildStatus::Finished)
    }
}

pub trait PerformForkClone<T> {
    const CLONEABLE: bool;

    fn perform_fork_clone(builder: &mut Builder) -> Result<DynForkClone, DiagramErrorCode>;
}

impl<T> PerformForkClone<T> for NotSupported {
    const CLONEABLE: bool = false;

    fn perform_fork_clone(_builder: &mut Builder) -> Result<DynForkClone, DiagramErrorCode> {
        Err(DiagramErrorCode::NotCloneable)
    }
}

impl<T> PerformForkClone<T> for Supported
where
    T: Send + Sync + 'static + Clone,
{
    const CLONEABLE: bool = true;

    fn perform_fork_clone(builder: &mut Builder) -> Result<DynForkClone, DiagramErrorCode> {
        let (input, outputs) = builder.create_fork_clone::<T>();

        Ok(DynForkClone {
            input: input.into(),
            outputs: DynForkCloneOutput::new(outputs),
        })
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

    use crate::{diagram::testing::DiagramTestFixture, Diagram};

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
            matches!(err.code, DiagramErrorCode::NotCloneable),
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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 36);
    }
}
