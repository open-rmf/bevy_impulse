use std::any::Any;
use bevy_ecs::prelude::Entity;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Builder, ForkCloneOutput, SingleInputStorage, UnusedTarget, AddBranchToForkClone};

use super::{
    impls::{DefaultImpl, NotSupported},
    BuildDiagramOperation, Diagram, DiagramConstruction, DiagramElementRegistry, DiagramErrorCode,
    DynInputSlot, DynOutput, NextOperation, OperationId, DynOutputInfo,
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
        construction: &mut DiagramConstruction,
        _: &Diagram,
        registry: &DiagramElementRegistry,
    ) -> Result<Option<DynInputSlot>, DiagramErrorCode> {
        let Some(incoming) = construction.get_outputs_into_operation_target(id) else {
            // There are no outputs ready for this target, so we can't do
            // anything yet. The builder should try again later.
            return Ok(None);
        };

        if incoming.is_empty() {
            // There are no outputs ready for this target, so we can't do
            // anything yet. The builder should try again later.
            return Ok(None);
        }

        let sample_input = &incoming[0];
        let fork = registry.messages.fork_clone(builder, sample_input.info().message_info())?;
        for target in &self.next {
            construction.add_output_into_target(target.clone(), fork.outputs.clone_output(builder));
        }

        Ok(Some(fork.input))
    }
}

pub trait PerformForkClone<T> {
    const CLONEABLE: bool;

    fn perform_fork_clone(
        builder: &mut Builder,
    ) -> Result<DynForkClone, DiagramErrorCode>;
}

impl<T> PerformForkClone<T> for NotSupported {
    const CLONEABLE: bool = false;

    fn perform_fork_clone(
        _builder: &mut Builder,
    ) -> Result<DynForkClone, DiagramErrorCode> {
        Err(DiagramErrorCode::NotCloneable)
    }
}

impl<T> PerformForkClone<T> for DefaultImpl
where
    T: Send + Sync + 'static + Clone,
{
    const CLONEABLE: bool = true;

    fn perform_fork_clone(
        builder: &mut Builder,
    ) -> Result<DynForkClone, DiagramErrorCode> {
        let (input, outputs) = builder.create_fork_clone::<T>();

        Ok(DynForkClone {
            input: input.into(),
            outputs: outputs.into(),
        })
    }
}

pub(super) struct DynForkCloneOutput {
    scope: Entity,
    source: Entity,
    info: DynOutputInfo,
}

impl DynForkCloneOutput {
    pub fn clone_output(&self, builder: &mut Builder) -> DynOutput {
        assert_eq!(self.scope, builder.scope);
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(self.id()), UnusedTarget))
            .id();
        builder.commands.add(AddBranchToForkClone {
            source: self.id(),
            target,
        });

        DynOutput::new(self.scope, target, self.info)
    }

    pub fn id(&self) -> Entity {
        self.source
    }
}

impl<T: 'static + Send + Sync + Any> From<ForkCloneOutput<T>> for DynForkCloneOutput {
    fn from(value: ForkCloneOutput<T>) -> Self {
        DynForkCloneOutput {
            scope: value.scope(),
            source: value.id(),
            info: DynOutputInfo::new::<T>(),
        }
    }
}

pub(super) struct DynForkClone {
    pub(super) input: DynInputSlot,
    pub(super) outputs: DynForkCloneOutput,
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
        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
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
