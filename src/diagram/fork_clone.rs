use std::any::TypeId;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    DiagramError, DynOutput, OperationId,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForkCloneOp {
    pub(super) next: Vec<OperationId>,
}

pub trait DynForkClone<T> {
    const CLONEABLE: bool;

    fn dyn_fork_clone(
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError>;
}

impl<T> DynForkClone<T> for NotSupported {
    const CLONEABLE: bool = false;

    fn dyn_fork_clone(
        _builder: &mut Builder,
        _output: DynOutput,
        _amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotCloneable)
    }
}

impl<T> DynForkClone<T> for DefaultImpl
where
    T: Send + Sync + 'static + Clone,
{
    const CLONEABLE: bool = true;

    fn dyn_fork_clone(
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        assert_eq!(output.type_id, TypeId::of::<T>());

        let fork_clone = output.into_output::<T>().fork_clone(builder);
        Ok((0..amount)
            .map(|_| fork_clone.clone_output(builder).into())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{diagram::testing::DiagramTestFixture, Diagram};

    use super::*;

    #[test]
    fn test_fork_clone_uncloneable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1"
                },
                "op1": {
                    "type": "node",
                    "nodeId": "multiply3",
                    "next": "forkClone"
                },
                "forkClone": {
                    "type": "forkClone",
                    "next": ["op2"]
                },
                "op2": {
                    "type": "node",
                    "nodeId": "multiply3",
                    "next": "terminate"
                },
                "terminate": {
                    "type": "terminate"
                },
            },
        }))
        .unwrap();
        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotCloneable), "{:?}", err);
    }

    #[test]
    fn test_fork_clone() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1"
                },
                "op1": {
                    "type": "node",
                    "nodeId": "multiply3_cloneable",
                    "next": "forkClone"
                },
                "forkClone": {
                    "type": "forkClone",
                    "next": ["op2"]
                },
                "op2": {
                    "type": "node",
                    "nodeId": "multiply3_cloneable",
                    "next": "terminate"
                },
                "terminate": {
                    "type": "terminate"
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 36);
    }
}
