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
    use std::collections::HashMap;

    use crate::{
        diagram::testing::DiagramTestFixture, Diagram, DiagramOperation, NodeOp, StartOp,
        TerminateOp,
    };

    use super::*;

    #[test]
    fn test_fork_clone_uncloneable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["op_3".to_string(), "terminate".to_string()],
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotCloneable), "{:?}", err);
    }

    #[test]
    fn test_fork_clone() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["op_3".to_string(), "terminate".to_string()],
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 12);
    }
}
