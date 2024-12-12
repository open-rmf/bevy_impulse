use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    DiagramError, DynOutput, OperationId,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForkResultOp {
    pub(super) ok: OperationId,
    pub(super) err: OperationId,
}

pub trait DynForkResult<T> {
    const SUPPORTED: bool;

    fn dyn_fork_result(
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError>;
}

impl<T> DynForkResult<T> for NotSupported {
    const SUPPORTED: bool = false;

    fn dyn_fork_result(
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        Err(DiagramError::CannotForkResult)
    }
}

impl<T, E> DynForkResult<Result<T, E>> for DefaultImpl
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    const SUPPORTED: bool = true;

    fn dyn_fork_result(
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        let chain = output.into_output::<Result<T, E>>().chain(builder);
        let (ok, err) = chain.fork_result(|c| c.output(), |c| c.output());
        Ok((ok.into(), err.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        diagram::{fork_result::ForkResultOp, testing::DiagramTestFixture},
        Builder, Diagram, DiagramOperation, NodeOp, StartOp, TerminateOp,
    };

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
            .registration_builder()
            .with_fork_result()
            .register_node(
                "check_even",
                "check_even",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&check_even),
            );

        fn echo(s: String) -> String {
            s
        }

        fixture
            .registry
            .register_node("echo", "echo", |builder: &mut Builder, _config: ()| {
                builder.create_map_block(&echo)
            });

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
                        node_id: "check_even".to_string(),
                        config: serde_json::Value::Null,
                        next: "fork_result".to_string(),
                    }),
                ),
                (
                    "fork_result".to_string(),
                    DiagramOperation::ForkResult(ForkResultOp {
                        ok: "op_2".to_string(),
                        err: "op_3".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "echo".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "echo".to_string(),
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
        assert_eq!(result, "even");

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(3))
            .unwrap();
        assert_eq!(result, "odd");
    }
}
