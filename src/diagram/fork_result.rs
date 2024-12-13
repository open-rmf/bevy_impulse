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
    use serde_json::json;

    use crate::{diagram::testing::DiagramTestFixture, Builder, Diagram};

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

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "nodeId": "check_even",
                    "next": "forkResult",
                },
                "forkResult": {
                    "type": "forkResult",
                    "ok": "op2",
                    "err": "op3",
                },
                "op2": {
                    "type": "node",
                    "nodeId": "echo",
                    "next": "terminate",
                },
                "op3": {
                    "type": "node",
                    "nodeId": "echo",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
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
