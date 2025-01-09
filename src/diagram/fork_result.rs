use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    DiagramError, DynOutput, NextOperation,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkResultOp {
    pub(super) ok: NextOperation,
    pub(super) err: NextOperation,
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
        debug!("fork result: {:?}", output);

        let chain = output.into_output::<Result<T, E>>()?.chain(builder);
        let outputs = chain.fork_result(|c| c.output().into(), |c| c.output().into());
        debug!("forked outputs: {:?}", outputs);
        Ok(outputs)
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
