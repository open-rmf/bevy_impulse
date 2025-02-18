use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{BuilderId, NextOperation};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeOp {
    pub(super) builder: BuilderId,
    #[serde(default)]
    pub(super) config: serde_json::Value,
    pub(super) next: NextOperation,
}
