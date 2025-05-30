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

use std::{collections::{HashMap, HashSet}, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;

use crate::{
    Builder, IncrementalScopeBuilder, IncrementalScopeRequest, IncrementalScopeResponse,
    ConnectIntoTarget, InferMessageType,BuildDiagramOperation, BuildStatus, DiagramContext,
    DiagramErrorCode, DynOutput, NextOperation, OperationName,
    Operations, OperationRef, ScopeSettings,
    standard_input_connection,
};

/// The schema to define a scope within a diagram.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ScopeSchema {
    /// Indicates which node inside the scope should receive the input into the
    /// scope.
    pub start: NextOperation,

    /// To simplify diagram definitions, the diagram workflow builder will
    /// sometimes insert implicit operations into the workflow, such as implicit
    /// serializing and deserializing. These implicit operations may be fallible.
    ///
    /// This field indicates how a failed implicit operation should be handled.
    /// If left unspecified, an implicit error will cause the entire workflow to
    /// be cancelled.
    #[serde(default)]
    pub on_implicit_error: Option<NextOperation>,

    /// Operations that exist inside this scope.
    pub ops: Operations,

    /// Where to connect streams that are coming out of this scope.
    #[serde(default)]
    pub stream_out: HashMap<OperationName, NextOperation>,

    /// Where to connect the output of this scope.
    pub next: NextOperation,
}

impl BuildDiagramOperation for ScopeSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let scope = IncrementalScopeBuilder::begin(ScopeSettings::default(), builder);

        for (child_id, op) in self.ops.iter() {
            ctx.add_child_operation(
                id,
                child_id,
                op,
                self.ops.clone(),
                Some(scope.builder_scope_context())
            );
        }

        ctx.set_connect_into_target(
            id,
            ConnectScopeRequest {
                scope: scope.clone(),
                start: ctx.into_child_operation_ref(id, &self.start),
                connection: None,
            },
        )?;

        ctx.set_connect_into_target(
            OperationRef::Terminate(smallvec![Arc::clone(id)]),
            ConnectScopeResponse {
                scope,
                next: ctx.into_operation_ref(&self.next),
                connection: None,
            },
        )?;

        Ok(BuildStatus::Finished)
    }
}

struct ConnectScopeRequest {
    scope: IncrementalScopeBuilder,
    start: OperationRef,
    connection: Option<Box<dyn ConnectIntoTarget + 'static>>,
}

impl ConnectIntoTarget for ConnectScopeRequest {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if let Some(connection) = &mut self.connection {
            return connection.connect_into_target(output, builder, ctx);
        } else {
            let IncrementalScopeRequest { external_input, begin_scope } = ctx
                .registry.messages.set_scope_request(
                    output.message_info(),
                    &mut self.scope,
                    builder.commands(),
                )?;

            if let Some(begin_scope) = begin_scope {
                ctx.add_output_into_target(self.start.clone(), begin_scope);
            }

            let mut connection = standard_input_connection(external_input, &ctx.registry)?;
            connection.connect_into_target(output, builder, ctx)?;
            self.connection = Some(connection);
        }

        Ok(())
    }

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        if let Some(connection) = &self.connection {
            connection.infer_input_type(ctx, visited)
        } else {
            ctx.redirect_infer_input_type(&self.start, visited)
        }
    }

    fn is_finished(&self) -> Result<(), DiagramErrorCode> {
        self.scope.is_finished().map_err(Into::into)
    }
}

struct ConnectScopeResponse {
    scope: IncrementalScopeBuilder,
    next: OperationRef,
    connection: Option<Box<dyn ConnectIntoTarget + 'static>>,
}

impl ConnectIntoTarget for ConnectScopeResponse {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if let Some(connection) = &mut self.connection {
            return connection.connect_into_target(output, builder, ctx);
        } else {
            let IncrementalScopeResponse { terminate, external_output } = ctx
                .registry.messages.set_scope_response(
                    output.message_info(),
                    &mut self.scope,
                    builder.commands(),
                )?;

            if let Some(external_output) = external_output {
                ctx.add_output_into_target(self.next.clone(), external_output);
            }

            let mut connection = standard_input_connection(terminate, ctx.registry)?;
            connection.connect_into_target(output, builder, ctx)?;
            self.connection = Some(connection);
        }

        Ok(())
    }

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        if let Some(connection) = &self.connection {
            connection.infer_input_type(ctx, visited)
        } else {
            ctx.redirect_infer_input_type(&self.next, visited)
        }
    }

    // We do not implement is_finished because just having it for
    // ConnectScopeRequest is sufficient, since it will report that the scope
    // isn't finished whether the request or the response didn't get a connection.
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, diagram::{*, testing::*}};
    use serde_json::json;

    #[test]
    fn test_simple_scope() {
        let mut fixture = DiagramTestFixture::new();
        fixture.context.set_flush_loop_limit(Some(10));

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "scope",
            "ops": {
                "scope": {
                    "type": "scope",
                    "start": "multiply",
                    "ops": {
                        "multiply": {
                            "type": "node",
                            "builder": "multiply3",
                            "next": { "builtin" : "terminate" },
                        }
                    },
                    "next": { "builtin" : "terminate" },
                }
            }
        }))
        .unwrap();

        // let result: i64 = fixture.spawn_and_run(&diagram, 4_i64).unwrap();
        let workflow: Service<i64, i64> = fixture.spawn_io_workflow(&diagram).unwrap();

        let mut promise = fixture.context.command(|commands| commands.request(4_i64, workflow).take_response());
        fixture.context.run_with_conditions(&mut promise, 1);
        let result = promise.take().available().unwrap();
        assert_eq!(result, 12);
    }
}
