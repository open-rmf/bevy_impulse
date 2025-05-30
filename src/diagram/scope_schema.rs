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

use schemars::{schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};

use crate::{
    AnyBuffer, AnyMessageBox, Buffer, Builder, InputSlot, JsonBuffer, JsonMessage, Output,
    IncrementalScopeBuilder, IncrementalScopeRequest, IncrementalScopeResponse, ConnectIntoTarget,
    InferMessageType,BuildDiagramOperation, BuildStatus, DiagramContext, DiagramElementRegistry,
    DiagramErrorCode, DynInputSlot, DynOutput, NamespacedOperation, NextOperation, OperationName,
    Operations, TypeInfo, OperationRef, ScopeSettings,
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
                &self.ops,
                Some(scope.builder_scope_context()),
            );
        }



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
                ctx.add_output_into_target(self.next.clone(), output);
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
