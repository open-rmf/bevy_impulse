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

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use crate::{AnyBuffer, BufferIdentifier, BufferMap, Builder, JsonMessage, Scope, StreamPack};

use super::{
    BufferInputs, BuiltinTarget, Diagram, DiagramElementRegistry, DiagramError, DiagramErrorCode,
    DiagramOperation, DynInputSlot, DynOutput, NextOperation, OperationId, TypeInfo,
};

#[derive(Clone, Copy)]
pub struct ConnectionContext<'a> {
    #[allow(unused)]
    pub diagram: &'a Diagram,
    pub registry: &'a DiagramElementRegistry,
}

pub type ConnectFn = Box<
    dyn FnMut(
        &NextOperation,
        DynOutput,
        &mut Builder,
        ConnectionContext,
    ) -> Result<(), DiagramErrorCode>,
>;

#[derive(Default)]
pub struct DiagramConstruction {
    connect_to_operation: HashMap<NextOperation, ConnectFn>,
    // We use a separate hashmap for OperationId vs BuiltinTarget so we can
    // efficiently fetch with an &OperationId
    outputs_to_operation_target: HashMap<OperationId, Vec<DynOutput>>,
    outputs_to_builtin_target: HashMap<BuiltinTarget, Vec<DynOutput>>,
    buffers: HashMap<OperationId, AnyBuffer>,
}

impl DiagramConstruction {
    /// Get all the currently known outputs that are aimed at this target operation.
    pub fn get_outputs_into_operation_target(&self, id: &OperationId) -> Option<&Vec<DynOutput>> {
        self.outputs_to_operation_target.get(id)
    }

    /// Get a single currently known output that is aimed at this target operation.
    /// This can be used to infer information for building the operation.
    ///
    /// The workflow builder will ensure that all outputs targeting this
    /// operation share the same message type, so getting a single sample is
    /// usually enough to infer what is needed to build the operation.
    pub fn get_sample_output_into_target(&self, id: &OperationId) -> Option<&DynOutput> {
        self.get_outputs_into_operation_target(id)
            .and_then(|outputs| outputs.first())
    }

    /// Add an output to connect into a target.
    ///
    /// # Arguments
    ///
    /// * target - The operation that needs to receive the output
    /// * output - The output channel that needs to be connected into the target.
    pub fn add_output_into_target(&mut self, target: NextOperation, output: DynOutput) {
        match target {
            NextOperation::Target(id) => {
                self.outputs_to_operation_target
                    .entry(id)
                    .or_default()
                    .push(output);
            }
            NextOperation::Builtin { builtin } => {
                self.outputs_to_builtin_target
                    .entry(builtin)
                    .or_default()
                    .push(output);
            }
        }
    }

    /// Set the input slot of an operation. This should not be called more than
    /// once per operation, because only one input slot can be used for any
    /// operation.
    pub fn set_input_for_target(
        &mut self,
        operation: &OperationId,
        input: DynInputSlot,
    ) -> Result<(), DiagramErrorCode> {
        self.set_connect_into_target(operation, move |_, output, builder, _| {
            output.connect_to(&input, builder)
        })
    }

    /// Set a callback that will allow outputs to connect into this target. This
    /// is a more general method than [`Self::set_input_for_target`]. This allows
    /// you to take further action before a target is connected, like serializing.
    pub fn set_connect_into_target(
        &mut self,
        operation: &OperationId,
        connect: impl FnMut(
                &NextOperation,
                DynOutput,
                &mut Builder,
                ConnectionContext,
            ) -> Result<(), DiagramErrorCode>
            + 'static,
    ) -> Result<(), DiagramErrorCode> {
        let f = Box::new(connect);
        match self
            .connect_to_operation
            .entry(NextOperation::Target(operation.clone()))
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleInputsCreated(operation.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(f);
            }
        }
        Ok(())
    }

    /// Set the buffer that should be used for a certain operation. This will
    /// also set its connection callback.
    pub fn set_buffer_for_operation(
        &mut self,
        operation: &OperationId,
        buffer: AnyBuffer,
    ) -> Result<(), DiagramErrorCode> {
        match self.buffers.entry(operation.clone()) {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleBuffersCreated(operation.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(buffer);
            }
        }

        let input: DynInputSlot = buffer.into();
        self.set_input_for_target(operation, input)
    }

    /// Create a buffer map based on the buffer inputs provided. If one or more
    /// of the buffers in BufferInputs is not available, get an error including
    /// the name of the missing buffer.
    pub fn create_buffer_map(&self, inputs: &BufferInputs) -> Result<BufferMap, String> {
        let attempt_get_buffer = |name: &String| -> Result<AnyBuffer, String> {
            self.buffers
                .get(name)
                .copied()
                .ok_or_else(|| format!("cannot find buffer named [{name}]"))
        };

        match inputs {
            BufferInputs::Single(op_id) => {
                let mut buffer_map = BufferMap::with_capacity(1);
                buffer_map.insert(BufferIdentifier::Index(0), attempt_get_buffer(op_id)?);
                Ok(buffer_map)
            }
            BufferInputs::Dict(mapping) => {
                let mut buffer_map = BufferMap::with_capacity(mapping.len());
                for (k, op_id) in mapping {
                    buffer_map.insert(
                        BufferIdentifier::Name(k.clone().into()),
                        attempt_get_buffer(op_id)?,
                    );
                }
                Ok(buffer_map)
            }
            BufferInputs::Array(arr) => {
                let mut buffer_map = BufferMap::with_capacity(arr.len());
                for (i, op_id) in arr.into_iter().enumerate() {
                    buffer_map.insert(BufferIdentifier::Index(i), attempt_get_buffer(op_id)?);
                }
                Ok(buffer_map)
            }
        }
    }
}

pub struct DiagramContext<'a> {
    pub construction: &'a mut DiagramConstruction,
    pub diagram: &'a Diagram,
    pub registry: &'a DiagramElementRegistry,
}

impl<'a> DiagramContext<'a> {
    /// Get the type information for the request message that goes into a node.
    ///
    /// # Arguments
    ///
    /// * `target` - Optionally indicate a specific node in the diagram to treat
    ///   as the target node, even if it is not the actual target. Using [`Some`]
    ///   for this will override whatever is used for `next`.
    /// * `next` - Indicate the next operation, i.e. the true target.
    pub fn get_node_request_type(
        &self,
        target: Option<&OperationId>,
        next: &NextOperation,
    ) -> Result<TypeInfo, DiagramErrorCode> {
        let target_node = if let Some(target) = target {
            self.diagram.get_op(target)?
        } else {
            match next {
                NextOperation::Target(op_id) => self.diagram.get_op(op_id)?,
                NextOperation::Builtin { builtin } => match builtin {
                    BuiltinTarget::Terminate => return Ok(TypeInfo::of::<JsonMessage>()),
                    BuiltinTarget::Dispose => return Err(DiagramErrorCode::UnknownTarget),
                },
            }
        };
        let node_op = match target_node {
            DiagramOperation::Node(op) => op,
            _ => return Err(DiagramErrorCode::UnknownTarget),
        };
        let target_type = self
            .registry
            .get_node_registration(&node_op.builder)?
            .request;
        Ok(target_type)
    }
}

/// Indicate whether the operation has finished building.
#[derive(Debug, Clone)]
pub enum BuildStatus {
    /// The operation has finished building.
    Finished,
    /// The operation needs to make another attempt at building after more
    /// information becomes available.
    Defer {
        /// Progress was made during this run. This can mean the operation has
        /// added some information into the diagram but might need to provide
        /// more later. If no operations make any progress within an entire
        /// iteration of the workflow build then we assume it is impossible to
        /// build the diagram.
        progress: bool,
        reason: Cow<'static, str>,
    },
}

impl BuildStatus {
    /// Indicate that the build of the operation needs to be deferred.
    pub fn defer(reason: impl Into<Cow<'static, str>>) -> Self {
        Self::Defer {
            progress: false,
            reason: reason.into(),
        }
    }

    /// Indicate that the operation made progress, even if it's deferred.
    #[allow(unused)]
    pub fn with_progress(mut self) -> Self {
        match &mut self {
            Self::Defer { progress, .. } => {
                *progress = true;
            }
            Self::Finished => {
                // Do nothing
            }
        }

        self
    }

    /// Did this operation make progress on this round?
    pub fn made_progress(&self) -> bool {
        match self {
            Self::Defer { progress, .. } => *progress,
            Self::Finished => true,
        }
    }

    /// Change this build status into its reason for deferral, if it is a
    /// deferral.
    pub fn into_deferral_reason(self) -> Option<Cow<'static, str>> {
        match self {
            Self::Defer { reason, .. } => Some(reason),
            Self::Finished => None,
        }
    }

    /// Check if the build finished.
    pub fn is_finished(&self) -> bool {
        matches!(self, BuildStatus::Finished)
    }
}

pub trait BuildDiagramOperation {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode>;
}

pub(super) fn create_workflow<Request, Response, Streams>(
    scope: Scope<Request, Response, Streams>,
    builder: &mut Builder,
    registry: &DiagramElementRegistry,
    diagram: &Diagram,
) -> Result<(), DiagramError>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    let mut construction = DiagramConstruction::default();

    // Put the input message into the diagram
    construction.add_output_into_target(diagram.start.clone(), scope.input.into());

    // Put the builtin operations into the diagram
    let terminate: DynInputSlot = scope.terminate.into();
    construction.connect_to_operation.insert(
        NextOperation::Builtin {
            builtin: BuiltinTarget::Terminate,
        },
        Box::new(move |_, output, builder, ctx| {
            if output.message_info() == terminate.message_info() {
                // If the message types match correctly then just connect as normal.
                output.connect_to(&terminate, builder)
            } else if terminate.message_info() == &TypeInfo::of::<JsonMessage>() {
                // If the terminate type is JsonMessage then we'll automatically
                // try to serialize the terminating message.
                //
                // TODO(@mxgrey): We should support arbitrary conversions in the
                // future and use that here instead.
                let output: DynOutput = ctx.registry.messages.serialize(builder, output)?.into();
                output.connect_to(&terminate, builder)
            } else {
                Err(DiagramErrorCode::TypeMismatch {
                    source_type: *output.message_info(),
                    target_type: *terminate.message_info(),
                })
            }
        }),
    );

    construction.connect_to_operation.insert(
        NextOperation::Builtin {
            builtin: BuiltinTarget::Dispose,
        },
        Box::new(move |_, _, _, _| {
            // Do nothing since the output is being disposed
            Ok(())
        }),
    );

    let mut unfinished_operations: Vec<&OperationId> = diagram.ops.keys().collect();
    let mut deferred_operations: Vec<(&OperationId, BuildStatus)> = Vec::new();

    // Iteratively build all the operations in the diagram
    while !unfinished_operations.is_empty() {
        let mut made_progress = false;
        for op in unfinished_operations.drain(..) {
            let ctx = DiagramContext {
                construction: &mut construction,
                diagram,
                registry,
            };

            let status = diagram
                .ops
                .get(op)
                .ok_or_else(|| {
                    DiagramErrorCode::UnknownOperation(NextOperation::Target(op.clone()))
                })?
                .build_diagram_operation(op, builder, ctx)
                .map_err(|code| DiagramError::in_operation(op.clone(), code))?;

            made_progress |= status.made_progress();
            if !status.is_finished() {
                deferred_operations.push((op, status));
            }
        }

        if made_progress {
            // Try another iteration if needed since we made progress last time
            unfinished_operations = deferred_operations.drain(..).map(|(op, _)| op).collect();
        } else {
            // No progress can be made any longer so return an error
            return Err(DiagramErrorCode::BuildHalted {
                reasons: deferred_operations
                    .drain(..)
                    .filter_map(|(op, status)| {
                        status
                            .into_deferral_reason()
                            .map(|reason| (op.clone(), reason))
                    })
                    .collect(),
            }
            .into());
        }
    }

    let ctx = ConnectionContext { diagram, registry };

    for (op, outputs) in construction.outputs_to_operation_target {
        let op = NextOperation::Target(op);
        let connect = construction
            .connect_to_operation
            .get_mut(&op)
            .ok_or_else(|| DiagramErrorCode::UnknownOperation(op.clone()))?;

        for output in outputs {
            connect(&op, output, builder, ctx)?;
        }
    }

    for (builtin, outputs) in construction.outputs_to_builtin_target {
        let op = NextOperation::Builtin { builtin };
        let connect = construction
            .connect_to_operation
            .get_mut(&op)
            .ok_or_else(|| DiagramErrorCode::UnknownOperation(op.clone()))?;

        for output in outputs {
            connect(&op, output, builder, ctx)?;
        }
    }

    Ok(())
}
