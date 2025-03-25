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
    DiagramOperation, DynInputSlot, DynOutput, ImplicitDeserialization, ImplicitSerialization,
    ImplicitStringify, NextOperation, OperationId, TypeInfo,
};

#[derive(Default)]
struct DiagramConstruction {
    connect_into_target: HashMap<NextOperation, Box<dyn ConnectIntoTarget>>,
    // We use a separate hashmap for OperationId vs BuiltinTarget so we can
    // efficiently fetch with an &OperationId
    outputs_to_operation_target: HashMap<OperationId, Vec<DynOutput>>,
    outputs_to_builtin_target: HashMap<BuiltinTarget, Vec<DynOutput>>,
    buffers: HashMap<OperationId, AnyBuffer>,
}

impl DiagramConstruction {
    fn is_finished(&self) -> bool {
        for outputs in self.outputs_to_builtin_target.values() {
            if !outputs.is_empty() {
                return false;
            }
        }

        for outputs in self.outputs_to_operation_target.values() {
            if !outputs.is_empty() {
                return false;
            }
        }

        return true;
    }
}

pub struct DiagramContext<'a> {
    construction: &'a mut DiagramConstruction,
    pub diagram: &'a Diagram,
    pub registry: &'a DiagramElementRegistry,
}

impl<'a> DiagramContext<'a> {
    /// Get all the currently known outputs that are aimed at this target operation.
    ///
    /// During the [`BuildDiagramOperation`] phase this will eventually contain
    /// all outputs targeting this operation that are explicitly listed in the
    /// diagram. It will never contain outputs that implicitly target this
    /// operation.
    ///
    /// During the [`ConnectIntoTarget`] phase this will not contain any outputs
    /// except new outputs added during the current call to [`ConnectIntoTarget`],
    /// so this function is generally not useful during that phase.
    pub fn get_outputs_into_operation_target(&self, id: &OperationId) -> Option<&Vec<DynOutput>> {
        self.construction.outputs_to_operation_target.get(id)
    }

    /// Infer the [`TypeInfo`] for the input messages into the specified operation.
    ///
    /// If this returns [`None`] then not enough of the diagram has been built
    /// yet to infer the input type. In that case you can return something like
    /// `Ok(BuildStatus::defer("waiting for an input"))`.
    ///
    /// The workflow builder will ensure that all outputs targeting this
    /// operation are compatible with this message type.
    ///
    /// During the [`ConnectIntoTarget`] phase all information about outputs
    /// going into this target will be drained, so this function is generally
    /// not useful during that phase. If you need to retain this information
    /// during the [`ConnectIntoTarget`] phase then you should capture the
    /// [`TypeInfo`] that you receive from this function during the
    /// [`BuildDiagramOperation`] phase.
    pub fn infer_input_type_into_target(&self, id: &OperationId) -> Option<&TypeInfo> {
        self.get_outputs_into_operation_target(id)
            .and_then(|outputs| outputs.first())
            .map(|o| o.message_info())
    }

    /// Add an output to connect into a target.
    ///
    /// This can be used during both the [`BuildDiagramOperation`] phase and the
    /// [`ConnectIntoTarget`] phase.
    ///
    /// # Arguments
    ///
    /// * target - The operation that needs to receive the output
    /// * output - The output channel that needs to be connected into the target.
    pub fn add_output_into_target(&mut self, target: NextOperation, output: DynOutput) {
        match target {
            NextOperation::Target(id) => {
                self.construction
                    .outputs_to_operation_target
                    .entry(id)
                    .or_default()
                    .push(output);
            }
            NextOperation::Builtin { builtin } => {
                self.construction
                    .outputs_to_builtin_target
                    .entry(builtin)
                    .or_default()
                    .push(output);
            }
        }
    }

    /// Set the input slot of an operation. This should not be called more than
    /// once per operation, because only one input slot can be used for any
    /// operation.
    ///
    /// This should be used during the [`BuildDiagramOperation`] phase to set
    /// the input slots for each operation. If you need non-standard connection
    /// behavior then you can use [`Self::set_connect_into_target`].
    ///
    /// This should never be necessary to use this during the [`ConnectIntoTarget`]
    /// phase because all connection behaviors should already be set by then.
    pub fn set_input_for_target(
        &mut self,
        operation: &OperationId,
        input: DynInputSlot,
    ) -> Result<(), DiagramErrorCode> {
        match self
            .construction
            .connect_into_target
            .entry(NextOperation::Target(operation.clone()))
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleInputsCreated(operation.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(standard_input_connection(input, &self.registry)?);
            }
        }

        Ok(())
    }

    /// Set a callback that will allow outputs to connect into this target. This
    /// is a more general method than [`Self::set_input_for_target`]. This allows
    /// you to take further action before a target is connected, like serializing.
    ///
    /// This should never be necessary to use this during the [`ConnectIntoTarget`]
    /// phase because all connection behaviors should already be set by then.
    pub fn set_connect_into_target<C: ConnectIntoTarget + 'static>(
        &mut self,
        operation: &OperationId,
        connect: C,
    ) -> Result<(), DiagramErrorCode> {
        let connect = Box::new(connect);
        match self
            .construction
            .connect_into_target
            .entry(NextOperation::Target(operation.clone()))
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleInputsCreated(operation.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(connect);
            }
        }
        Ok(())
    }

    /// Same as [`Self::set_connect_into_target`] but you can pass in a closure.
    pub fn set_connect_into_target_callback<F>(
        &mut self,
        operation: &OperationId,
        connect: F,
    ) -> Result<(), DiagramErrorCode>
    where
        F: FnMut(DynOutput, &mut Builder, &mut DiagramContext) -> Result<(), DiagramErrorCode>
            + 'static,
    {
        self.set_connect_into_target(operation, ConnectionCallback(connect))
    }

    /// Set the buffer that should be used for a certain operation. This will
    /// also set its connection callback.
    pub fn set_buffer_for_operation(
        &mut self,
        operation: &OperationId,
        buffer: AnyBuffer,
    ) -> Result<(), DiagramErrorCode> {
        match self.construction.buffers.entry(operation.clone()) {
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
            self.construction
                .buffers
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
                    BuiltinTarget::Cancel => return Ok(TypeInfo::of::<JsonMessage>()),
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

    pub fn get_implicit_error_target(&self) -> NextOperation {
        self.diagram
            .on_implicit_error
            .clone()
            .unwrap_or(NextOperation::Builtin {
                builtin: BuiltinTarget::Cancel,
            })
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

/// This trait is used to instantiate operations in the workflow. This trait
/// will be called on each operation in the diagram until it finishes building.
/// Each operation should use this to provide a [`ConnectOutput`] handle for
/// itself (if relevant) and deposit [`DynOutput`]s into [`DiagramContext`].
///
/// After all operations are fully built, [`ConnectIntoTarget`] will be used to
/// connect outputs into their target operations.
pub trait BuildDiagramOperation {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode>;
}

/// This trait is used to connect outputs to their target operations. This trait
/// will be called for each output produced by [`BuildDiagramOperation`].
///
/// It is possible
pub trait ConnectIntoTarget {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode>;
}

pub struct ConnectionCallback<F>(pub F)
where
    F: FnMut(DynOutput, &mut Builder, &mut DiagramContext) -> Result<(), DiagramErrorCode>;

impl<F> ConnectIntoTarget for ConnectionCallback<F>
where
    F: FnMut(DynOutput, &mut Builder, &mut DiagramContext) -> Result<(), DiagramErrorCode>,
{
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        (self.0)(output, builder, ctx)
    }
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

    initialize_builtin_operations(
        scope,
        builder,
        &mut DiagramContext {
            construction: &mut construction,
            diagram,
            registry,
        },
    )?;

    let mut unfinished_operations: Vec<&OperationId> = diagram.ops.keys().collect();
    let mut deferred_operations: Vec<(&OperationId, BuildStatus)> = Vec::new();

    let mut iterations = 0;
    const MAX_ITERATIONS: usize = 10_000;

    // Iteratively build all the operations in the diagram
    while !unfinished_operations.is_empty() {
        let mut made_progress = false;
        for op in unfinished_operations.drain(..) {
            let mut ctx = DiagramContext {
                construction: &mut construction,
                diagram,
                registry,
            };

            // Attempt to build this operation
            let status = diagram
                .ops
                .get(op)
                .ok_or_else(|| {
                    DiagramErrorCode::UnknownOperation(NextOperation::Target(op.clone()))
                })?
                .build_diagram_operation(op, builder, &mut ctx)
                .map_err(|code| DiagramError::in_operation(op.clone(), code))?;

            made_progress |= status.made_progress();
            if !status.is_finished() {
                // The operation did not finish, so pass it into the deferred
                // operations list.
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

        iterations += 1;
        if iterations > MAX_ITERATIONS {
            return Err(DiagramErrorCode::ExcessiveIterations.into());
        }
    }

    let mut new_construction = DiagramConstruction::default();
    new_construction.buffers = construction.buffers.clone();

    iterations = 0;
    while !construction.is_finished() {
        let mut ctx = DiagramContext {
            construction: &mut new_construction,
            diagram,
            registry,
        };

        // Attempt to connect to all regular operations
        for (op, outputs) in construction.outputs_to_operation_target.drain() {
            let op = NextOperation::Target(op);
            let connect = construction
                .connect_into_target
                .get_mut(&op)
                .ok_or_else(|| DiagramErrorCode::UnknownOperation(op.clone()))?;

            for output in outputs {
                connect.connect_into_target(output, builder, &mut ctx)?;
            }
        }

        // Attempt to connect to all builtin operations
        for (builtin, outputs) in construction.outputs_to_builtin_target.drain() {
            let op = NextOperation::Builtin { builtin };
            let connect = construction
                .connect_into_target
                .get_mut(&op)
                .ok_or_else(|| DiagramErrorCode::UnknownOperation(op.clone()))?;

            for output in outputs {
                connect.connect_into_target(output, builder, &mut ctx)?;
            }
        }

        construction
            .outputs_to_builtin_target
            .extend(new_construction.outputs_to_builtin_target.drain());

        construction
            .outputs_to_operation_target
            .extend(new_construction.outputs_to_operation_target.drain());

        iterations += 1;
        if iterations > MAX_ITERATIONS {
            return Err(DiagramErrorCode::ExcessiveIterations.into());
        }
    }

    Ok(())
}

fn initialize_builtin_operations<Request, Response, Streams>(
    scope: Scope<Request, Response, Streams>,
    builder: &mut Builder,
    ctx: &mut DiagramContext,
) -> Result<(), DiagramError>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    // Put the input message into the diagram
    ctx.add_output_into_target(ctx.diagram.start.clone(), scope.input.into());

    // Add the terminate operation
    ctx.construction.connect_into_target.insert(
        NextOperation::Builtin {
            builtin: BuiltinTarget::Terminate,
        },
        standard_input_connection(scope.terminate.into(), &ctx.registry)?,
    );

    // Add the dispose operation
    ctx.construction.connect_into_target.insert(
        NextOperation::Builtin {
            builtin: BuiltinTarget::Dispose,
        },
        Box::new(ConnectionCallback(move |_, _, _| {
            // Do nothing since the output is being disposed
            Ok(())
        })),
    );

    // Add the cancel operation
    ctx.construction.connect_into_target.insert(
        NextOperation::Builtin {
            builtin: BuiltinTarget::Cancel,
        },
        Box::new(ConnectToCancel::new(builder)?),
    );

    Ok(())
}

fn standard_input_connection(
    input_slot: DynInputSlot,
    registry: &DiagramElementRegistry,
) -> Result<Box<dyn ConnectIntoTarget + 'static>, DiagramErrorCode> {
    if input_slot.message_info() == &TypeInfo::of::<JsonMessage>() {
        return Ok(Box::new(ImplicitSerialization::new(input_slot)?));
    }

    if let Some(deserialization) = ImplicitDeserialization::try_new(input_slot, &registry.messages)?
    {
        // The target type is deserializable, so let's apply implicit deserialization
        // to it.
        return Ok(Box::new(deserialization));
    }

    Ok(Box::new(BasicConnect { input_slot }))
}

impl ConnectIntoTarget for ImplicitSerialization {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        self.implicit_serialize(output, builder, ctx)
    }
}

impl ConnectIntoTarget for ImplicitDeserialization {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        self.implicit_deserialize(output, builder, ctx)
    }
}

struct BasicConnect {
    input_slot: DynInputSlot,
}

impl ConnectIntoTarget for BasicConnect {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        _: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        output.connect_to(&self.input_slot, builder)
    }
}

struct ConnectToCancel {
    quiet_cancel: DynInputSlot,
    implicit_serialization: ImplicitSerialization,
    implicit_stringify: ImplicitStringify,
    triggers: HashMap<TypeInfo, DynInputSlot>,
}

impl ConnectToCancel {
    fn new(builder: &mut Builder) -> Result<Self, DiagramErrorCode> {
        Ok(Self {
            quiet_cancel: builder.create_quiet_cancel().into(),
            implicit_serialization: ImplicitSerialization::new(
                builder.create_cancel::<JsonMessage>().into(),
            )?,
            implicit_stringify: ImplicitStringify::new(builder.create_cancel::<String>().into())?,
            triggers: Default::default(),
        })
    }
}

impl ConnectIntoTarget for ConnectToCancel {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        let Err(output) = self
            .implicit_stringify
            .try_implicit_stringify(output, builder, ctx)?
        else {
            // We successfully converted the output into a string, so we are done.
            return Ok(());
        };

        // Try to implicitly serialize the incoming message if the message
        // type supports it. That way we can connect it to the regular
        // cancel operation.
        let Err(output) = self
            .implicit_serialization
            .try_implicit_serialize(output, builder, ctx)?
        else {
            // We successfully converted the output into a json, so we are done.
            return Ok(());
        };

        // In this case, the message type cannot be stringified or serialized so
        // we'll change it into a trigger and then connect it to the quiet
        // cancel instead.
        let input_slot = match self.triggers.entry(*output.message_info()) {
            Entry::Occupied(occupied) => occupied.get().clone(),
            Entry::Vacant(vacant) => {
                let trigger = ctx
                    .registry
                    .messages
                    .trigger(output.message_info(), builder)?;
                trigger.output.connect_to(&self.quiet_cancel, builder)?;
                vacant.insert(trigger.input).clone()
            }
        };

        output.connect_to(&input_slot, builder)?;

        Ok(())
    }
}
