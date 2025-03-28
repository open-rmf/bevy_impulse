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
    ImplicitStringify, NextOperation, OperationName, TypeInfo, NamespacedOperation,
    Operations, Templates,
};

/// This key is used so we can do a clone-free .get(&NextOperation) of a hashmap
/// that uses this as a key.
//
// TODO(@mxgrey): With this struct we could apply a lifetime to
// DiagramConstruction and then borrow all the names used in this struct instead
// of using Cow.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum OperationRef<'a> {
    Named(NamedOperationRef<'a>),
    Builtin { builtin: BuiltinTarget },
}

impl<'a> OperationRef<'a> {
    fn in_namespaces(self, parent_namespaces: &[Cow<'a, str>]) -> Self {
        match self {
            Self::Builtin { builtin } => Self::Builtin { builtin },
            Self::Named(named) => Self::Named(named.in_namespace(parent_namespaces)),
        }
    }

    fn get_namespaces(&'a self) -> Cow<'a, Vec<Cow<'a, str>>> {
        match self {
            Self::Named(named) => Cow::Borrowed(&named.namespaces),
            Self::Builtin { .. } => Cow::Owned(Vec::new()),
        }
    }

    pub fn as_static(&self) -> OperationRef<'static> {
        match self {
            Self::Builtin { builtin } => OperationRef::Builtin { builtin: builtin.clone() },
            Self::Named(named) => OperationRef::Named(named.as_static()),
        }
    }
}

impl<'a> From<NextOperation> for OperationRef<'a> {
    fn from(value: NextOperation) -> Self {
        match value {
            NextOperation::Name(name) => OperationRef::Named(name.into()),
            NextOperation::Namespace(id) => OperationRef::Named(id.into()),
            NextOperation::Builtin { builtin } => OperationRef::Builtin { builtin },
        }
    }
}

impl<'a> From<&'a NextOperation> for OperationRef<'a> {
    fn from(value: &'a NextOperation) -> Self {
        match value {
            NextOperation::Name(name) => OperationRef::Named(name.into()),
            NextOperation::Namespace(id) => OperationRef::Named(id.into()),
            NextOperation::Builtin { builtin } => OperationRef::Builtin { builtin: builtin.clone() },
        }
    }
}

impl<'a> From<&'a OperationName> for OperationRef<'a> {
    fn from(value: &'a OperationName) -> Self {
        OperationRef::Named(value.into())
    }
}

impl<'a> std::fmt::Display for OperationRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(named) => write!(f, "{named}"),
            Self::Builtin { builtin } => write!(f, "builtin:{builtin}"),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamedOperationRef<'a> {
    pub namespaces: Vec<Cow<'a, str>>,
    pub name: Cow<'a, str>,
    /// The exposed field indicated whether this is referring to an operation
    /// within a section which is exposed to a parent workflow. This flag
    /// prevents outputs in a parent workflow from accidentally or intentionally
    /// connecting to operations in a section that were not supposed to be
    /// exposed.
    pub exposed: bool,
}

impl<'a> NamedOperationRef<'a> {
    fn in_namespace(mut self, parent_namespaces: &[Cow<'a, str>]) -> Self {
        let mut namespaces = Vec::from_iter(parent_namespaces.into_iter().cloned());
        namespaces.extend(self.namespaces.into_iter());
        self.namespaces = namespaces;
        self
    }

    fn as_static(&self) -> NamedOperationRef<'static> {
        NamedOperationRef {
            namespaces: self
                .namespaces
                .iter()
                .map(|n| Cow::Owned(n.clone().into_owned()))
                .collect(),
            name: Cow::Owned(self.name.clone().into_owned()),
            exposed: self.exposed,
        }
    }
}

impl<'a> From<&'a OperationName> for NamedOperationRef<'a> {
    fn from(name: &'a OperationName) -> Self {
        NamedOperationRef {
            namespaces: Vec::new(),
            name: Cow::Borrowed(name),
            // This is a same-scope access, so it does not need to be exposed
            exposed: false,
        }
    }
}

impl<'a> From<OperationName> for NamedOperationRef<'a> {
    fn from(name: OperationName) -> Self {
        NamedOperationRef {
            namespaces: Vec::new(),
            name: Cow::Owned(name),
            // This is a same-scope access, so it does not need to be exposed
            exposed: false,
        }
    }
}

impl<'a> From<&'a NamespacedOperation> for NamedOperationRef<'a> {
    fn from(id: &'a NamespacedOperation) -> Self {
        NamedOperationRef {
            namespaces: Vec::from_iter([Cow::Borrowed(id.namespace.as_str())]),
            name: Cow::Borrowed(id.operation.as_str()),
            // This is an access happening from the parent scope, so it does
            // need to be exposed
            exposed: true,
        }
    }
}

impl<'a> From<NamespacedOperation> for NamedOperationRef<'static> {
    fn from(id: NamespacedOperation) -> Self {
        NamedOperationRef {
            namespaces: Vec::from_iter([Cow::Owned(id.namespace.clone())]),
            name: Cow::Owned(id.operation.clone()),
            // This is an access happening from the parent scope, so it does
            // need to be exposed
            exposed: true
        }
    }
}

impl<'a> std::fmt::Display for NamedOperationRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for namespace in &self.namespaces {
            write!(f, "{namespace}:")?;
        }

        if !self.namespaces.is_empty() && self.exposed {
            write!(f, "(exposed):")?;
        }

        f.write_str(&self.name)
    }
}

#[derive(Default)]
struct DiagramConstruction<'a> {
    /// Implementations that define how outputs can connect to their target operations
    connect_into_target: HashMap<OperationRef<'a>, Box<dyn ConnectIntoTarget>>,
    /// A map of what outputs are going into each target operation
    outputs_into_target: HashMap<OperationRef<'a>, Vec<DynOutput>>,
    /// A map of what buffers exist in the diagram
    buffers: HashMap<OperationRef<'a>, AnyBuffer>,
    /// Operations that were spawned by another operation.
    child_operations: Vec<(OperationRef<'a>, DiagramOperation)>,
}

impl<'a> DiagramConstruction<'a> {
    fn is_finished(&self) -> bool {
        for outputs in self.outputs_into_target.values() {
            if !outputs.is_empty() {
                return false;
            }
        }

        return true;
    }
}

pub struct DiagramContext<'c, 'a: 'c> {
    construction: &'c mut DiagramConstruction<'c>,
    pub registry: &'a DiagramElementRegistry,
    pub operations: &'a Operations,
    pub templates: &'a Templates,
    pub on_implicit_error: &'a OperationRef<'a>,
    namespaces: Cow<'a, Vec<Cow<'a, str>>>,
}

impl<'c, 'a: 'c> DiagramContext<'c, 'a> {
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
    pub fn infer_input_type_into_target(
        &self,
        id: &OperationRef,
    ) -> Option<TypeInfo> {
        self.construction
            .outputs_into_target
            .get(id)
            .and_then(|outputs| outputs.first())
            .map(|o| *o.message_info())
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
        self.construction.outputs_into_target.entry(target.into()).or_default().push(output);
    }

    /// Set the input slot of an operation. This should not be called more than
    /// once per operation, because only one input slot can be used for any
    /// operation.
    ///
    /// This should be used during the [`BuildDiagramOperation`] phase to set
    /// the input slot of each operation with standard connection behavior.
    /// Standard connection behavior means that implicit serialization or
    /// deserialization will be applied to its inputs as needed to match the
    /// [`DynInputSlot`] that you provide.
    ///
    /// If you need non-standard connection behavior then you can use
    /// [`Self::set_connect_into_target`] to set the exact connection behavior.
    /// No implicit behaviors will be provided for [`Self::set_connect_into_target`],
    /// but you can enable those behaviors using [`ImplicitSerialization`] and
    /// [`ImplicitDeserialization`].
    ///
    /// This should never be used during the [`ConnectIntoTarget`] phase because
    /// all connection behaviors must already be set by then.
    pub fn set_input_for_target(
        &mut self,
        operation: impl Into<OperationRef<'a>> + Clone,
        input: DynInputSlot,
    ) -> Result<(), DiagramErrorCode> {
        match self
            .construction
            .connect_into_target
            .entry(operation.clone().into())
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleInputsCreated(operation.into().as_static()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(standard_input_connection(input, &self.registry)?);
            }
        }

        Ok(())
    }

    /// Set the implementation for how outputs connect into this target. This is
    /// a more general method than [`Self::set_input_for_target`].
    ///
    /// There will be no additional connection behavior beyond what you specify
    /// with the object passed in for `connect`. That means we will not
    /// automatically add implicit serialization or deserialization when you use
    /// this method. If you want your connection behavior to have those implicit
    /// operations you can use [`ImplicitSerialization`] and
    /// [`ImplicitDeserialization`] inside your `connect` implementation.
    ///
    /// This should never be necessary to use this during the [`ConnectIntoTarget`]
    /// phase because all connection behaviors should already be set by then.
    pub fn set_connect_into_target<C: ConnectIntoTarget + 'static>(
        &mut self,
        operation: impl Into<OperationRef<'a>> + Clone,
        connect: C,
    ) -> Result<(), DiagramErrorCode> {
        let connect = Box::new(connect);
        match self
            .construction
            .connect_into_target
            .entry(operation.clone().into())
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleInputsCreated(operation.into().as_static()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(connect);
            }
        }
        Ok(())
    }

    /// Same as [`Self::set_connect_into_target`] but you can pass in a closure.
    ///
    /// This is equivalent to doing
    /// `set_connect_into_target(operation, ConnectionCallback(connect))`.
    pub fn set_connect_into_target_callback<F>(
        &mut self,
        operation: &'a OperationName,
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
        operation: impl Into<OperationRef<'a>> + Clone,
        buffer: AnyBuffer,
    ) -> Result<(), DiagramErrorCode> {
        match self.construction.buffers.entry(operation.clone().into()) {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleBuffersCreated(operation.into().as_static()));
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
        let attempt_get_buffer = |buffer: &NextOperation| -> Result<AnyBuffer, String> {
            let buffer_ref: OperationRef = buffer.into();
            self.construction
                .buffers
                .get(&buffer_ref)
                .copied()
                .ok_or_else(|| format!("cannot find buffer named [{buffer}]"))
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
        target: Option<&OperationName>,
        next: &NextOperation,
    ) -> Result<TypeInfo, DiagramErrorCode> {
        let target_node = if let Some(target) = target {
            self.operations.get_op(target)?
        } else {
            match next {
                NextOperation::Name(op_id) => self.operations.get_op(op_id)?,
                NextOperation::Namespace(NamespacedOperation { namespace, operation }) => {
                    let section = &self
                        .registry
                        .get_section_registration(namespace)?
                        .metadata;

                    if let Some(input) = section.inputs.get(operation) {
                        return Ok(input.message_type);
                    }

                    if let Some(buffer) = section.buffers.get(operation) {
                        if let Some(message_type) = buffer.item_type {
                            return Ok(message_type);
                        }
                    }

                    return Err(DiagramErrorCode::UnknownTarget);
                }
                NextOperation::Builtin { builtin } => match builtin {
                    BuiltinTarget::Terminate => return Ok(TypeInfo::of::<JsonMessage>()),
                    BuiltinTarget::Dispose => return Err(DiagramErrorCode::UnknownTarget),
                    BuiltinTarget::Cancel => return Ok(TypeInfo::of::<JsonMessage>()),
                }
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

    pub fn get_implicit_error_target(&self) -> OperationRef<'a> {
        self.on_implicit_error.clone()
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
    fn build_diagram_operation<'a, 'c>(
        &self,
        id: &OperationRef<'a>,
        builder: &mut Builder,
        ctx: &mut DiagramContext<'a, 'c>,
    ) -> Result<BuildStatus, DiagramErrorCode>;
}

/// This trait is used to connect outputs to their target operations. This trait
/// will be called for each output produced by [`BuildDiagramOperation`].
///
/// You are allowed to generate new outputs during the [`ConnectIntoTarget`]
/// phase by calling [`DiagramContext::add_outputs_into_target`].
///
/// However you cannot add new [`ConnectIntoTarget`] instances for operations.
/// Any use of [`DiagramContext::set_input_for_target`],
/// [`DiagramContext::set_connect_into_target`], or
/// [`DiagramContext::set_connect_into_target_callback`] will be discarded.
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
    diagram.validate_operation_names()?;

    let mut construction = DiagramConstruction::default();

    let default_on_implicit_error = OperationRef::Builtin { builtin: BuiltinTarget::Cancel };
    let opt_on_implicit_error: Option<OperationRef> = diagram
        .on_implicit_error
        .as_ref()
        .map(Into::into);

    let on_implicit_error = opt_on_implicit_error.as_ref().unwrap_or(&default_on_implicit_error);

    initialize_builtin_operations(
        diagram.start.clone(),
        scope,
        builder,
        &mut DiagramContext {
            construction: &mut construction,
            registry,
            operations: &diagram.ops,
            templates: &diagram.templates,
            on_implicit_error,
            namespaces: Cow::Owned(Vec::new()),
        },
    )?;

    let mut unfinished_operations: Vec<(OperationRef, &DiagramOperation)> = diagram
        .ops
        .iter()
        .map(|(id, op)| (id.into(), op))
        .collect();
    let mut deferred_operations: Vec<DeferredOperation> = Vec::new();

    let mut iterations = 0;
    const MAX_ITERATIONS: usize = 10_000;

    // Iteratively build all the operations in the diagram
    while !unfinished_operations.is_empty() {
        let mut made_progress = false;
        for (id, op) in unfinished_operations.drain(..) {
            let mut ctx = DiagramContext {
                construction: &mut construction,
                registry,
                operations: &diagram.ops,
                templates: &diagram.templates,
                on_implicit_error,
                namespaces: id.get_namespaces(),
            };

            // Attempt to build this operation
            let status = op
                .build_diagram_operation(&id, builder, &mut ctx)
                .map_err(|code| DiagramError::in_operation(id.as_static(), code))?;

            made_progress |= status.made_progress();
            if !status.is_finished() {
                // The operation did not finish, so pass it into the deferred
                // operations list.
                deferred_operations.push(DeferredOperation { id, op, status });
            }
        }

        if made_progress {
            // Try another iteration if needed since we made progress last time
            unfinished_operations = deferred_operations
                .drain(..)
                .map(|deferred| (deferred.id, deferred.op))
                .collect();
        } else {
            // No progress can be made any longer so return an error
            return Err(DiagramErrorCode::BuildHalted {
                reasons: deferred_operations
                    .drain(..)
                    .filter_map(|deferred| {
                        deferred
                            .status
                            .into_deferral_reason()
                            .map(|reason| (deferred.id.as_static(), reason))
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
        for (id, outputs) in construction.outputs_into_target.drain() {
            let mut ctx = DiagramContext {
                construction: &mut new_construction,
                registry,
                operations: &diagram.ops,
                templates: &diagram.templates,
                on_implicit_error,
                namespaces: id.get_namespaces(),
            };

            let connect = construction
                .connect_into_target
                .get_mut(&id)
                .ok_or_else(|| DiagramErrorCode::UnknownOperation(id.into()))?;

            for output in outputs {
                connect.connect_into_target(output, builder, &mut ctx)?;
            }
        }

        construction
            .outputs_into_target
            .extend(new_construction.outputs_into_target.drain());

        iterations += 1;
        if iterations > MAX_ITERATIONS {
            return Err(DiagramErrorCode::ExcessiveIterations.into());
        }
    }

    Ok(())
}

struct DeferredOperation<'a> {
    id: OperationRef<'a>,
    op: &'a DiagramOperation,
    status: BuildStatus,
}

fn initialize_builtin_operations<Request, Response, Streams>(
    start: NextOperation,
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
    ctx.add_output_into_target(start.clone(), scope.input.into());

    // Add the terminate operation
    ctx.construction.connect_into_target.insert(
        OperationRef::Builtin {
            builtin: BuiltinTarget::Terminate,
        },
        standard_input_connection(scope.terminate.into(), &ctx.registry)?,
    );

    // Add the dispose operation
    ctx.construction.connect_into_target.insert(
        OperationRef::Builtin {
            builtin: BuiltinTarget::Dispose,
        },
        Box::new(ConnectionCallback(move |_, _, _| {
            // Do nothing since the output is being disposed
            Ok(())
        })),
    );

    // Add the cancel operation
    ctx.construction.connect_into_target.insert(
        OperationRef::Builtin {
            builtin: BuiltinTarget::Cancel,
        },
        Box::new(ConnectToCancel::new(builder)?),
    );

    Ok(())
}

/// This returns an opaque [`ConnectIntoTarget`] implementation that provides
/// the standard behavior of an input slot that other operations are connecting
/// into.
pub fn standard_input_connection(
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
