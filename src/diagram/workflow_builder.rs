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
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use crate::{AnyBuffer, BufferIdentifier, BufferMap, Builder, JsonMessage, Scope, StreamPack};

use super::{
    BufferSelection, BuiltinTarget, Diagram, DiagramElementRegistry, DiagramError,
    DiagramErrorCode, DiagramOperation, DynInputSlot, DynOutput, ImplicitDeserialization,
    ImplicitSerialization, ImplicitStringify, NamespacedOperation, NextOperation, OperationName,
    Operations, Templates, TypeInfo,
};

use bevy_ecs::prelude::Entity;

use smallvec::SmallVec;

type NamespaceList = SmallVec<[OperationName; 4]>;

/// This key is used so we can do a clone-free .get(&NextOperation) of a hashmap
/// that uses this as a key.
//
// TODO(@mxgrey): With this struct we could apply a lifetime to
// DiagramConstruction and then borrow all the names used in this struct instead
// of using Cow.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum OperationRef {
    Named(NamedOperationRef),
    Builtin { builtin: BuiltinTarget },
}

impl OperationRef {
    fn in_namespaces(self, parent_namespaces: &[Arc<str>]) -> Self {
        match self {
            Self::Builtin { builtin } => Self::Builtin { builtin },
            Self::Named(named) => Self::Named(named.in_namespaces(parent_namespaces)),
        }
    }
}

impl<'a> From<&'a NextOperation> for OperationRef {
    fn from(value: &'a NextOperation) -> Self {
        match value {
            NextOperation::Name(name) => OperationRef::Named(name.into()),
            NextOperation::Namespace(id) => OperationRef::Named(id.into()),
            NextOperation::Builtin { builtin } => OperationRef::Builtin {
                builtin: builtin.clone(),
            },
        }
    }
}

impl<'a> From<&'a OperationName> for OperationRef {
    fn from(value: &'a OperationName) -> Self {
        OperationRef::Named(value.into())
    }
}

impl From<NamedOperationRef> for OperationRef {
    fn from(value: NamedOperationRef) -> Self {
        OperationRef::Named(value)
    }
}

impl std::fmt::Display for OperationRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(named) => write!(f, "{named}"),
            Self::Builtin { builtin } => write!(f, "builtin:{builtin}"),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamedOperationRef {
    pub namespaces: SmallVec<[Arc<str>; 4]>,
    /// If this references an exposed operation, such as an exposed input or
    /// output of a session, this will contain the session name. Suppose we have
    /// a section named `sec` and it has an exposed output named `out`. Then
    /// there are two values of `NamedOperationRef` to consider:
    ///
    /// ```
    /// # use bevy_impulse::diagram::NamedOperationRef;
    /// # use smallvec::smallvec;
    ///
    /// let op_id = NamedOperationRef {
    ///     namespaces: smallvec!["sec".into()],
    ///     exposed_namespace: None,
    ///     name: "out".into(),
    /// };
    /// ```
    ///
    /// is the internal reference to `sec:out` that will be used by other
    /// operations inside of `sec`. On the other hand, operations that are
    /// siblings of `sec` would instead connect to
    ///
    /// ```
    /// # use bevy_impulse::diagram::NamedOperationRef;
    /// # use smallvec::smallvec;
    ///
    /// let op_id = NamedOperationRef {
    ///     namespaces: smallvec![],
    ///     exposed_namespace: Some("sec".into()),
    ///     name: "out".into(),
    /// };
    /// ```
    ///
    /// We need to make this distinction because operations inside `sec` do not
    /// know which of their siblings are exposed, and we don't want operations
    /// outside of `sec` to accidentally connect to operations that are supposed
    /// to be internal to `sec`.
    pub exposed_namespace: Option<Arc<str>>,
    pub name: Arc<str>,
}

impl NamedOperationRef {
    fn in_namespaces(mut self, parent_namespaces: &[Arc<str>]) -> Self {
        // Put the parent namespaces at the front and append the operation's
        // existing namespaces at the back.
        let new_namespaces = parent_namespaces
            .iter()
            .cloned()
            .chain(self.namespaces.drain(..))
            .collect();

        self.namespaces = new_namespaces;
        self
    }
}

impl<'a> From<&'a OperationName> for NamedOperationRef {
    fn from(name: &'a OperationName) -> Self {
        NamedOperationRef {
            namespaces: SmallVec::new(),
            exposed_namespace: None,
            name: Arc::clone(name),
        }
    }
}

impl From<OperationName> for NamedOperationRef {
    fn from(name: OperationName) -> Self {
        NamedOperationRef {
            namespaces: SmallVec::new(),
            exposed_namespace: None,
            name,
        }
    }
}

impl<'a> From<&'a NamespacedOperation> for NamedOperationRef {
    fn from(id: &'a NamespacedOperation) -> Self {
        NamedOperationRef {
            namespaces: SmallVec::new(),
            // This is referring to an exposed operation, so the namespace
            // mentioned in the operation goes into the exposed_namespace field
            exposed_namespace: Some(Arc::clone(&id.namespace)),
            name: Arc::clone(&id.operation),
        }
    }
}

impl<'a> From<&'a NamespacedOperation> for OperationRef {
    fn from(value: &'a NamespacedOperation) -> Self {
        OperationRef::Named(value.into())
    }
}

impl std::fmt::Display for NamedOperationRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for namespace in &self.namespaces {
            write!(f, "{namespace}:")?;
        }

        if let Some(exposed) = &self.exposed_namespace {
            write!(f, "{exposed}:(exposed):")?;
        }

        f.write_str(&self.name)
    }
}

#[derive(Default)]
struct DiagramConstruction<'a> {
    /// Implementations that define how outputs can connect to their target operations
    connect_into_target: HashMap<OperationRef, Box<dyn ConnectIntoTarget>>,
    /// A map of what outputs are going into each target operation
    outputs_into_target: HashMap<OperationRef, Vec<DynOutput>>,
    /// A map of what buffers exist in the diagram
    buffers: HashMap<OperationRef, BufferRef>,
    /// Operations that were spawned by another operation.
    generated_operations: Vec<UnfinishedOperation<'a>>,
}

impl<'a> DiagramConstruction<'a> {
    fn transfer_generated_operations(
        &mut self,
        deferred_operations: &mut Vec<DeferredOperation<'a>>,
        made_progress: &mut bool,
    ) {
        deferred_operations.extend(self.generated_operations.drain(..).map(
            |unfinished| {
                *made_progress = true;
                DeferredOperation {
                    unfinished,
                    status: BuildStatus::Defer {
                        progress: true,
                        reason: Cow::Borrowed("generated operation"),
                    },
                }
            },
        ));
    }
}

#[derive(Clone, Debug)]
enum BufferRef {
    Ref(OperationRef),
    Value(AnyBuffer),
}

impl<'a> DiagramConstruction<'a> {
    fn has_outputs(&self) -> bool {
        for outputs in self.outputs_into_target.values() {
            if !outputs.is_empty() {
                return true;
            }
        }

        return false;
    }
}

pub struct DiagramContext<'a, 'c> {
    construction: &'c mut DiagramConstruction<'a>,
    pub registry: &'a DiagramElementRegistry,
    pub operations: &'a Operations,
    pub templates: &'a Templates,
    pub on_implicit_error: &'a OperationRef,
    namespaces: NamespaceList,
}

impl<'a, 'c> DiagramContext<'a, 'c> {
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
        id: impl Into<OperationRef>,
    ) -> Result<Option<TypeInfo>, DiagramErrorCode> {
        let id = self.into_operation_ref(id);
        if let Some(connect) = self.construction.connect_into_target.get(&id) {
            let mut visited = HashSet::new();
            visited.insert(id.clone());
            let preferred = connect
                .infer_input_type(self, &mut visited)?
                .map(|infer| infer.preferred_input_type())
                .flatten();
            if let Some(preferred) = preferred {
                return Ok(Some(preferred));
            }
        }

        let infer = self
            .construction
            .outputs_into_target
            .get(&id)
            .and_then(|outputs| outputs.first())
            .map(|o| *o.message_info());
        Ok(infer)
    }

    /// Add an output to connect into a target.
    ///
    /// This can be used during both the [`BuildDiagramOperation`] phase and the
    /// [`ConnectIntoTarget`] phase.
    ///
    /// # Arguments
    ///
    /// * `target` - The operation that needs to receive the output
    /// * `output` - The output channel that needs to be connected into the target.
    pub fn add_output_into_target(&mut self, target: impl Into<OperationRef>, output: DynOutput) {
        let target = self.into_operation_ref(target);
        self.construction
            .outputs_into_target
            .entry(target)
            .or_default()
            .push(output);
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
        operation: impl Into<OperationRef>,
        input: DynInputSlot,
    ) -> Result<(), DiagramErrorCode> {
        let operation = self.into_operation_ref(operation);
        match self
            .construction
            .connect_into_target
            .entry(operation.clone())
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::DuplicateInputsCreated(operation));
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
        operation: impl Into<OperationRef> + Clone,
        connect: C,
    ) -> Result<(), DiagramErrorCode> {
        let connect = Box::new(connect);
        match self
            .construction
            .connect_into_target
            .entry(operation.clone().into())
        {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::DuplicateInputsCreated(operation.into()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(connect);
            }
        }
        Ok(())
    }

    /// Set the buffer that should be used for a certain operation. This will
    /// also set its connection callback.
    pub fn set_buffer_for_operation(
        &mut self,
        operation: impl Into<OperationRef>,
        buffer: AnyBuffer,
    ) -> Result<(), DiagramErrorCode> {
        let operation: OperationRef = operation.into();
        match self.construction.buffers.entry(operation.clone()) {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::DuplicateBuffersCreated(operation));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(BufferRef::Value(buffer));
            }
        }

        let input: DynInputSlot = buffer.into();
        self.set_input_for_target(operation, input)
    }

    /// Create a buffer map based on the buffer inputs provided. If one or more
    /// of the buffers in BufferInputs is not available, get an error including
    /// the name of the missing buffer.
    pub fn create_buffer_map(&self, inputs: &BufferSelection) -> Result<BufferMap, String> {
        let attempt_get_buffer = |buffer: &NextOperation| -> Result<AnyBuffer, String> {
            let mut buffer_ref: OperationRef = buffer.into();
            let mut visited = HashSet::new();
            loop {
                if !visited.insert(buffer_ref.clone()) {
                    return Err(format!("circular reference for buffer [{buffer}]"));
                }

                let next = self
                    .construction
                    .buffers
                    .get(&buffer_ref)
                    .ok_or_else(|| format!("cannot find buffer named [{buffer}]"))?;

                buffer_ref = match next {
                    BufferRef::Value(value) => return Ok(*value),
                    BufferRef::Ref(reference) => reference.clone(),
                };
            }
        };

        match inputs {
            BufferSelection::Single(op_id) => {
                let mut buffer_map = BufferMap::with_capacity(1);
                buffer_map.insert(BufferIdentifier::Index(0), attempt_get_buffer(op_id)?);
                Ok(buffer_map)
            }
            BufferSelection::Dict(mapping) => {
                let mut buffer_map = BufferMap::with_capacity(mapping.len());
                for (k, op_id) in mapping {
                    buffer_map.insert(
                        BufferIdentifier::Name(k.clone().into()),
                        attempt_get_buffer(op_id)?,
                    );
                }
                Ok(buffer_map)
            }
            BufferSelection::Array(arr) => {
                let mut buffer_map = BufferMap::with_capacity(arr.len());
                for (i, op_id) in arr.into_iter().enumerate() {
                    buffer_map.insert(BufferIdentifier::Index(i), attempt_get_buffer(op_id)?);
                }
                Ok(buffer_map)
            }
        }
    }

    pub fn get_implicit_error_target(&self) -> OperationRef {
        self.on_implicit_error.clone()
    }

    pub fn into_operation_ref(&self, id: impl Into<OperationRef>) -> OperationRef {
        let id: OperationRef = id.into();
        id.in_namespaces(&self.namespaces)
    }

    /// Add an operation which exists as a child inside another operation.
    ///
    /// For example this is used by section templates to add their inner
    /// operations into the workflow builder.
    ///
    /// Operations added this way will be internal to the parent operation,
    /// which means they are not "exposed" to be connected to other operations
    /// that are not inside the parent.
    pub fn add_child_operation(
        &mut self,
        id: &OperationName,
        child_id: &OperationName,
        op: &'a DiagramOperation,
        sibling_ops: &'a Operations,
    ) {
        let mut namespaces = self.namespaces.clone();
        namespaces.push(Arc::clone(id));

        self.construction
            .generated_operations
            .push(UnfinishedOperation {
                id: Arc::clone(child_id),
                namespaces,
                op,
                sibling_ops,
            });
    }

    /// Create a connection for an exposed input that allows it to redirect any
    /// connections to an internal (child) input.
    ///
    /// # Arguments
    ///
    /// * `section_id` - The ID of the parent operation (e.g. ID of the section)
    ///   that is exposing an operation to its siblings.
    /// * `exposed_id` - The ID of the exposed operation that is being provided
    ///   by the section. E.g. siblings of the section will refer to the redirected
    ///   input by `{ section_id: exposed_id }`
    /// * `child_id` - The ID of the operation inside the section which the exposed
    ///   operation ID is being redirected to.
    ///
    /// This should not be used to expose a buffer. For that, use
    /// [`Self::redirect_to_child_buffer`].
    pub fn redirect_to_child_input(
        &mut self,
        section_id: &OperationName,
        exposed_id: &OperationName,
        child_id: &NextOperation,
    ) -> Result<(), DiagramErrorCode> {
        let (exposed, redirect_to) =
            self.get_exposed_and_inner_ids(section_id, exposed_id, child_id);

        self.set_connect_into_target(exposed, RedirectConnection::new(redirect_to))
    }

    /// Same as [`Self::redirecto_to_child_input`], but meant for buffers.
    pub fn redirect_to_child_buffer(
        &mut self,
        section_id: &OperationName,
        exposed_id: &OperationName,
        child_id: &NextOperation,
    ) -> Result<(), DiagramErrorCode> {
        let (exposed, redirect_to) =
            self.get_exposed_and_inner_ids(section_id, exposed_id, child_id);

        self.set_connect_into_target(
            exposed.clone(),
            RedirectConnection::new(redirect_to.clone()),
        )?;

        match self.construction.buffers.entry(exposed.clone()) {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::DuplicateBuffersCreated(exposed));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(BufferRef::Ref(redirect_to));
            }
        }

        Ok(())
    }

    /// Create a connection for an exposed output that allows it to redirect
    /// any connections to an external (sibling) input. This is used to implement
    /// the `"connect":` schema for section templates.
    ///
    /// # Arguments
    ///
    /// * `section_id` - The ID of the parent operation (e.g. ID of the section)
    ///   that is exposing an output to its siblings.
    /// * `output_id` - The ID of the exposed output that is being provided by
    ///   the section. E.g. siblings of the section will refer to the redirected
    ///   output by `{ section_id: output_id }`.
    /// * `sibling_id` - The sibling of the section that should receive the output.
    pub fn redirect_exposed_output_to_sibling(
        &mut self,
        section_id: &OperationName,
        output_id: &OperationName,
        sibling_id: &NextOperation,
    ) -> Result<(), DiagramErrorCode> {
        // This is the slot that operations inside of the section will direct
        // their outputs to. It will receive the outputs and then redirect them.
        let internal: OperationRef = NamedOperationRef {
            namespaces: SmallVec::from_iter([Arc::clone(section_id)]),
            exposed_namespace: None,
            name: Arc::clone(output_id),
        }
        .in_namespaces(&self.namespaces)
        .into();

        let redirect_to = self.into_operation_ref(sibling_id);
        self.set_connect_into_target(internal, RedirectConnection::new(redirect_to))
    }

    fn get_exposed_and_inner_ids(
        &self,
        section_id: &OperationName,
        exposed_id: &OperationName,
        child_id: &NextOperation,
    ) -> (OperationRef, OperationRef) {
        let mut child_namespaces = self.namespaces.clone();
        child_namespaces.push(Arc::clone(section_id));
        let inner = Into::<OperationRef>::into(child_id).in_namespaces(&child_namespaces);

        let exposed: OperationRef = NamedOperationRef {
            namespaces: self.namespaces.clone(),
            exposed_namespace: Some(section_id.clone()),
            name: exposed_id.clone(),
        }
        .into();

        (exposed, inner)
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
        id: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
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

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode>;
}

/// This trait helps to determine what types of messages can go into an input
/// slot.
///
/// For the first implementation of this, we are only considering the most
/// preferred message type, but in the future we should add support for
/// building a constraint graph to support inference in cases where an input
/// can accept multiple different message types.
///
/// NOTE(@mxgrey): We may expand on this trait in the future to enable message
/// type negotiation for operations that can accept a range of message types.
pub trait InferMessageType {
    fn preferred_input_type(&self) -> Option<TypeInfo>;
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
    diagram.validate_template_usage()?;

    let mut construction = DiagramConstruction::default();

    let default_on_implicit_error = OperationRef::Builtin {
        builtin: BuiltinTarget::Cancel,
    };
    let opt_on_implicit_error: Option<OperationRef> =
        diagram.on_implicit_error.as_ref().map(Into::into);

    let on_implicit_error = opt_on_implicit_error
        .as_ref()
        .unwrap_or(&default_on_implicit_error);

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
            namespaces: NamespaceList::new(),
        },
    )?;

    let mut unfinished_operations: Vec<UnfinishedOperation> = diagram
        .ops
        .iter()
        .map(|(id, op)| UnfinishedOperation::new(Arc::clone(id), op, &diagram.ops))
        .collect();
    let mut deferred_operations: Vec<DeferredOperation> = Vec::new();

    let mut deferred_connections = HashMap::new();
    let mut connector_construction = DiagramConstruction::default();

    let mut iterations = 0;
    const MAX_ITERATIONS: usize = 10_000;

    // Iteratively build all the operations in the diagram
    while !unfinished_operations.is_empty() || construction.has_outputs() {

        let mut made_progress = false;
        for unfinished in unfinished_operations.drain(..) {
            let mut ctx = DiagramContext {
                construction: &mut construction,
                registry,
                operations: &unfinished.sibling_ops,
                templates: &diagram.templates,
                on_implicit_error,
                namespaces: unfinished.namespaces.clone(),
            };

            // Attempt to build this operation
            let status = unfinished
                .op
                .build_diagram_operation(&unfinished.id, builder, &mut ctx)
                .map_err(|code| DiagramError::in_operation(unfinished.as_operation_ref(), code))?;

            ctx.construction.transfer_generated_operations(&mut deferred_operations, &mut made_progress);

            made_progress |= status.made_progress();
            if !status.is_finished() {
                // The operation did not finish, so pass it into the deferred
                // operations list.
                deferred_operations.push(DeferredOperation { unfinished, status });
            }
        }

        unfinished_operations.extend(
            deferred_operations
            .drain(..)
            .map(|deferred| deferred.unfinished)
        );

        // Transfer outputs into their connections. Sometimes this needs to be
        // done before other operations can be built, e.g. a connection may need
        // to be redirected before its target operation knows how to infer its
        // message type.
        connector_construction.buffers = construction.buffers.clone();
        loop {
            for (id, outputs) in construction.outputs_into_target.drain() {
                let mut ctx = DiagramContext {
                    construction: &mut connector_construction,
                    registry,
                    operations: &diagram.ops,
                    templates: &diagram.templates,
                    on_implicit_error,
                    namespaces: Default::default(),
                };

                let Some(connect) = construction.connect_into_target.get_mut(&id) else {
                    if unfinished_operations.is_empty() {
                        return Err(DiagramErrorCode::UnknownOperation(id.into()).into());
                    } else {
                        deferred_connections.insert(id, outputs);
                        continue;
                    }
                };

                for output in outputs {
                    made_progress = true;
                    connect.connect_into_target(output, builder, &mut ctx)?;
                }
            }

            let new_connections = !connector_construction.outputs_into_target.is_empty();

            construction
                .outputs_into_target
                .extend(connector_construction.outputs_into_target.drain());

            construction
                .outputs_into_target
                .extend(deferred_connections.drain());

            connector_construction
                .transfer_generated_operations(&mut deferred_operations, &mut made_progress);

            unfinished_operations.extend(
                deferred_operations
                .drain(..)
                .map(|deferred| deferred.unfinished)
            );

            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return Err(DiagramErrorCode::ExcessiveIterations.into());
            }

            if !new_connections {
                break;
            }
        }

        if !made_progress {
            // No progress can be made any longer so return an error
            return Err(DiagramErrorCode::BuildHalted {
                reasons: deferred_operations
                    .drain(..)
                    .filter_map(|deferred| {
                        deferred
                            .status
                            .into_deferral_reason()
                            .map(|reason| (deferred.unfinished.as_operation_ref(), reason))
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

    Ok(())
}

pub struct UnfinishedOperation<'a> {
    /// Name of the operation within its scope
    pub id: OperationName,
    /// The namespaces that this operation takes place inside
    pub namespaces: NamespaceList,
    /// Description of the operation
    pub op: &'a DiagramOperation,
    /// The sibling operations of the one that is being built
    pub sibling_ops: &'a Operations,
}

impl<'a> std::fmt::Debug for UnfinishedOperation<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnfinishedOperation")
            .field("id", &self.id)
            .field("namespaces", &self.namespaces)
            .finish()
    }
}

impl<'a> UnfinishedOperation<'a> {
    pub fn new(id: OperationName, op: &'a DiagramOperation, sibling_ops: &'a Operations) -> Self {
        Self {
            id,
            op,
            sibling_ops,
            namespaces: Default::default(),
        }
    }

    pub fn as_operation_ref(&self) -> OperationRef {
        NamedOperationRef {
            namespaces: self.namespaces.clone(),
            exposed_namespace: None,
            name: self.id.clone(),
        }
        .into()
    }
}

struct DeferredOperation<'a> {
    unfinished: UnfinishedOperation<'a>,
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
    ctx.add_output_into_target(&start, scope.input.into());

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
        Box::new(ConnectToDispose),
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

struct ConnectToDispose;

impl ConnectIntoTarget for ConnectToDispose {
    fn connect_into_target(
        &mut self,
        _output: DynOutput,
        _builder: &mut Builder,
        _ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        Ok(())
    }

    fn infer_input_type(
        &self,
        _ctx: &DiagramContext,
        _visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        Ok(Some(Arc::new(ConnectToDispose)))
    }
}

impl InferMessageType for ConnectToDispose {
    fn preferred_input_type(&self) -> Option<TypeInfo> {
        None
    }
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

    Ok(Box::new(BasicConnect {
        input_slot: Arc::new(input_slot),
    }))
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

    fn infer_input_type(
        &self,
        _ctx: &DiagramContext,
        _visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        let infer = Arc::clone(self.serialized_input_slot());
        Ok(Some(infer))
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

    fn infer_input_type(
        &self,
        _ctx: &DiagramContext,
        _visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        let infer = Arc::clone(self.deserialized_input_slot());
        Ok(Some(infer))
    }
}

struct BasicConnect {
    input_slot: Arc<DynInputSlot>,
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

    fn infer_input_type(
        &self,
        _ctx: &DiagramContext,
        _visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        let infer = Arc::clone(&self.input_slot);
        Ok(Some(infer))
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

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        self.implicit_serialization.infer_input_type(ctx, visited)
    }
}

impl InferMessageType for DynInputSlot {
    fn preferred_input_type(&self) -> Option<TypeInfo> {
        Some(*self.message_info())
    }
}

#[derive(Debug)]
struct RedirectConnection {
    redirect_to: OperationRef,
    /// Keep track of which DynOutputs have been redirected in the past so we
    /// can identify when a circular redirection is happening.
    redirected: HashSet<Entity>,
}

impl RedirectConnection {
    fn new(redirect_to: OperationRef) -> Self {
        Self {
            redirect_to,
            redirected: Default::default(),
        }
    }
}

impl ConnectIntoTarget for RedirectConnection {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        _builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if self.redirected.insert(output.id()) {
            // This DynOutput has not been redirected by this connector yet, so
            // we should go ahead and redirect it.
            ctx.add_output_into_target(self.redirect_to.clone(), output);
        } else {
            // This DynOutput has been redirected by this connector before, so
            // we have a circular connection, making it impossible for the
            // output to ever really be connected to anything.
            return Err(DiagramErrorCode::CircularRedirect(
                vec![self.redirect_to.clone()]
            ));
        }
        Ok(())
    }

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        if visited.insert(self.redirect_to.clone()) {
            if let Some(connect) = ctx.construction.connect_into_target.get(&self.redirect_to) {
                return connect.infer_input_type(ctx, visited);
            } else {
                return Ok(None);
            }
        } else {
            return Err(DiagramErrorCode::CircularRedirect(
                visited.drain().collect(),
            ));
        }
    }
}

impl<'a, 'c> std::fmt::Debug for DiagramContext<'a, 'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("DiagramContext")
            .field("construction", &DebugDiagramConstruction(self))
            .finish()
    }
}

struct DebugDiagramConstruction<'a, 'c, 'd>(&'d DiagramContext<'a, 'c>);

impl<'a, 'c, 'd> std::fmt::Debug for DebugDiagramConstruction<'a, 'c, 'd> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("DiagramConstruction")
            .field("connect_into_target", &DebugConnections(self.0))
            .field("outputs_into_target", &self.0.construction.outputs_into_target)
            .field("buffers", &self.0.construction.buffers)
            .field("generated_operations", &self.0.construction.generated_operations)
            .finish()
    }
}

struct DebugConnections<'a, 'c, 'd>(&'d DiagramContext<'a, 'c>);

impl<'a, 'c, 'd> std::fmt::Debug for DebugConnections<'a, 'c, 'd> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_map();
        for (op, connect) in &self.0.construction.connect_into_target {
            debug.entry(op, &DebugConnection { connect, context: self.0 });
        }

        debug.finish()
    }
}

struct DebugConnection<'a, 'c, 'd> {
    connect: &'d Box<dyn ConnectIntoTarget>,
    context: &'d DiagramContext<'a, 'c>,
}

impl<'a, 'c, 'd> std::fmt::Debug for DebugConnection<'a, 'c, 'd> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut visited = HashSet::new();
        let mut debug = f.debug_struct("ConnectIntoTarget");
        match self.connect.infer_input_type(self.context, &mut visited) {
            Ok(ok) => {
                let inferred = ok.map(|infer| infer.preferred_input_type()).flatten();
                debug.field("inferred type", &inferred);
            }
            Err(err) => {
                debug.field("infered type error", &err);
            }
        }

        debug.finish()
    }
}
