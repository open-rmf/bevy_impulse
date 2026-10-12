/*
 * Copyright (C) 2026 Open Source Robotics Foundation
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

use smallvec::{SmallVec, smallvec};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cell::Cell,
    collections::{HashMap, HashSet, VecDeque},
    ops::Deref,
    sync::Arc,
};

use crate::{
    BufferMapLayoutHints, BufferSelection, BuildDiagramOperation, Diagram, DiagramContext,
    DiagramElementRegistry, DiagramError, DiagramErrorCode, IdentifierRef, IncompatibleLayout,
    MessageTypeHint, MetadataAccess, NamedOutputRef, NamespaceList, NamespacedOperation,
    NextOperation, NodeSchema, OperationName, OperationRef, Operations, OutputRef, ScopeSchema,
    ScriptSchema, SectionError, SectionProvider, SectionSchema, StreamAvailability, StreamPack,
    WithContext, output_ref,
};

pub type InferredMessageTypes = HashMap<PortRef, usize>;

/// Specify the message types of a workflow's boundary operations.
pub struct InferenceBoundaryConditions {
    /// The message type for requests into the workflow
    pub request: usize,
    /// The message type for responses from the workflow
    pub response: usize,
    /// The message types of the streams for this workflow.
    pub streams: HashMap<String, usize>,
}

impl InferenceBoundaryConditions {
    pub fn new<Request, Response, Streams>(
        registry: &DiagramElementRegistry,
    ) -> Result<Self, DiagramErrorCode>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let request = registry.messages.registration.get_index::<Request>()?;
        let response = registry.messages.registration.get_index::<Response>()?;

        let mut streams = HashMap::new();
        let mut stream_availability = StreamAvailability::default();
        Streams::set_stream_availability(&mut stream_availability);
        for (name, stream_type) in stream_availability.named_streams() {
            let stream_msg_index = registry.messages.registration.get_index_dyn(&stream_type)?;
            streams.insert(name.to_string(), stream_msg_index);
        }

        Ok(Self {
            request,
            response,
            streams,
        })
    }

    pub fn json_messages(
        lookup: &dyn MetadataAccess,
        stream_names: impl IntoIterator<Item = String>,
    ) -> Result<Self, DiagramErrorCode> {
        let json_message_index = lookup.json_message_index()?;
        let streams = stream_names
            .into_iter()
            .map(|name| (name, json_message_index))
            .collect();
        Ok(Self {
            request: json_message_index,
            response: json_message_index,
            streams,
        })
    }
}

impl Diagram {
    pub fn infer_message_types(
        &self,
        lookup: &dyn MetadataAccess,
        boundary: InferenceBoundaryConditions,
    ) -> Result<InferredMessageTypes, DiagramError> {
        let inferences = self.evaluate_message_type_inferences(lookup, boundary, false)?;
        Ok(inferences.try_infer_types()?)
    }

    /// Infer message types only for the requested ports.
    ///
    /// This is intended for editor hints where the diagram may be in a
    /// partially edited state. Unlike [`Self::infer_message_types`], unrelated
    /// ports that cannot yet be inferred do not make this fail.
    pub fn infer_message_types_for_ports(
        &self,
        lookup: &dyn MetadataAccess,
        boundary: InferenceBoundaryConditions,
        ports: impl IntoIterator<Item = PortRef>,
    ) -> Result<InferredMessageTypes, DiagramError> {
        let inferences = self.evaluate_message_type_inferences(lookup, boundary, false)?;
        let mut inferred = InferredMessageTypes::new();
        for port in ports {
            let evaluation = inferences.get_evaluation(&port).in_port(|| port.clone())?;
            let Some(message_type) = evaluation.message_type else {
                return Err(DiagramErrorCode::CannotInferType(port.clone()).in_port(port));
            };
            inferred.insert(port, message_type);
        }

        Ok(inferred)
    }

    /// Evaluate a partially edited diagram once, then infer each requested port independently.
    ///
    /// Each successful result includes whether its type is determined by resolved dependencies.
    /// Template outputs return one result per internal producer, which may have different types.
    /// Scope outputs use their boundary types, after the scope's conversions.
    /// All other ports return one result, as do template outputs without any producers.
    /// A port error does not prevent the remaining ports from being inferred.
    pub fn infer_message_types_for_ports_individually(
        &self,
        lookup: &dyn MetadataAccess,
        boundary: InferenceBoundaryConditions,
        ports: impl IntoIterator<Item = PortRef>,
    ) -> Result<HashMap<PortRef, Vec<Result<(usize, bool), DiagramError>>>, DiagramError> {
        let inferences = self.evaluate_message_type_inferences(lookup, boundary, true)?;
        let context = ConstraintContext {
            inferences: &inferences,
            metadata: lookup,
            certain: None,
        };
        // A template forwards producers, but a scope first converts them at its boundary.
        let scope_boundaries: HashMap<_, _> = inferences
            .scope_outputs
            .iter()
            .map(|(output, input)| (input, output))
            .collect();
        Ok(ports
            .into_iter()
            .map(|port| {
                let template_output = match &port {
                    PortRef::Output(output) => inferences.template_outputs.get(output),
                    PortRef::Input(_) => None,
                };
                let inferred = if let Some(output) = template_output {
                    let mut producers = context.connections_into_with_boundary(output, |input| {
                        scope_boundaries.get(input).map(|output| (*output).clone())
                    });
                    producers.sort();
                    producers.dedup();
                    if producers.is_empty() {
                        vec![Err(
                            DiagramErrorCode::NoConnection(output.clone()).in_port(port.clone())
                        )]
                    } else {
                        producers
                            .into_iter()
                            .map(|producer| inferences.infer_port(&producer.into(), lookup))
                            .collect()
                    }
                } else {
                    vec![inferences.infer_port(&port, lookup)]
                };
                (port, inferred)
            })
            .collect())
    }

    fn evaluate_message_type_inferences(
        &self,
        lookup: &dyn MetadataAccess,
        boundary: InferenceBoundaryConditions,
        partial: bool,
    ) -> Result<Inferences, DiagramError> {
        if !partial {
            self.validate_operation_names()?;
            self.validate_template_usage()?;
        }

        let root_on_implicit_error: OperationRef = (&self.on_implicit_error()).into();

        let mut inferences = Inferences {
            partial,
            ..Default::default()
        };

        let mut unfinished_operations: Vec<UnfinishedOperation> = self
            .ops
            .iter()
            .map(|(id, op)| {
                UnfinishedOperation::new(
                    id,
                    op.clone() as Arc<dyn BuildDiagramOperation>,
                    &self.ops,
                    root_on_implicit_error.clone(),
                )
            })
            .collect();
        let mut generated_operations = Vec::new();

        while !unfinished_operations.is_empty() {
            for unfinished in unfinished_operations.drain(..) {
                let mut ctx = InferenceContext {
                    inference: &mut inferences,
                    diagram_context: DiagramContext {
                        operations: unfinished.sibling_ops.clone(),
                        templates: &self.templates,
                        script_environments: &self.script_environments,
                        on_implicit_error: &unfinished.on_implicit_error,
                        default_trace: self.default_trace,
                        namespaces: unfinished.namespaces.clone(),
                    },
                    metadata: lookup,
                    generated_operations: &mut generated_operations,
                };

                let result = super::validate_operation_name(&unfinished.id).and_then(|_| {
                    unfinished
                        .op
                        .apply_message_type_constraints(&unfinished.id, &mut ctx)
                });
                if let Err(error) = result {
                    let operation =
                        OperationRef::from(&unfinished.id).in_namespaces(&unfinished.namespaces);
                    if !partial {
                        return Err(error.in_port(operation));
                    }
                    inferences.setup_errors.insert(operation, error);
                }
            }

            unfinished_operations.extend(generated_operations.drain(..));
        }

        // Test for circular redirections, which are impossible to solve for and
        // also logically unsound.
        for redirect_from in inferences.redirected_input.keys() {
            let mut next = Some(redirect_from);
            let mut visited = Vec::new();

            while let Some(top) = next.take() {
                let circular = visited.contains(&top);
                visited.push(top);

                if circular {
                    let error = DiagramErrorCode::CircularRedirect(
                        visited.iter().map(|op| (*op).clone()).collect(),
                    );
                    if !partial {
                        return Err(error.into());
                    }
                    for operation in visited {
                        inferences
                            .setup_errors
                            .insert(operation.clone(), error.clone());
                        if let Some(owner) = port_operation(&operation.clone().into()) {
                            inferences.setup_errors.insert(owner, error.clone());
                        }
                    }
                    break;
                }

                next = inferences.redirected_input.get(top);
            }
        }

        set_boundary_conditions(&mut inferences, self, lookup, boundary)?;

        let dependents = {
            let mut dependents = HashMap::<_, Vec<PortRef>>::new();
            for (id, inference) in &inferences.evaluations {
                if let Some(constraint) = &inference.constraint {
                    let ctx = ConstraintContext {
                        inferences: &inferences,
                        metadata: lookup,
                        certain: None,
                    };
                    let dependencies = constraint.dependencies(&ctx);
                    for dependency in dependencies {
                        dependents.entry(dependency).or_default().push(id.clone());
                    }
                }
            }

            dependents
        };

        let mut queue = VecDeque::new();
        for port in inferences.evaluations.keys() {
            queue.push_back(port.clone());
        }

        while let Some(port) = queue.pop_front() {
            // Setup failures are exposed to dependents and queries through get_evaluation;
            // they must not abort this queue if the failed operation registered ports.
            let evaluation = &inferences.evaluations[&port];
            let (result, mut certain) = evaluation.evaluate(&inferences, lookup);
            let (message_type, error) = match result {
                Ok(None) => {
                    // Retain only a provisional hint, so feedback cannot keep
                    // erasing and recreating the same inferred type.
                    certain = false;
                    (evaluation.message_type, None)
                }
                Ok(message_type) => (message_type, None),
                Err(error) if partial => (None, Some(error)),
                Err(error) => return Err(error.in_port(port)),
            };
            // Editor inference must settle before reporting errors: downstream
            // hints may be superseded by upstream types later in this queue.
            if (partial || message_type.is_some())
                && (message_type != evaluation.message_type
                    || certain != evaluation.certain
                    || error.is_some() != evaluation.error.is_some())
            {
                if let Some(deps) = dependents.get(&port) {
                    for dep in deps {
                        if !queue.contains(dep) {
                            queue.push_back(dep.clone());
                        }
                    }
                }
            }
            let evaluation = inferences.evaluation(port);
            if partial || message_type.is_some() {
                evaluation.message_type = message_type;
            }
            evaluation.error = error;
            evaluation.certain = certain;
        }

        Ok(inferences)
    }
}

fn set_boundary_conditions(
    inferences: &mut Inferences,
    diagram: &Diagram,
    registry: &dyn MetadataAccess,
    boundary: InferenceBoundaryConditions,
) -> Result<(), DiagramErrorCode> {
    let root_on_implicit_error: OperationRef = (&diagram.on_implicit_error()).into();
    let mut generated_operations = Vec::new();

    let mut ctx = InferenceContext {
        inference: inferences,
        diagram_context: DiagramContext {
            operations: diagram.ops.clone(),
            templates: &diagram.templates,
            script_environments: &diagram.script_environments,
            on_implicit_error: &root_on_implicit_error,
            default_trace: diagram.default_trace,
            namespaces: Default::default(),
        },
        metadata: registry,
        generated_operations: &mut generated_operations,
    };

    // Add constraints for the start operation
    ctx.connect(OutputRef::start(), ctx.into_operation_ref(&diagram.start));
    let start = ctx.into_port_ref(OutputRef::start());
    ctx.fixed(start, boundary.request);

    // Add constraints for the terminate operation
    let terminate = ctx.into_port_ref(OperationRef::Terminate(Default::default()));
    ctx.fixed(terminate, boundary.response);

    for (name, stream_type) in boundary.streams {
        let stream = ctx.into_port_ref(OperationRef::stream_out(&Arc::from(name)));
        ctx.fixed(stream, stream_type);
    }

    Ok(())
}

pub struct InferenceContext<'a, 'b> {
    inference: &'b mut Inferences,
    diagram_context: DiagramContext<'a>,
    pub metadata: &'a dyn MetadataAccess,
    generated_operations: &'b mut Vec<UnfinishedOperation>,
}

impl<'a, 'b> InferenceContext<'a, 'b> {
    /// Specify exactly what message types a port may have, irrespective of
    /// any connections to other operations.
    fn fixed(&mut self, port: PortRef, message_type: usize) {
        let evaluation = self.inference.evaluation(port);
        evaluation.message_type = Some(message_type);
        evaluation.certain = true;
    }

    /// Specify that the message type of an output should be directly inferred
    /// from the message type of the input that it connects into.
    fn infer_from_downstream(&mut self, output: OutputRef, input: OperationRef) {
        self.inference
            .constrain(output.clone(), ExactMatch(input.clone().into()));
        self.connect(output, input);
    }

    /// Specify that an output connects into an input.
    fn connect(&mut self, output: OutputRef, input: OperationRef) {
        self.inference
            .connection_from
            .insert(output.clone(), input.clone());
        self.inference
            .connections_into
            .entry(input)
            .or_default()
            .insert(output);
    }

    /// Specify that an operation simply redirects its inputs into another operation
    fn redirect(&mut self, from: OperationRef, into: OperationRef) {
        self.inference
            .constrain(from.clone(), ExactMatch(into.clone().into()));
        self.inference
            .redirected_input
            .insert(from.clone(), into.clone());
        self.inference
            .redirections_into
            .entry(into)
            .or_default()
            .insert(from);
    }

    pub fn node(
        &mut self,
        operation_name: &OperationName,
        schema: &NodeSchema,
    ) -> Result<(), DiagramErrorCode> {
        let node = self.metadata.node_metadata(&schema.builder)?;
        let input = self.into_operation_ref(operation_name);

        // Set the exact message type of the input port
        self.fixed(input.into(), node.request());

        // Set the exact message type of the output port, and connect it to the
        // next operation.
        let output = self.into_output_ref(output_ref(operation_name).next());
        let target = self.into_operation_ref(&schema.next);
        self.fixed(output.clone().into(), node.response());
        self.connect(output, target);

        // Set the exact message type of each stream output.
        for (stream_id, stream_type) in node.streams() {
            let stream = self.into_output_ref(output_ref(operation_name).stream_out(stream_id));
            self.fixed(stream.into(), *stream_type);
        }

        // Connect each stream output to its target operation.
        for (stream_id, stream_target) in &schema.stream_out {
            let stream = self.into_output_ref(output_ref(operation_name).stream_out(stream_id));
            let stream_target = self.into_operation_ref(stream_target);
            self.connect(stream, stream_target);
        }

        Ok(())
    }

    pub fn section(
        &mut self,
        id: &OperationName,
        schema: &SectionSchema,
    ) -> Result<(), DiagramErrorCode> {
        match &schema.provider {
            SectionProvider::Builder(section_builder) => {
                let metadata = &self.metadata.section_metadata(section_builder)?;

                for (input_name, input_metadata) in &metadata.interface.inputs {
                    let op = NextOperation::Namespace(NamespacedOperation {
                        namespace: id.clone(),
                        operation: input_name.clone(),
                    });
                    let op = self.into_port_ref(&op);

                    self.fixed(op, input_metadata.message_type);
                }

                for (buffer_name, buffer_metadata) in &metadata.interface.buffers {
                    let op = NextOperation::Namespace(NamespacedOperation {
                        namespace: id.clone(),
                        operation: buffer_name.clone(),
                    });
                    let op = self.into_operation_ref(&op);
                    self.inference.buffer_inputs.insert(op.clone());

                    if let Some(buffer_message_type) = buffer_metadata.message_type {
                        self.fixed(op.into(), buffer_message_type);
                    }
                }

                for (output_name, output_metadata) in &metadata.interface.outputs {
                    let op = self.into_port_ref(output_ref(id).section_output(output_name));
                    self.fixed(op, output_metadata.message_type);
                }

                for (output_name, next) in &schema.connect {
                    let op = self.into_output_ref(output_ref(id).section_output(output_name));
                    let target = self.into_operation_ref(next);
                    self.connect(op, target);
                }
            }
            SectionProvider::Template(section_template) => {
                if self.inference.partial {
                    // Validate before expanding so an invalid template remains local and
                    // recursive templates cannot produce an endless editor inference loop.
                    self.templates.validate_template(section_template)?;
                }
                let section = self.templates.get_template(section_template)?;

                for expected_output in schema.connect.keys() {
                    if !section.outputs.contains(expected_output) {
                        return Err(SectionError::UnknownOutput(Arc::clone(expected_output)).into());
                    }
                }

                for (child_id, op) in section.ops.iter() {
                    self.add_child_operation(id, child_id, op, section.ops.clone(), None);
                }

                section.inputs.redirect(|op, next| {
                    let op = self.into_operation_ref(OperationRef::exposed_input(id, op));
                    let next = self.into_operation_ref(next.in_namespace(id));
                    self.redirect(op, next);
                    Ok(())
                })?;

                section.buffers.redirect(|op, next| {
                    let op = self.into_operation_ref(OperationRef::exposed_input(id, op));
                    self.inference.buffer_inputs.insert(op.clone());
                    let next = self.into_operation_ref(next.in_namespace(id));
                    self.redirect(op, next);
                    Ok(())
                })?;

                for output in &section.outputs {
                    let internal_output = self.into_operation_ref(
                        NextOperation::Name(Arc::clone(output)).in_namespace(id),
                    );
                    let public_output = self.into_output_ref(output_ref(id).section_output(output));
                    self.inference
                        .template_outputs
                        .insert(public_output, internal_output.clone());
                    if let Some(target) = schema.connect.get(output) {
                        let target = self.into_operation_ref(target);
                        self.redirect(internal_output, target);
                    }
                }
            }
        }

        let inner_terminate = self.into_operation_ref(OperationRef::terminate_for(id));
        let outer_terminate = self.into_operation_ref(&NextOperation::terminate());
        self.redirect(inner_terminate, outer_terminate);

        Ok(())
    }

    pub fn scope(&mut self, id: &OperationName, schema: &ScopeSchema) {
        let operation = self.into_operation_ref(id);
        let start = self.into_output_ref(OutputRef::start().in_namespaces(&[Arc::clone(id)]));
        self.inference
            .scope_outputs
            .insert(start, operation.clone());

        // The request type of this scope must exactly match the request type
        // of the starting operation.
        let start_target =
            self.into_operation_ref(OperationRef::from(&schema.start).in_namespace(id));
        self.redirect(operation, start_target);

        for (stream_name, stream_target) in &schema.stream_out {
            let stream = self.into_operation_ref(OperationRef::scope_stream_out(id, stream_name));
            let output = self.into_output_ref(output_ref(id).stream_out(stream_name));
            self.inference.scope_outputs.insert(output, stream.clone());
            let stream_target = self.into_operation_ref(stream_target);
            self.redirect(stream, stream_target);
        }

        // The terminating message type of this scope must exactly match the
        // request type of the next operation that the scope is connected to.
        let terminate = self.into_operation_ref(OperationRef::terminate_for(id));
        let output = self.into_output_ref(output_ref(id).next());
        self.inference
            .scope_outputs
            .insert(output, terminate.clone());
        let terminate_target = self.into_operation_ref(&schema.next);
        self.redirect(terminate, terminate_target);

        for (child_id, op) in schema.ops.iter() {
            self.add_child_operation(
                id,
                child_id,
                op,
                schema.ops.clone(),
                Some(schema.on_implicit_error()),
            );
        }
    }

    pub fn transform(
        &mut self,
        operation_name: &OperationName,
        next: &NextOperation,
    ) -> Result<(), DiagramErrorCode> {
        let json_message_index = self.metadata.json_message_index()?;
        let operation = self.into_operation_ref(operation_name);
        let output = self.into_output_ref(output_ref(operation_name).next());
        let target = self.into_operation_ref(next);

        self.fixed(operation.into(), json_message_index);
        self.fixed(output.clone().into(), json_message_index);
        self.connect(output, target);

        Ok(())
    }

    pub fn stream_out(&mut self, operation_name: &OperationName, stream_name: &OperationName) {
        let operation = self.into_operation_ref(operation_name);
        let stream = self.into_operation_ref(OperationRef::stream_out(stream_name));
        self.inference
            .constrain(operation, ExactMatch(stream.into()));
    }

    pub fn fork_clone(&mut self, operation_name: &OperationName, next: &[NextOperation]) {
        let operation = self.into_operation_ref(operation_name);

        let mut targets = Vec::new();
        for (i, target) in next.iter().enumerate() {
            let target = self.into_operation_ref(target);
            targets.push(target.clone());

            let output = self.into_output_ref(output_ref(operation_name).next_index(i));
            self.connect(output.clone(), target);
            self.inference
                .constrain(output, ExactMatch(operation.clone().into()));
        }

        self.inference
            .constrain(operation.clone(), CloneInput { operation, targets });
    }

    pub fn result(
        &mut self,
        operation_name: &OperationName,
        ok: &NextOperation,
        err: &NextOperation,
    ) {
        let operation = self.into_operation_ref(operation_name);
        let ok_output = self.into_output_ref(output_ref(operation_name).ok());
        let err_output = self.into_output_ref(output_ref(operation_name).err());

        self.inference
            .constrain(ok_output.clone(), OkFrom(operation.clone()));
        self.inference
            .constrain(err_output.clone(), ErrFrom(operation.clone()));

        let ok_target = self.into_operation_ref(ok);
        let err_target = self.into_operation_ref(err);
        self.inference.constrain(
            operation.clone(),
            ResultInto {
                operation,
                ok: ok_target.clone(),
                err: err_target.clone(),
            },
        );

        self.connect(ok_output, ok_target);
        self.connect(err_output, err_target);
    }

    pub fn unzip<'u>(
        &mut self,
        unzippable_name: &OperationName,
        targets: impl Iterator<Item = &'u NextOperation>,
    ) {
        let unzippable = self.into_operation_ref(unzippable_name);
        for (i, target) in targets.enumerate() {
            let element_output = self.into_output_ref(output_ref(unzippable_name).next_index(i));

            self.inference.constrain(
                element_output.clone(),
                UnzipOutput {
                    op: unzippable.clone(),
                    element: i,
                },
            );

            let target = self.into_operation_ref(target);
            self.connect(element_output, target);
        }

        self.inference
            .constrain(unzippable.clone(), UnzipInput(unzippable));
    }

    pub fn buffer(
        &mut self,
        operation_name: &OperationName,
        serialize: bool,
    ) -> Result<(), DiagramErrorCode> {
        let operation = self.into_operation_ref(operation_name);
        self.inference.buffer_inputs.insert(operation.clone());
        if serialize {
            let json_index = self.metadata.json_message_index()?;
            self.fixed(operation.into(), json_index);
        } else {
            self.inference
                .constrain(operation.clone(), BufferInput(operation));
        }

        Ok(())
    }

    pub fn join(
        &mut self,
        operation_name: &OperationName,
        selection: &BufferSelection,
        next: &NextOperation,
        serialize: bool,
        cloned: &[IdentifierRef<'static>],
    ) -> Result<(), DiagramErrorCode> {
        self.buffer_consumer(operation_name, selection, BufferConsumerKind::Join, cloned);
        let output = self.into_output_ref(output_ref(operation_name).next());
        let target = self.into_operation_ref(next);
        let evaluate = |context: &ConstraintContext, msg: usize, member: &IdentifierRef| {
            let layout = &context.metadata.join_layout(msg)?;
            evaluate_buffer_hint(layout, member)
        };

        for (member, buffer) in selection.iter() {
            let buffer = self.into_operation_ref(buffer);
            self.inference
                .buffer_hints
                .entry(buffer)
                .or_default()
                .push(BufferInference {
                    used_by: output.clone().into(),
                    member: member.to_owned(),
                    evaluate,
                });
        }

        if serialize {
            let json_message_index = self.metadata.json_message_index()?;
            self.fixed(output.clone().into(), json_message_index);
            self.connect(output, target);
        } else {
            self.infer_from_downstream(output, target);

            // TODO(@mxgrey): Consider applying a reverse constraint: inferring the
            // Joined type based on its constituent buffers. Unfortunately the Joined
            // type is ambiguous when basing it only on the parts that are being
            // joined into it, even accounting for the layout. That makes the
            // reverse inference both weak and expensive to calculate, so we skip it
            // for now.
        }
        Ok(())
    }

    pub fn buffer_access(
        &mut self,
        operation_name: &OperationName,
        selection: &BufferSelection,
        next: &NextOperation,
    ) {
        self.buffer_consumer(operation_name, selection, BufferConsumerKind::Access, &[]);
        let operation = self.into_operation_ref(operation_name);
        let output = self.into_output_ref(output_ref(operation_name).next());
        let target = self.into_operation_ref(next);
        let evaluate = |context: &ConstraintContext, msg: usize, member: &IdentifierRef| {
            let layout = context.metadata.buffer_access_layout(msg)?;
            evaluate_buffer_hint(layout, member)
        };

        for (member, buffer) in selection.iter() {
            let buffer = self.into_operation_ref(buffer);
            self.inference
                .buffer_hints
                .entry(buffer.into())
                .or_default()
                .push(BufferInference {
                    used_by: output.clone().into(),
                    member: member.to_owned(),
                    evaluate,
                });
        }

        self.infer_from_downstream(output, target.clone());
        self.inference
            .constrain(operation, BufferAccessInput { target });
    }

    pub fn listen(
        &mut self,
        operation_name: &OperationName,
        selection: &BufferSelection,
        next: &NextOperation,
    ) {
        self.buffer_consumer(operation_name, selection, BufferConsumerKind::Listen, &[]);
        let output = self.into_output_ref(output_ref(operation_name).next());
        let target = self.into_operation_ref(next);
        let evaluate = |context: &ConstraintContext, msg: usize, member: &IdentifierRef| {
            let layout = context.metadata.listen_layout(msg)?;
            evaluate_buffer_hint(layout, member)
        };

        for (member, buffer) in selection.iter() {
            let buffer = self.into_operation_ref(buffer);
            self.inference
                .buffer_hints
                .entry(buffer)
                .or_default()
                .push(BufferInference {
                    used_by: output.clone().into(),
                    member: member.to_owned(),
                    evaluate,
                });
        }

        self.infer_from_downstream(output, target);
    }

    fn buffer_consumer(
        &mut self,
        operation_name: &OperationName,
        selection: &BufferSelection,
        kind: BufferConsumerKind,
        cloned: &[IdentifierRef<'static>],
    ) {
        if !self.inference.partial {
            return;
        }
        let operation = self.into_operation_ref(operation_name);
        let output = self.into_port_ref(output_ref(operation_name).next());
        let buffers = selection
            .iter()
            .map(|(member, buffer)| (member.to_owned(), self.into_operation_ref(buffer)))
            .collect();
        self.inference.buffer_consumers.insert(
            operation,
            BufferConsumer {
                output,
                buffers,
                kind,
                cloned: cloned.to_vec(),
            },
        );
    }

    pub fn split<'s>(
        &mut self,
        split_name: &OperationName,
        sequential: &Vec<NextOperation>,
        keyed: &HashMap<OperationName, NextOperation>,
        remaining: &Option<NextOperation>,
    ) {
        let split = self.into_operation_ref(split_name);

        for (i, target) in sequential.iter().enumerate() {
            let output = self.into_output_ref(output_ref(split_name).next_index(i));

            self.inference
                .constrain(output.clone(), SplitOutput(split.clone()));

            let target = self.into_operation_ref(target);
            self.connect(output, target);
        }

        for (key, target) in keyed {
            let output = self.into_output_ref(output_ref(split_name).keyed(key));

            self.inference
                .constrain(output.clone(), SplitOutput(split.clone()));

            let target = self.into_operation_ref(target);
            self.connect(output, target);
        }

        if let Some(target) = remaining {
            let output = self.into_output_ref(output_ref(split_name).remaining());

            self.inference
                .constrain(output.clone(), SplitOutput(split.clone()));

            let target = self.into_operation_ref(target);
            self.connect(output, target);
        }

        self.inference.constrain(split.clone(), SplitInput(split));
    }

    pub fn script(
        &mut self,
        operation_name: &OperationName,
        schema: &ScriptSchema,
    ) -> Result<(), DiagramErrorCode> {
        let script_message_index = self.metadata.script_message_index()?;

        let input = self.into_operation_ref(operation_name);
        self.fixed(input.into(), script_message_index);

        let output = self.into_output_ref(output_ref(operation_name).next());
        let target = self.into_operation_ref(&schema.next);
        self.fixed(output.clone().into(), script_message_index);
        self.connect(output, target);

        for (stream_id, stream_target) in &schema.stream_out {
            let stream = self.into_output_ref(output_ref(operation_name).stream_out(stream_id));
            self.fixed(stream.clone().into(), script_message_index);

            let stream_target = self.into_operation_ref(stream_target);
            self.connect(stream, stream_target);
        }

        Ok(())
    }

    /// Add an operation that exists as a child inside another operation.
    pub fn add_child_operation<T: BuildDiagramOperation + 'static>(
        &mut self,
        id: &OperationName,
        child_id: &OperationName,
        op: &Arc<T>,
        sibling_ops: Operations,
        on_implicit_error: Option<NextOperation>,
    ) {
        let mut namespaces = self.namespaces.clone();
        namespaces.push(Arc::clone(id));

        let on_implicit_error = match on_implicit_error {
            Some(op) => {
                let op: OperationRef = (&op).into();
                op.in_namespaces(&namespaces)
            }
            None => self.on_implicit_error.clone(),
        };

        self.generated_operations.push(UnfinishedOperation {
            id: Arc::clone(child_id),
            namespaces,
            op: op.clone() as Arc<dyn BuildDiagramOperation>,
            sibling_ops,
            on_implicit_error,
        })
    }
}

#[derive(Clone, Copy)]
pub struct ConstraintContext<'a> {
    inferences: &'a Inferences,
    pub metadata: &'a dyn MetadataAccess,
    certain: Option<&'a Cell<bool>>,
}

impl<'a> ConstraintContext<'a> {
    pub fn get_inference_of(
        &self,
        port: impl Into<PortRef>,
    ) -> Result<&Option<usize>, DiagramErrorCode> {
        let port = port.into();
        let evaluation = self.inferences.get_evaluation(&port);
        if let Some(certain) = self.certain {
            if !evaluation
                .as_ref()
                .is_ok_and(|e| e.certain && e.message_type.is_some() && e.error.is_none())
            {
                certain.set(false);
            }
        }
        let evaluation = evaluation?;
        if evaluation.error.is_some() {
            // A broken dependency leaves this port unresolved; its error does
            // not prove that a connection involving this port is incompatible.
            return Err(DiagramErrorCode::CannotInferType(port));
        }
        Ok(&evaluation.message_type)
    }

    pub fn connections_into(&self, operation: &OperationRef) -> SmallVec<[OutputRef; 8]> {
        self.connections_into_with_boundary(operation, |_| None)
    }

    fn connections_into_with_boundary(
        &self,
        operation: &OperationRef,
        boundary: impl Fn(&OperationRef) -> Option<OutputRef>,
    ) -> SmallVec<[OutputRef; 8]> {
        let mut connections = SmallVec::new();
        let mut queue = vec![operation];
        let mut visited = HashSet::new();
        while let Some(input) = queue.pop() {
            if !visited.insert(input) {
                continue;
            }
            if let Some(output) = boundary(input) {
                connections.push(output);
                continue;
            }
            if let Some(incoming) = self.inferences.connections_into.get(input) {
                connections.extend(incoming.iter().cloned());
            }
            if let Some(redirects) = self.inferences.redirections_into.get(input) {
                queue.extend(redirects);
            }
        }
        connections
    }

    fn get_message_types_into(
        &self,
        input: &OperationRef,
    ) -> Result<SmallVec<[usize; 8]>, DiagramErrorCode> {
        self.impl_get_message_types_into(input, true)
    }

    fn try_get_message_types_into(
        &self,
        input: &OperationRef,
    ) -> Result<SmallVec<[usize; 8]>, DiagramErrorCode> {
        self.impl_get_message_types_into(input, false)
    }

    fn impl_get_message_types_into(
        &self,
        input: &OperationRef,
        connection_necessary: bool,
    ) -> Result<SmallVec<[usize; 8]>, DiagramErrorCode> {
        let mut message_types = SmallVec::new();
        let mut input_queue: SmallVec<[&OperationRef; 8]> = SmallVec::new();
        input_queue.push(input);
        let mut visited = HashSet::new();

        while let Some(input) = input_queue.pop() {
            if !visited.insert(input) {
                return Err(DiagramErrorCode::CircularRedirect(
                    visited.into_iter().cloned().collect(),
                ));
            }

            let connections = self.inferences.connections_into.get(input);
            let redirections = self.inferences.redirections_into.get(input);

            let no_connections = connections.is_none_or(|c| c.is_empty());
            let no_redirections = redirections.is_none_or(|r| r.is_empty());
            if connection_necessary && no_connections && no_redirections {
                return Err(DiagramErrorCode::NoConnection(input.clone()));
            }

            if let Some(connections) = connections {
                for connection in connections {
                    if let Some(message_type) = self.get_inference_of(connection.clone())? {
                        message_types.push(*message_type);
                    }
                }
            }

            if let Some(redirections) = redirections {
                for redirect in redirections {
                    input_queue.push(redirect);
                }
            }
        }

        message_types.sort();
        message_types.dedup();
        Ok(message_types)
    }

    pub fn evaluate_clone_input(
        &self,
        operation: &OperationRef,
        targets: &[OperationRef],
    ) -> MessageTypeEvaluation {
        let incoming_message_types = self.get_message_types_into(operation)?;
        let selected_input_type = if incoming_message_types.is_empty() {
            // We don't have any upstream hints for the message type, so we
            // should attempt to infer something from our target message types.
            let Some(message_type) = self.same_output_type(targets)? else {
                return Ok(None);
            };

            message_type
        } else if incoming_message_types.len() == 1 {
            // There's exactly one incoming message type so we should settle on
            // that as the message type.

            // SAFETY: We already verified that this has a value
            *incoming_message_types.first().unwrap()
        } else {
            // We need to disambiguate the multiple incoming message types.
            if let Some(output_message_type) = self.same_output_type(targets)? {
                // Check if all incoming message types can be converted to the
                // output message type. If they can, use that type.
                let mut compatible = true;
                for incoming in &incoming_message_types {
                    compatible &= self.metadata.can_convert(*incoming, output_message_type)?;
                }

                if !compatible {
                    return Err(DiagramErrorCode::AmbiguousMessageType(
                        self.type_info_for_slice(&incoming_message_types)?,
                    ));
                }

                output_message_type
            } else {
                // There isn't a clear choice for the output message type, so we
                // should see if we can resolve the incoming types.
                for incoming in &incoming_message_types {
                    if !self.metadata.can_seralize(*incoming)? {
                        return Err(DiagramErrorCode::AmbiguousMessageType(
                            self.type_info_for_slice(&incoming_message_types)?,
                        ));
                    }
                }

                // These are all serializable, so we should choose JsonMessage
                // if it's an option.
                let Ok(json_index) = self.metadata.json_message_index() else {
                    return Err(DiagramErrorCode::AmbiguousMessageType(
                        self.type_info_for_slice(&incoming_message_types)?,
                    ));
                };

                // TODO(@mxgrey): We could consider finding a different single
                // common type that all incoming messages can be converted into,
                // but there's a significant risk of ambiguity for that.

                json_index
            }
        };

        if !self.metadata.can_clone(selected_input_type)? {
            return Err(DiagramErrorCode::NotCloneable(
                self.type_name_for(selected_input_type)?,
            ));
        }

        Ok(Some(selected_input_type))
    }

    /// Find a single output type that is compatible with all the target inputs,
    /// i.e. either the same message type being sent to all of them, or a single
    /// message type that can be converted to all of them.
    pub fn same_output_type(&self, targets: &[OperationRef]) -> MessageTypeEvaluation {
        let mut message_types: SmallVec<[usize; 8]> = Default::default();
        for target in targets {
            let port: PortRef = target.clone().into();
            if let Some(message_type) = self.get_inference_of(port)? {
                message_types.push(*message_type);
            }
        }

        message_types.sort();
        message_types.dedup();
        if message_types.len() <= 1 {
            return Ok(message_types.pop());
        }

        // There is more than one message type consider. If all of them can be
        // deserialized then we will use JsonMessage.
        let Ok(json_index) = self.metadata.json_message_index() else {
            // This is only meant to be a hint, so no need to call it an error.
            return Ok(None);
        };

        let script_index = self.metadata.script_message_index();

        let mut deserializable = true;
        let mut scriptable = true;
        let mut has_script_message = false;
        for message_type in message_types {
            if !self.metadata.can_deserialize(message_type)? {
                deserializable = false;
            }

            if !self.metadata.from_script_message(message_type)? {
                scriptable = false;
            }

            if script_index
                .as_ref()
                .is_ok_and(|index| *index == message_type)
            {
                has_script_message = true;
            }
        }

        if !has_script_message && deserializable {
            // All target types are deserializable, and none of them are a script
            // message, so choose JsonMessage.
            return Ok(Some(json_index));
        } else if scriptable && let Ok(script_index) = script_index {
            // All messages can be created from a script message.
            return Ok(Some(script_index));
        }

        Ok(None)
        // TODO(@mxgrey): We could consider finding a different single common
        // type that all target message types can be converted from, but there's
        // a signfiicant risk of ambiguity for that.
    }

    pub fn evaluate_result_input(
        &self,
        operation: &OperationRef,
        ok_target: &OperationRef,
        err_target: &OperationRef,
    ) -> MessageTypeEvaluation {
        let incoming_message_types = self.get_message_types_into(operation)?;
        let selected_input_type = if incoming_message_types.is_empty() {
            // We don't have any upstream hints for the message type, so we
            // should attempt to infer something from our target message types.
            let Some(ok_inference) = self.get_inference_of(ok_target.clone())? else {
                return Ok(None);
            };

            let Some(err_inference) = self.get_inference_of(err_target.clone())? else {
                return Ok(None);
            };

            let key = [*ok_inference, *err_inference];
            let Some(r) = self.metadata.reverse_lookup().result.get(&key) else {
                // We can't conclude anything yet.
                return Ok(None);
            };

            *r
        } else if incoming_message_types.len() == 1 {
            *incoming_message_types.first().unwrap()
        } else {
            return Ok(None);
        };

        self.metadata
            .fork_result_output_types(selected_input_type)?;

        Ok(Some(selected_input_type))
    }

    pub fn evaluate_ok_from(&self, from_result: &OperationRef) -> MessageTypeEvaluation {
        self.evaluate_result_output(from_result, 0)
    }

    pub fn evaluate_err_from(&self, from_result: &OperationRef) -> MessageTypeEvaluation {
        self.evaluate_result_output(from_result, 1)
    }

    fn evaluate_result_output(
        &self,
        from_result: &OperationRef,
        index: usize,
    ) -> MessageTypeEvaluation {
        let Some(result_inference) = self.get_inference_of(from_result.clone())? else {
            return Ok(None);
        };

        let output_types = self.metadata.fork_result_output_types(*result_inference)?;
        Ok(Some(output_types[index]))
    }

    pub fn evaluate_unzip_input(&self, operation: &OperationRef) -> MessageTypeEvaluation {
        let incoming_message_types = self.get_message_types_into(operation)?;
        if incoming_message_types.len() > 1 {
            return Err(DiagramErrorCode::AmbiguousMessageType(
                self.type_info_for_slice(&incoming_message_types)?,
            ));
        }

        let Some(unzip_msg) = incoming_message_types.first() else {
            return Ok(None);
        };

        self.metadata.unzip_output_types(*unzip_msg)?;
        Ok(Some(*unzip_msg))

        // TODO(@mxgrey): We could consider backing out the unzip type based on
        // the target unzipped message types, but this seems unnecessary for now.
        // Unzip is a relatively specialized operation at the moment.
    }

    pub fn evaluate_unzip_output(
        &self,
        input: &OperationRef,
        element: usize,
    ) -> MessageTypeEvaluation {
        let Some(unzip_inference) = self.get_inference_of(input.clone())? else {
            return Ok(None);
        };

        let unzip_outputs = self.metadata.unzip_output_types(*unzip_inference)?;

        let Some(id) = unzip_outputs.get(element).copied() else {
            return Err(DiagramErrorCode::InvalidUnzip {
                message: self.type_name_for(*unzip_inference)?,
                element,
            });
        };

        Ok(Some(id))
    }

    pub fn evaluate_buffer_input(&self, input: &OperationRef) -> MessageTypeEvaluation {
        let incoming_message_types = self.try_get_message_types_into(input)?;
        if incoming_message_types.is_empty() {
            // There might not be any direct inputs to this buffer, so check for
            // buffer hints from accessors.
            let Some(hints) = self.inferences.buffer_hints.get(input) else {
                // Keep waiting for more operations to be inferred.
                return Ok(None);
            };

            let mut hint_message_types: SmallVec<[usize; 8]> = SmallVec::new();
            for hint in hints {
                if let Some(evaluation) = hint.evaluate(self)? {
                    hint_message_types.push(evaluation);
                }
            }

            hint_message_types.sort();
            hint_message_types.dedup();

            if hint_message_types.len() <= 1 {
                return Ok(hint_message_types.first().cloned());
            } else {
                return Err(DiagramErrorCode::AmbiguousMessageType(
                    self.type_info_for_slice(&hint_message_types)?,
                ));
            }
        } else if incoming_message_types.len() == 1 {
            return Ok(incoming_message_types.first().cloned());
        } else {
            return Err(DiagramErrorCode::AmbiguousMessageType(
                self.type_info_for_slice(&incoming_message_types)?,
            ));
        }
    }

    pub fn evaluate_buffer_access_input(&self, target: &OperationRef) -> MessageTypeEvaluation {
        let Some(target_inference) = self.get_inference_of(target.clone())? else {
            return Ok(None);
        };

        let request_message = self
            .metadata
            .buffer_access_request_type(*target_inference)?;
        Ok(Some(request_message))
    }

    pub fn evaluate_split_input(&self, operation: &OperationRef) -> MessageTypeEvaluation {
        let incoming_message_types = self.get_message_types_into(operation)?;
        if incoming_message_types.len() <= 1 {
            if let Some(incoming_message_type) = incoming_message_types.first() {
                let can_split = self.metadata.can_split(*incoming_message_type)?;
                let can_serialize = self.metadata.can_seralize(*incoming_message_type)?;
                if !can_split && can_serialize {
                    // The message cannot be split but it can be serialized, so
                    // we should change it to a JsonMessage.
                    let Ok(json_index) = self.metadata.json_message_index() else {
                        return Err(DiagramErrorCode::NotSplittable(
                            self.type_name_for(*incoming_message_type)?,
                        ));
                    };

                    return Ok(Some(json_index));
                } else if !can_split {
                    // The message cannot be split and cannot be serialized, so
                    // it is not a valid choice for the split operation.
                    return Err(DiagramErrorCode::NotSplittable(
                        self.type_name_for(*incoming_message_type)?,
                    ));
                }
            }

            return Ok(incoming_message_types.first().copied());
        }

        let Ok(json_index) = self.metadata.json_message_index() else {
            return Err(DiagramErrorCode::AmbiguousMessageType(
                self.type_info_for_slice(&incoming_message_types)?,
            ));
        };

        // Check if all incoming messages can be serialized. If so, we can funnel
        // them into a JsonMessage before splitting.
        for incoming_message_type in &incoming_message_types {
            if !self.metadata.can_seralize(*incoming_message_type)? {
                return Err(DiagramErrorCode::AmbiguousMessageType(
                    self.type_info_for_slice(&incoming_message_types)?,
                ));
            }
        }

        // All incoming message types can be serialized, so take in a JsonMessage instead.
        Ok(Some(json_index))
    }

    pub fn evaluate_split_output(&self, operation: &OperationRef) -> MessageTypeEvaluation {
        let Some(split_inference) = self.get_inference_of(operation.clone())? else {
            return Ok(None);
        };

        let output_type = self.metadata.split_output_type(*split_inference)?;
        Ok(Some(output_type))
    }

    pub fn type_name_for(&self, index: usize) -> Result<Cow<'static, str>, DiagramErrorCode> {
        Ok(Cow::Owned(
            self.metadata.message_type_name(index)?.to_owned(),
        ))
    }

    pub fn type_info_for_slice(
        &self,
        indices: &[usize],
    ) -> Result<Vec<Cow<'static, str>>, DiagramErrorCode> {
        let mut info = Vec::new();
        for index in indices {
            info.push(self.type_name_for(*index)?);
        }

        Ok(info)
    }
}

fn evaluate_buffer_hint(
    layout: &BufferMapLayoutHints<usize>,
    member: &IdentifierRef,
) -> MessageTypeEvaluation {
    match layout {
        BufferMapLayoutHints::Dynamic(dynamic) => {
            if dynamic.is_compatible(member) {
                if let Some(hint) = &dynamic.hint.map(|h| h.as_exact()).flatten() {
                    return Ok(Some(*hint));
                }

                return Ok(None);
            }

            let mut incompatibility = IncompatibleLayout::default();
            incompatibility.forbidden_buffers.push(member.to_owned());
            return Err(DiagramErrorCode::IncompatibleBuffers(incompatibility));
        }
        BufferMapLayoutHints::Static(hints) => {
            if let Some(hint) = hints.get(member) {
                return Ok(hint.as_exact());
            }
        }
    }

    Ok(None)
}

struct UnfinishedOperation {
    /// Name of the operation within its scope
    id: OperationName,
    /// The namespaces that this operation takes place inside
    namespaces: NamespaceList,
    /// Description of the operation
    op: Arc<dyn BuildDiagramOperation>,
    /// The sibling operations of the one that is being built
    sibling_ops: Operations,
    /// Where implicit errors should be connected if not overridden
    on_implicit_error: OperationRef,
}

impl UnfinishedOperation {
    fn new(
        id: &OperationName,
        op: Arc<dyn BuildDiagramOperation>,
        sibling_ops: &Operations,
        on_implicit_error: OperationRef,
    ) -> Self {
        Self {
            id: Arc::clone(id),
            op,
            sibling_ops: sibling_ops.clone(),
            on_implicit_error,
            namespaces: Default::default(),
        }
    }
}

impl<'a, 'b> Deref for InferenceContext<'a, 'b> {
    type Target = DiagramContext<'a>;
    fn deref(&self) -> &Self::Target {
        &self.diagram_context
    }
}

pub type MessageTypeEvaluation = Result<Option<usize>, DiagramErrorCode>;

pub trait MessageTypeConstraint: std::fmt::Debug + 'static + Send + Sync {
    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation;

    fn dependencies(&self, context: &ConstraintContext) -> SmallVec<[PortRef; 8]>;
}

#[derive(Debug, Default, Clone)]
struct MessageTypeInference {
    message_type: Option<usize>,
    constraint: Option<Arc<dyn MessageTypeConstraint>>,
    error: Option<DiagramErrorCode>,
    certain: bool,
}

impl MessageTypeInference {
    fn evaluate(
        &self,
        inference: &Inferences,
        registry: &dyn MetadataAccess,
    ) -> (MessageTypeEvaluation, bool) {
        let Some(constraint) = &self.constraint else {
            return (Ok(self.message_type), true);
        };

        let certain = Cell::new(true);
        let ctx = ConstraintContext {
            inferences: inference,
            metadata: registry,
            certain: Some(&certain),
        };
        (constraint.evaluate(&ctx), certain.get())
    }
}

/// An input or output port of an operation.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub enum PortRef {
    Input(OperationRef),
    Output(OutputRef),
}

impl PortRef {
    pub fn in_namespaces(self, parent_namespaces: &[Arc<str>]) -> Self {
        match self {
            Self::Input(input) => Self::Input(input.in_namespaces(parent_namespaces)),
            Self::Output(output) => Self::Output(output.in_namespaces(parent_namespaces)),
        }
    }
}

impl std::fmt::Display for PortRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(input) => {
                write!(f, "(input) {input}")
            }
            Self::Output(output) => {
                write!(f, "(output) {output}")
            }
        }
    }
}

impl std::fmt::Debug for PortRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl<T: Into<OperationRef>> From<T> for PortRef {
    fn from(value: T) -> Self {
        let op: OperationRef = value.into();
        Self::Input(op.into())
    }
}

impl From<OutputRef> for PortRef {
    fn from(value: OutputRef) -> Self {
        Self::Output(value)
    }
}

impl From<NamedOutputRef> for PortRef {
    fn from(value: NamedOutputRef) -> Self {
        Self::Output(value.into())
    }
}

#[derive(Debug, Default)]
struct Inferences {
    partial: bool,
    setup_errors: HashMap<OperationRef, DiagramErrorCode>,
    evaluations: HashMap<PortRef, MessageTypeInference>,
    connections_into: HashMap<OperationRef, HashSet<OutputRef>>,
    connection_from: HashMap<OutputRef, OperationRef>,
    redirected_input: HashMap<OperationRef, OperationRef>,
    redirections_into: HashMap<OperationRef, HashSet<OperationRef>>,
    template_outputs: HashMap<OutputRef, OperationRef>,
    // Editor queries use the actual typed scope boundary, after its conversions.
    // These aliases do not introduce required evaluations into runtime inference.
    scope_outputs: HashMap<OutputRef, OperationRef>,
    buffer_hints: HashMap<OperationRef, Vec<BufferInference>>,
    buffer_inputs: HashSet<OperationRef>,
    buffer_consumers: HashMap<OperationRef, BufferConsumer>,
}

#[derive(Debug)]
enum BufferConsumerKind {
    Join,
    Listen,
    Access,
}

#[derive(Debug)]
struct BufferConsumer {
    output: PortRef,
    buffers: Vec<(IdentifierRef<'static>, OperationRef)>,
    kind: BufferConsumerKind,
    cloned: Vec<IdentifierRef<'static>>,
}

impl BufferConsumer {
    fn validate(
        &self,
        inferences: &Inferences,
        metadata: &dyn MetadataAccess,
    ) -> Result<(), DiagramErrorCode> {
        let output_type = inferences.resolved_type(&self.output)?;
        let layout = match self.kind {
            BufferConsumerKind::Join => {
                if self.buffers.is_empty() {
                    return Err(DiagramErrorCode::EmptyJoin);
                }
                metadata.join_layout(output_type)?
            }
            BufferConsumerKind::Listen => metadata.listen_layout(output_type)?,
            BufferConsumerKind::Access => metadata.buffer_access_layout(output_type)?,
        };
        for member in &self.cloned {
            if !self.buffers.iter().any(|(selected, _)| selected == member) {
                return Err(DiagramErrorCode::UnknownJoinField {
                    unknown: member.clone(),
                    available: self
                        .buffers
                        .iter()
                        .map(|(member, _)| member.clone())
                        .collect(),
                });
            }
        }
        if let BufferMapLayoutHints::Static(hints) = layout {
            let missing_buffers: Vec<_> = hints
                .keys()
                .filter(|member| !self.buffers.iter().any(|(selected, _)| selected == *member))
                .cloned()
                .collect();
            if !missing_buffers.is_empty() {
                return Err(DiagramErrorCode::IncompatibleBuffers(IncompatibleLayout {
                    missing_buffers,
                    ..Default::default()
                }));
            }
        }
        let mut unresolved = None;
        for (member, buffer) in &self.buffers {
            let hint = match layout {
                BufferMapLayoutHints::Dynamic(dynamic) => {
                    if !dynamic.is_compatible(member) {
                        return Err(DiagramErrorCode::IncompatibleBuffers(IncompatibleLayout {
                            forbidden_buffers: vec![member.clone()],
                            ..Default::default()
                        }));
                    }
                    dynamic.hint.as_ref()
                }
                BufferMapLayoutHints::Static(hints) => hints.get(member),
            };
            let buffer_type = match inferences
                .validate_buffer(buffer)
                .and_then(|_| inferences.resolved_type(&buffer.clone().into()))
            {
                Ok(message_type) => message_type,
                Err(
                    error @ (DiagramErrorCode::InvalidOperation(_)
                    | DiagramErrorCode::CircularRedirect(_)),
                ) => return Err(error),
                Err(error) => {
                    unresolved.get_or_insert(error);
                    continue;
                }
            };
            let incompatible_with = match hint {
                Some(MessageTypeHint::Exact(required)) if *required != buffer_type => {
                    Some(*required)
                }
                Some(MessageTypeHint::Fallback(required))
                    if metadata
                        .json_message_index()
                        .is_ok_and(|json| json == *required)
                        && !metadata.can_seralize(buffer_type)? =>
                {
                    Some(*required)
                }
                _ => None,
            };
            if self.cloned.contains(member) && !metadata.can_clone(buffer_type)? {
                return Err(DiagramErrorCode::NotCloneable(
                    metadata.message_type_name(buffer_type)?.to_owned().into(),
                ));
            }
            if let Some(required) = incompatible_with {
                return Err(DiagramErrorCode::IncompatibleBufferType {
                    source_type: metadata.message_type_name(buffer_type)?.to_owned().into(),
                    target_type: metadata.message_type_name(required)?.to_owned().into(),
                });
            }
        }
        if let Some(error) = unresolved {
            return Err(error);
        }
        Ok(())
    }
}

struct BufferInference {
    used_by: PortRef,
    member: IdentifierRef<'static>,
    evaluate: EvaluateBufferLayoutHintFn,
}

impl BufferInference {
    pub fn evaluate(&self, ctx: &ConstraintContext) -> MessageTypeEvaluation {
        let Some(target_inference) = ctx.get_inference_of(self.used_by.clone())? else {
            return Ok(None);
        };

        (self.evaluate)(ctx, *target_inference, &self.member)
    }
}

impl std::fmt::Debug for BufferInference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferInference")
            .field("used_by", &self.used_by)
            .field("member", &self.member)
            .finish()
    }
}

type EvaluateBufferLayoutHintFn =
    fn(&ConstraintContext, usize, &IdentifierRef) -> MessageTypeEvaluation;

impl Inferences {
    fn validate_buffer<'a>(&'a self, mut buffer: &'a OperationRef) -> Result<(), DiagramErrorCode> {
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(buffer) {
                return Err(DiagramErrorCode::CircularRedirect(
                    visited.into_iter().cloned().collect(),
                ));
            }
            if !self.buffer_inputs.contains(buffer) {
                // Preserve missing-context/setup errors, but reject a known ordinary
                // input used where runtime construction requires a buffer.
                self.get_evaluation(&buffer.clone().into())?;
                return Err(DiagramErrorCode::InvalidOperation(buffer.clone()));
            }
            match self.redirected_input.get(buffer) {
                Some(next) => buffer = next,
                None => return Ok(()),
            }
        }
    }

    fn infer_port(
        &self,
        port: &PortRef,
        metadata: &dyn MetadataAccess,
    ) -> Result<(usize, bool), DiagramError> {
        let scope_input = match port {
            PortRef::Output(output) => self.scope_outputs.get(output).cloned().map(PortRef::Input),
            PortRef::Input(_) => None,
        };
        let evaluation = self
            .get_evaluation(scope_input.as_ref().unwrap_or(port))
            .in_port(|| port.clone())?;
        if let Some(error) = &evaluation.error {
            let error = if evaluation.certain {
                error.clone()
            } else {
                DiagramErrorCode::CannotInferType(port.clone())
            };
            return Err(error.in_port(port.clone()));
        }
        let operation = port_operation(port);
        if let Some(consumer) = operation
            .as_ref()
            .and_then(|op| self.buffer_consumers.get(op))
        {
            consumer.validate(self, metadata).in_port(|| port.clone())?;
        }
        let message_type = evaluation
            .message_type
            .ok_or_else(|| DiagramErrorCode::CannotInferType(port.clone()).in_port(port.clone()))?;
        Ok((message_type, evaluation.certain))
    }

    fn resolved_type(&self, port: &PortRef) -> Result<usize, DiagramErrorCode> {
        let evaluation = self.get_evaluation(port)?;
        if !evaluation.certain {
            return Err(DiagramErrorCode::CannotInferType(port.clone()));
        }
        if let Some(error) = &evaluation.error {
            return Err(error.clone());
        }
        evaluation
            .message_type
            .ok_or_else(|| DiagramErrorCode::CannotInferType(port.clone()))
    }

    fn evaluation(&mut self, key: impl Into<PortRef>) -> &mut MessageTypeInference {
        let key = key.into();
        self.evaluations.entry(key).or_default()
    }

    fn constrain(&mut self, key: impl Into<PortRef>, constraint: impl MessageTypeConstraint) {
        let port_constraint = &mut self.evaluation(key).constraint;

        // TODO(@mxgrey): Remove this assertion when done testing
        assert!(port_constraint.is_none());
        *port_constraint = Some(Arc::new(constraint));
    }

    fn get_evaluation(&self, key: &PortRef) -> Result<&MessageTypeInference, DiagramErrorCode> {
        if let PortRef::Input(input) = key {
            if let Some(error) = self.setup_errors.get(input) {
                return Err(error.clone());
            }
        }
        if let Some(operation) = port_operation(key) {
            if let Some(error) = self.setup_errors.get(&operation) {
                return Err(error.clone());
            }
        }
        self.evaluations
            .get(&key)
            .ok_or_else(|| DiagramErrorCode::UnknownPort(key.clone()))
    }

    fn try_infer_types(&self) -> Result<InferredMessageTypes, DiagramErrorCode> {
        let mut inferred = InferredMessageTypes::new();
        for (port, evaluation) in &self.evaluations {
            let Some(message_type) = evaluation.message_type else {
                return Err(DiagramErrorCode::CannotInferType(port.clone()));
            };
            inferred.insert(port.clone(), message_type);
        }
        Ok(inferred)
    }
}

fn port_operation(port: &PortRef) -> Option<OperationRef> {
    match port {
        PortRef::Input(OperationRef::Named(named)) => Some(
            OperationRef::from(named.exposed_namespace.as_ref().unwrap_or(&named.name))
                .in_namespaces(&named.namespaces),
        ),
        PortRef::Output(OutputRef::Named(named)) => {
            Some(OperationRef::from(&named.operation).in_namespaces(&named.namespaces))
        }
        _ => None,
    }
}

#[derive(Debug)]
struct ExactMatch(PortRef);

impl MessageTypeConstraint for ExactMatch {
    fn dependencies(&self, _: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        smallvec![self.0.clone()]
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.get_inference_of(self.0.clone()).cloned()
    }
}

#[derive(Debug)]
struct CloneInput {
    operation: OperationRef,
    targets: Vec<OperationRef>,
}

impl MessageTypeConstraint for CloneInput {
    fn dependencies(&self, context: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        self.targets
            .iter()
            .cloned()
            .map(Into::into)
            .chain(
                context
                    .connections_into(&self.operation)
                    .into_iter()
                    .map(Into::into),
            )
            .collect()
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_clone_input(&self.operation, &self.targets)
    }
}

#[derive(Debug)]
struct ResultInto {
    operation: OperationRef,
    ok: OperationRef,
    err: OperationRef,
}

impl MessageTypeConstraint for ResultInto {
    fn dependencies(&self, context: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        context
            .connections_into(&self.operation)
            .into_iter()
            .map(Into::into)
            .chain([self.ok.clone().into(), self.err.clone().into()])
            .collect()
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_result_input(&self.operation, &self.ok, &self.err)
    }
}

#[derive(Debug)]
struct OkFrom(OperationRef);

impl MessageTypeConstraint for OkFrom {
    fn dependencies(&self, _: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        smallvec![self.0.clone().into()]
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_ok_from(&self.0)
    }
}

#[derive(Debug)]
struct ErrFrom(OperationRef);

impl MessageTypeConstraint for ErrFrom {
    fn dependencies(&self, _: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        smallvec![self.0.clone().into()]
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_err_from(&self.0)
    }
}

#[derive(Debug)]
struct UnzipInput(OperationRef);

impl MessageTypeConstraint for UnzipInput {
    fn dependencies(&self, context: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        context
            .connections_into(&self.0)
            .into_iter()
            .map(Into::into)
            .collect()
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_unzip_input(&self.0)
    }
}

#[derive(Debug)]
struct UnzipOutput {
    op: OperationRef,
    element: usize,
}

impl MessageTypeConstraint for UnzipOutput {
    fn dependencies(&self, _: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        smallvec![self.op.clone().into()]
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_unzip_output(&self.op, self.element)
    }
}

#[derive(Debug)]
struct BufferInput(OperationRef);

impl MessageTypeConstraint for BufferInput {
    fn dependencies(&self, context: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        let mut deps = SmallVec::new();
        deps.extend(
            context
                .connections_into(&self.0)
                .into_iter()
                .map(Into::into),
        );
        if let Some(hints) = context.inferences.buffer_hints.get(&self.0) {
            for hint in hints {
                deps.push(hint.used_by.clone());
            }
        }

        deps
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_buffer_input(&self.0)
    }
}

#[derive(Debug)]
struct BufferAccessInput {
    target: OperationRef,
}

impl MessageTypeConstraint for BufferAccessInput {
    fn dependencies(&self, _: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        smallvec![self.target.clone().into()]
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_buffer_access_input(&self.target)
    }
}

#[derive(Debug)]
struct SplitInput(OperationRef);

impl MessageTypeConstraint for SplitInput {
    fn dependencies(&self, context: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        context
            .connections_into(&self.0)
            .into_iter()
            .map(Into::into)
            .collect()
    }

    fn evaluate(&self, context: &ConstraintContext) -> MessageTypeEvaluation {
        context.evaluate_split_input(&self.0)
    }
}

#[derive(Debug)]
struct SplitOutput(OperationRef);

impl MessageTypeConstraint for SplitOutput {
    fn dependencies(&self, _: &ConstraintContext) -> SmallVec<[PortRef; 8]> {
        smallvec![self.0.clone().into()]
    }

    fn evaluate(&self, ctx: &ConstraintContext) -> MessageTypeEvaluation {
        ctx.evaluate_split_output(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::InferenceBoundaryConditions;
    use crate::{
        MessageRegistrations, NextOperation, OperationRef, OutputRef, PortRef, diagram::testing::*,
        output_ref, prelude::*,
    };
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_split_type_inference() {
        let fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "split",
            "ops": {
                "x10-buffer": {
                    "type": "buffer"
                },
                "x100-buffer": {
                    "type": "buffer"
                },
                "split": {
                    "type": "split",
                    "sequential": [
                        "x100",
                        "x10"
                    ]
                },
                "x10": {
                    "type": "node",
                    "builder": "mul",
                    "next": "x10-buffer",
                    "config": 10,
                    "display_text": "x10"
                },
                "join": {
                    "type": "join",
                    "buffers": [
                        "x100-buffer",
                        "x10-buffer"
                    ],
                    "next": "sum"
                },
                "sum": {
                    "type": "node",
                    "builder": "add",
                    "next": {
                        "builtin": "terminate"
                    },
                    "display_text": "sum"
                },
                "x100": {
                    "type": "node",
                    "builder": "mul",
                    "next": "x100-buffer",
                    "config": 100,
                    "display_text": "x100"
                }
            }
        }))
        .unwrap();

        let inference = diagram
            .infer_message_types(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
            )
            .unwrap();
        validate_spit_type_inference(inference, &fixture.registry.messages.registration);

        let inference = diagram
            .infer_message_types(
                &fixture.registry.metadata(),
                InferenceBoundaryConditions::json_messages(&fixture.registry.metadata(), [])
                    .unwrap(),
            )
            .unwrap();
        validate_spit_type_inference(inference, &fixture.registry.messages.registration);
    }

    fn validate_spit_type_inference(
        inference: HashMap<PortRef, usize>,
        registration: &MessageRegistrations,
    ) {
        let op = |name: &str| -> PortRef { (&NextOperation::Name(name.into())).into() };
        let json_index = registration.get_index::<JsonMessage>().unwrap();
        let f64_index = registration.get_index::<f64>().unwrap();

        assert_eq!(inference[&op("x10-buffer")], f64_index,);

        assert_eq!(inference[&op("x100-buffer")], f64_index,);

        assert_eq!(
            inference[&output_ref(&"split".into()).next_index(0).into()],
            json_index,
        );

        assert_eq!(
            inference[&output_ref(&"split".into()).next_index(1).into()],
            json_index,
        );

        assert_eq!(inference[&op("sum")], json_index,);

        assert_eq!(inference[&op("x10")], json_index,);

        assert_eq!(inference[&op("x100")], json_index,);

        assert_eq!(inference[&op("split")], json_index,);

        assert_eq!(
            inference[&output_ref(&"join".into()).next().into()],
            json_index,
        );

        assert_eq!(
            inference[&output_ref(&"sum".into()).next().into()],
            f64_index,
        );

        assert_eq!(
            inference[&output_ref(&"x10".into()).next().into()],
            f64_index,
        );

        assert_eq!(
            inference[&output_ref(&"x100".into()).next().into()],
            f64_index,
        );

        assert_eq!(inference[&OutputRef::start().into()], json_index,);

        assert_eq!(
            inference[&OperationRef::Terminate(Default::default()).into()],
            json_index,
        );
    }

    #[test]
    fn test_fork_clone_type_inference() {
        let fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["add_one", "add_two"]
                },
                "add_one": {
                    "type": "node",
                    "builder": "add_to",
                    "config": 1,
                    "next": "buffer_one"
                },
                "add_two": {
                    "type": "node",
                    "builder": "add_to",
                    "config": 2,
                    "next": "buffer_two"
                },
                "buffer_one": { "type": "buffer" },
                "buffer_two": { "type": "buffer" },
                "join": {
                    "type": "join",
                    "buffers": [
                        "buffer_one",
                        "buffer_two"
                    ],
                    "next": "multiply"
                },
                "multiply": {
                    "type": "node",
                    "builder": "mul",
                    "next": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let inference = diagram
            .infer_message_types(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
            )
            .unwrap();
        validate_fork_clone_type_inference(inference, &fixture.registry.messages.registration);

        let inference = diagram
            .infer_message_types(
                &fixture.registry.metadata(),
                InferenceBoundaryConditions::json_messages(&fixture.registry.metadata(), [])
                    .unwrap(),
            )
            .unwrap();
        validate_fork_clone_type_inference(inference, &fixture.registry.messages.registration);
    }

    fn validate_fork_clone_type_inference(
        inference: HashMap<PortRef, usize>,
        registration: &MessageRegistrations,
    ) {
        let op = |name: &str| -> PortRef { (&NextOperation::Name(name.into())).into() };
        let json_index = registration.get_index::<JsonMessage>().unwrap();
        let i64_index = registration.get_index::<i64>().unwrap();
        let f64_index = registration.get_index::<f64>().unwrap();

        assert_eq!(inference[&op("fork_clone")], json_index,);

        assert_eq!(
            inference[&output_ref(&"fork_clone".into()).next_index(0).into()],
            json_index,
        );

        assert_eq!(
            inference[&output_ref(&"fork_clone".into()).next_index(1).into()],
            json_index,
        );

        assert_eq!(inference[&op("add_one")], i64_index,);

        assert_eq!(
            inference[&output_ref(&"add_one".into()).next().into()],
            i64_index,
        );

        assert_eq!(inference[&op("add_two")], i64_index,);

        assert_eq!(
            inference[&output_ref(&"add_two".into()).next().into()],
            i64_index,
        );

        assert_eq!(inference[&op("multiply")], json_index,);

        assert_eq!(
            inference[&output_ref(&"multiply".into()).next().into()],
            f64_index,
        );

        assert_eq!(inference[&op("buffer_one")], i64_index,);

        assert_eq!(inference[&op("buffer_two")], i64_index,);

        assert_eq!(inference[&OutputRef::start().into()], json_index,);

        assert_eq!(
            inference[&OperationRef::Terminate(Default::default()).into()],
            json_index,
        );
    }

    #[test]
    fn infer_message_types_for_ports_returns_only_requested_ports() {
        let fixture = DiagramTestFixture::new();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "source",
            "ops": {
                "source": {
                    "type": "node",
                    "builder": "add",
                    "next": "target"
                },
                "target": {
                    "type": "node",
                    "builder": "mul",
                    "next": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();
        let source_output: PortRef = output_ref(&"source".into()).next().into();
        let target_input: PortRef = (&NextOperation::Name("target".into())).into();

        let inference = diagram
            .infer_message_types_for_ports(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
                [source_output.clone(), target_input.clone()],
            )
            .unwrap();

        let json_index = fixture
            .registry
            .messages
            .registration
            .get_index::<JsonMessage>()
            .unwrap();
        let f64_index = fixture
            .registry
            .messages
            .registration
            .get_index::<f64>()
            .unwrap();
        assert_eq!(inference.len(), 2);
        assert_eq!(inference[&source_output], f64_index);
        assert_eq!(inference[&target_input], json_index);
    }

    #[test]
    fn infer_message_types_for_ports_ignores_unrequested_broken_ports() {
        let fixture = DiagramTestFixture::new();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "source",
            "ops": {
                "source": {
                    "type": "node",
                    "builder": "add",
                    "next": "target"
                },
                "target": {
                    "type": "node",
                    "builder": "mul",
                    "next": { "builtin": "terminate" }
                },
                "unfinished": {
                    "type": "buffer"
                }
            }
        }))
        .unwrap();
        let source_output: PortRef = output_ref(&"source".into()).next().into();
        let target_input: PortRef = (&NextOperation::Name("target".into())).into();
        let unfinished_input: PortRef = (&NextOperation::Name("unfinished".into())).into();

        assert!(
            diagram
                .infer_message_types(
                    &fixture.registry,
                    InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
                )
                .is_err()
        );

        let inference = diagram
            .infer_message_types_for_ports(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
                [source_output.clone(), target_input.clone()],
            )
            .unwrap();

        assert!(inference.contains_key(&source_output));
        assert!(inference.contains_key(&target_input));
        assert!(!inference.contains_key(&unfinished_input));
    }

    #[test]
    fn infer_message_types_for_ports_individually_preserves_good_ports_after_errors() {
        let fixture = DiagramTestFixture::new();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "source",
            "ops": {
                "source": { "type": "node", "builder": "add", "next": "unzip" },
                "unzip": { "type": "unzip", "next": [{ "builtin": "dispose" }] },
                "unfinished": { "type": "buffer" }
            }
        }))
        .unwrap();
        let source: PortRef = output_ref(&"source".into()).next().into();
        let unzip: PortRef = (&NextOperation::Name("unzip".into())).into();
        let unfinished: PortRef = (&NextOperation::Name("unfinished".into())).into();
        let missing: PortRef = (&NextOperation::Name("missing".into())).into();
        let ports = [
            unzip.clone(),
            unfinished.clone(),
            missing.clone(),
            source.clone(),
        ];
        assert!(
            diagram
                .infer_message_types_for_ports(
                    &fixture.registry,
                    InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
                    ports.clone(),
                )
                .is_err()
        );

        let inference = diagram
            .infer_message_types_for_ports_individually(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
                ports,
            )
            .unwrap();

        let f64_index = fixture
            .registry
            .messages
            .registration
            .get_index::<f64>()
            .unwrap();
        assert!(inference.values().all(|results| results.len() == 1));
        assert_eq!(inference[&source][0].as_ref().unwrap(), &(f64_index, true));
        assert!(matches!(
            inference[&unzip][0].as_ref().unwrap_err().code,
            crate::DiagramErrorCode::NotUnzippable(_)
        ));
        assert!(matches!(
            inference[&unfinished][0].as_ref().unwrap_err().code,
            crate::DiagramErrorCode::CannotInferType(_)
        ));
        assert!(matches!(
            inference[&missing][0].as_ref().unwrap_err().code,
            crate::DiagramErrorCode::UnknownPort(_)
        ));
    }

    #[test]
    fn infer_nested_template_output_from_internal_producer() {
        let fixture = DiagramTestFixture::new();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "inner": {
                    "inputs": ["source"],
                    "outputs": ["result"],
                    "ops": {
                        "source": { "type": "node", "builder": "add", "next": "result" }
                    }
                },
                "outer": {
                    "inputs": { "input": { "nested": "source" } },
                    "outputs": ["result", "unused"],
                    "ops": {
                        "nested": {
                            "type": "section",
                            "template": "inner",
                            "connect": { "result": "result" }
                        }
                    }
                }
            },
            "start": { "section": "input" },
            "ops": {
                "section": {
                    "type": "section",
                    "template": "outer",
                    "connect": { "result": { "builtin": "terminate" } }
                }
            }
        }))
        .unwrap();
        let output: PortRef = output_ref(&"section".into())
            .section_output(&"result")
            .into();
        let unused: PortRef = output_ref(&"section".into())
            .section_output(&"unused")
            .into();
        let f64_index = fixture
            .registry
            .messages
            .registration
            .get_index::<f64>()
            .unwrap();

        // Resolving public outputs must not add mandatory evaluations to full inference.
        diagram
            .infer_message_types(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
            )
            .unwrap();

        let inference = diagram
            .infer_message_types_for_ports_individually(
                &fixture.registry,
                InferenceBoundaryConditions::json_messages(&fixture.registry, []).unwrap(),
                [output.clone(), unused.clone()],
            )
            .unwrap();

        assert_eq!(inference[&output].len(), 1);
        assert_eq!(inference[&output][0].as_ref().unwrap(), &(f64_index, true));
        assert_eq!(inference[&unused].len(), 1);
        assert!(matches!(
            inference[&unused][0].as_ref().unwrap_err().code,
            crate::DiagramErrorCode::NoConnection(_)
        ));
    }

    // TODO(@mxgrey): Add tests with scopes to validate type inference inside namespaces.
}
