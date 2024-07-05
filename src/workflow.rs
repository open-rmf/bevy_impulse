/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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

use bevy::{
    prelude::{Commands, World},
    ecs::system::CommandQueue,
};

use crate::{
    Service, InputSlot, Output, StreamPack, AddOperation, OperateScope,
    Terminate, WorkflowService, Builder,
};

/// Trait to allow workflows to be spawned from a [`Commands`] or a [`World`].
pub trait SpawnWorkflow {
    /// Spawn a workflow with specific settings.
    ///
    /// * `settings` - Settings that describe some details of how the workflow
    /// should behave
    /// * `build` - A function that takes in a [`Scope`] and a [`Builder`] to
    /// build the workflow
    fn spawn_workflow<Request, Response, Streams>(
        &mut self,
        settings: WorkflowSettings,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack;

    /// Spawn a workflow with default settings.
    fn spawn_workflow_default<Request, Response, Streams>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack
    {
        self.spawn_workflow(WorkflowSettings::new(), build)
    }
}

/// A view of a scope's inputs and outputs from inside of the scope.
///
/// It is _not_ a mistake that the [`Scope::input`] field has an [`Output`]
/// type or that the [`Scope::terminate`] field has an [`InputSlot`] type. From
/// the perspective inside of the scope, the scope's input would be received as
/// an output, and the scope's output would be passed into an input slot.
pub struct Scope<Request, Response, Streams: StreamPack> {
    /// The data entering the scope. The workflow of the scope must be built
    /// out from here.
    pub input: Output<Request>,
    /// The slot that the final output of the scope must feed into. Once you
    /// provide an input into this slot, the entire session of the scope will
    /// wind down. The input will be passed out of the scope once all
    /// uninterruptible data flows within the scope have finished.
    pub terminate: InputSlot<Response>,
    /// The input slot(s) that receive data for the output streams of the scope.
    /// You can feed data into these input slots at any time during the execution
    /// of the workflow.
    pub streams: Streams::StreamInputPack,
}

/// Settings that describe some aspects of a workflow's behavior.
#[derive(Default)]
pub struct WorkflowSettings {
    delivery: DeliverySettings,
    scope: ScopeSettings,
}

impl WorkflowSettings {
    /// Use default workflow settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Make a workflow with serial delivery behavior.
    pub fn serial() -> Self {
        Self::default().with_delivery(DeliverySettings::Serial)
    }

    /// Make a workflow with parallel delivery behavior.
    pub fn parallel() -> Self {
        Self::default().with_delivery(DeliverySettings::Parallel)
    }

    pub fn with_delivery(mut self, delivery: DeliverySettings) -> Self {
        self.delivery = delivery;
        self
    }
    pub fn delivery(&self) -> &DeliverySettings {
        &self.delivery
    }
    pub fn delivery_mut(&mut self) -> &mut DeliverySettings {
        &mut self.delivery
    }

    pub fn with_scope(mut self, scope: ScopeSettings) -> Self {
        self.scope = scope;
        self
    }
    pub fn scope(&self) -> &ScopeSettings {
        &self.scope
    }
    pub fn scope_mut(&mut self) -> &mut ScopeSettings {
        &mut self.scope
    }

    /// Transform the settings to be uninterruptible
    pub fn uninterruptible(mut self) -> Self {
        self.scope.set_uninterruptible(true);
        self
    }
}

/// Settings which determine how the workflow delivers its requests: in serial
/// (handling one request at a time) or in parallel (allowing multiple requests
/// at a time).
#[derive(Default)]
pub enum DeliverySettings {
    /// This workflow can only run one session at a time. If a new request comes
    /// in for this workflow when the workflow is already being used, either
    /// * the new request will be queued until the current request is finished, or
    /// * the current request will be cancelled ([`Supplanted`][1]) so the new request can begin
    ///
    /// [1]: crate::Supplanted
    Serial,

    /// The workflow can run any number of sessions at a time. If multiple
    /// requests with the same [`RequestLabelId`][1] try to run at the same time,
    /// those requests will follow the serial delivery behavior.
    ///
    /// [1]: crate::RequestLabelId
    #[default]
    Parallel,
}

/// Settings which determine how the top-level scope of the workflow behaves.
#[derive(Default)]
pub struct ScopeSettings {
    /// Should we prevent the scope from being interrupted (e.g. cancelled)?
    /// False by default, meaning by default scopes can be cancelled or
    /// interrupted.
    uninterruptible: bool,
}

impl ScopeSettings {
    /// Make a new [`ScopeSettings`] with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Make a new [`ScopeSettings`] for an uninterruptible scope.
    pub fn uninterruptible() -> Self {
        Self { uninterruptible: true }
    }

    /// Check if the scope will be set to uninterruptible.
    pub fn is_uninterruptible(&self) -> bool {
        self.uninterruptible
    }

    /// Set whether the scope will be set to uninterruptible.
    pub fn set_uninterruptible(&mut self, uninterruptible: bool) {
        self.uninterruptible = uninterruptible;
    }
}

impl<'w, 's> SpawnWorkflow for Commands<'w, 's> {
    fn spawn_workflow<Request, Response, Streams>(
        &mut self,
        settings: WorkflowSettings,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let scope_id = self.spawn(()).id();
        let scope = OperateScope::<Request, Response, Streams>::new(
            scope_id, None, settings.scope, self,
        );
        self.add(AddOperation::new(scope.terminal(), Terminate::<Response>::new()));

        let (
            stream_storage,
            streams
        ) = Streams::spawn_scope_streams(scope_id, self);
        self.entity(scope_id).insert(stream_storage);

        let scope = Scope {
            input: Output::new(scope_id, scope.enter_scope()),
            terminate: InputSlot::new(scope_id, scope.terminal()),
            streams,
        };

        let mut builder = Builder { scope: scope_id, commands: self };
        build(scope, &mut builder);

        WorkflowService::<Request, Response, Streams>::cast(scope_id)
    }
}

impl SpawnWorkflow for World {
    fn spawn_workflow<Request, Response, Streams>(
        &mut self,
        settings: WorkflowSettings,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, self);
        let service = commands.spawn_workflow(settings, build);
        command_queue.apply(self);
        service
    }
}
