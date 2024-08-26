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

use bevy_ecs::{
    prelude::{Commands, World},
    system::CommandQueue,
};
use bevy_hierarchy::BuildChildren;

use crate::{
    Builder, DeliveryChoice, InputSlot, OperateScope, Output, ScopeEndpoints, ScopeSettingsStorage,
    Service, ServiceBundle, StreamPack, WorkflowService, WorkflowStorage,
};

mod internal;

/// Trait to allow workflows to be spawned from a [`Commands`] or a [`World`].
pub trait SpawnWorkflowExt {
    /// Spawn a workflow.
    ///
    /// * `build` - A function that takes in a [`Scope`] and a [`Builder`] to
    ///    build the workflow
    ///
    /// If you want any particular settings for your workflow, specify that with
    /// the return value of `build`. Returning nothing `()` will use default
    /// workflow settings.
    fn spawn_workflow<Request, Response, Streams, W>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder) -> W,
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        W: Into<WorkflowSettings>;

    /// Spawn a pure input/output (io) workflow with no streams. This is just a
    /// convenience wrapper around `spawn_workflow` which usually allows you to
    /// avoid specifying any generic parameters when there are no streams being
    /// used.
    fn spawn_io_workflow<Request, Response, W>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response>, &mut Builder) -> W,
    ) -> Service<Request, Response>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        W: Into<WorkflowSettings>,
    {
        self.spawn_workflow::<Request, Response, (), W>(build)
    }
}

/// A view of a scope's inputs and outputs from inside of the scope.
///
/// It is _not_ a mistake that the [`Scope::input`] field has an [`Output`]
/// type or that the [`Scope::terminate`] field has an [`InputSlot`] type. From
/// the perspective inside of the scope, the scope's input would be received as
/// an output, and the scope's output would be passed into an input slot.
pub struct Scope<Request, Response, Streams: StreamPack = ()> {
    /// The data entering the scope. The workflow of the scope must be built
    /// out from here.
    pub input: Output<Request>,
    /// The slot that the final output of the scope must connect into. Once you
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

impl From<()> for WorkflowSettings {
    fn from(_: ()) -> Self {
        WorkflowSettings::default()
    }
}

impl From<ScopeSettings> for WorkflowSettings {
    fn from(value: ScopeSettings) -> Self {
        WorkflowSettings::new().with_scope(value)
    }
}

impl From<DeliverySettings> for WorkflowSettings {
    fn from(value: DeliverySettings) -> Self {
        WorkflowSettings::new().with_delivery(value)
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
    /// requests with the same [`DeliveryLabelId`][1] try to run at the same time,
    /// those requests will follow the serial delivery behavior.
    ///
    /// [1]: crate::DeliveryLabelId
    #[default]
    Parallel,
}

/// Settings which determine how the top-level scope of the workflow behaves.
#[derive(Default, Clone)]
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
        Self {
            uninterruptible: true,
        }
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

impl From<()> for ScopeSettings {
    fn from(_: ()) -> Self {
        ScopeSettings::default()
    }
}

impl<'w, 's> SpawnWorkflowExt for Commands<'w, 's> {
    fn spawn_workflow<Request, Response, Streams, Settings>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder) -> Settings,
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        Settings: Into<WorkflowSettings>,
    {
        let scope_id = self.spawn(()).id();
        let ScopeEndpoints {
            terminal,
            enter_scope,
            finish_scope_cancel,
        } = OperateScope::<Request, Response, Streams>::add(None, scope_id, None, self);

        let mut builder = Builder {
            scope: scope_id,
            finish_scope_cancel,
            commands: self,
        };

        let streams = Streams::spawn_workflow_streams(&mut builder);

        let scope = Scope {
            input: Output::new(scope_id, enter_scope),
            terminate: InputSlot::new(scope_id, terminal),
            streams,
        };

        let settings: WorkflowSettings = build(scope, &mut builder).into();

        let mut service = self.spawn((
            ServiceBundle::<WorkflowService<Request, Response, Streams>>::new(),
            WorkflowStorage::new(scope_id),
            Streams::StreamAvailableBundle::default(),
        ));
        settings
            .delivery
            .apply_entity_commands::<Request>(&mut service);
        let service = service.id();
        self.entity(scope_id)
            .insert(ScopeSettingsStorage(settings.scope))
            .set_parent(service);

        WorkflowService::<Request, Response, Streams>::cast(service)
    }
}

impl SpawnWorkflowExt for World {
    fn spawn_workflow<Request, Response, Streams, W>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder) -> W,
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        W: Into<WorkflowSettings>,
    {
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, self);
        let service = commands.spawn_workflow(build);
        command_queue.apply(self);
        service
    }
}

#[cfg(test)]
mod tests {
    use crate::{testing::*, *};

    #[test]
    fn test_simple_workflows() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope
                .input
                .chain(builder)
                .map_block(add)
                .connect(scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request((2.0, 2.0), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(promise.take().available().is_some_and(|v| v == 4.0));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let add_node = builder.create_map_block(add);
            builder.connect(scope.input, add_node.input);
            builder.connect(add_node.output, scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request((3.0, 3.0), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(promise.take().available().is_some_and(|v| v == 6.0));
        assert!(context.no_unhandled_errors());
    }
}
