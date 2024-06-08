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

use crate::{
    ServiceTrait, ServiceRequest, OperationRequest, OperationResult, Stream,
    OrBroken, Input, ManageInput, DeliveryInstructions, ParentSession,
    OperationError, Delivery, DeliveryOrder, DeliveryUpdate, Blocker,
    OperationRoster, Disposal, Cancellation, Cancel, Deliver, SingleTargetStorage,
    Operation, OperationSetup, OperationCleanup, OperationReachability,
    ReachabilityResult,
    begin_scope, dispose_for_despawned_service, insert_new_order, emit_disposal,
    pop_next_delivery,
};

use bevy::prelude::{Entity, World, Component};

use std::collections::HashMap;

#[derive(Component, Clone, Copy)]
struct WorkflowStorage {
    /// The entity that stores the scope operation that encapsulates the entire
    /// workflow.
    scope: Entity,
    /// The entity that will receive the output of the workflow
    output: Entity,
}

struct WorkflowService<Request, Response, Streams> {
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Request, Response, Streams> ServiceTrait for WorkflowService<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: Stream,
{
    type Request = Request;
    type Response = Response;
    fn serve(
        ServiceRequest {
            provider,
            target,
            operation: OperationRequest { source, world, roster }
        }: ServiceRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let instructions = source_mut.get::<DeliveryInstructions>().cloned();
        let scoped_session = world.spawn(ParentSession::new(session)).id();

        let result = serve_workflow_impl::<Request, Response, Streams>(
            request,
            session,
            scoped_session,
            instructions,
            ServiceRequest {
                provider,
                target,
                operation: OperationRequest { source, world, roster },
            }
        );

        if result.is_err() {
            world.despawn(scoped_session);
        }

        result
    }
}

fn serve_workflow_impl<Request, Response, Streams>(
    request: Request,
    parent_session: Entity,
    scoped_session: Entity,
    instructions: Option<DeliveryInstructions>,
    ServiceRequest {
        provider,
        target,
        operation: OperationRequest { source, world, roster }
    }: ServiceRequest,
) -> OperationResult
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: Stream,
{
    let workflow = *world.get::<WorkflowStorage>(provider).or_broken()?;
    let Some(mut delivery) = world.get_mut::<Delivery<Request>>(provider) else {
        // The workflow has been despawned, so we should treat the request
        // as cancelled.
        dispose_for_despawned_service(provider, world, roster);
        return Err(OperationError::NotReady);
    };

    let update = insert_new_order::<Request>(
        delivery.as_mut(),
        DeliveryOrder {
            source,
            session: parent_session,
            task_id: scoped_session,
            request,
            instructions
        },
    );

    let (request, blocker) = match update {
        DeliveryUpdate::Immediate { blocking, request } => {
            let serve_next = serve_next_workflow_request::<Request>;
            let blocker = blocking.map(|label| Blocker {
                provider,
                source,
                session: parent_session,
                label,
                serve_next,
            });
            (request, blocker)
        }
        DeliveryUpdate::Queued { cancelled, stop, .. } => {
            for cancelled in cancelled {
                let disposal = Disposal::supplanted(cancelled.source, source, parent_session);
                emit_disposal(cancelled.source, cancelled.session, disposal, world, roster);
            }
            if let Some(stop) = stop {
                // This workflow is already running and we need to stop it at the
                // scope level
                roster.cancel(Cancel {
                    source,
                    target: workflow.scope,
                    session: Some(stop.session),
                    cancellation: Cancellation::supplanted(
                        stop.source, source, parent_session,
                    ),
                });
            }

            // The request has been queued up and should be delivered later.
            return Ok(());
        }
    };

    let input = Input { session: parent_session, data: request };
    begin_workflow::<Request>(
        input,
        target,
        scoped_session,
        workflow.scope,
        workflow.output,
        blocker,
        world,
        roster
    )
}

fn begin_workflow<Request>(
    input: Input<Request>,
    target: Entity,
    scoped_session: Entity,
    scope: Entity,
    output: Entity,
    blocker: Option<Blocker>,
    world: &mut World,
    roster: &mut OperationRoster,
) -> OperationResult
where
    Request: 'static + Send + Sync,
{
    let mut exit_target = world.get_mut::<ExitTargetStorage>(output).or_broken()?;
    exit_target.map.insert(input.session, ExitTarget { target, blocker });
    begin_scope(input, scoped_session, OperationRequest { source: scope, world, roster })
}

fn serve_next_workflow_request<Request>(
    unblock: Blocker,
    world: &mut World,
    roster: &mut OperationRoster,
)
where
    Request: 'static + Send + Sync,
{
    let Blocker { provider, label, .. } = unblock;
    let Some(workflow) = world.get::<WorkflowStorage>(provider) else {
        return;
    };
    let workflow = *workflow;

    loop {
        let Some(Deliver { request, task_id: scoped_session, blocker }) = pop_next_delivery::<Request>(
            provider, label, serve_next_workflow_request::<Request>, world,
        ) else {
            // No more deliveries to pop, so we should return
            return;
        };

        let parent_session = blocker.session;
        let source = blocker.source;

        let Some(target) = world.get::<SingleTargetStorage>(source) else {
            // This will not be able to run, so we should move onto the next
            // item in the queue.
            continue;
        };
        let target = target.get();

        if begin_workflow(
            Input { session: parent_session, data: request },
            target,
            scoped_session,
            workflow.scope,
            workflow.output,
            Some(blocker),
            world,
            roster,
        ).is_err() {
            // The workflow will not run, so we should despawn the scoped session
            world.despawn(scoped_session);

            // The service did not launch so we should move onto the next item
            // in the queue.
            continue;
        }

        // The next deivery has begun so we can return
        return;
    }
}

#[derive(Component, Default)]
struct ExitTargetStorage {
    /// Map from session value to the target
    map: HashMap<Entity, ExitTarget>,
}

struct ExitTarget {
    target: Entity,
    blocker: Option<Blocker>,
}

struct OperateExitTarget<Response> {
    exit_from_scope: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> Operation for OperateExitTarget<Response>
where
    Response: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source)
            .insert((
                ExitFromScope(self.exit_from_scope),
                ExitTargetStorage::default(),
            ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<Response>()?;
        let mut target_storage = source_mut.get_mut::<ExitTargetStorage>().or_broken()?;
        let exit = target_storage.map.remove(&session).or_broken()?;
        if let Some(blocker) = exit.blocker {
            // This session was blocking a service queue, so now we need to
            // unblock it
            let serve_next = blocker.serve_next;
            serve_next(blocker, world, roster);
        }
        world.get_entity_mut(exit.target).or_broken()?.give_input(session, data, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Response>()?;
        clean.world.get_mut::<ExitTargetStorage>(clean.source).or_broken()?
            .map
            .remove(&clean.session);
        Ok(())
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<Response>()? {
            return Ok(true);
        }

        let scope = r.world().get::<ExitFromScope>(r.source()).or_broken()?.0;
        r.check_upstream(scope)
    }
}

#[derive(Component)]
struct ExitFromScope(Entity);
