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
    dispatch_service, ActiveTasksStorage, AddImpulse, Cleanup, CleanupContents,
    DisposeForUnavailableService, FinalizeCleanup, FinalizeCleanupRequest, Impulsive, Input,
    InputBundle, ManageDisposal, ManageInput, OperateService, Operation, OperationCleanup,
    OperationReachability, OperationRequest, OperationResult, OperationSetup, OrBroken,
    ProviderStorage, ReachabilityResult, ScopeStorage, Service, ServiceRequest, SingleInputStorage,
    SingleTargetStorage, StreamPack, StreamTargetMap,
};

use bevy_ecs::{
    prelude::{Component, Entity},
    world::Command,
};
use bevy_hierarchy::prelude::DespawnRecursiveExt;

use smallvec::SmallVec;

use std::collections::HashMap;

pub(crate) struct Injection<Request, Response, Streams> {
    target: Entity,
    _ignore: std::marker::PhantomData<fn(Request, Response, Streams)>,
}

impl<Request, Response, Streams> Operation for Injection<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InjectionStorage::default(),
            InputBundle::<(Request, Service<Request, Response, Streams>)>::new(),
            SingleTargetStorage::new(self.target),
            CleanupContents::new(),
            AwaitingCleanup::default(),
            FinalizeCleanup::new(Self::finalize_cleanup),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input {
            session,
            data: (request, service),
        } = source_mut.take_input::<(Request, Service<Request, Response, Streams>)>()?;

        let scope = source_mut.get::<ScopeStorage>().or_broken()?.get();
        let provider = service.provider();
        let instructions = service.instructions().copied();

        let (streams, stream_map) = match source_mut.get::<InjectionStreams<Streams>>() {
            Some(s) => (s.streams.clone(), s.stream_map.clone()),
            None => {
                // We have to wait until the first execution to grab this information
                // because the targets can get changed by connect(~) actions while the
                // workflow is being built.
                let streams = Streams::extract_target_storage(source, world)?;
                let map = world.get::<StreamTargetMap>(source).or_broken()?.clone();
                world
                    .entity_mut(source)
                    .insert(InjectionStreams::<Streams>::new(
                        streams.clone(),
                        map.clone(),
                    ));
                (streams, map)
            }
        };

        let finish = world
            .spawn((InputBundle::<Response>::new(), InjectionSource(source)))
            .id();
        AddImpulse::new(finish, InjectionFinish::<Response>::new()).apply(world);

        let task = world
            .spawn((
                InputBundle::<Request>::new(),
                ProviderStorage(provider),
                SingleTargetStorage::new(finish),
                ActiveTasksStorage::default(),
                DisposeForUnavailableService::new::<Request>(),
                ScopeStorage::new(scope),
                streams,
                stream_map,
            ))
            .id();

        // SAFETY: We must do a sneak_input here because we do not want the
        // roster to register the task as an operation. In fact it does not
        // implement Operation at all. It is just a temporary container for the
        // input and the stream targets.
        unsafe {
            world
                .entity_mut(task)
                .sneak_input(session, request, false)?;
        }

        let mut storage = world.get_mut::<InjectionStorage>(source).or_broken()?;
        storage.list.push(Injected {
            session,
            task,
            finish,
        });
        dispatch_service(ServiceRequest {
            provider,
            target: finish,
            instructions,
            operation: OperationRequest {
                source: task,
                world,
                roster,
            },
        });

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<(Request, Service<Request, Response, Streams>)>()?;
        clean.cleanup_disposals()?;

        let OperationCleanup {
            source,
            cleanup,
            world,
            roster,
        } = clean;
        let session = cleanup.session;
        let cleanup_id = cleanup.cleanup_id;
        let mut storage = world.get_mut::<InjectionStorage>(source).or_broken()?;
        let nodes: SmallVec<[Entity; 16]> = storage
            .list
            .iter()
            .filter_map(|injected| {
                if injected.session == session {
                    Some(injected.task)
                } else {
                    None
                }
            })
            .collect();
        storage.list.retain(|injected| injected.session != session);

        if nodes.is_empty() {
            // No cleanup needed, just notify right away
            cleanup.notify_cleaned(world, roster)?;
            return Ok(());
        }

        world
            .get_mut::<CleanupContents>(source)
            .or_broken()?
            .add_cleanup(cleanup_id, nodes.clone());
        world
            .get_mut::<AwaitingCleanup>(source)
            .or_broken()?
            .map
            .insert(cleanup_id, cleanup);

        for node in nodes.iter().copied() {
            let cleanup = Cleanup {
                cleaner: source,
                node,
                session,
                cleanup_id,
            };
            let clean = OperationCleanup {
                source: node,
                cleanup,
                world,
                roster,
            };
            OperateService::<Request>::cleanup(clean)?;
        }

        Ok(())
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<(Request, Service<Request, Response, Streams>)>()? {
            return Ok(true);
        }

        if InjectionStorage::contains_session(&reachability)? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

impl<Request, Response, Streams> Injection<Request, Response, Streams> {
    fn finalize_cleanup(
        FinalizeCleanupRequest {
            cleanup,
            world,
            roster,
        }: FinalizeCleanupRequest,
    ) -> OperationResult {
        let source = cleanup.cleaner;
        let parent_cleanup = world
            .get_mut::<AwaitingCleanup>(source)
            .or_broken()?
            .map
            .remove(&cleanup.cleanup_id)
            .or_broken()?;
        parent_cleanup.notify_cleaned(world, roster)
    }

    pub(crate) fn new(target: Entity) -> Self {
        Self {
            target,
            _ignore: Default::default(),
        }
    }
}

#[derive(Component, Default)]
struct InjectionStorage {
    list: SmallVec<[Injected; 16]>,
}

#[derive(Component, Default)]
struct AwaitingCleanup {
    // Map from cleanup_id to the upstream cleaner
    map: HashMap<Entity, Cleanup>,
}

impl InjectionStorage {
    fn contains_session(r: &OperationReachability) -> ReachabilityResult {
        Ok(r.world()
            .get::<Self>(r.source())
            .or_broken()?
            .list
            .iter()
            .any(|injected| injected.session == r.session))
    }
}

#[derive(Component)]
struct InjectionStreams<Streams: StreamPack> {
    streams: Streams::StreamStorageBundle,
    stream_map: StreamTargetMap,
}

impl<Streams: StreamPack> Clone for InjectionStreams<Streams> {
    fn clone(&self) -> Self {
        Self {
            streams: self.streams.clone(),
            stream_map: self.stream_map.clone(),
        }
    }
}

impl<Streams: StreamPack> InjectionStreams<Streams> {
    fn new(streams: Streams::StreamStorageBundle, stream_map: StreamTargetMap) -> Self {
        Self {
            streams,
            stream_map,
        }
    }
}

#[derive(Clone, Copy)]
struct Injected {
    session: Entity,
    task: Entity,
    finish: Entity,
}

#[derive(Component)]
struct InjectionSource(Entity);

struct InjectionFinish<Response> {
    _ignore: std::marker::PhantomData<fn(Response)>,
}

impl<Response> InjectionFinish<Response> {
    fn new() -> Self {
        Self {
            _ignore: Default::default(),
        }
    }
}

impl<Response> Impulsive for InjectionFinish<Response>
where
    Response: 'static + Send + Sync,
{
    fn setup(self, _: OperationSetup) -> OperationResult {
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<Response>()?;
        let injector = source_mut.get::<InjectionSource>().or_broken()?.0;
        source_mut.despawn_recursive();
        let mut injector_mut = world.get_entity_mut(injector).or_broken()?;
        let target = injector_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let mut storage = injector_mut.get_mut::<InjectionStorage>().or_broken()?;
        let injected = *storage
            .list
            .iter()
            .find(|injected| injected.finish == source)
            .or_broken()?;
        storage.list.retain(|injected| injected.finish != source);
        let mut task_mut = world.get_entity_mut(injected.task).or_broken()?;
        task_mut.transfer_disposals(injector)?;
        task_mut.despawn_recursive();

        world
            .get_entity_mut(target)
            .or_broken()?
            .give_input(session, data, roster)?;

        Ok(())
    }
}
