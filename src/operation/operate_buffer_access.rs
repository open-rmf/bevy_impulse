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

use bevy_ecs::prelude::{Component, Entity, World};

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use smallvec::SmallVec;

use crate::{
    Accessed, BufferKeyBuilder, ChannelQueue, Input, InputBundle, ManageInput, Operation,
    OperationCleanup, OperationError, OperationReachability, OperationRequest, OperationResult,
    OperationSetup, OrBroken, ReachabilityResult, ScopeStorage, SingleInputStorage,
    SingleTargetStorage,
};

pub(crate) struct OperateBufferAccess<T, B>
where
    T: 'static + Send + Sync,
    B: Accessed,
{
    buffers: B,
    target: Entity,
    _ignore: std::marker::PhantomData<fn(T, B)>,
}

impl<T, B> OperateBufferAccess<T, B>
where
    T: 'static + Send + Sync,
    B: Accessed,
{
    pub(crate) fn new(buffers: B, target: Entity) -> Self {
        Self {
            buffers,
            target,
            _ignore: Default::default(),
        }
    }
}

#[derive(Component)]
pub struct BufferKeyUsage(pub(crate) fn(Entity, Entity, &World) -> ReachabilityResult);

#[derive(Component)]
pub(crate) struct BufferAccessStorage<B: Accessed> {
    pub(crate) buffers: B,
    pub(crate) keys: HashMap<Entity, B::Key>,
}

impl<B: Accessed> BufferAccessStorage<B> {
    pub(crate) fn new(buffers: B) -> Self {
        Self {
            buffers,
            keys: HashMap::new(),
        }
    }
}

impl<T, B> Operation for OperateBufferAccess<T, B>
where
    T: 'static + Send + Sync,
    B: Accessed + 'static + Send + Sync,
    B::Key: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        self.buffers.add_accessor(source, world)?;
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            BufferAccessStorage::new(self.buffers),
            SingleTargetStorage::new(self.target),
            BufferKeyUsage(buffer_key_usage::<B>),
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
        let Input { session, data } = world
            .get_entity_mut(source)
            .or_broken()?
            .take_input::<T>()?;

        let keys = get_access_keys::<B>(source, session, world)?;

        let target = world.get::<SingleTargetStorage>(source).or_broken()?.get();
        world
            .get_entity_mut(target)
            .or_broken()?
            .give_input(session, (data, keys), roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.cleanup_buffer_access::<B>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut r)
    }
}

pub(crate) fn get_access_keys<B>(
    source: Entity,
    session: Entity,
    world: &mut World,
) -> Result<B::Key, OperationError>
where
    B: Accessed + 'static + Send + Sync,
    B::Key: 'static + Send + Sync,
{
    let scope = world.get::<ScopeStorage>(source).or_broken()?.get();
    let sender = world
        .get_resource_or_insert_with(ChannelQueue::default)
        .sender
        .clone();

    let mut storage = world
        .get_mut::<BufferAccessStorage<B>>(source)
        .or_broken()?;
    let s = storage.as_mut();
    let mut made_key = false;
    let keys = match s.keys.entry(session) {
        Entry::Occupied(occupied) => B::deep_clone_key(occupied.get()),
        Entry::Vacant(vacant) => {
            made_key = true;
            let builder =
                BufferKeyBuilder::with_tracking(scope, session, source, sender, Arc::new(()));
            let new_key = vacant.insert(s.buffers.create_key(&builder));
            B::deep_clone_key(new_key)
        }
    };

    if made_key {
        // If we needed to make a new key for this session then we should
        // ensure that the session is active in the buffer before we send
        // off the keys.
        let buffers = s.buffers.clone();
        buffers.ensure_active_session(session, world)?;
    }

    Ok(keys)
}

pub(crate) fn buffer_key_usage<B>(
    accessor: Entity,
    session: Entity,
    world: &World,
) -> ReachabilityResult
where
    B: Accessed + 'static + Send + Sync,
    B::Key: 'static + Send + Sync,
{
    let key = world
        .get::<BufferAccessStorage<B>>(accessor)
        .or_broken()?
        .keys
        .get(&session);
    if let Some(key) = key {
        if B::is_key_in_use(key) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Buffer access nodes are siblings nodes in a workflow which can access the
/// buffer, potentially in a mutable way. Their outputs do not get fed to the
/// buffer, so they are not considered input nodes, but they may modify the
/// contents of the buffer, which includes pushing new data, so they affect
/// the reachability of the buffer.
#[derive(Component, Default)]
pub(crate) struct BufferAccessors(pub(crate) SmallVec<[Entity; 8]>);

impl BufferAccessors {
    pub(crate) fn add_accessor(&mut self, accessor: Entity) {
        self.0.push(accessor);
        self.0.sort();
        self.0.dedup();
    }

    pub(crate) fn is_reachable(r: &mut OperationReachability) -> ReachabilityResult {
        let Some(accessors) = r.world.get::<Self>(r.source) else {
            return Ok(false);
        };

        for accessor in &accessors.0 {
            let usage = r.world.get::<BufferKeyUsage>(*accessor).or_broken()?.0;
            if usage(*accessor, r.session, r.world)? {
                return Ok(true);
            }
        }

        for accessor in &accessors.0 {
            if r.check_upstream(*accessor)? {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
