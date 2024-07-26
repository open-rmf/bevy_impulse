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
    ManageInput, UnhandledErrors, ManageDisposal, MiscellaneousFailure, OperationRoster,
    OperationResult, OperationError, ScopeStorage, OrBroken, BufferAccessStorage,
    Buffered,
};

use bevy::prelude::{Entity, World, Component};

use std::{
    sync::Arc,
    collections::HashMap,
};

use anyhow::anyhow;

use smallvec::SmallVec;

pub struct OperationCleanup<'a> {
    pub source: Entity,
    pub cleanup: Cleanup,
    pub world: &'a mut World,
    pub roster: &'a mut OperationRoster,
}

impl<'a> OperationCleanup<'a> {
    pub fn clean(&mut self) {
        let Some(cleanup) = self.world.get::<OperationCleanupStorage>(self.source) else {
            return;
        };

        let cleanup = cleanup.0;
        if let Err(error) = cleanup(OperationCleanup {
            source: self.source,
            cleanup: self.cleanup,
            world: self.world,
            roster: self.roster
        }) {
            self.world
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .operations
                .push(error);
        }
    }

    pub fn cleanup_inputs<T: 'static + Send + Sync>(&mut self) -> OperationResult {
        self.world.get_entity_mut(self.source)
            .or_broken()?
            .cleanup_inputs::<T>(self.cleanup.session);
        Ok(())
    }

    pub fn cleanup_disposals(&mut self) -> OperationResult {
        self.world.get_entity_mut(self.source)
            .or_broken()?
            .clear_disposals(self.cleanup.session);
        Ok(())
    }

    pub fn cleanup_buffer_access<B>(&mut self) -> OperationResult
    where
        B: Buffered + 'static + Send + Sync,
        B::Key: 'static + Send + Sync,
    {
        let scope = self.world.get::<ScopeStorage>(self.source).or_broken()?.get();
        if self.cleanup.cleaner == scope {
            // If the scope is telling us to clean up, then we should fully
            // remove the key for this session. Otherwise we should not remove
            // it because it's important that we can continue to track the keys.
            self.world.get_mut::<BufferAccessStorage<B>>(self.source).or_broken()?
                .keys.remove(&self.cleanup.session);
        }
        Ok(())
    }

    pub fn notify_cleaned(&mut self) -> OperationResult {
        let mut cleaner_mut = self.world.get_entity_mut(self.cleanup.cleaner).or_broken()?;
        let mut scope_contents = cleaner_mut.get_mut::<CleanupContents>().or_broken()?;
        if scope_contents.register_cleanup_of_node(self.cleanup.cleanup_id, self.source) {
            self.roster.cleanup_finished(self.cleanup);
        }
        Ok(())
    }

    pub fn for_node(self, source: Entity) -> Self {
        Self { source, ..self }
    }
}

/// The contents that an operation is willing to clean.
#[derive(Default, Component)]
pub struct CleanupContents {
    nodes: SmallVec<[Entity; 16]>,
    cleanup: HashMap<Entity, SmallVec<[Entity; 16]>>,
}

impl CleanupContents {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: Entity) {
        if let Err(index) = self.nodes.binary_search(&node) {
            self.nodes.insert(index, node);
        }
    }

    pub fn register_cleanup_of_node(&mut self, cleanup_id: Entity, node: Entity) -> bool {
        let cleanup = self.cleanup.entry(cleanup_id).or_default();
        if let Err(index) = cleanup.binary_search(&node) {
            cleanup.insert(index, node);
        }

        self.nodes == *cleanup
    }

    pub fn nodes(&self) -> &SmallVec<[Entity; 16]> {
        &self.nodes
    }
}

pub struct FinalizeCleanupRequest<'a> {
    pub cleanup: Cleanup,
    pub world: &'a mut World,
    pub roster: &'a mut OperationRoster,
}

#[derive(Component)]
pub(super) struct OperationCleanupStorage(pub(super) fn(OperationCleanup) -> OperationResult);

#[derive(Component, Clone, Copy)]
pub struct FinalizeCleanup(pub(crate) fn(FinalizeCleanupRequest) -> OperationResult);

impl FinalizeCleanup {
    pub fn new(f: fn(FinalizeCleanupRequest) -> OperationResult) -> Self {
        Self(f)
    }
}

/// Notify the scope manager that the request may be finished with cleanup
#[derive(Clone, Copy, Debug)]
pub struct Cleanup {
    pub cleaner: Entity,
    pub session: Entity,
    // A unique ID for this cleanup operation. For final scope cleanup, this
    // will be equal to the session ID.
    pub cleanup_id: Entity,
}

impl Cleanup {
    pub(crate) fn trigger(self, world: &mut World, roster: &mut OperationRoster) {
        let Some(FinalizeCleanup(f)) = world.get::<FinalizeCleanup>(self.cleaner).copied() else {
            return;
        };
        if let Err(OperationError::Broken(backtrace)) = (f)(
            FinalizeCleanupRequest { cleanup: self, world, roster }
        ) {
            world.get_resource_or_insert_with(|| UnhandledErrors::default())
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow!("Failed to finalize cleanup: {self:?}")),
                    backtrace,
                })
        }
    }
}
