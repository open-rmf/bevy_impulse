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
    prelude::Entity,
    world::{EntityWorldMut, EntityRef},
};

use smallvec::SmallVec;

use crate::{OperationError, OrBroken, OperationResult, BufferStorage};

pub trait InspectBuffer {
    fn buffered_count<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<usize, OperationError>;

    fn try_clone_from_buffer<T: 'static + Send + Sync + Clone>(
        &self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>;

    fn buffered_sessions<T: 'static + Send + Sync>(
        &self,
    ) -> Result<SmallVec<[Entity; 16]>, OperationError>;
}

impl<'w> InspectBuffer for EntityRef<'w> {
    fn buffered_count<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<usize, OperationError> {
        let buffer = self.get::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.count(session))
    }

    fn try_clone_from_buffer<T: 'static + Send + Sync + Clone>(
        &self,
        session: Entity,
    ) -> Result<Option<T>, OperationError> {
        let buffer = self.get::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.oldest(session).cloned())
    }

    fn buffered_sessions<T: 'static + Send + Sync>(
        &self,
    ) -> Result<SmallVec<[Entity; 16]>, OperationError> {
        let sessions = self.get::<BufferStorage<T>>().or_broken()?
            .active_sessions();

        Ok(sessions)
    }
}

pub trait ManageBuffer {
    fn pull_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<T, OperationError> {
        self.try_pull_from_buffer(session).and_then(|r| r.or_broken())
    }

    fn try_pull_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>;

    fn consume_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<SmallVec<[T; 16]>, OperationError>;

    fn clear_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> OperationResult;
}

impl<'w> ManageBuffer for EntityWorldMut<'w> {
    fn try_pull_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<Option<T>, OperationError> {
        let mut buffer = self.get_mut::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.pull(session))
    }

    fn consume_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<SmallVec<[T; 16]>, OperationError> {
        let mut buffer = self.get_mut::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.consume(session))
    }

    fn clear_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> OperationResult {
        self.get_mut::<BufferStorage<T>>().or_broken()?
            .clear_session(session);
        Ok(())
    }

}
