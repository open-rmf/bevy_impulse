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

use bevy_ecs::prelude::{Component, Entity};

use smallvec::{Drain, SmallVec};

use std::collections::HashMap;

use std::{
    iter::Rev,
    ops::RangeBounds,
    slice::{Iter, IterMut},
};

use crate::{BufferSettings, RetentionPolicy};

#[derive(Component)]
pub(crate) struct BufferStorage<T> {
    /// Settings that determine how this buffer will behave.
    settings: BufferSettings,
    /// Map from session ID to a queue of data that has arrived for it. This
    /// is used by nodes that feed into joiner nodes to store input so that it's
    /// readily available when needed.
    ///
    /// The main reason we use this as a reverse queue instead of a forward queue
    /// is because SmallVec doesn't have a version of pop that we can use on the
    /// front. We should reconsider whether this is really a sensible choice.
    reverse_queues: HashMap<Entity, SmallVec<[T; 16]>>,
}

impl<T> BufferStorage<T> {
    pub(crate) fn count(&self, session: Entity) -> usize  {
        self.reverse_queues
            .get(&session)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    pub(crate) fn active_sessions(&self) -> SmallVec<[Entity; 16]> {
        self.reverse_queues.keys().copied().collect()
    }

    fn impl_push(
        reverse_queue: &mut SmallVec<[T; 16]>,
        retention: RetentionPolicy,
        value: T,
    ) -> Option<T> {
        let replaced = match retention {
            RetentionPolicy::KeepFirst(n) => {
                if reverse_queue.len() >= n {
                    // We're at the limit for inputs in this queue so just send
                    // this back
                    return Some(value);
                }

                None
            }
            RetentionPolicy::KeepLast(n) => {
                if n > 0 && reverse_queue.len() >= n {
                    reverse_queue.pop()
                } else if n == 0 {
                    // This can never store any number of entries
                    return Some(value);
                } else {
                    None
                }
            }
            RetentionPolicy::KeepAll => None,
        };

        reverse_queue.insert(0, value);
        replaced
    }

    pub(crate) fn force_push(&mut self, session: Entity, value: T) -> Option<T> {
        Self::impl_push(
            self.reverse_queues.entry(session).or_default(),
            self.settings.retention(),
            value,
        )
    }

    pub(crate) fn push(&mut self, session: Entity, value: T) -> Option<T> {
        let Some(reverse_queue) = self.reverse_queues.get_mut(&session) else {
            return Some(value);
        };

        Self::impl_push(reverse_queue, self.settings.retention(), value)
    }

    pub(crate) fn push_as_oldest(&mut self, session: Entity, value: T) -> Option<T> {
        let Some(reverse_queue) = self.reverse_queues.get_mut(&session) else {
            return Some(value);
        };

        let replaced = match self.settings.retention() {
            RetentionPolicy::KeepFirst(n) => {
                if n > 0 && reverse_queue.len() >= n {
                    Some(reverse_queue.remove(0))
                } else {
                    None
                }
            }
            RetentionPolicy::KeepLast(n) => {
                if reverse_queue.len() >= n {
                    return Some(value);
                }

                None
            }
            RetentionPolicy::KeepAll => None,
        };

        reverse_queue.push(value);
        replaced
    }

    pub(crate) fn pull(&mut self, session: Entity) -> Option<T> {
        self.reverse_queues.get_mut(&session)?.pop()
    }

    pub(crate) fn pull_newest(&mut self, session: Entity) -> Option<T> {
        let reverse_queue = self.reverse_queues.get_mut(&session)?;
        if reverse_queue.is_empty() {
            return None;
        }

        Some(reverse_queue.remove(0))
    }

    pub(crate) fn consume(&mut self, session: Entity) -> SmallVec<[T; 16]> {
        let Some(reverse_queue) = self.reverse_queues.get_mut(&session) else {
            return SmallVec::new();
        };

        let mut result = SmallVec::new();
        std::mem::swap(reverse_queue, &mut result);
        result.reverse();
        result
    }

    pub(crate) fn clear_session(&mut self, session: Entity) {
        self.reverse_queues.remove(&session);
    }

    pub(crate) fn ensure_session(&mut self, session: Entity) {
        self.reverse_queues.entry(session).or_default();
    }

    pub(crate) fn iter_mut(&mut self, session: Entity) -> IterBufferMut<'_, T>
    where
        T: 'static + Send + Sync,
    {
        IterBufferMut {
            iter: self
                .reverse_queues
                .get_mut(&session)
                .map(|q| q.iter_mut().rev()),
        }
    }

    pub(crate) fn oldest_mut(&mut self, session: Entity) -> Option<&mut T> {
        self.reverse_queues
            .get_mut(&session)
            .and_then(|q| q.last_mut())
    }

    pub(crate) fn newest_mut(&mut self, session: Entity) -> Option<&mut T> {
        self.reverse_queues
            .get_mut(&session)
            .and_then(|q| q.first_mut())
    }

    pub(crate) fn get_mut(&mut self, session: Entity, index: usize) -> Option<&mut T> {
        let reverse_queue = self.reverse_queues.get_mut(&session)?;
        let len = reverse_queue.len();
        if len <= index {
            return None;
        }

        reverse_queue.get_mut(len - index - 1)
    }

    pub(crate) fn drain<R>(&mut self, session: Entity, range: R) -> DrainBuffer<'_, T>
    where
        T: 'static + Send + Sync,
        R: RangeBounds<usize>,
    {
        DrainBuffer {
            drain: self
                .reverse_queues
                .get_mut(&session)
                .map(|q| q.drain(range).rev()),
        }
    }

    pub(crate) fn iter(&self, session: Entity) -> IterBufferView<'_, T>
    where
        T: 'static + Send + Sync,
    {
        IterBufferView {
            iter: self.reverse_queues.get(&session).map(|q| q.iter().rev()),
        }
    }

    pub(crate) fn oldest(&self, session: Entity) -> Option<&T> {
        self.reverse_queues.get(&session).and_then(|q| q.last())
    }

    pub(crate) fn newest(&self, session: Entity) -> Option<&T> {
        self.reverse_queues.get(&session).and_then(|q| q.first())
    }

    pub(crate) fn get(&self, session: Entity, index: usize) -> Option<&T> {
        let reverse_queue = self.reverse_queues.get(&session)?;
        let len = reverse_queue.len();
        if len <= index {
            return None;
        }

        reverse_queue.get(len - index - 1)
    }

    pub(crate) fn new(settings: BufferSettings) -> Self {
        Self {
            settings,
            reverse_queues: Default::default(),
        }
    }
}

pub struct IterBufferView<'b, T>
where
    T: 'static + Send + Sync,
{
    iter: Option<Rev<Iter<'b, T>>>,
}

impl<'b, T> Iterator for IterBufferView<'b, T>
where
    T: 'static + Send + Sync,
{
    type Item = &'b T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.iter {
            iter.next()
        } else {
            None
        }
    }
}

pub struct IterBufferMut<'b, T>
where
    T: 'static + Send + Sync,
{
    iter: Option<Rev<IterMut<'b, T>>>,
}

impl<'b, T> Iterator for IterBufferMut<'b, T>
where
    T: 'static + Send + Sync,
{
    type Item = &'b mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.iter {
            iter.next()
        } else {
            None
        }
    }
}

pub struct DrainBuffer<'b, T>
where
    T: 'static + Send + Sync,
{
    drain: Option<Rev<Drain<'b, [T; 16]>>>,
}

impl<'b, T> Iterator for DrainBuffer<'b, T>
where
    T: 'static + Send + Sync,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(drain) = &mut self.drain {
            drain.next()
        } else {
            None
        }
    }
}
