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

use bevy::prelude::Entity;

use crate::{Builder, Chain, UnusedTarget, OnNewBufferValue, InputSlot};

mod buffered;
pub use buffered::*;

mod bufferable;
pub use bufferable::*;

/// A buffer is a special type of node within a workflow that is able to store
/// and release data. When a session is finished, the buffered data from the
/// session will be automatically cleared.
pub struct Buffer<T> {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) _ignore: std::marker::PhantomData<T>,
}

impl<T> Buffer<T> {
    /// Get a unit `()` trigger output each time a new value is added to the buffer.
    pub fn on_new_value<'w, 's, 'a, 'b>(
        &self,
        builder: &'b mut Builder<'w, 's, 'a>
    ) -> Chain<'w, 's, 'a, 'b, ()> {
        assert_eq!(self.scope, builder.scope);
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(OnNewBufferValue::new(self.source, target));
        Chain::new(target, builder)
    }

    /// Specify that you want to pull from this Buffer by cloning. This can be
    /// used by operations like join to tell them that they should clone from
    /// the buffer instead of consuming from it.
    pub fn by_cloning(self) -> CloneFromBuffer<T>
    where
        T: Clone,
    {
        CloneFromBuffer {
            scope: self.scope,
            source: self.source,
            _ignore: Default::default()
        }
    }

    /// Get an input slot for this buffer.
    pub fn input_slot(self) -> InputSlot<T> {
        InputSlot::new(self.scope, self.source)
    }
}

pub struct CloneFromBuffer<T: Clone> {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) _ignore: std::marker::PhantomData<T>,
}

/// Settings to describe the behavior of a buffer.
#[derive(Default, Clone, Copy)]
pub struct BufferSettings {
    retention: RetentionPolicy,
}

impl BufferSettings {
    /// Get the retention policy for the buffer.
    pub fn retention(&self) -> RetentionPolicy {
        self.retention
    }
}

/// Describe how data within a buffer gets retained. Most mechanisms that pull
/// data from a buffer will remove the oldest item in the buffer, so this policy
/// is for dealing with situations where items are being stored faster than they
/// are being pulled.
///
/// The default value is KeepLast(1).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RetentionPolicy {
    /// Keep the last N items that were stored into the buffer. Once the limit
    /// is reached, the oldest item will be removed any time a new item arrives.
    KeepLast(usize),
    /// Keep the first N items that are stored into the buffer. Once the limit
    /// is reached, any new item that arrives will be discarded.
    KeepFirst(usize),
    /// Do not limit how many items can be stored in the buffer.
    KeepAll,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::KeepLast(1)
    }
}

impl<T> Clone for Buffer<T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope,
            source: self.source,
            _ignore: Default::default(),
        }
    }
}

impl<T> Copy for Buffer<T> {}

impl<T: Clone> Clone for CloneFromBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope,
            source: self.source,
            _ignore: Default::default(),
        }
    }
}

impl<T: Clone> Copy for CloneFromBuffer<T> {}
