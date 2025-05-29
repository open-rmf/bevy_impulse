/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

use bevy_ecs::prelude::Entity;

use std::{cell::RefCell, rc::Rc};

use smallvec::SmallVec;

pub struct StreamBuffer<T> {
    // TODO(@mxgrey): Consider replacing the Rc with an unsafe pointer so that
    // no heap allocation is needed each time a stream is used in a blocking
    // function.
    pub(crate) container: Rc<RefCell<DefaultStreamBufferContainer<T>>>,
    pub(crate) target: Option<Entity>,
}

impl<Container> Clone for StreamBuffer<Container> {
    fn clone(&self) -> Self {
        Self {
            container: Rc::clone(&self.container),
            target: self.target,
        }
    }
}

impl<T> StreamBuffer<T> {
    pub fn send(&self, input: T) {
        self.container.borrow_mut().push(input);
    }

    pub fn target(&self) -> Option<Entity> {
        self.target
    }
}

pub type DefaultStreamBufferContainer<T> = SmallVec<[T; 16]>;
