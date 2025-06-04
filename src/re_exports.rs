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

//! This module contains symbols that are being re-exported so they can be used
//! by bevy_impulse_derive.

pub use bevy_ecs::prelude::{Commands, Entity, With, World};

pub use smallvec::{smallvec, SmallVec};

pub use std::{
    clone::Clone,
    marker::Copy,
};

// The std::any implementation of this is not stable in v1.75, so we provide a
// simple implementation in this module for the derive macros.
pub fn type_name_of_val<T>(_: &T) -> &'static str {
    std::any::type_name::<T>()
}
