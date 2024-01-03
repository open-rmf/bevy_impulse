/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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

use bevy::{
    prelude::{Component, Bundle},
    ecs::{
        world::EntityMut,
        system::EntityCommands,
    },
};

pub trait Stream: Send + Sync + 'static {}

/// StreamOut is a marker component that indicates what streams are offered by
/// a service.
#[derive(Component)]
pub struct StreamOut<T: Stream>(std::marker::PhantomData<T>);

impl<T: Stream> Default for StreamOut<T> {
    fn default() -> Self {
        StreamOut(Default::default())
    }
}

#[derive(Component)]
struct StreamHandler<T: Stream> {
    handler: Box<dyn FnMut(T) + 'static + Send + Sync>,
}

pub trait IntoStreamBundle {
    type StreamOutBundle: Bundle + Default;
}

impl<T: Stream> IntoStreamBundle for T {
    type StreamOutBundle = StreamOut<T>;
}

impl IntoStreamBundle for () {
    type StreamOutBundle = ();
}

impl<T1: Stream> IntoStreamBundle for (T1,) {
    type StreamOutBundle = StreamOut<T1>;
}

impl<T1: IntoStreamBundle, T2: IntoStreamBundle> IntoStreamBundle for (T1, T2) {
    type StreamOutBundle = (T1::StreamOutBundle, T2::StreamOutBundle);
}

impl<T1: IntoStreamBundle, T2: IntoStreamBundle, T3: IntoStreamBundle> IntoStreamBundle for (T1, T2, T3) {
    type StreamOutBundle = (T1::StreamOutBundle, T2::StreamOutBundle, T3::StreamOutBundle);
}

