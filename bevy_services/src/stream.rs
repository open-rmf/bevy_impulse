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
    prelude::Component,
    ecs::system::EntityCommands,
};

pub trait Stream: Send + Sync + 'static {}

#[derive(Component)]
struct StreamOut<T: Stream>(std::marker::PhantomData<T>);

impl<T: Stream> Default for StreamOut<T> {
    fn default() -> Self {
        StreamOut(Default::default())
    }
}

pub trait IntoStreamOutComponents {
    fn into_stream_out_components(cmds: &mut EntityCommands);
}

impl<T: Stream> IntoStreamOutComponents for T {
    fn into_stream_out_components(cmds: &mut EntityCommands) {
        cmds.insert(StreamOut::<T>::default());
    }
}

impl IntoStreamOutComponents for () {
    fn into_stream_out_components(_: &mut EntityCommands) { }
}

impl<T1: IntoStreamOutComponents> IntoStreamOutComponents for (T1,) {
    fn into_stream_out_components(cmds: &mut EntityCommands) {
        T1::into_stream_out_components(cmds);
    }
}

impl<T1: IntoStreamOutComponents, T2: IntoStreamOutComponents> IntoStreamOutComponents for (T1, T2) {
    fn into_stream_out_components(cmds: &mut EntityCommands) {
        T1::into_stream_out_components(cmds);
        T2::into_stream_out_components(cmds);
    }
}

impl<T1: IntoStreamOutComponents, T2: IntoStreamOutComponents, T3: IntoStreamOutComponents> IntoStreamOutComponents for (T1, T2, T3) {
    fn into_stream_out_components(cmds: &mut EntityCommands) {
        T1::into_stream_out_components(cmds);
        T2::into_stream_out_components(cmds);
        T3::into_stream_out_components(cmds);
    }
}

