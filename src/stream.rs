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

use bevy::prelude::{Component, Bundle, Entity, Commands};

use crate::{InputSlot, Output, UnusedTarget};

pub trait Stream: 'static + Send + Sync + Sized {
    fn spawn_stream(scope: Entity, commands: &mut Commands) -> (
        StreamTargetStorage<Self>, InputSlot<Self>,
    ) {
        let target = commands.spawn(UnusedTarget).id();
        (
            StreamTargetStorage::new(target),
            InputSlot::new(scope, target),
        )
    }
}

/// [`StreamAvailable`] is a marker component that indicates what streams are offered by
/// a service.
#[derive(Component)]
pub struct StreamAvailable<T: Stream> {
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> Default for StreamAvailable<T> {
    fn default() -> Self {
        Self { _ignore: Default::default() }
    }
}

/// [`StreamTargetStorage`] keeps track of the target for each stream for a source.
#[derive(Component)]
pub struct StreamTargetStorage<T: Stream> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> StreamTargetStorage<T> {
    fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }

    pub fn get(&self) -> Entity {
        self.target
    }
}

pub trait StreamPack: 'static + Send + Sync {
    type StreamAvailableBundle: Bundle + Default;
    type StreamStorageBundle: Bundle;
    type StreamInputPack;
    type StreamOutputPack;

    fn spawn_scope_streams(scope: Entity, commands: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::StreamInputPack,
    );

    fn spawn_output_streams(scope: Entity, source: Entity) -> Self::StreamOutputPack;
}

impl<T: Stream> StreamPack for T {
    type StreamAvailableBundle = StreamAvailable<Self>;
    type StreamStorageBundle = StreamTargetStorage<Self>;
    type StreamInputPack = InputSlot<Self>;
    type StreamOutputPack = Output<Self>;

    fn spawn_streams(scope: Entity, commands: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::StreamInputPack,
    ) {
        T::spawn_stream(scope, commands)
    }

    fn stream_outputs(scope: Entity, source: Entity) -> Self::StreamOutputPack {
        InputSlot::new(scope, source)
    }
}

impl StreamPack for () {
    type StreamAvailableBundle = ();
    type StreamStorageBundle = ();
    type StreamInputPack = ();
    type StreamOutputPack = ();

    fn spawn_streams(_: Entity, _: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::StreamInputPack,
    ) {
        ((), ())
    }

    fn stream_outputs(_: Entity, _: Entity) -> Self::StreamOutputPack {
        ()
    }
}

impl<T1: StreamPack> StreamPack for (T1,) {
    type StreamAvailableBundle = T1::StreamAvailableBundle;
    type StreamStorageBundle = T1::StreamStorageBundle;
    type StreamInputPack = T1::StreamInputPack;
    type StreamOutputPack = T1::StreamOutputPack;

    fn spawn_streams(scope: Entity, commands: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::StreamInputPack,
    ) {
        T1::spawn_streams(scope, commands)
    }

    fn stream_outputs(scope: Entity, source: Entity) -> Self::StreamOutputPack {
        T1::stream_outputs(scope, source)
    }
}

impl<T1: StreamPack, T2: StreamPack> StreamPack for (T1, T2) {
    type StreamAvailableBundle = (T1::StreamAvailableBundle, T2::StreamAvailableBundle);
    type StreamStorageBundle = (T1::StreamStorageBundle, T2::StreamStorageBundle);
    type StreamInputPack = (T1::StreamInputPack, T2::StreamInputPack);
    type StreamOutputPack = (T1::StreamOutputPack, T2::StreamOutputPack);

    fn spawn_streams(scope: Entity, commands: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::StreamInputPack,
    ) {
        let t1 = T1::spawn_streams(scope, commands);
        let t2 = T2::spawn_streams(scope, commands);
        ((t1.0, t2.0), (t1.1, t2.1))
    }

    fn stream_outputs(scope: Entity, source: Entity) -> Self::StreamOutputPack {
        (T1::stream_outputs(scope, source), T2::stream_outputs(scope, source))
    }
}

impl<T1: StreamPack, T2: StreamPack, T3: StreamPack> StreamPack for (T1, T2, T3) {
    type StreamAvailableBundle = (T1::StreamAvailableBundle, T2::StreamAvailableBundle, T3::StreamAvailableBundle);
    type StreamStorageBundle = (T1::StreamStorageBundle, T2::StreamStorageBundle, T3::StreamStorageBundle);
    type StreamInputPack = (T1::StreamInputPack, T2::StreamInputPack, T3::StreamInputPack);
    type StreamOutputPack = (T1::StreamOutputPack, T2::StreamOutputPack, T3::StreamOutputPack);

    fn spawn_streams(scope: Entity, commands: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::StreamInputPack,
    ) {
        let t1 = T1::spawn_streams(scope, commands);
        let t2 = T2::spawn_streams(scope, commands);
        let t3 = T3::spawn_streams(scope, commands);
        ((t1.0, t2.0, t3.0), (t1.1, t2.1, t3.1))
    }

    fn stream_outputs(scope: Entity, source: Entity) -> Self::StreamOutputPack {
        (
            T1::stream_outputs(scope, source),
            T2::stream_outputs(scope, source),
            T3::stream_outputs(scope, source),
        )
    }
}
