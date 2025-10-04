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

use crate::{
    FunnelInputStorage, Input, InputBundle, Joining, ManageInput, Operation, OperationCleanup,
    OperationError, OperationReachability, OperationRequest, OperationResult, OperationSetup,
    OrBroken, ReachabilityResult, SingleInputStorage, SingleTargetStorage,
};

pub(crate) struct Join<Buffers> {
    buffers: Buffers,
    target: Entity,
}

impl<Buffers> Join<Buffers> {
    pub(crate) fn new(buffers: Buffers, target: Entity) -> Self {
        Self { buffers, target }
    }
}

#[derive(Component)]
struct BufferStorage<Buffers>(Buffers);

impl<Buffers: Joining + 'static + Send + Sync> Operation for Join<Buffers>
where
    Buffers::Item: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        self.buffers.add_listener(source, world)?;

        world.entity_mut(source).insert((
            FunnelInputStorage::from(self.buffers.as_input()),
            BufferStorage(self.buffers),
            InputBundle::<()>::new(),
            SingleTargetStorage::new(self.target),
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
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, .. } = source_mut.take_input::<()>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let buffers = source_mut
            .get::<BufferStorage<Buffers>>()
            .or_broken()?
            .0
            .clone();
        if buffers.buffered_count(session, world)? < 1 {
            return Err(OperationError::NotReady);
        }

        let output = buffers.pull(session, world)?;
        world
            .get_entity_mut(target)
            .or_broken()?
            .give_input(session, output, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<()>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        let inputs = r
            .world
            .get_entity(r.source)
            .or_broken()?
            .get::<FunnelInputStorage>()
            .or_broken()?;
        for input in &inputs.0 {
            if !r.check_upstream(*input)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*};

    #[test]
    fn test_join_reachability() {
        // This is a regression test for a subtle bug in how reachability was
        // being calculated for join. Items already waiting inside the buffer
        // weren't being accounted for. This test will make sure that one of the
        // buffers being joined has an item sitting inside it with no other
        // activity upstream when a disposal notification gets triggered.

        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let node = builder.create_map(|input: AsyncMap<f64, StreamOf<f64>>| async move {
                input.streams.send(input.request);
                let never = async_std::future::pending::<()>();
                let _ = async_std::future::timeout(Duration::from_millis(1), never).await;
                input.request
            });

            builder.connect(scope.input, node.input);

            let lhs_buffer = builder.create_buffer(BufferSettings::default());
            builder.connect(node.streams, lhs_buffer.input_slot());

            let rhs_buffer = builder.create_buffer(BufferSettings::default());
            builder.connect(node.output, rhs_buffer.input_slot());

            builder
                .join((lhs_buffer, rhs_buffer))
                .map_block(|(a, b)| a + b)
                .connect(scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request(2.0, workflow).take_response());
        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(
            context.no_unhandled_errors(),
            "{:?}",
            context.get_unhandled_errors()
        );
        let r = promise.take().available().unwrap();
        assert_eq!(r, 4.0);
    }
}
