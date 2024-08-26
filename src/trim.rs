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

use crate::InputSlot;

use bevy_ecs::prelude::Entity;

use smallvec::SmallVec;

/// Define a branch for the trim operator to cancel all activity along.
#[derive(Clone, Debug)]
pub struct TrimBranch {
    from_point: TrimPoint,
    policy: TrimPolicy,
}

impl TrimBranch {
    /// Just cancel a single node in the workflow. If the provided [`TrimPoint`]
    /// is not inclusive then this will do nothing at all.
    pub fn single_point<T>(point: &InputSlot<T>) -> Self {
        let from_point = TrimPoint::inclusive(point);
        Self {
            from_point,
            policy: TrimPolicy::Span(SmallVec::from_iter(Some(from_point))),
        }
    }

    /// Trim everything downstream from the initial point.
    ///
    /// In the event of any cycles, any nodes between the scope entry point and
    /// the initial trim point will not be included.
    pub fn downstream(from_point: impl Into<TrimPoint>) -> Self {
        Self {
            from_point: from_point.into(),
            policy: TrimPolicy::Downstream,
        }
    }

    /// Trim the nodes that fill the span between two points.
    pub fn between(from_point: impl Into<TrimPoint>, to_point: impl Into<TrimPoint>) -> Self {
        Self::span(from_point, [to_point])
    }

    /// Trim every node that exists along some path between the initial point and
    /// any point in the set of endpoints.
    ///
    /// In the event of any cycles, any nodes which lead back to the initial
    /// point without also leading to one of the endpoints will not be included.
    ///
    /// If the set of endpoints are emtpy, this behaves the same as [`Self::single_point`].
    pub fn span<Endpoints>(from_point: impl Into<TrimPoint>, endpoints: Endpoints) -> Self
    where
        Endpoints: IntoIterator,
        Endpoints::Item: Into<TrimPoint>,
    {
        Self {
            from_point: from_point.into(),
            policy: TrimPolicy::Span(endpoints.into_iter().map(|p| p.into()).collect()),
        }
    }

    pub fn from_point(&self) -> TrimPoint {
        self.from_point
    }

    pub(crate) fn policy(&self) -> &TrimPolicy {
        &self.policy
    }

    pub(crate) fn verify_scope(&self, scope: Entity) {
        assert_eq!(self.from_point.scope, scope);
        if let TrimPolicy::Span(span) = &self.policy {
            for point in span {
                assert_eq!(point.scope, scope);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TrimPoint {
    id: Entity,
    scope: Entity,
    inclusive: bool,
}

impl TrimPoint {
    /// Define where a trim will begin or end
    //
    // TODO(@mxgrey): It would be good if we could also accept an Output<T> as
    // a reference point, but there is a risk that the output ID will be
    // invalidated after it gets connected to another node. We would need to do
    // additional bookkeeping to update every trim operation about the change
    // during the connection command. This is doable but seems error prone, so
    // we are deprioritizing it for now.
    pub fn new<T>(input: &InputSlot<T>, inclusive: bool) -> Self {
        Self {
            id: input.id(),
            scope: input.scope(),
            inclusive,
        }
    }

    /// Define where a trim will begin or end, and include the point as part of
    /// the trimming.
    pub fn inclusive<T>(input: &InputSlot<T>) -> Self {
        Self::new(input, true)
    }

    /// Define where a trim will begin or end, and exclude the point from being
    /// trimmed.
    pub fn exclusive<T>(input: &InputSlot<T>) -> Self {
        Self::new(input, false)
    }

    /// Get the ID of this point
    pub fn id(&self) -> Entity {
        self.id
    }

    /// Check if this point should be included in the branch
    pub fn is_inclusive(&self) -> bool {
        self.inclusive
    }

    pub(crate) fn accept(&self, id: Entity) -> bool {
        self.is_inclusive() || id != self.id
    }
}

impl<T> From<InputSlot<T>> for TrimPoint {
    fn from(input: InputSlot<T>) -> Self {
        TrimPoint::inclusive(&input)
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum TrimPolicy {
    Downstream,
    Span(SmallVec<[TrimPoint; 16]>),
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*};
    use std::sync::mpsc::channel;

    #[test]
    fn test_trimming() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let fork_input = scope.input.fork_clone(builder);

            let noop = fork_input.clone_chain(builder).noop_node();
            let doubler_a =
                builder.create_node((|value| async move { 2.0 * value }).into_async_map());
            builder.connect(noop.output, doubler_a.input);
            builder.connect(doubler_a.output, scope.terminate);

            let trim = builder.create_trim::<f64>(Some(TrimBranch::downstream(noop.input)));
            fork_input.clone_chain(builder).connect(trim.input);

            let doubler_b = builder.create_node(double.into_blocking_map());
            builder.connect(trim.output, doubler_b.input);
            builder.connect(doubler_b.output, doubler_a.input);
        });

        let mut promise =
            context.command(|commands| commands.request(2.0, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|v| v == 8.0));
        assert!(context.no_unhandled_errors());

        let delay = context.spawn_delay::<i32>(Duration::from_secs(20));
        let workflow = context.spawn_io_workflow(|scope, builder| {
            let injection: Node<_, _, StreamOf<()>> =
                scope.input.chain(builder).then_injection_node();
            injection.output.chain(builder).connect(scope.terminate);

            injection
                .streams
                .chain(builder)
                .map(print_debug("About to trim"))
                .then_trim(Some(TrimBranch::single_point(&injection.input)))
                .map_block(|_| 2)
                .connect(scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request((1, delay), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(promise.take().available().is_some_and(|v| v == 2));
        assert!(context.no_unhandled_errors());

        let (sender, receiver) = channel::<()>();
        let inner_workflow = context.spawn_workflow(|scope, builder| {
            let node = scope.input.chain(builder).then_node(delay);
            builder.connect(node.output, scope.terminate);
            builder.connect(node.streams, scope.streams);

            let buffer = builder.create_buffer::<()>(BufferSettings::keep_all());
            builder.on_cancel(buffer, |scope, builder| {
                scope
                    .input
                    .chain(builder)
                    .map_block(move |_| {
                        // This is the real test: That the cleanup of the
                        // workflow worked as intended.
                        sender.send(()).unwrap();
                    })
                    .connect(scope.terminate);
            });
        });

        let mut promise = context.command(|commands| {
            commands
                .request((1, inner_workflow), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(promise.take().available().is_some_and(|v| v == 2));
        assert!(receiver.try_recv().is_ok());
        assert!(context.no_unhandled_errors());
    }

    // TODO(@mxgrey): It would be good to have a testing-only node whose entire
    // purpose is to track when it's been told to cleanup so we can test that
    // the right nodes in the topology are being trimmed.
}
