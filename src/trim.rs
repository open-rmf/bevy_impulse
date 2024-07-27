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

use bevy::prelude::Entity;

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
        Self {
            from_point: TrimPoint::inclusive(point),
            policy: TrimPolicy::Span(Default::default())
        }
    }

    /// Trim everything downstream from the initial point.
    ///
    /// In the event of any cycles, any nodes between the scope entry point and
    /// the initial trim point will not be included.
    pub fn downstream(from_point: impl Into<TrimPoint>) -> Self {
        Self { from_point: from_point.into(), policy: TrimPolicy::Downstream }
    }

    /// Trim the nodes that fill the span between two points.
    pub fn between(
        from_point: impl Into<TrimPoint>,
        to_point: impl Into<TrimPoint>,
    ) -> Self {
        Self::span(from_point, [to_point])
    }

    /// Trim every node that exists along some path between the initial point and
    /// any point in the set of endpoints.
    ///
    /// In the event of any cycles, any nodes which lead back to the initial
    /// point without also leading to one of the endpoints will not be included.
    ///
    /// If the set of endpoints are emtpy, this behaves the same as [`Self::single_point`].
    pub fn span<Endpoints>(
        from_point: impl Into<TrimPoint>,
        endpoints: Endpoints,
    ) -> Self
    where
        Endpoints: IntoIterator,
        Endpoints::Item: Into<TrimPoint>,
    {
        Self {
            from_point: from_point.into(),
            policy: TrimPolicy::Span(
                endpoints.into_iter().map(|p| p.into()).collect()
            ),
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
        Self { id: input.id(), scope: input.scope(), inclusive }
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
pub(crate) enum TrimPolicy {
    Downstream,
    Span(SmallVec<[TrimPoint; 16]>),
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};

    #[test]
    fn test_trimming() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let fork_input = scope.input.fork_clone(builder);

            let doubler_a = builder.create_node((
                |value| async move {
                    2.0 * value
                }
            ).into_async_map());
            fork_input.clone_chain(builder).connect(doubler_a.input);
            builder.connect(doubler_a.output, scope.terminate);

            let trim = builder.create_trim::<f64>(Some(TrimBranch::downstream(doubler_a.input)));
            fork_input.clone_chain(builder).connect(trim.input);

            let doubler_b = builder.create_node(double.into_blocking_map());
            builder.connect(trim.output, doubler_b.input);
            builder.connect(doubler_b.output, doubler_a.input);
        });

        let mut promise = context.command(|commands| {
            commands
            .request(2.0, workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|v| v == 8.0));
        assert!(context.no_unhandled_errors());
    }
}
