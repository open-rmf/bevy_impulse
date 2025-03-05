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

use bevy_ecs::{
    prelude::{Entity, Query, With},
    query::{QueryEntityError, QueryIter, ReadOnlyWorldQuery},
    system::SystemParam,
};

use crate::{Service, ServiceMarker, StreamFilter, StreamAvailability};

/// `ServiceDiscovery` is a system parameter that lets you find services that
/// exist in the world.
///
/// You must specify a `Request` and a `Response` type which the service's input
/// and output are required to match. You can optionally specify a [`StreamFilter`]
/// to indicate which streams you are interested in.
#[derive(SystemParam)]
#[allow(clippy::type_complexity)]
pub struct ServiceDiscovery<'w, 's, Request, Response, StreamFilters = (), ServiceFilter = ()>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    StreamFilters: StreamFilter + 'static,
    ServiceFilter: ReadOnlyWorldQuery + 'static,
{
    query: Query<
        'w,
        's,
        (
            Entity,
            Option<&'static StreamAvailability>,
        ),
        (
            With<ServiceMarker<Request, Response>>,
            ServiceFilter,
        ),
    >,
    _ignore: std::marker::PhantomData<fn(StreamFilters)>,
}

impl<'w, 's, Request, Response, StreamFilters, ServiceFilter>
    ServiceDiscovery<'w, 's, Request, Response, StreamFilters, ServiceFilter>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    StreamFilters: StreamFilter,
    ServiceFilter: ReadOnlyWorldQuery + 'static,
{
    pub fn iter(&self) -> IterServiceDiscovery<'_, 's, Request, Response, StreamFilters, ServiceFilter> {
        IterServiceDiscovery {
            inner: self.query.iter(),
            _ignore: Default::default(),
        }
    }

    pub fn get(
        &self,
        entity: Entity,
    ) -> Result<Service<Request, Response, StreamFilters::Pack>, QueryEntityError> {
        self.query.get(entity)
            .and_then(|(e, availability)| {
                if StreamFilters::are_required_streams_available(availability) {
                    Ok(Service::new(e))
                } else {
                    Err(QueryEntityError::QueryDoesNotMatch(e))
                }
            })
    }
}

#[allow(clippy::type_complexity)]
pub struct IterServiceDiscovery<'w, 's, Request, Response, StreamFilters, ServiceFilter>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    StreamFilters: StreamFilter,
    ServiceFilter: ReadOnlyWorldQuery + 'static,
{
    inner: QueryIter<
        'w,
        's,
        (
            Entity,
            Option<&'static StreamAvailability>,
        ),
        (
            With<ServiceMarker<Request, Response>>,
            ServiceFilter,
        ),
    >,
    _ignore: std::marker::PhantomData<fn(StreamFilters)>,
}

impl<'w, 's, Request, Response, StreamFilters, ServiceFilter> Iterator
    for IterServiceDiscovery<'w, 's, Request, Response, StreamFilters, ServiceFilter>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    StreamFilters: StreamFilter,
    ServiceFilter: ReadOnlyWorldQuery + 'static,
{
    type Item = Service<Request, Response, StreamFilters::Pack>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Some((service, availability)) => {
                    if StreamFilters::are_required_streams_available(availability) {
                        return Some(Service::new(service));
                    }

                    // Continue looping
                }
                None => {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, Require};
    use bevy_ecs::prelude::With;

    type NumberStreams = (StreamOf<u32>, StreamOf<i32>, StreamOf<f32>);

    #[test]
    fn test_discovery() {
        let mut context = TestingContext::minimal_plugins();
        let doubling_service = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<f64, NumberStreams>| {
                let double = 2.0 * input.request;
                input.streams.0.send(double as u32);
                input.streams.1.send(double as i32);
                input.streams.2.send(double as f32);
                double
            })
        });

        let service_finder = context.command(|commands| {
            commands.spawn_service(
                |_: BlockingServiceInput<()>, discover: ServiceDiscovery<f64, f64, ()>| {
                    discover.iter().next()
                },
            )
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        let found_service = found_service.take().available().flatten().unwrap();
        assert_eq!(doubling_service.provider(), found_service.provider());

        let service_finder = context.command(|commands| {
            commands.spawn_service(
                |_: BlockingServiceInput<()>,
                 discover: ServiceDiscovery<f64, f64, Require<NumberStreams>>| {
                    discover.iter().next()
                },
            )
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        let found_service = found_service.take().available().flatten().unwrap();
        assert_eq!(doubling_service.provider(), found_service.provider());

        let service_finder = context.command(|commands| {
            commands.spawn_service(
                |_: BlockingServiceInput<()>,
                 discover: ServiceDiscovery<f64, f64, Option<NumberStreams>>| {
                    discover.iter().next()
                },
            )
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        let found_service = found_service.take().available().flatten().unwrap();
        assert_eq!(doubling_service.provider(), found_service.provider());

        let service_finder = context.command(|commands| {
            commands.spawn_service(
                |_: BlockingServiceInput<()>,
                 discover: ServiceDiscovery<f64, f64, Require<StreamOf<String>>>| {
                    discover.iter().next()
                },
            )
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        assert!(found_service.take().available().flatten().is_none());

        let service_finder = context.command(|commands| {
            commands.spawn_service(
                |_: BlockingServiceInput<()>,
                 discover: ServiceDiscovery<
                    f64,
                    f64,
                    (Require<StreamOf<String>>, Option<StreamOf<f32>>),
                >| { discover.iter().next() },
            )
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        assert!(found_service.take().available().flatten().is_none());

        let service_finder = context.command(|commands| {
            commands.spawn_service(
                |_: BlockingServiceInput<()>,
                 discover: ServiceDiscovery<
                    f64,
                    f64,
                    Require<NumberStreams>,
                    With<TestComponent>,
                >| { discover.iter().next() },
            )
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        assert!(found_service.take().available().flatten().is_none());

        // Add the missing component that's filtering the discovery and then try
        // running the service finder again.
        context.command(|commands| {
            commands
                .entity(doubling_service.provider())
                .insert(TestComponent);
        });

        let mut found_service =
            context.command(|commands| commands.request((), service_finder).take_response());

        context.run_while_pending(&mut found_service);
        let found_service = found_service.take().available().flatten().unwrap();
        assert_eq!(doubling_service.provider(), found_service.provider());
    }
}
