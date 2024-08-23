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

use bevy_ecs::prelude::{Bundle, Commands, Component, Entity, Event};
use bevy_hierarchy::BuildChildren;

use std::future::Future;

use crate::{
    AsMapOnce, Cancellable, IntoAsyncMapOnce, IntoBlockingMapOnce, Promise, ProvideOnce, Sendish,
    StreamPack, StreamTargetMap, UnusedTarget,
};

mod detach;
pub(crate) use detach::*;

mod finished;
pub(crate) use finished::*;

mod insert;
pub(crate) use insert::*;

mod internal;
pub(crate) use internal::*;

mod map;
pub(crate) use map::*;

mod push;
pub(crate) use push::*;

mod send_event;
pub(crate) use send_event::*;

mod store;
pub(crate) use store::*;

mod taken;
pub(crate) use taken::*;

/// Impulses can be chained as a simple sequence of [providers](crate::Provider).
pub struct Impulse<'w, 's, 'a, Response, Streams> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) commands: &'a mut Commands<'w, 's>,
    pub(crate) _ignore: std::marker::PhantomData<fn(Response, Streams)>,
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    /// Keep executing the impulse chain up to here even if a downstream
    /// dependent gets dropped. If you continue building the chain from this
    /// point, then the later impulses will not be affected by this use of
    /// `.detach()` and may be dropped if its downstream dependent gets dropped.
    ///
    /// Dependency gets dropped in the following situations:
    ///
    /// | Operation                                                 | Drop condition                                        |
    /// |-----------------------------------------------------------|-------------------------------------------------------|
    /// | [`Self::take`] <br> [`Self::take_response`]               | The promise containing the final response is dropped. |
    /// | [`Self::store`] <br> [`Self::push`] <br> [`Self::insert`] | The target entity of the operation is despawned.      |
    /// | [`Self::detach`] <br> [`Self::send_event`]                | This will never be dropped                            |
    /// | Using none of the above                                   | The impulse will immediately be dropped during a flush, so it will never be run at all. <br> This will also push an error into [`UnhandledErrors`](crate::UnhandledErrors). |
    pub fn detach(self) -> Impulse<'w, 's, 'a, Response, Streams> {
        self.commands.add(Detach {
            target: self.target,
        });
        self
    }

    /// Take the data that comes out of the request, including both the response
    /// and the streams.
    #[must_use]
    pub fn take(self) -> Recipient<Response, Streams> {
        let (response_sender, response_promise) = Promise::<Response>::new();
        self.commands.add(AddImpulse::new(
            self.target,
            TakenResponse::<Response>::new(response_sender),
        ));
        let mut map = StreamTargetMap::default();
        let (bundle, stream_receivers) =
            Streams::take_streams(self.target, &mut map, self.commands);
        self.commands.entity(self.source).insert((bundle, map));

        Recipient {
            response: response_promise,
            streams: stream_receivers,
        }
    }

    /// Take only the response data that comes out of the request.
    #[must_use]
    pub fn take_response(self) -> Promise<Response> {
        let (response_sender, response_promise) = Promise::<Response>::new();
        self.commands.add(AddImpulse::new(
            self.target,
            TakenResponse::<Response>::new(response_sender),
        ));
        response_promise
    }

    /// Pass the outcome of the request to another provider.
    #[must_use]
    pub fn then<P: ProvideOnce<Request = Response>>(
        self,
        provider: P,
    ) -> Impulse<'w, 's, 'a, P::Response, P::Streams> {
        let source = self.target;
        let target = self
            .commands
            .spawn((Detached::default(), UnusedTarget, ImpulseMarker))
            .id();

        // We should automatically delete the previous step in the chain once
        // this one is finished.
        self.commands
            .entity(source)
            .insert(Cancellable::new(cancel_impulse))
            .set_parent(target);
        provider.connect(None, source, target, self.commands);
        Impulse {
            source,
            target,
            commands: self.commands,
            _ignore: Default::default(),
        }
    }

    /// Apply a one-time callback whose input is the Response of the current
    /// target. The output of the map will become the Response of the returned
    /// target.
    ///
    /// This takes in a regular blocking function, which means all systems will
    /// be blocked from running while the function gets executed.
    #[must_use]
    pub fn map_block<U>(
        self,
        f: impl FnOnce(Response) -> U + 'static + Send + Sync,
    ) -> Impulse<'w, 's, 'a, U, ()>
    where
        U: 'static + Send + Sync,
    {
        self.then(f.into_blocking_map_once())
    }

    /// Apply a one-time callback whose output is a [`Future`] that will be run
    /// in the [`AsyncComputeTaskPool`][1] (unless the `single_threaded_async`
    /// feature is active). The output of the [`Future`] will be the Response of
    /// the returned Impulse.
    ///
    /// [1]: bevy::tasks::AsyncComputeTaskPool
    #[must_use]
    pub fn map_async<Task>(
        self,
        f: impl FnOnce(Response) -> Task + 'static + Send + Sync,
    ) -> Impulse<'w, 's, 'a, Task::Output, ()>
    where
        Task: Future + 'static + Sendish,
        Task::Output: 'static + Send + Sync,
    {
        self.then(f.into_async_map_once())
    }

    /// Apply a one-time map that implements one of
    /// - [`FnOnce(BlockingMap<Request, Streams>) -> Response`](crate::BlockingMap)
    /// - [`FnOnce(AsyncMap<Request, Streams>) -> impl Future<Response>`](crate::AsyncMap)
    ///
    /// If you do not care about providing streams then you can use
    /// [`Self::map_block`] or [`Self::map_async`] instead.
    pub fn map<M, F: AsMapOnce<M>>(
        self,
        f: F,
    ) -> Impulse<
        'w,
        's,
        'a,
        <F::MapType as ProvideOnce>::Response,
        <F::MapType as ProvideOnce>::Streams,
    >
    where
        F::MapType: ProvideOnce<Request = Response>,
        <F::MapType as ProvideOnce>::Response: 'static + Send + Sync,
        <F::MapType as ProvideOnce>::Streams: StreamPack,
    {
        self.then(f.as_map_once())
    }

    /// Store the response in a [`Storage`] component in the specified entity.
    ///
    /// Each stream will be collected into [`Collection`] components in the
    /// specified entity, one for each stream type. To store the streams in a
    /// different entity, call [`Self::collect_streams`] before this.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn store(self, target: Entity) {
        self.commands
            .add(AddImpulse::new(self.target, Store::<Response>::new(target)));

        let mut map = StreamTargetMap::default();
        let stream_targets = Streams::collect_streams(self.source, target, &mut map, self.commands);
        self.commands
            .entity(self.source)
            .insert((stream_targets, map));
    }

    /// Collect the stream data into [`Collection<T>`] components in the
    /// specified target, one collection for each stream data type. You must
    /// still decide what to do with the final response data.
    #[must_use]
    pub fn collect_streams(self, target: Entity) -> Impulse<'w, 's, 'a, Response, ()> {
        let mut map = StreamTargetMap::default();
        let stream_targets = Streams::collect_streams(self.source, target, &mut map, self.commands);
        self.commands
            .entity(self.source)
            .insert((stream_targets, map));

        Impulse {
            source: self.source,
            target: self.target,
            commands: self.commands,
            _ignore: Default::default(),
        }
    }

    /// Push the response to the back of a [`Collection<T>`] component in an
    /// entity.
    ///
    /// Similar to [`Self::store`] this will also collect streams into this
    /// entity.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn push(self, target: Entity) {
        self.commands.add(AddImpulse::new(
            self.target,
            Push::<Response>::new(target, false),
        ));

        let mut map = StreamTargetMap::default();
        let stream_targets = Streams::collect_streams(self.source, target, &mut map, self.commands);
        self.commands
            .entity(self.source)
            .insert((stream_targets, map));
    }

    // TODO(@mxgrey): Consider offering ways for users to respond to cancellations.
    // For example, offer an on_cancel method that lets users provide a callback
    // to be triggered when a cancellation happens. Or focus on terminal impulses,
    // like offer store_or_else(~), push_or_else(~) etc which accept a callback
    // that will be triggered after a cancellation.
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: Bundle,
{
    /// Insert the response as a bundle in the specified entity. Stream data
    /// will be dropped unless you use [`Self::collect_streams`] before this.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    ///
    /// If the response is not a bundle then you can store it in an entity using
    /// [`Self::store`] or [`Self::push`]. Alternatively you can transform it
    /// into a bundle using [`Self::map_block`] or [`Self::map_async`].
    pub fn insert(self, target: Entity) {
        self.commands.add(AddImpulse::new(
            self.target,
            Insert::<Response>::new(target),
        ));
    }
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: Event,
{
    /// Send the response out as an event once it is ready. Stream data will be
    /// dropped unless you use [`Self::collect_streams`] before this.
    ///
    /// Using this will also effectively [detach](Self::detach) the impulse.
    pub fn send_event(self) {
        self.commands
            .add(AddImpulse::new(self.target, SendEvent::<Response>::new()));
    }
}

/// Contains the final response and streams produced at the end of an impulse chain.
pub struct Recipient<Response, Streams: StreamPack> {
    pub response: Promise<Response>,
    pub streams: Streams::Receiver,
}

/// Used to store a response of an impulse as a component of an entity.
#[derive(Component)]
pub struct Storage<T> {
    pub data: T,
    pub session: Entity,
}

/// Used to collect responses from multiple impulse chains into a container
/// attached to an entity.
//
// TODO(@mxgrey): Consider allowing the user to choose the container type.
#[derive(Component)]
pub struct Collection<T> {
    /// The items that have been collected.
    pub items: Vec<Storage<T>>,
}

impl<T> Default for Collection<T> {
    fn default() -> Self {
        Self {
            items: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{testing::*, *};
    use bevy_utils::label::DynEq;
    use smallvec::SmallVec;
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn test_dropped_chain() {
        let mut context = TestingContext::minimal_plugins();

        let (detached_sender, mut detached_receiver) = unbounded_channel();
        let (attached_sender, mut attached_receiver) = unbounded_channel();

        context.command(|commands| {
            let _ = commands
                .request("hello".to_owned(), to_uppercase.into_blocking_map())
                .map_block(move |value| {
                    detached_sender.send(value.clone()).unwrap();
                    value
                })
                .detach()
                .map_block(to_lowercase)
                .map_block(move |value| {
                    attached_sender.send(value.clone()).unwrap();
                    value
                })
                .map_block(to_uppercase);
        });

        context.run(1);
        assert_eq!(detached_receiver.try_recv().unwrap(), "HELLO");
        assert!(attached_receiver.try_recv().is_err());
        assert!(context
            .get_unhandled_errors()
            .is_some_and(|e| e.unused_targets.len() == 1));
    }

    #[test]
    fn test_blocking_map() {
        let mut context = TestingContext::minimal_plugins();

        let mut promise = context.command(|commands| {
            commands
                .request("hello".to_owned(), to_uppercase.into_blocking_map())
                .take_response()
        });

        context.run_while_pending(&mut promise);
        assert!(promise.take().available().is_some_and(|v| v == "HELLO"));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
                .request("hello".to_owned(), to_uppercase.into_blocking_map_once())
                .take_response()
        });

        context.run_while_pending(&mut promise);
        assert!(promise.take().available().is_some_and(|v| v == "HELLO"));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
                .provide("hello".to_owned())
                .map_block(to_uppercase)
                .take_response()
        });

        context.run_while_pending(&mut promise);
        assert!(promise.take().available().is_some_and(|v| v == "HELLO"));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
                .provide("hello".to_owned())
                .map_block(|request| request.to_uppercase())
                .take_response()
        });

        context.run_while_pending(&mut promise);
        assert!(promise.take().available().is_some_and(|v| v == "HELLO"));
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_async_map() {
        let mut context = TestingContext::minimal_plugins();

        let request = WaitRequest {
            duration: Duration::from_secs_f64(0.001),
            value: "hello".to_owned(),
        };

        let conditions = FlushConditions::new().with_timeout(Duration::from_secs_f64(5.0));

        let mut promise = context.command(|commands| {
            commands
                .request(request.clone(), wait.into_async_map())
                .take_response()
        });

        assert!(context.run_with_conditions(&mut promise, conditions.clone()));
        assert!(promise.take().available().is_some_and(|v| v == "hello"));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
                .request(request.clone(), wait.into_async_map_once())
                .take_response()
        });

        assert!(context.run_with_conditions(&mut promise, conditions.clone()));
        assert!(promise.take().available().is_some_and(|v| v == "hello"));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
                .provide(request.clone())
                .map_async(wait)
                .take_response()
        });

        assert!(context.run_with_conditions(&mut promise, conditions.clone()));
        assert!(promise.take().available().is_some_and(|v| v == "hello"));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
                .provide(request.clone())
                .map_async(|request| {
                    async move {
                        let t = Instant::now();
                        while t.elapsed() < request.duration {
                            // Busy wait
                        }
                        request.value
                    }
                })
                .take_response()
        });

        assert!(context.run_with_conditions(&mut promise, conditions.clone()));
        assert!(promise.take().available().is_some_and(|v| v == "hello"));
        assert!(context.no_unhandled_errors());
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct TestLabel;

    // TODO(luca) create a proc-macro for this, because library crates can't
    // export proc macros we will need to create a new macros only crate
    impl DeliveryLabel for TestLabel {
        fn dyn_clone(&self) -> Box<dyn DeliveryLabel> {
            Box::new(self.clone())
        }

        fn as_dyn_eq(&self) -> &dyn DynEq {
            self
        }

        fn dyn_hash(&self, mut state: &mut dyn std::hash::Hasher) {
            let ty_id = std::any::TypeId::of::<Self>();
            std::hash::Hash::hash(&ty_id, &mut state);
            std::hash::Hash::hash(self, &mut state);
        }
    }

    #[test]
    fn test_delivery_instructions() {
        let mut context = TestingContext::minimal_plugins();
        let service = context.spawn_delayed_map_with_viewer(
            Duration::from_secs_f32(0.01),
            |counter: &Arc<Mutex<u64>>| {
                *counter.lock().unwrap() += 1;
            },
            |view: &ContinuousQueueView<_, ()>| {
                assert!(view.len() <= 1);
            },
        );

        verify_delivery_instruction_matrix(service.optional_stream_cast(), &mut context);

        let service = context.spawn_async_delayed_map(
            Duration::from_secs_f32(0.01),
            |counter: Arc<Mutex<u64>>| {
                *counter.lock().unwrap() += 1;
            },
        );

        verify_delivery_instruction_matrix(service, &mut context);
    }

    fn verify_delivery_instruction_matrix(
        service: Service<Arc<Mutex<u64>>, ()>,
        context: &mut TestingContext,
    ) {
        let queuing_service = service.instruct(TestLabel);
        let preempting_service = service.instruct(TestLabel.preempt());

        // Test by queuing up a bunch of requests before preempting them all at once.
        verify_preemption(1, queuing_service, preempting_service, context);
        verify_preemption(2, queuing_service, preempting_service, context);
        verify_preemption(3, queuing_service, preempting_service, context);
        verify_preemption(4, queuing_service, preempting_service, context);

        // Test by repeatedly preempting each request with the next.
        verify_preemption(1, preempting_service, preempting_service, context);
        verify_preemption(2, preempting_service, preempting_service, context);
        verify_preemption(3, preempting_service, preempting_service, context);
        verify_preemption(4, preempting_service, preempting_service, context);

        // Test by queuing up a bunch of requests and making sure they all get
        // delivered.
        verify_queuing(2, queuing_service, context);
        verify_queuing(3, queuing_service, context);
        verify_queuing(4, queuing_service, context);
        verify_queuing(5, queuing_service, context);

        // Test by queuing up a mix of ensured and unensured requests, and then
        // sending in one that preempts them all. The ensured requests should
        // remain in the queue and execute despite the preempter. The unensured
        // requests should all be cancelled.
        verify_ensured([false, true, false, true], service, context);
        verify_ensured([true, false, false, false], service, context);
        verify_ensured([true, true, false, false], service, context);
        verify_ensured([false, false, true, true], service, context);
        verify_ensured([true, false, false, true], service, context);
        verify_ensured([false, false, false, false], service, context);
        verify_ensured([true, true, true, true], service, context);
    }

    fn verify_preemption(
        preemptions: usize,
        preempted_service: Service<Arc<Mutex<u64>>, ()>,
        preempting_service: Service<Arc<Mutex<u64>>, ()>,
        context: &mut TestingContext,
    ) {
        let counter = Arc::new(Mutex::new(0_u64));
        let mut preempted: SmallVec<[Promise<()>; 16]> = SmallVec::new();
        for _ in 0..preemptions {
            let promise = context.command(|commands| {
                commands
                    .request(Arc::clone(&counter), preempted_service)
                    .take_response()
            });
            preempted.push(promise);
        }

        let mut final_promise = context.command(|commands| {
            commands
                .request(Arc::clone(&counter), preempting_service)
                .take_response()
        });

        for promise in &mut preempted {
            context.run_with_conditions(promise, Duration::from_secs(2));
            assert!(promise.take().is_cancelled());
        }

        context.run_with_conditions(&mut final_promise, Duration::from_secs(2));
        assert!(final_promise.take().is_available());
        assert_eq!(*counter.lock().unwrap(), 1);
        assert!(context.no_unhandled_errors());
    }

    fn verify_queuing(
        queue_size: usize,
        queuing_service: Service<Arc<Mutex<u64>>, ()>,
        context: &mut TestingContext,
    ) {
        let counter = Arc::new(Mutex::new(0_u64));
        let mut queued: SmallVec<[Promise<()>; 16]> = SmallVec::new();
        for _ in 0..queue_size {
            let promise = context.command(|commands| {
                commands
                    .request(Arc::clone(&counter), queuing_service)
                    .take_response()
            });
            queued.push(promise);
        }

        for promise in &mut queued {
            context.run_with_conditions(promise, Duration::from_secs(2));
            assert!(promise.take().is_available());
        }

        assert_eq!(*counter.lock().unwrap(), queue_size as u64);
        assert!(context.no_unhandled_errors());
    }

    fn verify_ensured(
        queued: impl IntoIterator<Item = bool>,
        service: Service<Arc<Mutex<u64>>, ()>,
        context: &mut TestingContext,
    ) {
        let counter = Arc::new(Mutex::new(0_u64));
        let mut queued_promises: SmallVec<[(Promise<()>, bool); 16]> = SmallVec::new();
        // This counter starts out at 1 to account for the preempting request.
        let mut expected_count = 1;
        for ensured in queued {
            let srv = if ensured {
                expected_count += 1;
                service.instruct(TestLabel.ensure())
            } else {
                service.instruct(TestLabel)
            };

            let promise = context
                .command(|commands| commands.request(Arc::clone(&counter), srv).take_response());

            queued_promises.push((promise, ensured));
        }

        let mut preempter = context.command(|commands| {
            commands
                .request(Arc::clone(&counter), service.instruct(TestLabel.preempt()))
                .take_response()
        });

        for (promise, ensured) in &mut queued_promises {
            context.run_with_conditions(promise, Duration::from_secs(2));
            if *ensured {
                assert!(promise.take().is_available());
            } else {
                assert!(promise.take().is_cancelled());
            }
        }

        context.run_with_conditions(&mut preempter, Duration::from_secs(2));
        assert!(preempter.take().is_available());
        assert_eq!(*counter.lock().unwrap(), expected_count);
        assert!(context.no_unhandled_errors());
    }
}
