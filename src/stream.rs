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

use bevy_ecs::prelude::{Entity, World};

use std::sync::OnceLock;

use crate::{ManageInput, OperationError, OperationResult, OperationRoster, OrBroken};

mod anonymous_stream;
pub use anonymous_stream::*;

mod dynamically_named_stream;
pub use dynamically_named_stream::*;

mod named_stream;
pub use named_stream::*;

mod stream_availability;
pub use stream_availability::*;

mod stream_buffer;
pub use stream_buffer::*;

mod stream_channel;
pub use stream_channel::*;

mod stream_filter;
pub use stream_filter::*;

mod stream_pack;
pub use stream_pack::*;

mod stream_target_map;
pub use stream_target_map::*;

// TODO(@mxgrey): Add module-level documentation for stream.rs

pub use bevy_impulse_derive::{Stream, StreamPack};
/// You can create custom stream types that have side-effects, such as:
/// - applying a transformation to the stream input to produce a different type of output
/// - logging the message data
/// - triggering an event or modifying a resource/component in the [`World`]
///
/// After you have implemented `StreamEffect` for your struct, you can apply
/// `#[derive(Stream)]` to the struct and then use as a [`StreamPack`], either
/// on its own or in a tuple.
///
/// If you just want to stream a message with no side-effects, you can simply
/// wrap your message type in [`StreamOf`] to get a [`StreamPack`]. Users only
/// need to use `StreamEffect` if you want custom stream side-effects.
pub trait StreamEffect: 'static + Send + Sync + Sized {
    type Input: 'static + Send + Sync;
    type Output: 'static + Send + Sync;

    /// Specify a side effect that is meant to happen whenever a stream value is
    /// sent.
    fn side_effect(
        input: Self::Input,
        request: &mut StreamRequest,
    ) -> Result<Self::Output, OperationError>;
}

/// A wrapper to make an anonymous (unnamed) stream for any type `T` that
/// implements `'static + Send + Sync`. This simply transmits the `T` with no
/// side-effects.
#[derive(Stream)]
pub struct StreamOf<T: 'static + Send + Sync>(std::marker::PhantomData<fn(T)>);

impl<T: 'static + Send + Sync> Clone for StreamOf<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static + Send + Sync> Copy for StreamOf<T> {}

impl<T: 'static + Send + Sync> std::fmt::Debug for StreamOf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        static NAME: OnceLock<String> = OnceLock::new();
        let name = NAME.get_or_init(|| format!("StreamOf<{}>", std::any::type_name::<T>(),));

        f.debug_struct(name.as_str()).finish()
    }
}

impl<T: 'static + Send + Sync> StreamEffect for StreamOf<T> {
    type Input = T;
    type Output = T;

    /// `StreamOf` has no side-effect
    fn side_effect(
        input: Self::Input,
        _: &mut StreamRequest,
    ) -> Result<Self::Output, OperationError> {
        Ok(input)
    }
}

/// `StreamRequest` is provided to types that implement [`StreamEffect`] so they
/// can process a stream input and apply any side effects to it. Note that your
/// implementation of [`StreamEffect::side_effect`] should return the output
/// data; it should *not* use the `StreamRequest` send it the output to the target.
pub struct StreamRequest<'a> {
    /// The node that emitted the stream
    pub source: Entity,
    /// The session of the stream
    pub session: Entity,
    /// The target of the stream, if a specific target exists.
    pub target: Option<Entity>,
    /// The world that the stream exists inside
    pub world: &'a mut World,
    /// The roster of the stream
    pub roster: &'a mut OperationRoster,
}

impl<'a> StreamRequest<'a> {
    pub fn send_output<T: 'static + Send + Sync>(self, output: T) -> OperationResult {
        let Self {
            session,
            target,
            world,
            roster,
            ..
        } = self;
        if let Some(target) = target {
            world
                .get_entity_mut(target)
                .or_broken()?
                .give_input(session, output, roster)?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{dyn_node::*, prelude::*, testing::*};
    use std::borrow::Cow;

    #[test]
    fn test_single_stream() {
        let mut context = TestingContext::minimal_plugins();

        let count_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<u32, StreamOf<u32>>| {
                for i in 0..input.request {
                    input.streams.send(i);
                }
                return input.request;
            })
        });

        test_counting_stream(count_blocking_srv, &mut context);

        let count_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<u32, StreamOf<u32>>| async move {
                    for i in 0..input.request {
                        input.streams.send(i);
                    }
                    return input.request;
                },
            )
        });

        test_counting_stream(count_async_srv, &mut context);

        let count_blocking_callback = (|In(input): BlockingCallbackInput<u32, StreamOf<u32>>| {
            for i in 0..input.request {
                input.streams.send(i);
            }
            return input.request;
        })
        .as_callback();

        test_counting_stream(count_blocking_callback, &mut context);

        let count_async_callback =
            (|In(input): AsyncCallbackInput<u32, StreamOf<u32>>| async move {
                for i in 0..input.request {
                    input.streams.send(i);
                }
                return input.request;
            })
            .as_callback();

        test_counting_stream(count_async_callback, &mut context);

        let count_blocking_map = (|input: BlockingMap<u32, StreamOf<u32>>| {
            for i in 0..input.request {
                input.streams.send(i);
            }
            return input.request;
        })
        .as_map();

        test_counting_stream(count_blocking_map, &mut context);

        let count_async_map = (|input: AsyncMap<u32, StreamOf<u32>>| async move {
            for i in 0..input.request {
                input.streams.send(i);
            }
            return input.request;
        })
        .as_map();

        test_counting_stream(count_async_map, &mut context);
    }

    fn test_counting_stream(
        provider: impl Provider<Request = u32, Response = u32, Streams = StreamOf<u32>>,
        context: &mut TestingContext,
    ) {
        let mut recipient = context.command(|commands| commands.request(10, provider).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient
            .response
            .take()
            .available()
            .is_some_and(|v| v == 10));

        let mut stream: Vec<u32> = Vec::new();
        while let Ok(r) = recipient.streams.try_recv() {
            stream.push(r);
        }
        assert_eq!(stream, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(context.no_unhandled_errors());
    }

    type FormatStreams = (StreamOf<u32>, StreamOf<i32>, StreamOf<f32>);
    #[test]
    fn test_tuple_stream() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            })
        });

        validate_formatting_stream(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<String, FormatStreams>| async move {
                    impl_formatting_streams_async(input.request, input.streams);
                },
            )
        });

        validate_formatting_stream(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_formatting_streams_continuous);

        validate_formatting_stream(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            })
            .as_callback();

        validate_formatting_stream(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<String, FormatStreams>| async move {
                impl_formatting_streams_async(input.request, input.streams);
            })
            .as_callback();

        validate_formatting_stream(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<String, FormatStreams>| {
            impl_formatting_streams_blocking(input.request, input.streams);
        })
        .as_map();

        validate_formatting_stream(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<String, FormatStreams>| async move {
            impl_formatting_streams_async(input.request, input.streams);
        })
        .as_map();

        validate_formatting_stream(parse_async_map, &mut context);

        let make_workflow = |service: Service<String, (), FormatStreams>| {
            move |scope: Scope<String, (), FormatStreams>, builder: &mut Builder| {
                let node = scope
                    .input
                    .chain(builder)
                    .map_block(move |value| (value, service))
                    .then_injection_node();

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                builder.connect(node.output, scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        validate_formatting_stream(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        validate_formatting_stream(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        validate_formatting_stream(continuous_injection_workflow, &mut context);

        let nested_workflow = context.spawn_workflow::<_, _, FormatStreams, _>(|scope, builder| {
            let inner_node = scope
                .input
                .chain(builder)
                .then_node(continuous_injection_workflow);
            builder.connect(inner_node.streams.0, scope.streams.0);
            builder.connect(inner_node.streams.1, scope.streams.1);
            builder.connect(inner_node.streams.2, scope.streams.2);
            builder.connect(inner_node.output, scope.terminate);
        });
        validate_formatting_stream(nested_workflow, &mut context);

        let double_nested_workflow =
            context.spawn_workflow::<_, _, FormatStreams, _>(|scope, builder| {
                let inner_node = builder.create_node(nested_workflow);
                builder.connect(scope.input, inner_node.input);
                builder.connect(inner_node.streams.0, scope.streams.0);
                builder.connect(inner_node.streams.1, scope.streams.1);
                builder.connect(inner_node.streams.2, scope.streams.2);
                builder.connect(inner_node.output, scope.terminate);
            });
        validate_formatting_stream(double_nested_workflow, &mut context);
    }

    fn impl_formatting_streams_blocking(
        request: String,
        streams: <FormatStreams as StreamPack>::StreamBuffers,
    ) {
        if let Ok(value) = request.parse::<u32>() {
            streams.0.send(value);
        }

        if let Ok(value) = request.parse::<i32>() {
            streams.1.send(value);
        }

        if let Ok(value) = request.parse::<f32>() {
            streams.2.send(value);
        }
    }

    fn impl_formatting_streams_async(
        request: String,
        streams: <FormatStreams as StreamPack>::StreamChannels,
    ) {
        if let Ok(value) = request.parse::<u32>() {
            streams.0.send(value);
        }

        if let Ok(value) = request.parse::<i32>() {
            streams.1.send(value);
        }

        if let Ok(value) = request.parse::<f32>() {
            streams.2.send(value);
        }
    }

    fn impl_formatting_streams_continuous(
        In(ContinuousService { key }): In<ContinuousService<String, (), FormatStreams>>,
        mut param: ContinuousQuery<String, (), FormatStreams>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            if let Ok(value) = order.request().parse::<u32>() {
                order.streams().0.send(value);
            }

            if let Ok(value) = order.request().parse::<i32>() {
                order.streams().1.send(value);
            }

            if let Ok(value) = order.request().parse::<f32>() {
                order.streams().2.send(value);
            }

            order.respond(());
        });
    }

    fn validate_formatting_stream(
        provider: impl Provider<Request = String, Response = (), Streams = FormatStreams> + Clone,
        context: &mut TestingContext,
    ) {
        let mut recipient =
            context.command(|commands| commands.request("5".to_owned(), provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, [5]);
        assert_eq!(outcome.stream_i32, [5]);
        assert_eq!(outcome.stream_f32, [5.0]);

        let mut recipient =
            context.command(|commands| commands.request("-2".to_owned(), provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert_eq!(outcome.stream_i32, [-2]);
        assert_eq!(outcome.stream_f32, [-2.0]);

        let mut recipient =
            context.command(|commands| commands.request("6.7".to_owned(), provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert!(outcome.stream_i32.is_empty());
        assert_eq!(outcome.stream_f32, [6.7]);

        let mut recipient = context.command(|commands| {
            commands
                .request("hello".to_owned(), provider.clone())
                .take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert!(outcome.stream_i32.is_empty());
        assert!(outcome.stream_f32.is_empty());
    }

    #[derive(Default)]
    struct FormatOutcome {
        stream_u32: Vec<u32>,
        stream_i32: Vec<i32>,
        stream_f32: Vec<f32>,
    }

    impl From<Recipient<(), FormatStreams>> for FormatOutcome {
        fn from(mut recipient: Recipient<(), FormatStreams>) -> Self {
            let mut result = Self::default();
            while let Ok(r) = recipient.streams.0.try_recv() {
                result.stream_u32.push(r);
            }

            while let Ok(r) = recipient.streams.1.try_recv() {
                result.stream_i32.push(r);
            }

            while let Ok(r) = recipient.streams.2.try_recv() {
                result.stream_f32.push(r);
            }

            result
        }
    }

    #[test]
    fn test_stream_pack() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): BlockingServiceInput<Vec<String>, TestStreamPack>| {
                    impl_stream_pack_test_blocking(input.request, input.streams);
                },
            )
        });

        validate_stream_pack(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<Vec<String>, TestStreamPack>| async move {
                    impl_stream_pack_test_async(input.request, input.streams);
                },
            )
        });

        validate_stream_pack(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_stream_pack_test_continuous);

        validate_stream_pack(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<Vec<String>, TestStreamPack>| {
                impl_stream_pack_test_blocking(input.request, input.streams);
            })
            .as_callback();

        validate_stream_pack(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<Vec<String>, TestStreamPack>| async move {
                impl_stream_pack_test_async(input.request, input.streams);
            })
            .as_callback();

        validate_stream_pack(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<Vec<String>, TestStreamPack>| {
            impl_stream_pack_test_blocking(input.request, input.streams);
        })
        .as_map();

        validate_stream_pack(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<Vec<String>, TestStreamPack>| async move {
            impl_stream_pack_test_async(input.request, input.streams);
        })
        .as_map();

        validate_stream_pack(parse_async_map, &mut context);

        let make_workflow = |service: Service<Vec<String>, (), TestStreamPack>| {
            move |scope: Scope<Vec<String>, (), TestStreamPack>, builder: &mut Builder| {
                let node = scope
                    .input
                    .chain(builder)
                    .map_block(move |value| (value, service))
                    .then_injection_node();

                builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
                builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
                builder.connect(node.streams.stream_string, scope.streams.stream_string);

                builder.connect(node.output, scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        validate_stream_pack(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        validate_stream_pack(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        validate_stream_pack(continuous_injection_workflow, &mut context);

        let nested_workflow =
            context.spawn_workflow::<_, _, TestStreamPack, _>(|scope, builder| {
                let node = scope.input.chain(builder).then_node(parse_continuous_srv);

                builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
                builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
                builder.connect(node.streams.stream_string, scope.streams.stream_string);

                builder.connect(node.output, scope.terminate);
            });
        validate_stream_pack(nested_workflow, &mut context);

        let double_nested_workflow =
            context.spawn_workflow::<_, _, TestStreamPack, _>(|scope, builder| {
                let node = scope.input.chain(builder).then_node(nested_workflow);

                builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
                builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
                builder.connect(node.streams.stream_string, scope.streams.stream_string);

                builder.connect(node.output, scope.terminate);
            });
        validate_stream_pack(double_nested_workflow, &mut context);

        let scoped_workflow =
            context.spawn_workflow::<_, _, TestStreamPack, _>(|scope, builder| {
                let inner_scope =
                    builder.create_scope::<_, _, TestStreamPack, _>(|scope, builder| {
                        let node = scope.input.chain(builder).then_node(parse_continuous_srv);

                        builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
                        builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
                        builder.connect(node.streams.stream_string, scope.streams.stream_string);

                        builder.connect(node.output, scope.terminate);
                    });

                builder.connect(scope.input, inner_scope.input);

                builder.connect(inner_scope.streams.stream_u32, scope.streams.stream_u32);
                builder.connect(inner_scope.streams.stream_i32, scope.streams.stream_i32);
                builder.connect(
                    inner_scope.streams.stream_string,
                    scope.streams.stream_string,
                );

                builder.connect(inner_scope.output, scope.terminate);
            });
        validate_stream_pack(scoped_workflow, &mut context);

        let dyn_stream_workflow =
            context.spawn_workflow::<Vec<String>, (), TestStreamPack, _>(|scope, builder| {
                let dyn_scope_input: DynOutput = scope.input.into();

                let node = builder.create_node(parse_continuous_srv);
                let mut dyn_node: DynNode = node.into();

                dyn_scope_input
                    .connect_to(&dyn_node.input, builder)
                    .unwrap();

                let dyn_scope_stream_u32: DynInputSlot = scope.streams.stream_u32.into();
                let dyn_node_stream_u32 = dyn_node.streams.take_named("stream_u32").unwrap();
                dyn_node_stream_u32
                    .connect_to(&dyn_scope_stream_u32, builder)
                    .unwrap();

                let dyn_scope_stream_i32: DynInputSlot = scope.streams.stream_i32.into();
                let dyn_node_stream_i32 = dyn_node.streams.take_named("stream_i32").unwrap();
                dyn_node_stream_i32
                    .connect_to(&dyn_scope_stream_i32, builder)
                    .unwrap();

                let dyn_scope_stream_string: DynInputSlot = scope.streams.stream_string.into();
                let dyn_node_stream_string = dyn_node.streams.take_named("stream_string").unwrap();
                dyn_node_stream_string
                    .connect_to(&dyn_scope_stream_string, builder)
                    .unwrap();

                let terminate: DynInputSlot = scope.terminate.into();
                dyn_node.output.connect_to(&terminate, builder).unwrap();
            });
        validate_stream_pack(dyn_stream_workflow, &mut context);

        // We can do a stream cast for the service-type providers but not for
        // the callbacks or maps.
        validate_dynamically_named_stream_receiver(parse_blocking_srv, &mut context);
        validate_dynamically_named_stream_receiver(parse_async_srv, &mut context);
        validate_dynamically_named_stream_receiver(parse_continuous_srv, &mut context);
        validate_dynamically_named_stream_receiver(blocking_injection_workflow, &mut context);
        validate_dynamically_named_stream_receiver(async_injection_workflow, &mut context);
        validate_dynamically_named_stream_receiver(continuous_injection_workflow, &mut context);
        validate_dynamically_named_stream_receiver(nested_workflow, &mut context);
        validate_dynamically_named_stream_receiver(double_nested_workflow, &mut context);
    }

    fn validate_stream_pack(
        provider: impl Provider<Request = Vec<String>, Response = (), Streams = TestStreamPack> + Clone,
        context: &mut TestingContext,
    ) {
        let request = vec![
            "5".to_owned(),
            "10".to_owned(),
            "-3".to_owned(),
            "-27".to_owned(),
            "hello".to_owned(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(
            context.no_unhandled_errors(),
            "{:#?}",
            context.get_unhandled_errors()
        );

        let outcome: StreamMapOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, [5, 10]);
        assert_eq!(outcome.stream_i32, [5, 10, -3, -27]);
        assert_eq!(outcome.stream_string, ["5", "10", "-3", "-27", "hello"]);

        let request = vec![];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(
            context.no_unhandled_errors(),
            "{:#?}",
            context.get_unhandled_errors()
        );

        let outcome: StreamMapOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, Vec::<i32>::new());
        assert_eq!(outcome.stream_string, Vec::<String>::new());

        let request = vec![
            "foo".to_string(),
            "bar".to_string(),
            "1.32".to_string(),
            "-8".to_string(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());

        let outcome: StreamMapOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, [-8]);
        assert_eq!(outcome.stream_string, ["foo", "bar", "1.32", "-8"]);
    }

    fn validate_dynamically_named_stream_receiver(
        provider: Service<Vec<String>, (), TestStreamPack>,
        context: &mut TestingContext,
    ) {
        let provider: Service<Vec<String>, (), TestDynamicNamedStreams> =
            provider.optional_stream_cast();

        let request = vec![
            "5".to_owned(),
            "10".to_owned(),
            "-3".to_owned(),
            "-27".to_owned(),
            "hello".to_owned(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(
            context.no_unhandled_errors(),
            "{:#?}",
            context.get_unhandled_errors()
        );

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, [5, 10]);
        assert_eq!(outcome.stream_i32, [5, 10, -3, -27]);
        assert_eq!(outcome.stream_string, ["5", "10", "-3", "-27", "hello"]);

        let request = vec![];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(
            context.no_unhandled_errors(),
            "{:#?}",
            context.get_unhandled_errors()
        );

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, Vec::<i32>::new());
        assert_eq!(outcome.stream_string, Vec::<String>::new());

        let request = vec![
            "foo".to_string(),
            "bar".to_string(),
            "1.32".to_string(),
            "-8".to_string(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, [-8]);
        assert_eq!(outcome.stream_string, ["foo", "bar", "1.32", "-8"]);
    }

    fn impl_stream_pack_test_blocking(
        request: Vec<String>,
        streams: <TestStreamPack as StreamPack>::StreamBuffers,
    ) {
        for r in request {
            if let Ok(value) = r.parse::<u32>() {
                streams.stream_u32.send(value);
            }

            if let Ok(value) = r.parse::<i32>() {
                streams.stream_i32.send(value);
            }

            streams.stream_string.send(r);
        }
    }

    fn impl_stream_pack_test_async(
        request: Vec<String>,
        streams: <TestStreamPack as StreamPack>::StreamChannels,
    ) {
        for r in request {
            if let Ok(value) = r.parse::<u32>() {
                streams.stream_u32.send(value);
            }

            if let Ok(value) = r.parse::<i32>() {
                streams.stream_i32.send(value);
            }

            streams.stream_string.send(r);
        }
    }

    fn impl_stream_pack_test_continuous(
        In(ContinuousService { key }): In<ContinuousService<Vec<String>, (), TestStreamPack>>,
        mut param: ContinuousQuery<Vec<String>, (), TestStreamPack>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            for r in order.request().clone() {
                if let Ok(value) = r.parse::<u32>() {
                    order.streams().stream_u32.send(value);
                }

                if let Ok(value) = r.parse::<i32>() {
                    order.streams().stream_i32.send(value);
                }

                order.streams().stream_string.send(r);
            }

            order.respond(());
        });
    }

    #[derive(Default)]
    struct StreamMapOutcome {
        stream_u32: Vec<u32>,
        stream_i32: Vec<i32>,
        stream_string: Vec<String>,
    }

    impl From<Recipient<(), TestStreamPack>> for StreamMapOutcome {
        fn from(mut recipient: Recipient<(), TestStreamPack>) -> Self {
            let mut result = Self::default();
            while let Ok(r) = recipient.streams.stream_u32.try_recv() {
                result.stream_u32.push(r);
            }

            while let Ok(r) = recipient.streams.stream_i32.try_recv() {
                result.stream_i32.push(r);
            }

            while let Ok(r) = recipient.streams.stream_string.try_recv() {
                result.stream_string.push(r);
            }

            result
        }
    }

    type TestDynamicNamedStreams = (
        DynamicallyNamedStream<StreamOf<u32>>,
        DynamicallyNamedStream<StreamOf<i32>>,
        DynamicallyNamedStream<StreamOf<String>>,
    );

    impl TryFrom<Recipient<(), TestDynamicNamedStreams>> for StreamMapOutcome {
        type Error = UnknownName;
        fn try_from(
            mut recipient: Recipient<(), TestDynamicNamedStreams>,
        ) -> Result<Self, Self::Error> {
            let mut result = Self::default();
            while let Ok(NamedValue { name, value }) = recipient.streams.0.try_recv() {
                if name == "stream_u32" {
                    result.stream_u32.push(value);
                } else {
                    return Err(UnknownName { name });
                }
            }

            while let Ok(NamedValue { name, value }) = recipient.streams.1.try_recv() {
                if name == "stream_i32" {
                    result.stream_i32.push(value);
                } else {
                    return Err(UnknownName { name });
                }
            }

            while let Ok(NamedValue { name, value }) = recipient.streams.2.try_recv() {
                if name == "stream_string" {
                    result.stream_string.push(value);
                } else {
                    return Err(UnknownName { name });
                }
            }

            Ok(result)
        }
    }

    #[test]
    fn test_dynamically_named_streams() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): BlockingServiceInput<NamedInputs, TestDynamicNamedStreams>| {
                    impl_dynamically_named_streams_blocking(input.request, input.streams);
                },
            )
        });

        validate_dynamically_named_streams(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<NamedInputs, TestDynamicNamedStreams>| async move {
                    impl_dynamically_named_streams_async(input.request, input.streams);
                },
            )
        });

        validate_dynamically_named_streams(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_dynamically_named_streams_continuous);

        validate_dynamically_named_streams(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<NamedInputs, TestDynamicNamedStreams>| {
                impl_dynamically_named_streams_blocking(input.request, input.streams);
            })
            .as_callback();

        validate_dynamically_named_streams(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<NamedInputs, TestDynamicNamedStreams>| async move {
                impl_dynamically_named_streams_async(input.request, input.streams);
            })
            .as_callback();

        validate_dynamically_named_streams(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<NamedInputs, TestDynamicNamedStreams>| {
            impl_dynamically_named_streams_blocking(input.request, input.streams);
        })
        .as_map();

        validate_dynamically_named_streams(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<NamedInputs, TestDynamicNamedStreams>| async move {
            impl_dynamically_named_streams_async(input.request, input.streams);
        })
        .as_map();

        validate_dynamically_named_streams(parse_async_map, &mut context);

        let make_workflow = |service: Service<NamedInputs, (), TestDynamicNamedStreams>| {
            move |scope: Scope<NamedInputs, (), TestDynamicNamedStreams>, builder: &mut Builder| {
                let node = scope
                    .input
                    .chain(builder)
                    .map_block(move |value| (value, service))
                    .then_injection_node();

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                builder.connect(node.output, scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        validate_dynamically_named_streams(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        validate_dynamically_named_streams(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        validate_dynamically_named_streams(continuous_injection_workflow, &mut context);

        let nested_workflow =
            context.spawn_workflow::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
                let node = scope.input.chain(builder).then_node(parse_continuous_srv);

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                builder.connect(node.output, scope.terminate);
            });
        validate_dynamically_named_streams(nested_workflow, &mut context);

        let double_nested_workflow =
            context.spawn_workflow::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
                let node = scope.input.chain(builder).then_node(nested_workflow);

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                builder.connect(node.output, scope.terminate);
            });
        validate_dynamically_named_streams(double_nested_workflow, &mut context);

        let scoped_workflow =
            context.spawn_workflow::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
                let inner_scope =
                    builder.create_scope::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
                        let node = scope.input.chain(builder).then_node(parse_continuous_srv);

                        builder.connect(node.streams.0, scope.streams.0);
                        builder.connect(node.streams.1, scope.streams.1);
                        builder.connect(node.streams.2, scope.streams.2);

                        builder.connect(node.output, scope.terminate);
                    });

                builder.connect(scope.input, inner_scope.input);

                builder.connect(inner_scope.streams.0, scope.streams.0);
                builder.connect(inner_scope.streams.1, scope.streams.1);
                builder.connect(inner_scope.streams.2, scope.streams.2);

                builder.connect(inner_scope.output, scope.terminate);
            });
        validate_dynamically_named_streams(scoped_workflow, &mut context);

        // We can do a stream cast for the service-type providers but not for
        // the callbacks or maps.
        validate_dynamically_named_streams_into_stream_pack(parse_blocking_srv, &mut context);
        validate_dynamically_named_streams_into_stream_pack(parse_async_srv, &mut context);
        validate_dynamically_named_streams_into_stream_pack(parse_continuous_srv, &mut context);
        validate_dynamically_named_streams_into_stream_pack(
            blocking_injection_workflow,
            &mut context,
        );
        validate_dynamically_named_streams_into_stream_pack(async_injection_workflow, &mut context);
        validate_dynamically_named_streams_into_stream_pack(
            continuous_injection_workflow,
            &mut context,
        );
        validate_dynamically_named_streams_into_stream_pack(nested_workflow, &mut context);
        validate_dynamically_named_streams_into_stream_pack(double_nested_workflow, &mut context);
    }

    fn validate_dynamically_named_streams(
        provider: impl Provider<Request = NamedInputs, Response = (), Streams = TestDynamicNamedStreams>
            + Clone,
        context: &mut TestingContext,
    ) {
        let expected_values_u32 = vec![
            NamedValue::new("stream_u32", 5),
            NamedValue::new("stream_u32", 10),
            NamedValue::new("stream_i32", 12),
        ];

        let expected_values_i32 = vec![
            NamedValue::new("stream_i32", 2),
            NamedValue::new("stream_i32", -5),
            NamedValue::new("stream_u32", 7),
        ];

        let expected_values_string = vec![
            NamedValue::new("stream_string", "hello".to_owned()),
            NamedValue::new("stream_string", "8".to_owned()),
            NamedValue::new("stream_u32", "22".to_owned()),
        ];

        let request = NamedInputs {
            values_u32: expected_values_u32.clone(),
            values_i32: expected_values_i32.clone(),
            values_string: expected_values_string.clone(),
        };

        let mut recipient = context.command(|commands| commands.request(request, provider).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let received_values_u32 = collect_received_values(recipient.streams.0);
        assert_eq!(expected_values_u32, received_values_u32);

        let received_values_i32 = collect_received_values(recipient.streams.1);
        assert_eq!(expected_values_i32, received_values_i32);

        let received_values_string = collect_received_values(recipient.streams.2);
        assert_eq!(expected_values_string, received_values_string);
    }

    pub fn collect_received_values<T>(mut receiver: crate::Receiver<T>) -> Vec<T> {
        let mut result = Vec::new();
        while let Ok(value) = receiver.try_recv() {
            result.push(value);
        }
        result
    }

    fn validate_dynamically_named_streams_into_stream_pack(
        provider: Service<NamedInputs, (), TestDynamicNamedStreams>,
        context: &mut TestingContext,
    ) {
        let provider: Service<NamedInputs, (), TestStreamPack> = provider.optional_stream_cast();

        let request = NamedInputs {
            values_u32: vec![
                NamedValue::new("stream_u32", 5),
                NamedValue::new("stream_u32", 10),
                // This won't appear because its name isn't being listened for
                // for this value type
                NamedValue::new("stream_i32", 12),
            ],
            values_i32: vec![
                NamedValue::new("stream_i32", 2),
                NamedValue::new("stream_i32", -5),
                // This won't appear because its name isn't being listened for
                // for this value type
                NamedValue::new("stream_u32", 7),
            ],
            values_string: vec![
                NamedValue::new("stream_string", "hello".to_owned()),
                NamedValue::new("stream_string", "8".to_owned()),
                // This won't appear because its named isn't being listened for
                // for this value type
                NamedValue::new("stream_u32", "22".to_owned()),
            ],
        };

        let mut recipient = context.command(|commands| commands.request(request, provider).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, [5, 10]);
        assert_eq!(outcome.stream_i32, [2, -5]);
        assert_eq!(outcome.stream_string, ["hello", "8"]);
    }

    fn impl_dynamically_named_streams_blocking(
        request: NamedInputs,
        streams: <TestDynamicNamedStreams as StreamPack>::StreamBuffers,
    ) {
        for nv in request.values_u32 {
            streams.0.send(nv);
        }

        for nv in request.values_i32 {
            streams.1.send(nv);
        }

        for nv in request.values_string {
            streams.2.send(nv);
        }
    }

    fn impl_dynamically_named_streams_async(
        request: NamedInputs,
        streams: <TestDynamicNamedStreams as StreamPack>::StreamChannels,
    ) {
        for nv in request.values_u32 {
            streams.0.send(nv);
        }

        for nv in request.values_i32 {
            streams.1.send(nv);
        }

        for nv in request.values_string {
            streams.2.send(nv);
        }
    }

    fn impl_dynamically_named_streams_continuous(
        In(ContinuousService { key }): In<
            ContinuousService<NamedInputs, (), TestDynamicNamedStreams>,
        >,
        mut param: ContinuousQuery<NamedInputs, (), TestDynamicNamedStreams>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            for nv in order.request().values_u32.iter() {
                order.streams().0.send(nv.clone());
            }

            for nv in order.request().values_i32.iter() {
                order.streams().1.send(nv.clone());
            }

            for nv in order.request().values_string.iter() {
                order.streams().2.send(nv.clone());
            }

            order.respond(());
        });
    }

    struct NamedInputs {
        values_u32: Vec<NamedValue<u32>>,
        values_i32: Vec<NamedValue<i32>>,
        values_string: Vec<NamedValue<String>>,
    }

    #[derive(thiserror::Error, Debug)]
    #[error("received unknown name: {name}")]
    struct UnknownName {
        name: Cow<'static, str>,
    }

    #[derive(StreamPack)]
    pub(crate) struct TestStreamPack {
        stream_u32: u32,
        stream_i32: i32,
        stream_string: String,
    }
}
