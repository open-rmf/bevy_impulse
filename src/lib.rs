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

pub mod buffer;
pub use buffer::*;

pub mod builder;
pub use builder::*;

pub mod callback;
pub use callback::*;

pub mod cancel;
pub use cancel::*;

pub mod chain;
pub use chain::*;

pub mod channel;
pub use channel::*;

pub mod discovery;
pub use discovery::*;

pub mod disposal;
pub use disposal::*;

pub mod errors;
pub use errors::*;

pub mod flush;
pub use flush::*;

pub mod impulse;
pub use impulse::*;

pub mod input;
pub use input::*;

pub mod map;
pub use map::*;

pub mod map_once;
pub use map_once::*;

pub mod node;
pub use node::*;

pub mod operation;
pub use operation::*;

pub mod promise;
pub use promise::*;

pub mod provider;
pub use provider::*;

pub mod request;
pub use request::*;

pub mod service;
pub use service::*;

pub mod stream;
pub use stream::*;

pub mod workflow;
pub use workflow::*;

pub mod testing;

use bevy::prelude::{Entity, In};

/// Use `BlockingService` to indicate that your system is a blocking [`Service`].
///
/// A blocking service will have exclusive world access while it runs, which
/// means no other system will be able to run simultaneously. Each service is
/// associated with its own unique entity which can be used to store state or
/// configuration parameters.
///
/// Some services might need to know what entity is providing the service, e.g.
/// if the service provider is configured with additional components that need
/// to be queried when a request comes in. For that you can check the `provider`
/// field of `BlockingService`:
///
/// ```
/// use bevy::prelude::*;
/// use bevy_impulse::*;
///
/// #[derive(Component, Resource)]
/// struct Precision(i32);
///
/// fn rounding_service(
///     In(BlockingService{request, provider, ..}): BlockingServiceInput<f64>,
///     service_precision: Query<&Precision>,
///     global_precision: Res<Precision>,
/// ) -> f64 {
///     let precision = service_precision.get(provider).unwrap_or(&*global_precision).0;
///     (request * 10_f64.powi(precision)).floor() * 10_f64.powi(-precision)
/// }
/// ```
#[non_exhaustive]
pub struct BlockingService<Request, Streams: StreamPack = ()> {
    /// The input data of the request
    pub request: Request,
    /// The buffer to hold stream output data until the function is finished
    pub streams: Streams::Buffer,
    /// The entity providing the service
    pub provider: Entity,
    /// The node in a workflow or impulse chain that asked for the service
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// Use this to reduce bracket noise when you need `In<BlockingService<R>>`.
pub type BlockingServiceInput<Request, Streams = ()> = In<BlockingService<Request, Streams>>;

/// Use AsyncService to indicate that your system is an async [`Service`]. Being
/// async means it must return a [`Future<Output=Response>`](std::future::Future)
/// which will be processed by a task pool.
///
/// This comes with a [`Channel`] that allows your Future to interact with Bevy's
/// ECS asynchronously while it is polled from inside the task pool.
#[non_exhaustive]
pub struct AsyncService<Request, Streams: StreamPack = ()> {
    /// The input data of the request
    pub request: Request,
    /// Stream channels that will let you send stream information. This will
    /// usually be a [`StreamChannel`] or a (possibly nested) tuple of
    /// `StreamChannel`s, whichever matches the [`StreamPack`] description.
    pub streams: Streams::Channel,
    /// The channel that allows querying and syncing with the world while the
    /// service runs asynchronously.
    pub channel: Channel,
    /// The entity providing the service
    pub provider: Entity,
    /// The node in a workflow or impulse chain that asked for the service
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// Use this to reduce backet noise when you need `In<`[`AsyncService<R, S>`]`>`.
pub type AsyncServiceInput<Request, Streams = ()> = In<AsyncService<Request, Streams>>;

/// Use BlockingCallback to indicate that your system is meant to define a
/// blocking [`Callback`]. Callbacks are different from services because they are
/// not associated with any entity.
///
/// Alternatively any Bevy system with an input of `In<Request>` can be converted
/// into a blocking callback by applying
/// [`.into_blocking_callback()`](crate::IntoBlockingCallback).
#[non_exhaustive]
pub struct BlockingCallback<Request, Streams: StreamPack = ()> {
    /// The input data of the request
    pub request: Request,
    /// The buffer to hold stream output data until the function is finished
    pub streams: Streams::Buffer,
    /// The node in a workflow or impulse chain that asked for the callback
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// Use this to reduce bracket noise when you need `In<`[`BlockingCallback<R, S>`]`>`.
pub type BlockingCallbackInput<Request, Streams = ()> = In<BlockingCallback<Request, Streams>>;

/// Use AsyncCallback to indicate that your system or function is meant to define
/// an async [`Callback`]. An async callback is not associated with any entity,
/// and it must return a [`Future<Output=Response>`](std::future::Future) that
/// will be polled by the async task pool.
#[non_exhaustive]
pub struct AsyncCallback<Request, Streams: StreamPack = ()> {
    /// The input data of the request
    pub request: Request,
    /// Stream channels that will let you send stream information. This will
    /// usually be a [`StreamChannel`] or a (possibly nested) tuple of
    /// `StreamChannel`s, whichever matches the [`StreamPack`] description.
    pub streams: Streams::Channel,
    /// The channel that allows querying and syncing with the world while the
    /// service runs asynchronously.
    pub channel: Channel,
    /// The node in a workflow or impulse chain that asked for the callback
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// Use this to reduce bracket noise when you need `In<`[`AsyncCallback<R, S>`]`>`.
pub type AsyncCallbackInput<Request, Streams = ()> = In<AsyncCallback<Request, Streams>>;

/// Use `BlockingMap`` to indicate that your function is a blocking map. A map
/// is not associated with any entity, and it cannot be a Bevy System. These
/// restrictions allow them to be processed more efficiently.
#[non_exhaustive]
pub struct BlockingMap<Request, Streams: StreamPack = ()> {
    /// The input data of the request
    pub request: Request,
    /// The buffer to hold stream output data until the function is finished
    pub streams: Streams::Buffer,
    /// The node in a workflow or impulse chain that asked for the callback
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// Use AsyncMap to indicate that your function is an async map. A Map is not
/// associated with any entity, and it cannot be a Bevy System. These
/// restrictions allow them to be processed more efficiently.
///
/// An async map must return a [`Future<Output=Response>`](std::future::Future)
/// that will be polled by the async task pool.
#[non_exhaustive]
pub struct AsyncMap<Request, Streams: StreamPack = ()> {
    /// The input data of the request
    pub request: Request,
    /// Stream channels that will let you send stream information. This will
    /// usually be a [`StreamChannel`] or a (possibly nested) tuple of
    /// `StreamChannel`s, whichever matches the [`StreamPack`] description.
    pub streams: Streams::Channel,
    /// The channel that allows querying and syncing with the world while the
    /// service runs asynchronously.
    pub channel: Channel,
    /// The node in a workflow or impulse chain that asked for the callback
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}
