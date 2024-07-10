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

pub mod handler;
pub use handler::*;

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

/// Use BlockingService to indicate that your system is a blocking service and
/// to specify its input request type. For example:
///
/// ```
/// use bevy::prelude::*;
/// use bevy_impulse::*;
///
/// #[derive(Component, Resource)]
/// struct Precision(i32);
///
/// fn rounding_service(
///     In(input): InBlockingService<f64>,
///     global_precision: Res<Precision>,
/// ) -> f64 {
///     (input.request * 10_f64.powi(global_precision.0)).floor() * 10_f64.powi(-global_precision.0)
/// }
/// ```
///
/// The systems of more complex services might need to know what entity is
/// providing the service, e.g. if the service provider is configured with
/// additional components that need to be queried when a request comes in. For
/// that you can check the `provider` field of `BlockingService`:
///
/// ```
/// use bevy::prelude::*;
/// use bevy_impulse::*;
///
/// #[derive(Component, Resource)]
/// struct Precision(i32);
///
/// fn rounding_service(
///     In(BlockingService{request, provider, ..}): InBlockingService<f64>,
///     service_precision: Query<&Precision>,
///     global_precision: Res<Precision>,
/// ) -> f64 {
///     let precision = service_precision.get(provider).unwrap_or(&*global_precision).0;
///     (request * 10_f64.powi(precision)).floor() * 10_f64.powi(-precision)
/// }
/// ```
#[non_exhaustive]
pub struct BlockingService<Request, Streams: StreamPack = ()> {
    pub request: Request,
    pub provider: Entity,
    pub streams: Streams::Buffer,
}

/// Use this to reduce bracket noise when you need `In<BlockingService<R>>`.
pub type InBlockingService<Request, Streams = ()> = In<BlockingService<Request, Streams>>;

/// Use AsyncService to indicate that your system is an async service and to
/// specify its input request type. Being async means it must return a
/// `Future<Output=Response>` which will be processed by a task pool.
///
/// This comes with a Channel that allows your Future to interact with Bevy's
/// ECS asynchronously while it is polled from inside the task pool.
#[non_exhaustive]
pub struct AsyncService<Request, Streams: StreamPack = ()> {
    pub request: Request,
    pub channel: Channel<Streams>,
    pub provider: Entity,
}

/// Use this to reduce backet noise when you need `In<AsyncService<R, S>>`.
pub type InAsyncService<Request, Streams = ()> = In<AsyncService<Request, Streams>>;

/// Use BlockingHandler to indicate that your system is meant to define a
/// blocking [`Handler`]. Handlers are different from services because they are
/// not associated with any entity.
#[non_exhaustive]
pub struct BlockingHandler<Request, Streams: StreamPack = ()> {
    pub request: Request,
    pub streams: Streams::Buffer,
}

/// Use AsyncHandler to indicate that your system or function is meant to define
/// an async [`Handler`]. An async handler is not associated with any entity,
/// and it must return a [`Future<Output=Response>`](std::future::Future) that
/// will be polled by the async task pool.
#[non_exhaustive]
pub struct AsyncHandler<Request, Streams: StreamPack = ()> {
    pub request: Request,
    pub channel: Channel<Streams>,
}

/// Use BlockingMap to indicate that your function is meant to define a blocking
/// [`Map`]. A Map is not associated with any entity, it cannot be a Bevy System,
/// and it can only be used once, making it suitable for functions that only
/// implement [`FnOnce`].
#[non_exhaustive]
pub struct BlockingMap<Request, Streams: StreamPack = ()> {
    pub request: Request,
    pub streams: Streams::Buffer,
}

/// Use AsyncMap to indicate that your function is meant to define an async
/// [`Map`]. A Map is not associated with any entity, it cannot be a Bevy System,
/// and it can only be used once, making it suitable for functions that only
/// implement [`FnOnce`].
///
/// An async Map must return a [`Future<Output=Response>`](std::future::Future)
/// that will be polled by the async task pool.
#[non_exhaustive]
pub struct AsyncMap<Request, Streams: StreamPack = ()> {
    pub request: Request,
    pub channel: Channel<Streams>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {

    }
}
