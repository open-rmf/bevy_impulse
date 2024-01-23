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

pub mod channel;
pub use channel::*;

pub mod discovery;
pub use discovery::*;

pub mod operation;
pub use operation::*;

pub mod promise;
pub use promise::*;

pub mod request;
pub use request::*;

pub mod service;
pub use service::*;

pub mod stream;
pub use stream::*;

pub(crate) mod private;

use bevy::prelude::{Entity, In};

/// Use BlockingReq to indicate that your service is blocking and to specify its
/// input request type. For example:
///
/// ```
/// use bevy::prelude::*;
/// use bevy_services::*;
///
/// #[derive(Component, Resource)]
/// struct Precision(i32);
///
/// fn rounding_service(
///     In(input): InBlockingReq<f64>,
///     global_precision: Res<Precision>,
/// ) -> f64 {
///     (input.request * 10_f64.powi(global_precision.0)).floor() * 10_f64.powi(-global_precision.0)
/// }
/// ```
///
/// The systems of more complex services might need to know what entity is
/// providing the service, e.g. if the service provider is configured with
/// additional components that need to be queried when a request comes in. For
/// that you can check the `provider` field of [`BlockingReq`]`:
///
/// ```
/// use bevy::prelude::*;
/// use bevy_services::*;
///
/// #[derive(Component, Resource)]
/// struct Precision(i32);
///
/// fn rounding_service(
///     In(BlockingReq{request, provider}): InBlockingReq<f64>,
///     service_precision: Query<&Precision>,
///     global_precision: Res<Precision>,
/// ) -> f64 {
///     let precision = service_precision.get(provider).unwrap_or(&*global_precision).0;
///     (request * 10_f64.powi(precision)).floor() * 10_f64.powi(-precision)
/// }
/// ```
pub struct BlockingReq<Request> {
    pub request: Request,
    pub provider: Entity,
}

/// Use this to reduce bracket noise when you need `In<BlockingReq<R>>`.
pub type InBlockingReq<Request> = In<BlockingReq<Request>>;

/// Use AsyncReq to indicate that your service is async and to specify its input
/// request type. Being async means it will return a `Future<Output=Option<Response>>`
/// which will be processed by a task pool.
///
/// This comes with a Channel that allows your Future to interact with Bevy's
/// ECS asynchronously from inside the task pool.
pub struct AsyncReq<Request, Streams = ()> {
    pub request: Request,
    pub channel: Channel<Streams>,
    pub provider: Entity,
}

/// Use this to reduce backet noise when you need `In<AsyncReq<R, S>>`.
pub type InAsyncReq<Request, Streams = ()> = In<AsyncReq<Request, Streams>>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {

    }
}
