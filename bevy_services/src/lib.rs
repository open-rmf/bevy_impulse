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

pub mod assistant;
pub use assistant::*;

mod delivery;
pub(crate) use delivery::*;

pub mod discovery;
pub use discovery::*;

pub mod promise;
pub use promise::*;

pub mod request;
pub use request::*;

pub mod service;
pub use service::*;

pub mod stream;
pub use stream::*;

pub(crate) mod private;

/// Use Req to indicate the request data structure that your service's system
/// takes as input. For example this signature can be used for simple services
/// that only need the request data as input:
///
/// ```
/// fn my_service(
///     In(Req(request)): In<Req<MyRequestData>>,
///     other: Query<&OtherComponents>,
/// ) -> Job<impl FnOnce(Assistant) -> Option<MyResponseData>> {
///     /* ... */
/// }
/// ```
///
/// On the other hand, the systems of more complex services might also need to
/// know what entity is providing the service, e.g. if the service provider is
/// configured with additional components that need to be queried when a request
/// comes in. For that you can use the self-aware signature:
///
/// ```
/// fn my_self_aware_service(
///     In((me, Req(request))): In<(Entity, Req<MyRequestData>)>,
///     query_service_params: Query<&MyServiceParams>,
///     other: Query<&OtherComponents>,
/// ) -> Job<impl FnOnce(Assistant) -> Option<MyResponseData>> {
///     let my_params = query_service_params.get(me).unwrap();
///     /* ... */
/// }
/// ```
pub struct Req<Request>(pub Request);

/// Wrap [`Resp`] around the return value of your service's system to indicate
/// it will immediately return a response to the request. This means your
/// service is blocking and all other system execution will halt while it is
/// running. It should only be used for services that execute very quickly.
/// Here is an example:
/// ```
/// fn my_blocking_service(
///     In(Req(request)): In<Req<MyRequestData>>,
///     other: Query<&OtherComponents>,
/// ) -> Resp<MyResponseData> {
///     /* ... */
///
///     Resp(my_response)
/// }
/// ```
///
/// To define an async service use [`Job`].
pub struct Resp<Response>(pub Response);

/// Wrap [`Job`] around the return value of your service's system to provide a
/// function that will be passed along as a task instead of immediately
/// returning a response. Here is an example:
///
/// ```
/// fn my_async_service(
///     In(Req(request)): In<Req<MyRequestData>>,
///     other: Query<&OtherComponents>,
/// ) -> Job<impl FnOnce(Assistant) -> MyResponseData> {
///     /* ... */
///
///     let job = |assistant: Assistant| {
///         /* ... */
///         my_response
///     };
///
///     Job(job)
/// }
/// ```
///
/// By returning [`Job`] you are indicating that your service is async. To have
/// a blocking service that immediately returns a response, return [`Resp`]
/// instead.
pub struct Job<Task>(pub Task);


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {

    }
}
