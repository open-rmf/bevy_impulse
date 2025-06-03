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

//! ![sense-think-act workflow](https://raw.githubusercontent.com/open-rmf/bevy_impulse/main/assets/figures/sense-think-act_workflow.svg)
//!
//! Bevy impulse is an extension to the [Bevy](https://bevyengine.org) game
//! engine that allows you to transform [bevy systems](https://bevyengine.org/learn/quick-start/getting-started/ecs/)
//! into services and workflows that can be used for reactive service-oriented
//! programming.
//!
//! ## Services
//!
//! One primitive element of reactive programming is a [service](https://en.wikipedia.org/wiki/Service_(systems_architecture)).
//! In bevy impulse, a [`Service`] is a bevy system that is associated with an
//! entity and can be created using [`Commands::spawn_service`](SpawnServicesExt::spawn_service)
//! or [`App::add_service`](AddServicesExt::add_service).
//!
//! When you spawn a service you will immediately receive a [`Service`] object
//! which references the newly spawned service. If you do not want to hang onto the [`Service`]
//! object, you can find previously spawned services later using the [`ServiceDiscovery`]
//! system parameter.
//!
//! Sometimes [`Service`] is not quite the right fit for your use case, so bevy impulse
//! offers a generalization of services callled [`Provider`] which has some
//! more options for defining a reactive element.
//!
//! ## Workflows
//!
//! For complex async workflows, a single bevy system may not be sufficient.
//! You can instead build workflows using [`.spawn_workflow`](SpawnWorkflowExt::spawn_workflow)
//! on [`Commands`](bevy_ecs::prelude::Commands) or [`World`](bevy_ecs::prelude::World).
//! A workflow lets you create a graph of [nodes](Node) where each node is a
//! [service](Service) (or more generally a [provider](Provider)) with an input,
//! an output, and possibly streams.
//!
//! There are various operations that can be performed between nodes, such as
//! forking and joining. These operations are built using [`Chain`].
//!
//! When you spawn your workflow, you will receive a [`Service`] object that
//! lets you use the workflow as if it's an ordinary service.
//!
//! ## Impulses
//!
//! Services and workflows are reusable building blocks for creating a reactive
//! application. In order to actually run them, call [`Commands::request`](RequestExt::request)
//! which will provide you with an [`Impulse`]. An impulse is a one-time-use
//! reaction to a request which you can chain to subsequent reactions using
//! [`Impulse::then`]. Any impulse chain that you create will only run exactly
//! once.
//!
//! Once you've finished building your chain, use [`Impulse::detach`] to let it
//! run freely, or use [`Impulse::take`] to get a [`Recipient`] of the final
//! result.

mod async_execution;
pub use async_execution::Sendish;

pub mod buffer;
pub use buffer::*;

pub mod re_exports;

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

#[cfg(feature = "diagram")]
pub mod diagram;
#[cfg(feature = "diagram")]
pub use diagram::*;

pub mod disposal;
pub use disposal::*;

pub mod errors;
pub use errors::*;

pub mod flush;
pub use flush::*;

pub mod gate;
pub use gate::*;

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

pub mod trim;
pub use trim::*;

pub mod type_info;

use bevy_app::prelude::{App, Plugin, Update};
use bevy_ecs::prelude::{Entity, In};

extern crate self as bevy_impulse;

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
/// use bevy_ecs::prelude::*;
/// use bevy_impulse::prelude::*;
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
    pub streams: Streams::StreamBuffers,
    /// The entity providing the service
    pub provider: Entity,
    /// The node in a workflow or impulse chain that asked for the service
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// Use this to reduce bracket noise when you need `In<BlockingService<R>>`.
pub type BlockingServiceInput<Request, Streams = ()> = In<BlockingService<Request, Streams>>;

/// Use `AsyncService` to indicate that your system is an async [`Service`]. Being
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
    pub streams: Streams::StreamChannels,
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

/// Use `ContinuousService` to indicate that your system is a [`Service`] that
/// runs incrementally inside of a schedule with each update of the Bevy ECS.
pub struct ContinuousService<Request, Response, Streams: StreamPack = ()> {
    /// Pass this into a [`ContinuousQuery`] to access the ongoing requests for
    /// this service. While accessing the ongoing requests, you will also be
    /// able to send streams and responses for the requests.
    pub key: ContinuousServiceKey<Request, Response, Streams>,
}

/// Use this to reduce the bracket noise when you need `In<`[`ContinuousService`]`>`.
pub type ContinuousServiceInput<Request, Response, Streams = ()> =
    In<ContinuousService<Request, Response, Streams>>;

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
    pub streams: Streams::StreamBuffers,
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
    pub streams: Streams::StreamChannels,
    /// The channel that allows querying and syncing with the world while the
    /// callback executes asynchronously.
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
    pub streams: Streams::StreamBuffers,
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
    pub streams: Streams::StreamChannels,
    /// The channel that allows querying and syncing with the world while the
    /// map executes asynchronously.
    pub channel: Channel,
    /// The node in a workflow or impulse chain that asked for the callback
    pub source: Entity,
    /// The unique session ID for the workflow
    pub session: Entity,
}

/// This plugin simply adds [`flush_impulses()`] to the [`Update`] schedule of your
/// applicatation. For more fine-grained control you can call `flush_impulses`
/// yourself and configure its relationship to other systems as you see fit.
///
/// If you do not have at least one usage of `flush_impulses()` somewhere in
/// your application then workflows will not work.
#[derive(Default)]
pub struct ImpulsePlugin {}

impl Plugin for ImpulsePlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Update, flush_impulses());
    }
}

pub mod prelude {
    pub use crate::{
        buffer::{
            Accessible, Accessor, AnyBuffer, AnyBufferKey, AnyBufferMut, AnyBufferWorldAccess,
            AnyMessageBox, AsAnyBuffer, Buffer, BufferAccess, BufferAccessMut, BufferKey,
            BufferMap, BufferMapLayout, BufferSettings, BufferWorldAccess, Bufferable, Buffering,
            IncompatibleLayout, IterBufferable, Joinable, Joined, RetentionPolicy,
        },
        builder::Builder,
        callback::{AsCallback, Callback, IntoAsyncCallback, IntoBlockingCallback},
        chain::{Chain, ForkCloneBuilder, UnzipBuilder, Unzippable},
        flush::flush_impulses,
        impulse::{Impulse, Recipient},
        map::{AsMap, IntoAsyncMap, IntoBlockingMap},
        map_once::{AsMapOnce, IntoAsyncMapOnce, IntoBlockingMapOnce},
        node::{ForkCloneOutput, InputSlot, Node, Output},
        promise::{Promise, PromiseState},
        provider::{ProvideOnce, Provider},
        request::{RequestExt, RunCommandsOnWorldExt},
        service::{
            traits::*, AddContinuousServicesExt, AddServicesExt, AsDeliveryInstructions,
            DeliveryInstructions, DeliveryLabel, DeliveryLabelId, IntoAsyncService,
            IntoBlockingService, Service, ServiceDiscovery, SpawnServicesExt,
        },
        stream::{DynamicallyNamedStream, NamedValue, Stream, StreamFilter, StreamOf, StreamPack},
        trim::{TrimBranch, TrimPoint},
        workflow::{DeliverySettings, Scope, ScopeSettings, SpawnWorkflowExt, WorkflowSettings},
        AsyncCallback, AsyncCallbackInput, AsyncMap, AsyncService, AsyncServiceInput,
        BlockingCallback, BlockingCallbackInput, BlockingMap, BlockingService,
        BlockingServiceInput, ContinuousQuery, ContinuousService, ContinuousServiceInput,
        ImpulsePlugin,
    };

    pub use bevy_ecs::prelude::In;

    #[cfg(feature = "diagram")]
    pub use crate::{
        buffer::{JsonBuffer, JsonBufferKey, JsonBufferMut, JsonBufferWorldAccess, JsonMessage},
        diagram::{Diagram, DiagramElementRegistry, DiagramError, NodeBuilderOptions},
    };

    pub use futures::FutureExt;
}
