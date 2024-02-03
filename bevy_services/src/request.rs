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

use crate::{
    PromiseCommands, UnusedTarget, InputBundle, Stream, Provider,
};

use bevy::{
    prelude::Commands,
    utils::define_label,
};

mod internal;
pub use internal::{ApplyLabel, BuildLabel};

define_label!(
    /// A strongly-typed class of labels used to identify requests that have been
    /// issued to a service.
    RequestLabel,
    /// Strongly-typed identifier for a [`RequestLabel`].
    RequestLabelId,
);

pub trait RequestExt<'w, 's> {
    /// Call this with [`Commands`] to request a service
    fn request<'a, P: Provider>(
        &'a mut self,
        provider: P,
        request: P::Request,
    ) -> PromiseCommands<'w, 's, 'a, P::Response, P::Streams, ()>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: Stream;
}

impl<'w, 's> RequestExt<'w, 's> for Commands<'w, 's> {
    fn request<'a, P: Provider>(
        &'a mut self,
        provider: P,
        request: P::Request,
    ) -> PromiseCommands<'w, 's, 'a, P::Response, P::Streams, ()>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: Stream,
    {
        let source = self.spawn(InputBundle::new(request)).id();
        let target = self.spawn(UnusedTarget).id();
        provider.provide(source, target, self);

        PromiseCommands::new(source, target, self)
    }
}

/// By default when a service provider receives a new request with the same
/// label as an earlier request, the earlier request will be canceled,
/// whether it is already being executed or whether it is sitting in a
/// queue. If the earlier request was already delivered then the labeling
/// has no effect.
///
/// To change the default behavior there are two modifiers you can apply to
/// this label:
/// - `.queue()` asks for the request to be queued up to run after all
///   other requests with this same label have been fulfilled and not cancel
///   any of them.
/// - `.ensure()` asks for this request to not be canceled even if another
///   request comes in with the same label. The new request will instead be
///   queued after this one.
///
/// You can choose to use either, both, or neither of these modifiers in
/// whatever way fits your use case. No matter what modifiers you choose
/// (or don't choose) the same service provider will never simultaneously
/// execute its service for two requests with the same label value. To that
/// extent, applying a label always guarantees mutual exclusivity between
/// requests.
///
/// This mutual exclusivity can be useful if the service involves making
/// modifications to the world which would conflict with each other when two
/// related requests are being delivered at the same time.
pub struct LabelBuilder<Q, E> {
    label: RequestLabelId,
    queue: bool,
    ensure: bool,
    _ignore: std::marker::PhantomData<(Q, E)>,
}

pub struct Chosen;

impl LabelBuilder<(), ()> {
    /// Begin building a label for a request. You do not need to call this
    /// function explicitly. You can instead use `.queue()` or `.ensure()`
    /// directly on a `RequestLabel` instance.
    pub fn new(label: impl RequestLabel) -> LabelBuilder<(), ()> {
        LabelBuilder {
            label: label.as_label(),
            queue: false,
            ensure: false,
            _ignore: Default::default()
        }
    }
}

impl<E> LabelBuilder<(), E> {
    /// Queue this labeled request to be handled **after** all other requests
    /// with the same label have been fulfilled. Do not automatically cancel
    /// pending requests that have the same label.
    ///
    /// The default behavior, if you do **not** trigger this method, is for this
    /// new labeled request to supplant all prior requests that share the same
    /// label, sending them to the canceled state (unless the prior request was
    /// marked with [`ensure()`]).
    ///
    /// This modifer can only be applied to a labeled request because it does
    /// not make sense for unlabeled requests.
    pub fn queue(self) -> LabelBuilder<Chosen, E> {
        LabelBuilder {
            label: self.label,
            queue: true,
            ensure: self.ensure,
            _ignore: Default::default(),
        }
    }
}

impl<Q> LabelBuilder<Q, ()> {
    /// Ensure that this request is resolved even if another request with the
    /// same label arrives.
    ///
    /// Ordinarily a new labeled request would supplant all earlier requests
    /// with the same label, sending them into the canceled state. But any
    /// of those requests that are "ensured" will remain queued and finish
    /// executing, one at a time.
    ///
    /// This modifier can only be applied to labeled requests because it does
    /// not make sense for unlabeled requests.
    pub fn ensure(self) -> LabelBuilder<Q, Chosen> {
        LabelBuilder {
            label: self.label,
            queue: self.queue,
            ensure: true,
            _ignore: Default::default(),
        }
    }
}
