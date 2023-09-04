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

use crate::{Provider, PromiseCommands, Req, UnusedTarget};

use bevy::{
    prelude::Commands,
    utils::define_label,
};

mod internal;
pub use internal::SubmitRequest;

define_label!(
    /// A strongly-typed class of labels used to identify requests that have been
    /// issued to a service.
    RequestLabel,
    /// Strongly-typed identifier for a [`RequestLabel`].
    RequestLabelId,
);

pub trait RequestExt<'w, 's> {
    /// Call this with [`Commands`] to request a service
    fn request<'a, Request, Response, Streams>(
        &'a mut self,
        provider: Provider<Request, Response, Streams>,
        request: impl SubmitRequest<Request>,
    ) -> PromiseCommands<'w, 's, 'a, Response, Streams>;
}

impl<'w, 's> RequestExt<'w, 's> for Commands<'w, 's> {
    fn request<'a, Request, Response, Streams>(
        &'a mut self,
        provider: Provider<Request, Response, Streams>,
        request: impl SubmitRequest<Request>,
    ) -> PromiseCommands<'w, 's, 'a, Response, Streams> {
        let target = request.apply(self);
        self.entity(target).insert(UnusedTarget);
        PromiseCommands::new(provider.get(), target, self)
    }
}

pub struct RequestBuilder<Request, L, Q, E> {
    request: Request,
    label: Option<RequestLabelId>,
    queue: bool,
    ensure: bool,
    _ignore: std::marker::PhantomData<(L, Q, E)>,
}

pub struct Chosen;

impl<Request> Req<Request> {
    /// Convert the request into a labeled request managed by a [`RequestBuilder`].
    pub fn label(self, label: impl RequestLabel) -> RequestBuilder<Request, Chosen, (), ()> {
        RequestBuilder::new(self.0).label(label)
    }
}

impl<Request> RequestBuilder<Request, (), (), ()> {
    /// Put a label on this request.
    ///
    /// By default when a service provider receives a new request with the same
    /// label as an earlier request, the earlier request will be canceled,
    /// whether it is already being executed or whether it is sitting in a
    /// queue. If the earlier request was already delivered then the labeling
    /// has no effect.
    ///
    /// To change the default behavior there are two modifiers you can call on
    /// this request after the label is applied:
    /// - `.queue()` asks for this request to be queued up to run after all
    ///   other requests with this same label have been fulfilled and not cancel
    ///   any of them.
    /// - `.ensure()` asks for this request to not be canceled even if another
    ///   request comes in with the same label. The new request will instead be
    ///   queued.
    ///
    /// You can choose to use either, both, or neither of these modifiers in
    /// whatever way fits your use case. No matter what modifiers you choose
    /// (or don't choose) the same service provider will never simultaneously
    /// execute its service for two requests with the same label value. To that
    /// extent, giving a request label always guarantees mutual exclusivity.
    ///
    /// This mutual exclusivity can be useful if the service involves making
    /// modifications to the world which would conflict with each other when two
    /// related requests are being delivered at the same time.
    pub fn label(self, label: impl RequestLabel) -> RequestBuilder<Request, Chosen, (), ()> {
        RequestBuilder {
            request: self.request,
            label: Some(label.as_label()),
            queue: false,
            ensure: false,
            _ignore: Default::default()
        }
    }

    /// Create a new request builder to pass into the [`Commands`]`::request`
    /// method. Alternatively you can use [`Req`] as a shortcut, e.g.:
    ///
    /// Simple request with default settings:
    /// ```
    /// commands
    ///     .request(provider, Req(request))
    ///     .detach();
    /// ```
    ///
    /// Labeled request with default settings:
    /// ```
    /// commands
    ///     .request(
    ///         provider,
    ///         Req(request).label(my_label)
    ///     )
    ///     .detach();
    /// ```
    ///
    /// Using `.label()` on `Req` will automatically change it into a
    /// [`RequestBuilder`].
    pub fn new(request: Request) -> Self {
        Self {
            request,
            label: None,
            queue: false,
            ensure: false,
            _ignore: Default::default(),
        }
    }
}

impl<Request, E> RequestBuilder<Request, Chosen, (), E> {
    /// Queue this labeled request to be handled **after** all other requests
    /// with the same label have been fulfilled. Do not automatically cancel
    /// pending requests that have the same label.
    ///
    /// The default behavior, if you do **not** trigger this method, is for this
    /// new labeled request to supplant all prior requests that share the same
    /// label, sending them to the canceled state (unless the request
    /// was marked with [`ensure()`]).
    ///
    /// This modifer can only be applied to a labeled request because it does
    /// not make sense for unlabeled requests.
    pub fn queue(self) -> RequestBuilder<Request, Chosen, Chosen, E> {
        RequestBuilder {
            request: self.request,
            label: self.label,
            queue: true,
            ensure: self.ensure,
            _ignore: Default::default(),
        }
    }
}

impl<Request, Q> RequestBuilder<Request, Chosen, Q, ()> {
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
    pub fn ensure(self) -> RequestBuilder<Request, Chosen, Q, Chosen> {
        RequestBuilder {
            request: self.request,
            label: self.label,
            queue: self.queue,
            ensure: true,
            _ignore: Default::default(),
        }
    }
}

