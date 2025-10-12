/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

//! Miscellaneous utilities used internally
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Used with `#[serde(default, skip_serializing_if = "is_default")]` for fields
/// that don't need to be serialized if they are a default value.
#[allow(unused)]
pub(crate) fn is_default<T: std::default::Default + PartialEq>(value: &T) -> bool {
    let default = T::default();
    *value == default
}

/// Used with `#[serde(default = "default_as_true", skip_serializing_if = "is_true")]`
/// for bools that should be true by default.
#[allow(unused)]
pub(crate) fn default_as_true() -> bool {
    true
}

/// Used with `#[serde(default = "default_as_true", skip_serializing_if = "is_true")]`
/// for bools that should be true by default.
#[allow(unused)]
pub(crate) fn is_true(value: &bool) -> bool {
    *value
}

/// This is used to block a future from ever returning. This should only be used
/// in a race to force one of the contesting futures to lose. Make sure that at
/// least one contesting future will finish or else this will lead to a deadlock.
#[allow(unused)]
pub(crate) struct NeverFinish;

impl Future for NeverFinish {
    type Output = Never;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

/// A data structure that can never be instantiated
pub(crate) enum Never {}

/// Used by some tests in the grpc and zenoh modules
#[allow(unused)]
pub(crate) fn clamp(val: f32, limit: f32) -> f32 {
    if f32::abs(val) > limit {
        return f32::signum(val) * limit;
    }

    val
}
