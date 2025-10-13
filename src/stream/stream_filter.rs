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

use variadics_please::all_tuples;

use crate::{StreamAvailability, StreamPack};

/// Used by [`ServiceDiscovery`](crate::ServiceDiscovery) to filter services
/// based on what streams they provide. If a stream is required, you should wrap
/// the stream type in [`Require`]. If a stream is optional, then wrap it in
/// [`Option`].
///
/// The service you receive will appear as though it provides all the stream
/// types wrapped in both your `Require` and `Option` filters, but it might not
/// actually provide any of the streams that were wrapped in `Option`. A service
/// that does not actually provide the optional stream can be treated as if it
/// does provide the stream, except it will never actually send out any of that
/// optional stream data.
///
/// ```
/// use bevy_impulse::{Require, prelude::*, testing::*};
///
/// fn service_discovery_system(
///     discover: ServiceDiscovery<
///         f32,
///         f32,
///         (
///             Require<(StreamOf<f32>, StreamOf<u32>)>,
///             Option<(StreamOf<String>, StreamOf<u8>)>,
///         )
///     >
/// ) {
///     let service: Service<
///         f32,
///         f32,
///         (
///             (StreamOf<f32>, StreamOf<u32>),
///             (StreamOf<String>, StreamOf<u8>),
///         )
///     > = discover.iter().next().unwrap();
/// }
/// ```
pub trait StreamFilter {
    type Pack: StreamPack;
    fn are_required_streams_available(availability: Option<&StreamAvailability>) -> bool;
}

/// Used by [`ServiceDiscovery`](crate::ServiceDiscovery) to indicate that a
/// certain pack of streams is required.
///
/// For streams that are optional, wrap them in [`Option`] instead.
///
/// See [`StreamFilter`] for a usage example.
pub struct Require<T> {
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T: StreamPack> StreamFilter for Require<T> {
    type Pack = T;
    fn are_required_streams_available(availability: Option<&StreamAvailability>) -> bool {
        let Some(availability) = availability else {
            return false;
        };
        T::are_streams_available(availability)
    }
}

impl<T: StreamPack> StreamFilter for Option<T> {
    type Pack = T;

    fn are_required_streams_available(_: Option<&StreamAvailability>) -> bool {
        true
    }
}

macro_rules! impl_streamfilter_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: StreamFilter),*> StreamFilter for ($($T,)*) {
            type Pack = ($($T::Pack,)*);

            fn are_required_streams_available(_availability: Option<&StreamAvailability>) -> bool {
                true
                $(
                    && $T::are_required_streams_available(_availability)
                )*
            }
        }
    }
}

// Implements the `StreamFilter` trait for all tuples between size 0 and 12
// (inclusive) made of types that implement `StreamFilter`
all_tuples!(impl_streamfilter_for_tuple, 0, 12, T);
