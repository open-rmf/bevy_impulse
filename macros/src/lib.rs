/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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

use proc_macro::TokenStream;

mod stream_macro;
use stream_macro::*;

/// Provide the Stream derive macro
#[proc_macro_derive(Stream)]
pub fn stream_macro(item: TokenStream) -> TokenStream {
    impl_stream_macro(item)
}

mod delivery_label_macro;
use delivery_label_macro::*;

/// Provide the DeliveryLabel derive macro
#[proc_macro_derive(DeliveryLabel)]
pub fn delivery_label_macro(item: TokenStream) -> TokenStream {
    impl_delivery_label_macro(item)
}

mod unzip_macro;
use unzip_macro::*;

/// Provide the Unzip derive macro
#[proc_macro_derive(Unzip, attributes(unzip))]
pub fn unzip_macro(item: TokenStream) -> TokenStream {
    match impl_unzip_macro(item) {
        Ok(ts) => ts.into(),
        Err(ts) => ts.into(),
    }
}
