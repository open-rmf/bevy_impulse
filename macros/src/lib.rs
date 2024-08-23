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
use quote::quote;
use syn::DeriveInput;

#[proc_macro_derive(Stream)]
pub fn simple_stream_macro(item: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(item).unwrap();
    let struct_name = &ast.ident;
    let (impl_generics, type_generics, where_clause) = &ast.generics.split_for_impl();
    // let bevy_impulse_path: Path =

    quote! {
        impl #impl_generics Stream for #struct_name #type_generics #where_clause {
            type Container = ::bevy_impulse::DefaultStreamContainer<Self>;
        }
    }
    .into()
}
