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

mod buffer;
use buffer::{impl_buffer_key_map, impl_joined_value};

mod section;
use section::impl_section;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, ItemStruct};

#[proc_macro_derive(Stream)]
pub fn simple_stream_macro(item: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(item).unwrap();
    let struct_name = &ast.ident;
    let (impl_generics, type_generics, where_clause) = &ast.generics.split_for_impl();

    quote! {
        impl #impl_generics ::bevy_impulse::Stream for #struct_name #type_generics #where_clause {
            type Container = ::bevy_impulse::DefaultStreamContainer<Self>;
        }
    }
    .into()
}

#[proc_macro_derive(DeliveryLabel)]
pub fn delivery_label_macro(item: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(item).unwrap();
    let struct_name = &ast.ident;
    let (impl_generics, type_generics, where_clause) = &ast.generics.split_for_impl();

    quote! {
        impl #impl_generics ::bevy_impulse::DeliveryLabel for #struct_name #type_generics #where_clause {
            fn dyn_clone(&self) -> Box<dyn DeliveryLabel> {
                ::std::boxed::Box::new(::std::clone::Clone::clone(self))
            }

            fn as_dyn_eq(&self) -> &dyn ::bevy_impulse::utils::DynEq {
                self
            }

            fn dyn_hash(&self, mut state: &mut dyn ::std::hash::Hasher) {
                let ty_id = ::std::any::TypeId::of::<Self>();
                ::std::hash::Hash::hash(&ty_id, &mut state);
                ::std::hash::Hash::hash(self, &mut state);
            }
        }
    }
    .into()
}

/// The result error is the compiler error message to be displayed.
type Result<T> = std::result::Result<T, String>;

#[proc_macro_derive(Joined, attributes(joined))]
pub fn derive_joined_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    match impl_joined_value(&input) {
        Ok(tokens) => tokens.into(),
        Err(msg) => quote! {
            compile_error!(#msg);
        }
        .into(),
    }
}

#[proc_macro_derive(Accessor, attributes(key))]
pub fn derive_buffer_key_map(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    match impl_buffer_key_map(&input) {
        Ok(tokens) => tokens.into(),
        Err(msg) => quote! {
            compile_error!(#msg);
        }
        .into(),
    }
}

#[proc_macro_derive(Section)]
pub fn derive_section(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    match impl_section(&input) {
        Ok(tokens) => tokens.into(),
        Err(msg) => quote! {
            compile_error!(#msg);
        }
        .into(),
    }
}
