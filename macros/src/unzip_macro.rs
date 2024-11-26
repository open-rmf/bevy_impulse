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

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned, format_ident};
use syn::{
    DeriveInput, Data, DataStruct, Attribute, Meta, DataEnum, Error,
    spanned::Spanned,
};

pub(super) fn impl_unzip_macro(item: proc_macro::TokenStream) -> Result<TokenStream, TokenStream> {
    let ast: DeriveInput = syn::parse(item).unwrap();
    let struct_name = &ast.ident;
    let vis = &ast.vis;
    let unzipped_name = format_ident!("{struct_name}Unzipped");
    let (impl_generics, type_generics, where_clause) = &ast.generics.split_for_impl();

    let fields = match &ast.data {
        Data::Struct(s) => {
            unzipped_struct_fields(s)?
        }
        Data::Enum(e) => {
            return Err(quote_spanned! {
                e.enum_token.span() =>
                ::std::compile_error!("Unzip macro will eventually support enums");
            });
        }
        Data::Union(u) => {
            return Err(quote_spanned! {
                u.union_token.span() =>
                ::std::compile_error!("Unzip macro does not support unions");
            });
        }
    };

    Ok(quote! {
        #vis struct #unzipped_name #type_generics #where_clause {
            #(#fields),*
        }
    })
}

fn unzipped_struct_fields(s: &DataStruct) -> Result<Vec<TokenStream>, TokenStream> {

    let mut fields = Vec::new();
    for field in &s.fields {
        if unzip_discard_field(&field.attrs)? {
            continue;
        }

        let Some(ident) = &field.ident else {
            continue;
        };
        let vis = &field.vis;
        let ty = &field.ty;

        fields.push(quote! {
            #vis #ident: ::bevy_impulse::Output<#ty>
        });
    }

    Ok(fields)
}

fn unzip_discard_field(attributes: &Vec<Attribute>) -> Result<bool, TokenStream> {
    for attr in attributes {
        match &attr.meta {
            Meta::List(list) => {
                if !list.path.is_ident("unzip") {
                    continue;
                }

                let mut discard = false;
                list.parse_nested_meta(|meta| {
                    if meta.path.is_ident("discard") {
                        discard = true;
                        return Ok(());
                    }

                    if meta.path.is_ident("default") {
                        return Ok(());
                    }

                    Err(Error::new_spanned(meta.path, "Unrecognized argument for unzip attribute"))
                })
                .map_err(Error::into_compile_error)?;

                if discard {
                    return Ok(true);
                }
            }
            Meta::NameValue(nv) => {
                if nv.path.is_ident("unzip") {
                    return Err(Error::new_spanned(
                        &nv.path,
                        "Invalid format for unzip attribute, it should take arguments",
                    ).into_compile_error());
                }
            }
            Meta::Path(path) => {
                if path.is_ident("unzip") {
                    return Err(Error::new_spanned(
                        path,
                        "unzip attribute requires arguments like 'discard' or 'default'"
                    ).into_compile_error());
                }
            }
        }
    }

    Ok(false)
}

