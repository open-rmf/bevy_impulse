use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, Type};

use crate::Result;

pub(crate) fn impl_joined_value(ast: DeriveInput) -> Result<TokenStream> {
    let struct_ident = ast.ident;
    let (field_ident, field_type): (Vec<Ident>, Vec<Type>) = match ast.data {
        syn::Data::Struct(data) => get_fields_map(data.fields)?.into_iter().unzip(),
        _ => return Err("expected struct".to_string()),
    };
    let map_key: Vec<String> = field_ident.iter().map(|v| v.to_string()).collect();
    let struct_buffer_ident = format_ident!("__bevy_impulse_{}_Buffers", struct_ident);

    let impl_buffer_map_layout =
        buffer_map_layout(&struct_buffer_ident, &field_ident, &field_type, &map_key);
    let impl_joined = joined(&struct_buffer_ident, &struct_ident, &field_ident);

    let gen = quote! {
        impl ::bevy_impulse::JoinedValue for #struct_ident {
            type Buffers = #struct_buffer_ident;
        }

        #[derive(Clone)]
        #[allow(non_camel_case_types)]
        struct #struct_buffer_ident {
            #(
                #field_ident: ::bevy_impulse::Buffer<#field_type>,
            )*
        }

        #impl_buffer_map_layout

        #impl_joined
    };

    Ok(gen.into())
}

fn get_fields_map(fields: syn::Fields) -> Result<Vec<(Ident, Type)>> {
    match fields {
        syn::Fields::Named(data) => {
            let mut idents_types = Vec::with_capacity(data.named.len());
            for field in data.named {
                let ident = field.ident.ok_or("expected named fields".to_string())?;
                idents_types.push((ident, field.ty));
            }
            Ok(idents_types)
        }
        _ => return Err("expected named fields".to_string()),
    }
}

fn buffer_map_layout(
    struct_ident: &Ident,
    field_ident: &Vec<Ident>,
    field_type: &Vec<Type>,
    map_key: &Vec<String>,
) -> proc_macro2::TokenStream {
    quote! {
        impl ::bevy_impulse::BufferMapLayout for #struct_ident {
            fn buffer_list(&self) -> ::smallvec::SmallVec<[AnyBuffer; 8]> {
                use smallvec::smallvec;
                smallvec![#(
                    self.#field_ident.as_any_buffer(),
                )*]
            }

            fn try_from_buffer_map(buffers: &::bevy_impulse::BufferMap) -> Result<Self, ::bevy_impulse::IncompatibleLayout> {
                let mut compatibility = ::bevy_impulse::IncompatibleLayout::default();
                #(
                    let #field_ident = if let Ok(buffer) = compatibility.require_message_type::<#field_type>(#map_key, buffers) {
                        buffer
                    } else {
                        return Err(compatibility);
                    };
                )*

                Ok(Self {#(
                    #field_ident,
                )*})
            }
        }
    }
}

fn joined(
    struct_ident: &Ident,
    item_struct_ident: &Ident,
    field_ident: &Vec<Ident>,
) -> proc_macro2::TokenStream {
    quote! {
        impl ::bevy_impulse::Joined for #struct_ident {
            type Item = #item_struct_ident;

            fn pull(&self, session: ::bevy_ecs::prelude::Entity, world: &mut ::bevy_ecs::prelude::World) -> Result<Self::Item, ::bevy_impulse::OperationError> {
                #(
                    let #field_ident = self.#field_ident.pull(session, world)?;
                )*

                Ok(Self::Item {#(
                    #field_ident,
                )*})
            }
        }
    }
}
