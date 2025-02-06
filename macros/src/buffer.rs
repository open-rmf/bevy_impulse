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
    let struct_buffer_ident = format_ident!("__{}Buffers", struct_ident);

    let impl_buffer_map_layout =
        buffer_map_layout(&struct_ident, &field_ident, &field_type, &map_key);

    let gen = quote! {
        #impl_buffer_map_layout

        impl ::bevy_impulse::JoinedValue for #struct_ident {
            type Buffers = #struct_buffer_ident;

            fn pull(
                buffers: &::bevy_impulse::BufferMap,
                session: ::bevy_ecs::prelude::Entity,
                world: &mut ::bevy_ecs::prelude::World,
            ) -> Result<Self, ::bevy_impulse::OperationError> {
                use ::bevy_impulse::{ManageBuffer, OrBroken};

                #(
                    let #field_ident = world
                        .get_entity_mut(buffers.get(#map_key).or_broken()?.id())
                        .or_broken()?
                        .pull_from_buffer::<#field_type>(session)?;
                )*

                Ok(Self {
                    #(
                        #field_ident
                    ),*
                })
            }
        }

        struct #struct_buffer_ident {
            #(
                #field_ident: ::bevy_impulse::Buffer<#field_type>
            ),*
        }

        impl From<#struct_buffer_ident> for ::bevy_impulse::BufferMap {
            fn from(value: #struct_buffer_ident) -> Self {
                let mut buffers = ::bevy_impulse::BufferMap::default();
                #(
                    buffers.insert(std::borrow::Cow::Borrowed(#map_key), value.#field_ident);
                )*
                buffers
            }
        }
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
            fn is_compatible(buffers: &BufferMap) -> Result<(), ::bevy_impulse::IncompatibleLayout> {
                let mut compatibility = ::bevy_impulse::IncompatibleLayout::default();
                #(
                    compatibility.require_buffer::<#field_type>(#map_key, buffers);
                )*
                compatibility.into_result()
            }

            fn buffered_count(
                buffers: &::bevy_impulse::BufferMap,
                session: ::bevy_ecs::prelude::Entity,
                world: &::bevy_ecs::prelude::World,
            ) -> Result<usize, ::bevy_impulse::OperationError> {
                use ::bevy_impulse::{InspectBuffer, OrBroken};

                #(
                    let #field_ident = world
                        .get_entity(buffers.get(#map_key).or_broken()?.id())
                        .or_broken()?
                        .buffered_count::<#field_type>(session)?;
                )*

                Ok([#( #field_ident ),*]
                    .iter()
                    .min()
                    .copied()
                    .unwrap_or(0))
            }

            fn ensure_active_session(
                buffers: &::bevy_impulse::BufferMap,
                session: ::bevy_ecs::prelude::Entity,
                world: &mut ::bevy_ecs::prelude::World,
            ) -> ::bevy_impulse::OperationResult {
                use ::bevy_impulse::{ManageBuffer, OrBroken};

                #(
                    world
                        .get_entity_mut(buffers.get(#map_key).or_broken()?.id())
                        .or_broken()?
                        .ensure_session::<#field_type>(session)?;
                )*

                Ok(())
            }
        }
    }
}

pub(crate) fn impl_buffer_key_map(ast: DeriveInput) -> Result<TokenStream> {
    let struct_ident = ast.ident;
    let (field_ident, field_type): (Vec<Ident>, Vec<Type>) = match ast.data {
        syn::Data::Struct(data) => get_fields_map(data.fields)?.into_iter().unzip(),
        _ => return Err("expected struct".to_string()),
    };
    let map_key: Vec<String> = field_ident.iter().map(|v| v.to_string()).collect();

    let impl_buffer_map_layout =
        buffer_map_layout(&struct_ident, &field_ident, &field_type, &map_key);

    // FIXME(koonpeng): `create_key` does not allow failure and we can't guarantee that the buffer
    // from the buffer map is valid.
    let gen = quote! {
        impl ::bevy_impulse::BufferKeyMap for #struct_ident {
            fn add_accessor(buffers: &::bevy_impulse::BufferMap, accessor: ::bevy_ecs::prelude::Entity, world: &mut ::bevy_ecs::prelude::World) -> ::bevy_impulse::OperationResult {
                use ::bevy_impulse::{Accessed, OrBroken};

                #(
                    buffers.get(#map_key).or_broken()?.add_accessor(accessor, world)?;
                )*

                Ok(())
            }

            fn create_key(buffers: &::bevy_impulse::BufferMap, builder: &::bevy_impulse::BufferKeyBuilder) -> Self {
                use ::bevy_impulse::{Accessed, OrBroken};

                Self {#(
                    #field_ident: buffers.get(#map_key).unwrap().create_key(builder).try_into().unwrap(),
                )*}
            }

            fn deep_clone_key(&self) -> Self {
                Self {#(
                    #field_ident: self.#field_ident.deep_clone(),
                )*}
            }

            fn is_key_in_use(&self) -> bool {
                #(
                    self.#field_ident.is_in_use()
                )||*
            }
        }

        #impl_buffer_map_layout
    };

    Ok(gen.into())
}
