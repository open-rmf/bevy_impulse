use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_quote, Ident, ItemStruct, Type};

use crate::Result;

pub(crate) fn impl_joined_value(input_struct: &ItemStruct) -> Result<TokenStream> {
    let struct_ident = &input_struct.ident;
    let (impl_generics, ty_generics, where_clause) = input_struct.generics.split_for_impl();
    let (field_ident, field_type) = get_fields_map(&input_struct.fields)?;
    let BufferStructConfig {
        struct_name: buffer_struct_ident,
    } = BufferStructConfig::from_data_struct(&input_struct);
    let buffer_struct_vis = &input_struct.vis;

    let buffer_struct: ItemStruct = parse_quote! {
        #[derive(Clone)]
        #[allow(non_camel_case_types)]
        #buffer_struct_vis struct #buffer_struct_ident #impl_generics #where_clause {
            #(
                #buffer_struct_vis #field_ident: ::bevy_impulse::Buffer<#field_type>,
            )*
        }
    };

    let impl_buffer_map_layout = impl_buffer_map_layout(&buffer_struct, &input_struct)?;
    let impl_joined = impl_joined(&buffer_struct, &input_struct)?;

    let gen = quote! {
        impl #impl_generics ::bevy_impulse::JoinedValue for #struct_ident #ty_generics #where_clause {
            type Buffers = #buffer_struct_ident #ty_generics;
        }

        #buffer_struct

        impl #impl_generics #struct_ident #ty_generics #where_clause {
            fn select_buffers(
                #(
                    #field_ident: ::bevy_impulse::Buffer<#field_type>,
                )*
            ) -> #buffer_struct_ident #ty_generics {
                #buffer_struct_ident {
                    #(
                        #field_ident,
                    )*
                }
            }
        }

        #impl_buffer_map_layout

        #impl_joined
    };

    Ok(gen.into())
}

struct BufferStructConfig {
    struct_name: Ident,
}

impl BufferStructConfig {
    fn from_data_struct(data_struct: &ItemStruct) -> Self {
        let mut config = Self {
            struct_name: format_ident!("__bevy_impulse_{}_Buffers", data_struct.ident),
        };

        let attr = data_struct
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("buffers"));

        if let Some(attr) = attr {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("struct_name") {
                    config.struct_name = meta.value()?.parse()?;
                }
                Ok(())
            })
            // panic if attribute is malformed, this will result in a compile error which is intended.
            .unwrap();
        }

        config
    }
}

fn get_fields_map(fields: &syn::Fields) -> Result<(Vec<&Ident>, Vec<&Type>)> {
    match fields {
        syn::Fields::Named(data) => {
            let mut idents = Vec::new();
            let mut types = Vec::new();
            for field in &data.named {
                let ident = field
                    .ident
                    .as_ref()
                    .ok_or("expected named fields".to_string())?;
                idents.push(ident);
                types.push(&field.ty);
            }
            Ok((idents, types))
        }
        _ => return Err("expected named fields".to_string()),
    }
}

/// Params:
///   buffer_struct: The struct to implement `BufferMapLayout`.
///   item_struct: The struct which `buffer_struct` is derived from.
fn impl_buffer_map_layout(
    buffer_struct: &ItemStruct,
    item_struct: &ItemStruct,
) -> Result<proc_macro2::TokenStream> {
    let struct_ident = &buffer_struct.ident;
    let (impl_generics, ty_generics, where_clause) = buffer_struct.generics.split_for_impl();
    let (field_ident, field_type) = get_fields_map(&item_struct.fields)?;
    let map_key: Vec<String> = field_ident.iter().map(|v| v.to_string()).collect();

    Ok(quote! {
        impl #impl_generics ::bevy_impulse::BufferMapLayout for #struct_ident #ty_generics #where_clause {
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
    .into())
}

/// Params:
///   joined_struct: The struct to implement `Joined`.
///   item_struct: The associated `Item` type to use for the `Joined` implementation.
fn impl_joined(
    joined_struct: &ItemStruct,
    item_struct: &ItemStruct,
) -> Result<proc_macro2::TokenStream> {
    let struct_ident = &joined_struct.ident;
    let item_struct_ident = &item_struct.ident;
    let (impl_generics, ty_generics, where_clause) = item_struct.generics.split_for_impl();
    let (field_ident, _) = get_fields_map(&item_struct.fields)?;

    Ok(quote! {
        impl #impl_generics ::bevy_impulse::Joined for #struct_ident #ty_generics #where_clause {
            type Item = #item_struct_ident #ty_generics;

            fn pull(&self, session: ::bevy_ecs::prelude::Entity, world: &mut ::bevy_ecs::prelude::World) -> Result<Self::Item, ::bevy_impulse::OperationError> {
                #(
                    let #field_ident = self.#field_ident.pull(session, world)?;
                )*

                Ok(Self::Item {#(
                    #field_ident,
                )*})
            }
        }
    }.into())
}
