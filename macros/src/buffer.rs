use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_quote, Field, Generics, Ident, ItemStruct, Type, TypePath};

use crate::Result;

pub(crate) fn impl_joined_value(input_struct: &ItemStruct) -> Result<TokenStream> {
    let struct_ident = &input_struct.ident;
    let (impl_generics, ty_generics, where_clause) = input_struct.generics.split_for_impl();
    let StructConfig {
        buffer_struct_name: buffer_struct_ident,
    } = StructConfig::from_data_struct(&input_struct);
    let buffer_struct_vis = &input_struct.vis;

    let (field_ident, _, field_config) = get_fields_map(&input_struct.fields)?;
    let buffer_type: Vec<&Type> = field_config
        .iter()
        .map(|config| &config.buffer_type)
        .collect();

    let buffer_struct: ItemStruct = parse_quote! {
        #[derive(Clone)]
        #[allow(non_camel_case_types)]
        #buffer_struct_vis struct #buffer_struct_ident #impl_generics #where_clause {
            #(
                #buffer_struct_vis #field_ident: #buffer_type,
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
                    #field_ident: #buffer_type,
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

/// Converts a list of generics to a [`PhantomData`] TypePath.
/// e.g. `::std::marker::PhantomData<(T,)>`
// Currently unused but could be used in the future
fn _to_phantom_data(generics: &Generics) -> TypePath {
    let lifetimes: Vec<Type> = generics
        .lifetimes()
        .map(|lt| {
            let lt = &lt.lifetime;
            let ty: Type = parse_quote! { & #lt () };
            ty
        })
        .collect();
    let ty_params: Vec<&Ident> = generics.type_params().map(|ty| &ty.ident).collect();
    parse_quote! { ::std::marker::PhantomData<(#(#lifetimes,)* #(#ty_params,)*)> }
}

struct StructConfig {
    buffer_struct_name: Ident,
}

impl StructConfig {
    fn from_data_struct(data_struct: &ItemStruct) -> Self {
        let mut config = Self {
            buffer_struct_name: format_ident!("__bevy_impulse_{}_Buffers", data_struct.ident),
        };

        let attr = data_struct
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("buffers"));

        if let Some(attr) = attr {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("buffer_struct_name") {
                    config.buffer_struct_name = meta.value()?.parse()?;
                }
                Ok(())
            })
            // panic if attribute is malformed, this will result in a compile error which is intended.
            .unwrap();
        }

        config
    }
}

struct FieldConfig {
    buffer_type: Type,
}

impl FieldConfig {
    fn from_field(field: &Field) -> Self {
        let ty = &field.ty;
        let mut config = Self {
            buffer_type: parse_quote! { ::bevy_impulse::Buffer<#ty> },
        };

        let attr = field
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("buffers"));

        if let Some(attr) = attr {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("buffer_type") {
                    config.buffer_type = meta.value()?.parse()?;
                }
                Ok(())
            })
            // panic if attribute is malformed, this will result in a compile error which is intended.
            .unwrap();
        }

        config
    }
}

fn get_fields_map(fields: &syn::Fields) -> Result<(Vec<&Ident>, Vec<&Type>, Vec<FieldConfig>)> {
    match fields {
        syn::Fields::Named(data) => {
            let mut idents = Vec::new();
            let mut types = Vec::new();
            let mut configs = Vec::new();
            for field in &data.named {
                let ident = field
                    .ident
                    .as_ref()
                    .ok_or("expected named fields".to_string())?;
                idents.push(ident);
                types.push(&field.ty);
                configs.push(FieldConfig::from_field(field));
            }
            Ok((idents, types, configs))
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
    let (field_ident, _, field_config) = get_fields_map(&item_struct.fields)?;
    let buffer_type: Vec<&Type> = field_config
        .iter()
        .map(|config| &config.buffer_type)
        .collect();
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
                    let #field_ident = if let Ok(buffer) = compatibility.require_buffer_type::<#buffer_type>(#map_key, buffers) {
                        buffer
                    } else {
                        return Err(compatibility);
                    };
                )*

                Ok(Self {
                    #(
                        #field_ident,
                    )*
                })
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
    let (field_ident, _, _) = get_fields_map(&item_struct.fields)?;

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
