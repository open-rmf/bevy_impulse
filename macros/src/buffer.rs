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
    let buffer: Vec<&Type> = field_config.iter().map(|config| &config.buffer).collect();
    let noncopy = field_config.iter().any(|config| config.noncopy);

    let buffer_struct: ItemStruct = parse_quote! {
        #[allow(non_camel_case_types, unused)]
        #buffer_struct_vis struct #buffer_struct_ident #impl_generics #where_clause {
            #(
                #buffer_struct_vis #field_ident: #buffer,
            )*
        }
    };

    let buffer_clone_impl = if noncopy {
        // Clone impl for structs with a buffer that is not copyable
        quote! {
            impl #impl_generics ::std::clone::Clone for #buffer_struct_ident #ty_generics #where_clause {
                fn clone(&self) -> Self {
                    Self {
                        #(
                            #field_ident: self.#field_ident.clone(),
                        )*
                    }
                }
            }
        }
    } else {
        // Clone and copy impl for structs with buffers that are all copyable
        quote! {
            impl #impl_generics ::std::clone::Clone for #buffer_struct_ident #ty_generics #where_clause {
                fn clone(&self) -> Self {
                    *self
                }
            }

            impl #impl_generics ::std::marker::Copy for #buffer_struct_ident #ty_generics #where_clause {}
        }
    };

    let impl_buffer_map_layout = impl_buffer_map_layout(&buffer_struct, &input_struct)?;
    let impl_joined = impl_joined(&buffer_struct, &input_struct)?;

    let gen = quote! {
        impl #impl_generics ::bevy_impulse::JoinedValue for #struct_ident #ty_generics #where_clause {
            type Buffers = #buffer_struct_ident #ty_generics;
        }

        #buffer_struct

        #buffer_clone_impl

        impl #impl_generics #struct_ident #ty_generics #where_clause {
            fn select_buffers(
                #(
                    #field_ident: #buffer,
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

/// Code that are currently unused but could be used in the future, move them out of this mod if
/// they are ever used.
#[allow(unused)]
mod _unused {
    use super::*;

    /// Converts a list of generics to a [`PhantomData`] TypePath.
    /// e.g. `::std::marker::PhantomData<fn(T,)>`
    fn to_phantom_data(generics: &Generics) -> TypePath {
        let lifetimes: Vec<Type> = generics
            .lifetimes()
            .map(|lt| {
                let lt = &lt.lifetime;
                let ty: Type = parse_quote! { & #lt () };
                ty
            })
            .collect();
        let ty_params: Vec<&Ident> = generics.type_params().map(|ty| &ty.ident).collect();
        parse_quote! { ::std::marker::PhantomData<fn(#(#lifetimes,)* #(#ty_params,)*)> }
    }
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
            .find(|attr| attr.path().is_ident("joined"));

        if let Some(attr) = attr {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("buffers_struct_name") {
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
    buffer: Type,
    noncopy: bool,
}

impl FieldConfig {
    fn from_field(field: &Field) -> Self {
        let ty = &field.ty;
        let mut config = Self {
            buffer: parse_quote! { ::bevy_impulse::Buffer<#ty> },
            noncopy: false,
        };

        for attr in field.attrs.iter().filter(|attr| attr.path().is_ident("joined")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("buffer") {
                    config.buffer = meta.value()?.parse()?;
                }
                if meta.path.is_ident("noncopy_buffer") {
                    config.noncopy = true;
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
    let buffer: Vec<&Type> = field_config.iter().map(|config| &config.buffer).collect();
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
                    let #field_ident = if let Ok(buffer) = compatibility.require_buffer_type::<#buffer>(#map_key, buffers) {
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
