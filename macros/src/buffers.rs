use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, Type};

use crate::Result;

pub(crate) fn impl_buffer_map_layout(ast: DeriveInput) -> Result<TokenStream> {
    let struct_ident = ast.ident;
    let (field_ident, field_type) = match ast.data {
        syn::Data::Struct(data) => get_fields_map(data.fields)?,
        _ => return Err("expected struct".to_string()),
    };
    let map_key: Vec<String> = field_ident.iter().map(|v| v.to_string()).collect();
    let struct_buffer_ident = format_ident!("__{}Buffers", struct_ident);

    let gen = quote! {
        impl BufferMapLayout for #struct_ident {
            fn is_compatible(buffers: &BufferMap) -> Result<(), IncompatibleLayout> {
                let mut compatibility = IncompatibleLayout::default();
                #(
                    compatibility.require_buffer::<#field_type>(#map_key, buffers);
                )*
                compatibility.into_result()
            }

            fn buffered_count(
                buffers: &BufferMap,
                session: Entity,
                world: &World,
            ) -> Result<usize, OperationError> {
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
                buffers: &BufferMap,
                session: Entity,
                world: &mut World,
            ) -> OperationResult {
                #(
                    world
                        .get_entity_mut(buffers.get(#map_key).or_broken()?.id())
                        .or_broken()?
                        .ensure_session::<#field_type>(session)?;
                )*

                Ok(())
            }
        }

        impl JoinedValue for #struct_ident {
            type Buffers = #struct_buffer_ident;

            fn pull(
                buffers: &BufferMap,
                session: Entity,
                world: &mut World,
            ) -> Result<Self, OperationError> {
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
                #field_ident: Buffer<#field_type>
            ),*
        }

        impl From<#struct_buffer_ident> for BufferMap {
            fn from(value: #struct_buffer_ident) -> Self {
                let mut buffers = BufferMap::default();
                #(
                    buffers.insert(Cow::Borrowed(#map_key), value.#field_ident);
                )*
                buffers
            }
        }
    };

    Ok(gen.into())
}

fn get_fields_map(fields: syn::Fields) -> Result<(Vec<Ident>, Vec<Type>)> {
    match fields {
        syn::Fields::Named(data) => {
            let mut idents = Vec::with_capacity(data.named.len());
            let mut types = Vec::with_capacity(data.named.len());
            for field in data.named {
                let ident = field.ident.ok_or("expected named fields".to_string())?;
                idents.push(ident);
                types.push(field.ty);
            }
            Ok((idents, types))
        }
        _ => return Err("expected named fields".to_string()),
    }
}
