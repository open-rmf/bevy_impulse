use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_quote, Field, Generics, Ident, ImplGenerics, ItemStruct, Type, TypeGenerics, TypePath,
    Visibility, WhereClause,
};

use crate::Result;

const JOINED_ATTR_TAG: &'static str = "joined";
const KEY_ATTR_TAG: &'static str = "key";

pub(crate) fn impl_joined_value(input_struct: &ItemStruct) -> Result<TokenStream> {
    let struct_ident = &input_struct.ident;
    let (impl_generics, ty_generics, where_clause) = input_struct.generics.split_for_impl();
    let StructConfig {
        buffer_struct_name: buffer_struct_ident,
    } = StructConfig::from_data_struct(&input_struct, &JOINED_ATTR_TAG);
    let buffer_struct_vis = &input_struct.vis;

    let (field_ident, _, field_config) =
        get_fields_map(&input_struct.fields, FieldSettings::for_joined())?;
    let buffer: Vec<&Type> = field_config.iter().map(|config| &config.buffer).collect();
    let noncopy = field_config.iter().any(|config| config.noncopy);

    let buffer_struct: ItemStruct = generate_buffer_struct(
        &buffer_struct_ident,
        buffer_struct_vis,
        &impl_generics,
        &where_clause,
        &field_ident,
        &buffer,
    );

    let impl_buffer_clone = impl_buffer_clone(
        &buffer_struct_ident,
        &impl_generics,
        &ty_generics,
        &where_clause,
        &field_ident,
        noncopy,
    );

    let impl_select_buffers = impl_select_buffers(
        struct_ident,
        &buffer_struct_ident,
        buffer_struct_vis,
        &impl_generics,
        &ty_generics,
        &where_clause,
        &field_ident,
        &buffer,
    );

    let impl_buffer_map_layout =
        impl_buffer_map_layout(&buffer_struct, &field_ident, &field_config)?;
    let impl_joined = impl_joined(&buffer_struct, &input_struct, &field_ident)?;

    let gen = quote! {
        impl #impl_generics ::bevy_impulse::Joined for #struct_ident #ty_generics #where_clause {
            type Buffers = #buffer_struct_ident #ty_generics;
        }

        #buffer_struct

        #impl_buffer_clone

        #impl_select_buffers

        #impl_buffer_map_layout

        #impl_joined
    };

    Ok(gen.into())
}

pub(crate) fn impl_buffer_key_map(input_struct: &ItemStruct) -> Result<TokenStream> {
    let struct_ident = &input_struct.ident;
    let (impl_generics, ty_generics, where_clause) = input_struct.generics.split_for_impl();
    let StructConfig {
        buffer_struct_name: buffer_struct_ident,
    } = StructConfig::from_data_struct(&input_struct, &KEY_ATTR_TAG);
    let buffer_struct_vis = &input_struct.vis;

    let (field_ident, field_type, field_config) =
        get_fields_map(&input_struct.fields, FieldSettings::for_key())?;
    let buffer: Vec<&Type> = field_config.iter().map(|config| &config.buffer).collect();
    let noncopy = field_config.iter().any(|config| config.noncopy);

    let buffer_struct: ItemStruct = generate_buffer_struct(
        &buffer_struct_ident,
        buffer_struct_vis,
        &impl_generics,
        &where_clause,
        &field_ident,
        &buffer,
    );

    let impl_buffer_clone = impl_buffer_clone(
        &buffer_struct_ident,
        &impl_generics,
        &ty_generics,
        &where_clause,
        &field_ident,
        noncopy,
    );

    let impl_select_buffers = impl_select_buffers(
        struct_ident,
        &buffer_struct_ident,
        buffer_struct_vis,
        &impl_generics,
        &ty_generics,
        &where_clause,
        &field_ident,
        &buffer,
    );

    let impl_buffer_map_layout =
        impl_buffer_map_layout(&buffer_struct, &field_ident, &field_config)?;
    let impl_accessed = impl_accessed(&buffer_struct, &input_struct, &field_ident, &field_type)?;

    let gen = quote! {
        impl #impl_generics ::bevy_impulse::Accessor for #struct_ident #ty_generics #where_clause {
            type Buffers = #buffer_struct_ident #ty_generics;
        }

        #buffer_struct

        #impl_buffer_clone

        #impl_select_buffers

        #impl_buffer_map_layout

        #impl_accessed
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
    fn from_data_struct(data_struct: &ItemStruct, attr_tag: &str) -> Self {
        let mut config = Self {
            buffer_struct_name: format_ident!("__bevy_impulse_{}_Buffers", data_struct.ident),
        };

        let attr = data_struct
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident(attr_tag));

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

struct FieldSettings {
    default_buffer: fn(&Type) -> Type,
    attr_tag: &'static str,
}

impl FieldSettings {
    fn for_joined() -> Self {
        Self {
            default_buffer: Self::default_field_for_joined,
            attr_tag: JOINED_ATTR_TAG,
        }
    }

    fn for_key() -> Self {
        Self {
            default_buffer: Self::default_field_for_key,
            attr_tag: KEY_ATTR_TAG,
        }
    }

    fn default_field_for_joined(ty: &Type) -> Type {
        parse_quote! { ::bevy_impulse::FetchFromBuffer<#ty> }
    }

    fn default_field_for_key(ty: &Type) -> Type {
        parse_quote! { <#ty as ::bevy_impulse::BufferKeyLifecycle>::TargetBuffer }
    }
}

struct FieldConfig {
    buffer: Type,
    noncopy: bool,
}

impl FieldConfig {
    fn from_field(field: &Field, settings: &FieldSettings) -> Self {
        let ty = &field.ty;
        let mut config = Self {
            buffer: (settings.default_buffer)(ty),
            noncopy: false,
        };

        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident(settings.attr_tag))
        {
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

fn get_fields_map(
    fields: &syn::Fields,
    settings: FieldSettings,
) -> Result<(Vec<&Ident>, Vec<&Type>, Vec<FieldConfig>)> {
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
                configs.push(FieldConfig::from_field(field, &settings));
            }
            Ok((idents, types, configs))
        }
        _ => return Err("expected named fields".to_string()),
    }
}

fn generate_buffer_struct(
    buffer_struct_ident: &Ident,
    buffer_struct_vis: &Visibility,
    impl_generics: &ImplGenerics,
    where_clause: &Option<&WhereClause>,
    field_ident: &Vec<&Ident>,
    buffer: &Vec<&Type>,
) -> ItemStruct {
    parse_quote! {
        #[allow(non_camel_case_types, unused)]
        #buffer_struct_vis struct #buffer_struct_ident #impl_generics #where_clause {
            #(
                #buffer_struct_vis #field_ident: #buffer,
            )*
        }
    }
}

fn impl_select_buffers(
    struct_ident: &Ident,
    buffer_struct_ident: &Ident,
    buffer_struct_vis: &Visibility,
    impl_generics: &ImplGenerics,
    ty_generics: &TypeGenerics,
    where_clause: &Option<&WhereClause>,
    field_ident: &Vec<&Ident>,
    buffer: &Vec<&Type>,
) -> TokenStream {
    quote! {
        impl #impl_generics #struct_ident #ty_generics #where_clause {
            #buffer_struct_vis fn select_buffers(
                #(
                    #field_ident: impl Into< #buffer >,
                )*
            ) -> #buffer_struct_ident #ty_generics {
                #buffer_struct_ident {
                    #(
                        #field_ident: #field_ident .into(),
                    )*
                }
            }
        }
    }
    .into()
}

fn impl_buffer_clone(
    buffer_struct_ident: &Ident,
    impl_generics: &ImplGenerics,
    ty_generics: &TypeGenerics,
    where_clause: &Option<&WhereClause>,
    field_ident: &Vec<&Ident>,
    noncopy: bool,
) -> TokenStream {
    if noncopy {
        // Clone impl for structs with a buffer that is not copyable
        quote! {
            impl #impl_generics ::bevy_impulse::re_exports::Clone for #buffer_struct_ident #ty_generics #where_clause {
                fn clone(&self) -> Self {
                    Self {
                        #(
                            #field_ident: ::bevy_impulse::re_exports::Clone::clone(&self.#field_ident),
                        )*
                    }
                }
            }
        }
    } else {
        // Clone and copy impl for structs with buffers that are all copyable
        quote! {
            impl #impl_generics ::bevy_impulse::re_exports::Clone for #buffer_struct_ident #ty_generics #where_clause {
                fn clone(&self) -> Self {
                    *self
                }
            }

            impl #impl_generics ::bevy_impulse::re_exports::Copy for #buffer_struct_ident #ty_generics #where_clause {}
        }
    }
}

/// Params:
///   buffer_struct: The struct to implement `BufferMapLayout`.
///   item_struct: The struct which `buffer_struct` is derived from.
///   settings: [`FieldSettings`] to use when parsing the field attributes
fn impl_buffer_map_layout(
    buffer_struct: &ItemStruct,
    field_ident: &Vec<&Ident>,
    field_config: &Vec<FieldConfig>,
) -> Result<proc_macro2::TokenStream> {
    let struct_ident = &buffer_struct.ident;
    let (impl_generics, ty_generics, where_clause) = buffer_struct.generics.split_for_impl();
    let buffer: Vec<&Type> = field_config.iter().map(|config| &config.buffer).collect();
    let map_key: Vec<String> = field_ident.iter().map(|v| v.to_string()).collect();

    Ok(quote! {
        impl #impl_generics ::bevy_impulse::BufferMapLayout for #struct_ident #ty_generics #where_clause {
            fn try_from_buffer_map(buffers: &::bevy_impulse::BufferMap) -> Result<Self, ::bevy_impulse::IncompatibleLayout> {
                let mut compatibility = ::bevy_impulse::IncompatibleLayout::default();
                #(
                    let #field_ident = compatibility.require_buffer_for_identifier::<#buffer>(#map_key, buffers);
                )*

                // Unwrap the Ok after inspecting every field so that the
                // IncompatibleLayout error can include all information about
                // which fields were incompatible.
                #(
                    let Ok(#field_ident) = #field_ident else {
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

        impl #impl_generics ::bevy_impulse::BufferMapStruct for #struct_ident #ty_generics #where_clause {
            fn buffer_list(&self) -> ::bevy_impulse::re_exports::SmallVec<[::bevy_impulse::AnyBuffer; 8]> {
                ::bevy_impulse::re_exports::smallvec![#(
                    ::bevy_impulse::AsAnyBuffer::as_any_buffer(&self.#field_ident),
                )*]
            }
        }
    }
    .into())
}

/// Params:
///   joined_struct: The struct to implement `Joining`.
///   item_struct: The associated `Item` type to use for the `Joining` implementation.
fn impl_joined(
    joined_struct: &ItemStruct,
    item_struct: &ItemStruct,
    field_ident: &Vec<&Ident>,
) -> Result<proc_macro2::TokenStream> {
    let struct_ident = &joined_struct.ident;
    let item_struct_ident = &item_struct.ident;
    let (impl_generics, ty_generics, where_clause) = item_struct.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::bevy_impulse::Joining for #struct_ident #ty_generics #where_clause {
            type Item = #item_struct_ident #ty_generics;

            fn fetch_for_join(&self, session: ::bevy_impulse::re_exports::Entity, world: &mut ::bevy_impulse::re_exports::World) -> Result<Self::Item, ::bevy_impulse::OperationError> {
                #(
                    let #field_ident = self.#field_ident.fetch_for_join(session, world)?;
                )*

                Ok(Self::Item {#(
                    #field_ident,
                )*})
            }
        }
    }.into())
}

fn impl_accessed(
    accessed_struct: &ItemStruct,
    key_struct: &ItemStruct,
    field_ident: &Vec<&Ident>,
    field_type: &Vec<&Type>,
) -> Result<proc_macro2::TokenStream> {
    let struct_ident = &accessed_struct.ident;
    let key_struct_ident = &key_struct.ident;
    let (impl_generics, ty_generics, where_clause) = key_struct.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::bevy_impulse::Accessing for #struct_ident #ty_generics #where_clause {
            type Key = #key_struct_ident #ty_generics;

            fn add_accessor(
                &self,
                accessor: ::bevy_impulse::re_exports::Entity,
                world: &mut ::bevy_impulse::re_exports::World,
            ) -> ::bevy_impulse::OperationResult {
                #(
                    ::bevy_impulse::Accessing::add_accessor(&self.#field_ident, accessor, world)?;
                )*
                Ok(())
            }

            fn create_key(&self, builder: &::bevy_impulse::BufferKeyBuilder) -> Self::Key {
                Self::Key {#(
                    // TODO(@mxgrey): This currently does not have good support for the user
                    // substituting in a different key type than what the BufferKeyLifecycle expects.
                    // We could consider adding a .clone().into() to help support that use case, but
                    // this would be such a niche use case that I think we can ignore it for now.
                    #field_ident: <#field_type as ::bevy_impulse::BufferKeyLifecycle>::create_key(&self.#field_ident, builder),
                )*}
            }

            fn deep_clone_key(key: &Self::Key) -> Self::Key {
                Self::Key {#(
                    #field_ident: ::bevy_impulse::BufferKeyLifecycle::deep_clone(&key.#field_ident),
                )*}
            }

            fn is_key_in_use(key: &Self::Key) -> bool {
                false
                    #(
                        || ::bevy_impulse::BufferKeyLifecycle::is_in_use(&key.#field_ident)
                    )*
            }
        }
    }.into())
}
