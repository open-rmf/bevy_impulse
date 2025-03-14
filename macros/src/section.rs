use std::iter::zip;

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::{
    parse_quote, parse_quote_spanned, spanned::Spanned, Field, Ident, ItemStruct, Member, Type,
};

use crate::Result;

pub(crate) fn impl_section(input_struct: &ItemStruct) -> Result<TokenStream> {
    let struct_ident = &input_struct.ident;
    let field_ident: Vec<Ident> = input_struct
        .fields
        .members()
        .filter_map(|m| match m {
            Member::Named(m) => Some(m),
            _ => None,
        })
        .collect();
    let field_name_str: Vec<String> = field_ident.iter().map(|ident| ident.to_string()).collect();
    let field_type: Vec<&Type> = input_struct.fields.iter().map(|f| &f.ty).collect();
    let field_configs: Vec<(FieldConfig, Span)> = input_struct
        .fields
        .iter()
        .map(|f| (FieldConfig::from_field(f), f.ty.span()))
        .collect();

    let (impl_generics, ty_generics, where_clause) = input_struct.generics.split_for_impl();
    let mut add_generics = input_struct.generics.clone();
    add_generics
        .params
        .push(parse_quote!(__SerializationOptionsT__));
    let add_where_clause = add_generics.make_where_clause();
    add_where_clause
        .predicates
        .push(parse_quote!(__SerializationOptionsT__: ::bevy_impulse::SerializationOptions));
    add_where_clause
        .predicates
        .push(parse_quote!(__SerializationOptionsT__::DefaultSerializer: 'static));
    for ((field_config, _), ty) in zip(&field_configs, &field_type) {
        if !field_config.no_deserialize {
            add_where_clause.predicates.push(parse_quote!(__SerializationOptionsT__::DefaultDeserializer: ::bevy_impulse::DeserializeMessage<<#ty as ::bevy_impulse::SectionItem>::MessageType, __SerializationOptionsT__::Serialized>));
        }
        if !field_config.no_serialize {
            add_where_clause.predicates.push(parse_quote!(__SerializationOptionsT__::DefaultSerializer: ::bevy_impulse::SerializeMessage<<#ty as ::bevy_impulse::SectionItem>::MessageType, __SerializationOptionsT__::Serialized>));
        }
        if field_config.unzip {
            add_where_clause.predicates.push(parse_quote!(__SerializationOptionsT__::DefaultSerializer: ::bevy_impulse::UnzipSerialize<<#ty as ::bevy_impulse::SectionItem>::MessageType, __SerializationOptionsT__::Serialized>));
        }
    }
    let (impl_generics_with_ser, _, where_clause_with_ser) = add_generics.split_for_impl();

    let register_deserialize = gen_register_deserialize(&field_configs);
    let register_serialize = gen_register_serialize(&field_configs);
    let register_fork_clone = gen_register_fork_clone(&field_configs);
    let register_unzip = gen_register_unzip(&field_configs);
    let register_fork_result = gen_register_fork_result(&field_configs);
    let register_split = gen_register_split(&field_configs);
    let register_join = gen_register_join(&field_configs);
    let register_buffer_access = gen_register_buffer_access(&field_configs);
    let register_listen = gen_register_listen(&field_configs);

    let register_message: Vec<TokenStream> = zip(&field_type, &field_configs)
        .map(|(field_type, (_config, span))| {
            quote_spanned! {*span=>
                let mut _message = _opt_out.register_message::<<#field_type as ::bevy_impulse::SectionItem>::MessageType>();
            }
        })
        .collect();

    let gen = quote! {
        impl #impl_generics_with_ser ::bevy_impulse::Section<__SerializationOptionsT__> for #struct_ident #ty_generics #where_clause_with_ser {
            fn into_slots(
                self: Box<Self>,
            ) -> SectionSlots {
                let mut slots = SectionSlots::new();
                #(
                    self.#field_ident.insert_into_slots(#field_name_str.to_string(), &mut slots);
                )*
                slots
            }

            fn on_register(registry: &mut ::bevy_impulse::DiagramElementRegistry<__SerializationOptionsT__>)
            {
                #({
                    let _opt_out = registry.opt_out();
                    #register_deserialize
                    #register_serialize
                    #register_fork_clone

                    #register_message

                    #register_unzip
                    #register_fork_result
                    #register_split
                    #register_join
                    #register_buffer_access
                    #register_listen
                })*
            }
        }

        impl #impl_generics ::bevy_impulse::SectionMetadataProvider for #struct_ident #ty_generics #where_clause {
            fn metadata() -> &'static ::bevy_impulse::SectionMetadata {
                static METADATA: ::std::sync::OnceLock<::bevy_impulse::SectionMetadata> = ::std::sync::OnceLock::new();
                METADATA.get_or_init(|| {
                    let mut metadata = ::bevy_impulse::SectionMetadata::new();
                    #(
                        <#field_type as ::bevy_impulse::SectionItem>::build_metadata(
                            &mut metadata,
                            #field_name_str,
                        );
                    )*
                    metadata
                })
            }
        }
    };

    Ok(gen)
}

struct FieldConfig {
    no_deserialize: bool,
    no_serialize: bool,
    no_clone: bool,
    unzip: bool,
    fork_result: bool,
    split: bool,
    join: bool,
    buffer_access: bool,
    listen: bool,
}

impl FieldConfig {
    fn from_field(field: &Field) -> Self {
        let mut config = Self {
            no_deserialize: false,
            no_serialize: false,
            no_clone: false,
            unzip: false,
            fork_result: false,
            split: false,
            join: false,
            buffer_access: false,
            listen: false,
        };

        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("section"))
        {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("no_deserialize") {
                    config.no_deserialize = true;
                } else if meta.path.is_ident("no_serialize") {
                    config.no_serialize = true;
                } else if meta.path.is_ident("no_clone") {
                    config.no_clone = true;
                } else if meta.path.is_ident("unzip") {
                    config.unzip = true;
                } else if meta.path.is_ident("fork_result") {
                    config.fork_result = true;
                } else if meta.path.is_ident("split") {
                    config.split = true;
                } else if meta.path.is_ident("join") {
                    config.join = true;
                } else if meta.path.is_ident("buffer_access") {
                    config.buffer_access = true;
                } else if meta.path.is_ident("listen") {
                    config.listen = true;
                }
                Ok(())
            })
            // panic if attribute is malformed, this will result in a compile error which is intended.
            .unwrap();
        }

        config
    }
}

fn gen_register_deserialize(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.no_deserialize {
                quote_spanned! {*span=>
                    let _opt_out = _opt_out.no_request_deserializing();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_serialize(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.no_serialize {
                parse_quote_spanned! {*span=>
                    let _opt_out = _opt_out.no_response_serializing();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_fork_clone(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.no_clone {
                quote_spanned! {*span=>
                    let _opt_out = _opt_out.no_response_cloning();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_unzip(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.unzip {
                quote_spanned! {*span=>
                    _message.with_unzip();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_fork_result(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.fork_result {
                quote_spanned! {*span=>
                    _message.with_fork_result();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_split(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.split {
                quote_spanned! {*span=>
                    _message.with_split();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_join(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.join {
                quote_spanned! {*span=>
                    _message.with_join();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_buffer_access(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.buffer_access {
                quote_spanned! {*span=>
                    _message.with_buffer_access();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}

fn gen_register_listen(fields: &Vec<(FieldConfig, Span)>) -> Vec<TokenStream> {
    fields
        .into_iter()
        .map(|(config, span)| {
            if config.listen {
                quote_spanned! {*span=>
                    _message.with_listen();
                }
            } else {
                TokenStream::new()
            }
        })
        .collect()
}
