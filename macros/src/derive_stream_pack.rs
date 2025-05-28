/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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
use quote::{format_ident, quote, quote_spanned};
use syn::{spanned::Spanned, Generics, Ident, ItemStruct};

// Top-level attr for StreamPack
const STREAM_ATTR_TAG: &'static str = "stream";

// Inner attrs for the names of generated structs
const INPUTS_ATTR_TAG: &'static str = "inputs";
const OUTPUTS_ATTR_TAG: &'static str = "outputs";
const RECEIVERS_ATTR_TAG: &'static str = "receivers";
const CHANNELS_ATTR_TAG: &'static str = "channels";
const BUFFERS_ATTR_TAG: &'static str = "buffers";

// Inner attr to mark a custom stream effect field
const EFFECT_ATTR_TAG: &'static str = "effect";

pub(crate) fn impl_stream_pack(pack_struct: &ItemStruct) -> Result<TokenStream, TokenStream> {
    let StructIdentities {
        inputs,
        outputs,
        receivers,
        channels,
        buffers,
    } = StructIdentities::from_pack_struct(pack_struct)?;
    let stream_configs = StreamConfig::from_pack_struct(pack_struct)?;
    let (field_idents, stream_effects, field_names_str) = unzip_stream_configs(&stream_configs);
    let (impl_generics, ty_generics, where_clause) = pack_struct.generics.split_for_impl();
    let pack_ident = &pack_struct.ident;
    let generics = &pack_struct.generics;
    let vis = &pack_struct.vis;

    let input_streams: Vec<Ident> = field_idents
        .iter()
        .map(|ident| Ident::new(&format!("input_{ident}"), ident.span()))
        .collect();
    let output_streams: Vec<Ident> = field_idents
        .iter()
        .map(|ident| Ident::new(&format!("output_{ident}"), ident.span()))
        .collect();
    let has_streams = !field_idents.is_empty();

    // note: cannot clone for outputs or receivers
    let clone_for_inputs = impl_clone_for(&inputs, &field_idents, generics);
    let clone_for_channels = impl_clone_for(&channels, &field_idents, generics);
    let clone_for_buffers = impl_clone_for(&buffers, &field_idents, generics);

    Ok(quote! {
        #[allow(non_camel_case_types, unused)]
        #vis struct #inputs #generics {
            #(
                pub #field_idents: ::bevy_impulse::InputSlot<<#stream_effects as ::bevy_impulse::StreamEffect>::Input>,
            )*
        }

        #clone_for_inputs

        #[allow(non_camel_case_types, unused)]
        #vis struct #outputs #generics {
            #(
                pub #field_idents: ::bevy_impulse::Output<<#stream_effects as ::bevy_impulse::StreamEffect>::Output>,
            )*
        }

        #[allow(non_camel_case_types, unused)]
        #vis struct #receivers #generics {
            #(
                pub #field_idents: ::bevy_impulse::Receiver<<#stream_effects as ::bevy_impulse::StreamEffect>::Output>,
            )*
        }

        #[allow(non_camel_case_types, unused)]
        #vis struct #channels #generics {
            #(
                pub #field_idents: ::bevy_impulse::NamedStreamChannel<#stream_effects>,
            )*
        }

        #clone_for_channels

        #[allow(non_camel_case_types, unused)]
        #vis struct #buffers #generics {
            #(
                pub #field_idents: ::bevy_impulse::NamedStreamBuffer<<#stream_effects as ::bevy_impulse::StreamEffect>::Input>,
            )*
        }

        #clone_for_buffers

        impl #impl_generics #pack_ident #ty_generics #where_clause {
            #[allow(unused)]
            fn __bevy_impulse_allow_unused_fields(&self) {
                println!(
                    "This function suppresses unused field warnings. \
                    There is no need to call this. {:#?}",
                    [
                        #(
                            (#field_names_str, ::std::any::type_name_of_val(&self.#field_idents)),
                        )*
                    ]
                );
            }
        }

        impl #impl_generics ::bevy_impulse::StreamPack for #pack_ident #ty_generics #where_clause {
            type StreamInputPack = #inputs;
            type StreamOutputPack = #outputs;
            type StreamReceivers = #receivers;
            type StreamChannels = #channels;
            type StreamBuffers = #buffers;

            fn spawn_scope_streams(
                in_scope: ::bevy_impulse::re_exports::Entity,
                out_scope: ::bevy_impulse::re_exports::Entity,
                commands: &mut ::bevy_impulse::re_exports::Commands,
            ) -> (Self::StreamInputPack, Self::StreamOutputPack) {
                #(
                    let (#input_streams, #output_streams) = ::bevy_impulse::NamedStream::< #stream_effects >::spawn_scope_stream(
                        in_scope, out_scope, commands,
                    );
                )*

                (
                    #inputs { #(
                        #field_idents: #input_streams,
                    )* },
                    #outputs { #(
                        #field_idents: #output_streams,
                    )* }
                )
            }

            fn spawn_workflow_streams(builder: &mut ::bevy_impulse::Builder) -> Self::StreamInputPack {
                #inputs { #(
                    #field_idents: ::bevy_impulse::NamedStream::< #stream_effects >::spawn_workflow_stream(#field_names_str, builder),
                )* }
            }

            fn spawn_node_streams(
                source: ::bevy_impulse::re_exports::Entity,
                map: &mut ::bevy_impulse::StreamTargetMap,
                builder: &mut ::bevy_impulse::Builder,
            ) -> Self::StreamOutputPack {
                #outputs { #(
                    #field_idents: ::bevy_impulse::NamedStream::< #stream_effects >::spawn_node_stream(#field_names_str, source, map, builder),
                )* }
            }

            fn take_streams(
                source: ::bevy_impulse::re_exports::Entity,
                map: &mut ::bevy_impulse::StreamTargetMap,
                commands: &mut ::bevy_impulse::re_exports::Commands,
            ) -> Self::StreamReceivers {
                #receivers { #(
                    #field_idents: ::bevy_impulse::NamedStream::< #stream_effects >::take_stream(#field_names_str, source, map, commands),
                )* }
            }

            fn collect_streams(
                source: ::bevy_impulse::re_exports::Entity,
                target: ::bevy_impulse::re_exports::Entity,
                map: &mut ::bevy_impulse::StreamTargetMap,
                commands: &mut ::bevy_impulse::re_exports::Commands,
            ) {
                #(
                    ::bevy_impulse::NamedStream::< #stream_effects >::collect_stream(
                        #field_names_str, source, target, map, commands,
                    );
                )*
            }

            fn make_stream_channels(
                inner: &::std::sync::Arc<::bevy_impulse::InnerChannel>,
                world: &::bevy_impulse::re_exports::World,
            ) -> Self::StreamChannels {
                #channels { #(
                    #field_idents: ::bevy_impulse::NamedStream::< #stream_effects >::make_stream_channel(#field_names_str, inner, world),
                )* }
            }

            fn make_stream_buffers(
                target_map: Option<&::bevy_impulse::StreamTargetMap>,
            ) -> Self::StreamBuffers {
                #buffers { #(
                    #field_idents: ::bevy_impulse::NamedStream::< #stream_effects >::make_stream_buffer(target_map),
                )* }
            }

            fn process_stream_buffers(
                buffer: Self::StreamBuffers,
                source: ::bevy_impulse::re_exports::Entity,
                session: ::bevy_impulse::re_exports::Entity,
                unused: &mut ::bevy_impulse::UnusedStreams,
                world: &mut ::bevy_impulse::re_exports::World,
                roster: &mut ::bevy_impulse::OperationRoster,
            ) -> ::bevy_impulse::OperationResult {
                #(
                    ::bevy_impulse::NamedStream::< #stream_effects >::process_stream_buffer(
                        #field_names_str, buffer.#field_idents, source, session, unused, world, roster,
                    )?;
                )*
                Ok(())
            }

            fn defer_buffers(
                buffer: Self::StreamBuffers,
                source: ::bevy_impulse::re_exports::Entity,
                session: ::bevy_impulse::re_exports::Entity,
                commands: &mut ::bevy_impulse::re_exports::Commands,
            ) {
                #(
                    ::bevy_impulse::NamedStream::< #stream_effects >::defer_buffer(
                        #field_names_str, buffer.#field_idents, source, session, commands,
                    );
                )*
            }

            fn set_stream_availability(availability: &mut ::bevy_impulse::StreamAvailability) {
                #(
                    let _ = availability.add_named::<< #stream_effects as ::bevy_impulse::StreamEffect>::Output>(#field_names_str);
                )*
            }

            fn are_streams_available(availability: &::bevy_impulse::StreamAvailability) -> bool {
                true
                #(
                    && availability.has_named::<< #stream_effects as ::bevy_impulse::StreamEffect>::Output>(#field_names_str)
                )*
            }

            fn into_dyn_stream_input_pack(
                pack: &mut ::bevy_impulse::dyn_node::DynStreamInputPack,
                inputs: Self::StreamInputPack,
            ) {
                #(
                    pack.add_named(#field_names_str, inputs.#field_idents);
                )*
            }

            fn into_dyn_stream_output_pack(
                pack: &mut ::bevy_impulse::dyn_node::DynStreamOutputPack,
                outputs: Self::StreamOutputPack,
            ) {
                #(
                    pack.add_named(#field_names_str, outputs.#field_idents);
                )*
            }

            fn has_streams() -> bool {
                #has_streams
            }
        }
    })
}

fn impl_clone_for(ident: &Ident, field_idents: &Vec<&Ident>, generics: &Generics) -> TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::std::clone::Clone for #ident #ty_generics #where_clause {
            fn clone(&self) -> Self {
                Self {
                    #(
                        #field_idents: ::std::clone::Clone::clone(&self.#field_idents),
                    )*
                }
            }
        }
    }
}

struct StructIdentities {
    inputs: Ident,
    outputs: Ident,
    receivers: Ident,
    channels: Ident,
    buffers: Ident,
}

impl StructIdentities {
    fn from_pack_struct(pack_struct: &ItemStruct) -> Result<Self, TokenStream> {
        let mut identities = Self {
            inputs: format_ident!("__bevy_impulse_{}_StreamInputPack", pack_struct.ident),
            outputs: format_ident!("__bevy_impulse_{}_StreamOutputPack", pack_struct.ident),
            receivers: format_ident!("__bevy_impulse_{}_StreamReceivers", pack_struct.ident),
            channels: format_ident!("__bevy_impulse_{}_StreamChannels", pack_struct.ident),
            buffers: format_ident!("__bevy_impulse_{}_StreamBuffers", pack_struct.ident),
        };

        for attr in pack_struct.attrs.iter() {
            if !attr.path().is_ident(STREAM_ATTR_TAG) {
                continue;
            }

            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident(INPUTS_ATTR_TAG) {
                    identities.inputs = meta.value()?.parse()?;
                } else if meta.path.is_ident(OUTPUTS_ATTR_TAG) {
                    identities.outputs = meta.value()?.parse()?;
                } else if meta.path.is_ident(RECEIVERS_ATTR_TAG) {
                    identities.receivers = meta.value()?.parse()?;
                } else if meta.path.is_ident(CHANNELS_ATTR_TAG) {
                    identities.channels = meta.value()?.parse()?;
                } else if meta.path.is_ident(BUFFERS_ATTR_TAG) {
                    identities.buffers = meta.value()?.parse()?;
                } else {
                    return Err(syn::Error::new(
                        meta.path.span(),
                        format!(
                            "Unrecognized attribute for StreamPack macro. Choices are \
                            {INPUTS_ATTR_TAG}, {OUTPUTS_ATTR_TAG}, {RECEIVERS_ATTR_TAG}, \
                            {CHANNELS_ATTR_TAG}, {BUFFERS_ATTR_TAG}.",
                        ),
                    ));
                }

                Ok(())
            })
            .map_err(syn::Error::into_compile_error)?;
        }

        Ok(identities)
    }
}

struct StreamConfig {
    field_name: Ident,
    stream_effect: TokenStream,
}

impl StreamConfig {
    fn from_pack_struct(pack_struct: &ItemStruct) -> Result<Vec<Self>, TokenStream> {
        match &pack_struct.fields {
            syn::Fields::Named(fields) => {
                let mut configs = Vec::new();
                for field in &fields.named {
                    let field_name = field.ident.as_ref().cloned().ok_or_else(|| {
                        syn::Error::new(field.span(), "expected named field").into_compile_error()
                    })?;
                    let mut has_effect = false;
                    for attr in field.attrs.iter() {
                        if !attr.path().is_ident(STREAM_ATTR_TAG) {
                            continue;
                        }

                        attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident(EFFECT_ATTR_TAG) {
                                has_effect = true;
                            } else {
                                return Err(syn::Error::new(
                                    meta.path.span(),
                                    format!(
                                        "Unrecognized field attribute for StreamPack macro. \
                                        Only {STREAM_ATTR_TAG}({EFFECT_ATTR_TAG}) is supported."
                                    ),
                                ));
                            }

                            Ok(())
                        })
                        .map_err(syn::Error::into_compile_error)?;
                    }

                    // If the user tagged a custom stream in their stream pack,
                    // just use it directly instead of wrapping in StreamOf.
                    let stream_effect: TokenStream = if has_effect {
                        quote_spanned! { field.ty.span() =>
                            #field.ty
                        }
                    } else {
                        let ty = &field.ty;
                        quote_spanned! { field.ty.span() =>
                            ::bevy_impulse::StreamOf< #ty >
                        }
                    };

                    configs.push(StreamConfig {
                        field_name,
                        stream_effect,
                    });
                }

                Ok(configs)
            }
            _ => {
                Err(syn::Error::new(pack_struct.span(), "expected named fields")
                    .into_compile_error())
            }
        }
    }
}

fn unzip_stream_configs(
    configs: &Vec<StreamConfig>,
) -> (Vec<&Ident>, Vec<&TokenStream>, Vec<String>) {
    let mut field_idents = Vec::new();
    let mut field_names_str = Vec::new();
    let mut stream_effects = Vec::new();
    for StreamConfig {
        field_name,
        stream_effect,
    } in configs
    {
        field_idents.push(field_name);
        field_names_str.push(field_name.to_string());
        stream_effects.push(stream_effect);
    }

    (field_idents, stream_effects, field_names_str)
}
