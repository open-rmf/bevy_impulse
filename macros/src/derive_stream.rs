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
use quote::quote;
use syn::ItemStruct;

pub(crate) fn impl_derive_stream(stream_struct: &ItemStruct) -> Result<TokenStream, TokenStream> {
    let stream_ident = &stream_struct.ident;
    let (impl_generics, ty_generics, where_clause) = &stream_struct.generics.split_for_impl();

    let from_anonymous_stream_pack = quote! {
        <::crossflow::AnonymousStream<Self> as ::crossflow::StreamPack>
    };

    Ok(quote! {
        impl #impl_generics ::crossflow::StreamPack for #stream_ident #ty_generics #where_clause {
            type StreamInputPack = #from_anonymous_stream_pack::StreamInputPack;
            type StreamOutputPack = #from_anonymous_stream_pack::StreamOutputPack;
            type StreamReceivers = #from_anonymous_stream_pack::StreamReceivers;
            type StreamChannels = #from_anonymous_stream_pack::StreamChannels;
            type StreamBuffers = #from_anonymous_stream_pack::StreamBuffers;


            fn spawn_scope_streams(
                in_scope: ::crossflow::re_exports::Entity,
                out_scope: ::crossflow::re_exports::Entity,
                commands: &mut ::crossflow::re_exports::Commands,
            ) -> (Self::StreamInputPack, Self::StreamOutputPack) {
                #from_anonymous_stream_pack::spawn_scope_streams(in_scope, out_scope, commands)
            }

            fn spawn_workflow_streams(builder: &mut ::crossflow::Builder) -> Self::StreamInputPack {
                #from_anonymous_stream_pack::spawn_workflow_streams(builder)
            }

            fn spawn_node_streams(
                source: ::crossflow::re_exports::Entity,
                map: &mut ::crossflow::StreamTargetMap,
                builder: &mut ::crossflow::Builder,
            ) -> Self::StreamOutputPack {
                #from_anonymous_stream_pack::spawn_node_streams(source, map, builder)
            }

            fn take_streams(
                source: ::crossflow::re_exports::Entity,
                map: &mut ::crossflow::StreamTargetMap,
                commands: &mut ::crossflow::re_exports::Commands,
            ) -> Self::StreamReceivers {
                #from_anonymous_stream_pack::take_streams(source, map, commands)
            }

            fn collect_streams(
                source: ::crossflow::re_exports::Entity,
                target: ::crossflow::re_exports::Entity,
                map: &mut ::crossflow::StreamTargetMap,
                commands: &mut ::crossflow::re_exports::Commands,
            ) {
                #from_anonymous_stream_pack::collect_streams(source, target, map, commands)
            }

            fn make_stream_channels(
                inner: &::std::sync::Arc<::crossflow::InnerChannel>,
                world: &::crossflow::re_exports::World,
            ) -> Self::StreamChannels {
                #from_anonymous_stream_pack::make_stream_channels(inner, world)
            }

            fn make_stream_buffers(
                target_map: Option<&::crossflow::StreamTargetMap>,
            ) -> Self::StreamBuffers {
                #from_anonymous_stream_pack::make_stream_buffers(target_map)
            }

            fn process_stream_buffers(
                buffer: Self::StreamBuffers,
                source: ::crossflow::re_exports::Entity,
                session: ::crossflow::re_exports::Entity,
                unused: &mut ::crossflow::UnusedStreams,
                world: &mut ::crossflow::re_exports::World,
                roster: &mut ::crossflow::OperationRoster,
            ) -> ::crossflow::OperationResult {
                #from_anonymous_stream_pack::process_stream_buffers(
                    buffer, source, session, unused, world, roster
                )
            }

            fn defer_buffers(
                buffer: Self::StreamBuffers,
                source: ::crossflow::re_exports::Entity,
                session: ::crossflow::re_exports::Entity,
                commands: &mut ::crossflow::re_exports::Commands,
            ) {
                #from_anonymous_stream_pack::defer_buffers(buffer, source, session, commands)
            }

            fn set_stream_availability(availability: &mut ::crossflow::StreamAvailability) {
                #from_anonymous_stream_pack::set_stream_availability(availability)
            }

            fn are_streams_available(availability: &::crossflow::StreamAvailability) -> bool {
                #from_anonymous_stream_pack::are_streams_available(availability)
            }

            fn into_dyn_stream_input_pack(
                pack: &mut ::crossflow::dyn_node::DynStreamInputPack,
                input: Self::StreamInputPack,
            ) {
                #from_anonymous_stream_pack::into_dyn_stream_input_pack(pack, input)
            }

            fn into_dyn_stream_output_pack(
                pack: &mut ::crossflow::dyn_node::DynStreamOutputPack,
                outputs: Self::StreamOutputPack,
            ) {
                #from_anonymous_stream_pack::into_dyn_stream_output_pack(pack, outputs)
            }

            fn has_streams() -> bool {
                #from_anonymous_stream_pack::has_streams()
            }
        }
    })
}
