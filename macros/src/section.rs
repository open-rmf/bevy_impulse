use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, ItemStruct, Member, Type};

use crate::Result;

pub(crate) fn impl_section(input_struct: &ItemStruct) -> Result<TokenStream> {
    let struct_ident = &input_struct.ident;
    let (impl_generics, ty_generics, where_clause) = input_struct.generics.split_for_impl();
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

    let gen = quote! {
        impl #impl_generics ::bevy_impulse::Section for #struct_ident #ty_generics #where_clause {
            fn try_connect(
                self: Box<Self>,
                builder: &mut ::bevy_impulse::Builder,
                mut inputs: ::std::collections::HashMap<String, ::bevy_impulse::DynOutput>,
                mut outputs: ::std::collections::HashMap<String, ::bevy_impulse::DynInputSlot>,
                buffers: &mut HashMap<OperationId, AnyBuffer>,
            ) -> Result<(), ::bevy_impulse::DiagramErrorCode> {
                #(
                    self.#field_ident.try_connect(&#field_name_str.to_string(), builder, &mut inputs, &mut outputs, buffers)?;
                )*
                Ok(())
            }

            fn on_register(registry: &mut MessageRegistry)
            where
                Self: Sized,
            {
                #({
                    let builder = <#field_type as ::bevy_impulse::SectionItem>::message_registration(registry);
                })*
                panic!("TODO: Register operations based on helper attributes")
            }
        }

        impl #impl_generics ::bevy_impulse::SectionMetadataProvider for #struct_ident #ty_generics #where_clause {
            fn metadata() -> &'static ::bevy_impulse::SectionMetadata {
                static METADATA: ::std::sync::LazyLock<::bevy_impulse::SectionMetadata> =
                    ::std::sync::LazyLock::new(|| {
                        let mut metadata = ::bevy_impulse::SectionMetadata::new();
                        #(
                            <#field_type as ::bevy_impulse::SectionItem>::build_metadata(
                                &mut metadata,
                                #field_name_str,
                            );
                        )*
                        metadata
                    });
                &*METADATA
            }
        }
    };

    Ok(gen)
}
