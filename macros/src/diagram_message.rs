use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

pub fn impl_diagram_message(ast: DeriveInput) -> TokenStream {
    let name = ast.ident;

    let gen = quote! {
        impl<Serializer> DiagramMessage<Serializer> for #name {
            type DynUnzipImpl = NotSupported;
        }
    };

    gen.into()
}
