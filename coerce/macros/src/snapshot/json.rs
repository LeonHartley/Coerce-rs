use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::DeriveInput;


pub(crate) fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    quote! {
        impl #impl_generics ::coerce::persistent::journal::snapshot::Snapshot for #name #ty_generics #where_clause {
            fn as_remote_envelope(&self) -> Result<coerce::actor::message::Envelope<Self>, coerce::actor::message::MessageWrapErr> {
                serde_json::to_vec(&self)
                    .map_err(|_e| coerce::actor::message::MessageWrapErr::SerializationErr)
                    .map(|bytes| coerce::actor::message::Envelope::Remote(bytes))
            }

            fn from_remote_envelope(bytes: Vec<u8>) -> Result<Self, coerce::actor::message::MessageUnwrapErr> {
                serde_json::from_slice(bytes.as_slice()).map_err(|_e| MessageUnwrapErr::DeserializationErr)
            }
        }
    }
}
