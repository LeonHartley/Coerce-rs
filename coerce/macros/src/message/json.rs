use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};

use crate::get_attribute_type_multiple;

const RESULT_ATTR: &str = "result";

pub(crate) fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let item_type = {
        match get_attribute_type_multiple(ast, RESULT_ATTR) {
            Ok(ty) => match ty.len() {
                1 => ty[0].clone(),
                _ => {
                    return syn::Error::new(
                        Span::call_site(),
                        format!(
                            "#[{}(type)] takes 1 parameters, given {}",
                            RESULT_ATTR,
                            ty.len()
                        ),
                    )
                    .to_compile_error()
                }
            },
            Err(err) => return err.to_compile_error(),
        }
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let item_type = item_type
        .map(ToTokens::into_token_stream)
        .unwrap_or_else(|| quote! { () });

    quote! {
        impl #impl_generics ::coerce::actor::message::Message for #name #ty_generics #where_clause {
            type Result = #item_type;

            fn as_remote_envelope(&self) -> Result<coerce::actor::message::Envelope<Self>, coerce::actor::message::MessageWrapErr> {
                serde_json::to_vec(&self)
                    .map_err(|_e| coerce::actor::message::MessageWrapErr::SerializationErr)
                    .map(|bytes| coerce::actor::message::Envelope::Remote(bytes))
            }

            fn from_remote_envelope(bytes: Vec<u8>) -> Result<Self, coerce::actor::message::MessageUnwrapErr> {
                serde_json::from_slice(bytes.as_slice()).map_err(|_e| coerce::actor::message::MessageUnwrapErr::DeserializationErr)
            }

            fn read_remote_result(bytes: Vec<u8>) -> Result<Self::Result, coerce::actor::message::MessageUnwrapErr> {
                serde_json::from_slice(bytes.as_slice()).map_err(|_e| coerce::actor::message::MessageUnwrapErr::DeserializationErr)
            }

            fn write_remote_result(res: Self::Result) -> Result<Vec<u8>, coerce::actor::message::MessageWrapErr> {
                serde_json::to_vec(&res).map_err(|_e| coerce::actor::message::MessageWrapErr::SerializationErr)
            }
        }
    }
}
