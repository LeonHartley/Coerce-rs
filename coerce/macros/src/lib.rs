#[macro_use]
extern crate quote;

#[macro_use]
extern crate syn;

mod message;
mod snapshot;

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::DeriveInput;

const MESSAGE_ATTR: &str = "result";

#[proc_macro_derive(JsonMessage, attributes(result))]
pub fn json_message_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    message::json::expand(&ast).into()
}

#[proc_macro_derive(JsonSnapshot)]
pub fn json_snapshot_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    snapshot::json::expand(&ast).into()
}

pub(crate) fn get_attribute_type_multiple(
    ast: &syn::DeriveInput,
    name: &str,
) -> syn::Result<Vec<Option<syn::Type>>> {
    let attr = ast
        .attrs
        .iter()
        .find_map(|a| {
            let a = a.parse_meta();
            match a {
                Ok(meta) => {
                    if meta.path().is_ident(name) {
                        Some(meta)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .ok_or_else(|| {
            syn::Error::new(Span::call_site(), format!("Expect a attribute `{}`", name))
        })?;

    if let syn::Meta::List(ref list) = attr {
        Ok(list
            .nested
            .iter()
            .map(|m| meta_item_to_ty(m).ok())
            .collect())
    } else {
        return Err(syn::Error::new_spanned(
            attr,
            format!("The correct syntax is #[{}(type, type, ...)]", name),
        ));
    }
}

pub(crate) fn meta_item_to_ty(meta_item: &syn::NestedMeta) -> syn::Result<syn::Type> {
    match meta_item {
        syn::NestedMeta::Meta(syn::Meta::Path(ref path)) => match path.get_ident() {
            Some(ident) => syn::parse_str::<syn::Type>(&ident.to_string())
                .map_err(|_| syn::Error::new_spanned(ident, "Expect type")),
            None => Err(syn::Error::new_spanned(path, "Expect type")),
        },
        syn::NestedMeta::Meta(syn::Meta::NameValue(val)) => match val.path.get_ident() {
            Some(ident) if ident == "result" => {
                if let syn::Lit::Str(ref s) = val.lit {
                    if let Ok(ty) = syn::parse_str::<syn::Type>(&s.value()) {
                        return Ok(ty);
                    }
                }
                Err(syn::Error::new_spanned(&val.lit, "Expect type"))
            }
            _ => Err(syn::Error::new_spanned(
                &val.lit,
                r#"Expect `result = "TYPE"`"#,
            )),
        },
        syn::NestedMeta::Lit(syn::Lit::Str(ref s)) => syn::parse_str::<syn::Type>(&s.value())
            .map_err(|_| syn::Error::new_spanned(s, "Expect type")),

        meta => Err(syn::Error::new_spanned(meta, "Expect type")),
    }
}
#[proc_macro_attribute]
pub fn coerce_test(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);

    let ret = &input.sig.output;
    let name = &input.sig.ident;
    let body = &input.block;
    let attrs = &input.attrs;

    for attr in attrs {
        if attr.path.is_ident("test") {
            let msg = "second test attribute is supplied";
            return syn::Error::new_spanned(&attr, msg)
                .to_compile_error()
                .into();
        }
    }

    if input.sig.asyncness.is_none() {
        let msg = "the async keyword is missing from the function declaration";
        return syn::Error::new_spanned(&input, msg)
            .to_compile_error()
            .into();
    } else if !input.sig.inputs.is_empty() {
        let msg = "the test function cannot accept arguments";
        return syn::Error::new_spanned(&input.sig.inputs, msg)
            .to_compile_error()
            .into();
    }

    let result = quote! {
        #[test]
        #(#attrs)*
        fn #name() #ret {
            let mut rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { #body })
        }
    };

    result.into()
}
