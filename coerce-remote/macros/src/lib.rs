
use syn::export::TokenStream;

#[macro_use]
extern crate quote;

#[proc_macro_attribute]
pub fn coerce_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
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