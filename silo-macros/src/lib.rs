use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Attribute macro for async tests with per-test tracing.
/// Usage:
/// #[silo::test]
/// async fn my_test() { ... }
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args_ts = proc_macro2::TokenStream::from(attr);
    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;
    let name = &input.sig.ident;

    let paren_args = if args_ts.is_empty() {
        quote! {}
    } else {
        quote! { ( #args_ts ) }
    };

    let output = quote! {
        #[tokio::test #paren_args]
        #vis #sig {
            silo::trace::with_test_tracing(stringify!(#name), || async move { #block }).await
        }
    };
    output.into()
}
