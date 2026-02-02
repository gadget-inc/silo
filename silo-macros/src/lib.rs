use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Attribute macro for tests with per-test tracing.
/// Works with both sync and async test functions.
/// Usage:
/// #[silo::test]
/// fn my_sync_test() { ... }
///
/// #[silo::test]
/// async fn my_async_test() { ... }
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args_ts = proc_macro2::TokenStream::from(attr);
    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;
    let name = &input.sig.ident;

    let is_async = sig.asyncness.is_some();

    let paren_args = if args_ts.is_empty() {
        quote! {}
    } else {
        quote! { ( #args_ts ) }
    };

    let output = if is_async {
        quote! {
            #[tokio::test #paren_args]
            #vis #sig {
                silo::trace::with_test_tracing(stringify!(#name), || async move { #block }).await
            }
        }
    } else {
        quote! {
            #[test]
            #vis #sig {
                silo::trace::with_test_tracing_sync(stringify!(#name), || { #block })
            }
        }
    };
    output.into()
}
