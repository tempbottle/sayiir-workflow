pub mod ast;
pub mod codegen;
pub mod parse;

use proc_macro2::TokenStream;

use self::ast::WorkflowDef;

/// Entry point: parse workflow! invocation and generate builder code.
pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let def: WorkflowDef = syn::parse2(input)?;
    generate(&def)
}

enum BuildMode<'a> {
    New,
    FromDeps(&'a syn::Expr),
}

impl BuildMode<'_> {
    fn instance(&self, name: &syn::Ident) -> TokenStream {
        match self {
            Self::New => quote::quote! { #name::new() },
            Self::FromDeps(_) => quote::quote! { #name::from_deps(__deps) },
        }
    }

    /// Emit a verify-deps preamble. Returns empty when running in `New` mode.
    ///
    /// Runs *before* any task instance is constructed so missing deps surface
    /// as `BuildErrors::MissingDep` rather than panicking at first invocation.
    fn verify_block(&self, names: &[&syn::Ident]) -> TokenStream {
        let Self::FromDeps(deps_expr) = self else {
            return TokenStream::new();
        };
        let verify_stmts = names.iter().map(|name| {
            quote::quote! {
                for __m in #name::verify_deps(__deps) {
                    __build_errors.push(::sayiir_core::error::BuildError::MissingDep {
                        task_id: #name::task_id(),
                        type_name: __m.type_name,
                    });
                }
            }
        });
        quote::quote! {
            let __deps: &::sayiir_core::deps::Deps = #deps_expr;
            let mut __build_errors = ::sayiir_core::error::BuildErrors::new();
            #(#verify_stmts)*
            if !__build_errors.is_empty() {
                return ::std::result::Result::Err(__build_errors);
            }
        }
    }
}

fn generate(def: &WorkflowDef) -> syn::Result<TokenStream> {
    let id = &def.id;
    let codec = &def.codec;

    let registry_expr = match &def.registry {
        Some(expr) => quote::quote! { #expr },
        None => quote::quote! { ::sayiir_core::registry::TaskRegistry::new() },
    };

    let metadata_expr = match &def.metadata {
        Some(expr) => quote::quote! { ::std::sync::Arc::new(#expr) },
        None => quote::quote! { ::std::sync::Arc::new(()) },
    };

    let step_chain = codegen::gen_step_chain(&def.steps)?;

    let task_refs = codegen::collect_task_refs(&def.steps);
    let aliases = codegen::collect_route_aliases(&def.steps);

    let mode = match &def.deps {
        Some(expr) => BuildMode::FromDeps(expr),
        None => BuildMode::New,
    };

    let verify_block = {
        let mut all: Vec<&syn::Ident> = task_refs.to_vec();
        all.extend(aliases.iter().map(|(name, _)| *name));
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        all.retain(|n| seen.insert(n.to_string()));
        mode.verify_block(&all)
    };

    let register_stmts = task_refs.iter().map(|name| {
        let instance = mode.instance(name);
        quote::quote! {
            #name::register(&mut __registry, __codec.clone(), #instance);
        }
    });

    let alias_stmts = aliases.iter().map(|(name, alias_id)| {
        let instance = mode.instance(name);
        quote::quote! {
            __registry.register_with_metadata(
                #alias_id,
                __codec.clone(),
                #instance,
                #name::metadata(),
            );
        }
    });

    // Compile-time exhaustiveness checks for typed route nodes.
    let exhaustiveness_checks = codegen::collect_exhaustiveness_checks(&def.steps);

    Ok(quote::quote! {
        (|| -> ::std::result::Result<_, ::sayiir_core::error::BuildErrors> {
            #(#exhaustiveness_checks)*
            #verify_block
            let __codec = ::std::sync::Arc::new(#codec);
            let mut __registry = #registry_expr;
            #(#register_stmts)*
            #(#alias_stmts)*
            let __ctx = ::sayiir_core::context::WorkflowContext::new(
                #id,
                __codec.clone(),
                #metadata_expr,
            );
            let __wf = ::sayiir_core::builder::WorkflowBuilder::new(__ctx)
                .with_existing_registry(__registry)
                #step_chain
                .build()?;
            ::std::result::Result::Ok(__wf)
        })()
    })
}
