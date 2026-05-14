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

    // Builds the token expression that constructs a task instance for the
    // given task ident — either `Task::new()` (no-deps mode) or
    // `Task::from_deps(__deps)` (when `deps:` is present).
    type InstanceFn = Box<dyn Fn(&syn::Ident) -> TokenStream>;

    // When `deps:` is present, every #[task] is built via `from_deps`, and we
    // emit verify_deps checks up-front so missing dependencies surface as a
    // `BuildErrors::MissingDep` at construction time rather than panicking at
    // first task invocation.
    let (verify_block, instance_expr): (TokenStream, InstanceFn) = if let Some(deps_expr) =
        &def.deps
    {
        let mut all_task_names: Vec<&syn::Ident> = Vec::new();
        for name in &task_refs {
            all_task_names.push(name);
        }
        for (name, _) in &aliases {
            all_task_names.push(name);
        }
        // De-duplicate while preserving order.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        all_task_names.retain(|n| seen.insert(n.to_string()));

        let verify_stmts = all_task_names.iter().map(|name| {
                quote::quote! {
                    for __m in #name::verify_deps(__deps) {
                        __build_errors.push(::sayiir_core::error::BuildError::MissingDep {
                            task_id: <#name as ::sayiir_core::task::RegisterableTask>::task_id().to_string(),
                            type_name: __m.type_name.to_string(),
                        });
                    }
                }
            });

        let verify = quote::quote! {
            let __deps: &::sayiir_core::deps::Deps = #deps_expr;
            let mut __build_errors = ::sayiir_core::error::BuildErrors::new();
            #(#verify_stmts)*
            if !__build_errors.is_empty() {
                return ::std::result::Result::Err(__build_errors);
            }
        };

        let make_instance: InstanceFn =
            Box::new(|name: &syn::Ident| quote::quote! { #name::from_deps(__deps) });

        (verify, make_instance)
    } else {
        let make_instance: InstanceFn =
            Box::new(|name: &syn::Ident| quote::quote! { #name::new() });
        (quote::quote! {}, make_instance)
    };

    // Auto-register all #[task] struct refs into the registry.
    let register_stmts = task_refs.iter().map(|name| {
        let instance = instance_expr(name);
        quote::quote! {
            #name::register(&mut __registry, __codec.clone(), #instance);
        }
    });

    // Register route key function aliases (e.g. "route::key_fn").
    let alias_stmts = aliases.iter().map(|(name, alias_id)| {
        let instance = instance_expr(name);
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
