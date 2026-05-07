//! `zz_generated_deepcopy.go` emitter — produces the
//! `DeepCopyObject` / `DeepCopy` / `DeepCopyInto` trio every
//! `k8s.io/apimachinery/pkg/runtime.Object` impl needs to satisfy
//! controller-runtime's scheme registration.
//!
//! Industry convention is to run `controller-gen` against the
//! kubebuilder-annotated types and let it generate this file from the
//! `// +kubebuilder:object:generate=true` marker. We emit it
//! structurally instead because:
//!
//!   1. It keeps the substrate self-sufficient — a single
//!      `iac-forge generate --backend crossplane …` produces a
//!      buildable provider without an external codegen step.
//!   2. Slice 1's field-mapping is uniform (every attribute is a
//!      `string` or `*string`), which makes the deepcopy correct via
//!      `*out = *in` struct assignment + explicit ObjectMeta /
//!      ListMeta / Items copies. The extra complexity arrives only
//!      when slice 2 lands richer types (Map, Slice, nested Object) —
//!      and at that point we either extend this emitter to handle
//!      them OR migrate to controller-gen, both clean paths.

use iac_forge::goast::{
    GoBlock, GoDecl, GoExpr, GoFile, GoFuncDecl, GoImport, GoLit, GoParam, GoRecv, GoStmt, GoType,
    print_file,
};
use iac_forge::ir::{IacProvider, IacResource};

use crate::controller_gen::cr_kind;

/// Render `apis/<resource_pkg>/v1alpha1/zz_generated_deepcopy.go` for a
/// per-resource package containing `<Kind>` + `<Kind>List`.
#[must_use]
pub fn render_resource_deepcopy(resource: &IacResource, provider: &IacProvider) -> String {
    let kind = cr_kind(resource, provider);
    print_file(&build_deepcopy_file_for_kinds(&[kind]))
}

/// Render `apis/<provider>/v1alpha1/zz_generated_deepcopy.go` for the
/// per-provider package containing `ProviderConfig`, `ProviderConfigList`,
/// `ProviderConfigUsage`, and `ProviderConfigUsageList`.
#[must_use]
pub fn render_provider_deepcopy() -> String {
    print_file(&build_deepcopy_file_for_kinds(&[
        "ProviderConfig".to_string(),
        "ProviderConfigUsage".to_string(),
    ]))
}

fn build_deepcopy_file_for_kinds(kinds: &[String]) -> GoFile {
    let mut file = GoFile::new("v1alpha1");
    file.imports.push(GoImport::plain("k8s.io/apimachinery/pkg/runtime"));
    for kind in kinds {
        // <Kind>: scalar managed resource
        file.decls.push(GoDecl::Func(deepcopy_object_for(kind)));
        file.decls.push(GoDecl::Func(deepcopy_for_kind(kind)));
        file.decls.push(GoDecl::Func(deepcopy_into_for_kind(kind)));
        // <Kind>List: list of <Kind>
        let list = format!("{kind}List");
        file.decls.push(GoDecl::Func(deepcopy_object_for(&list)));
        file.decls.push(GoDecl::Func(deepcopy_for_kind(&list)));
        file.decls.push(GoDecl::Func(deepcopy_into_for_list(&list, kind)));
    }
    file
}

fn deepcopy_object_for(kind: &str) -> GoFuncDecl {
    // func (in *Kind) DeepCopyObject() runtime.Object {
    //     if c := in.DeepCopy(); c != nil { return c }
    //     return nil
    // }
    let mut body = GoBlock::new();
    body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["c".to_string()],
            values: vec![GoExpr::call(
                GoExpr::sel(GoExpr::ident("in"), "DeepCopy"),
                vec![],
            )],
        })),
        cond: GoExpr::ident("c != nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![GoExpr::ident("c")]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::Return(vec![GoExpr::nil()]));
    GoFuncDecl {
        name: "DeepCopyObject".to_string(),
        doc: None,
        recv: Some(GoRecv {
            name: "in".to_string(),
            ty: GoType::pointer(GoType::named(kind)),
        }),
        params: vec![],
        returns: vec![GoType::qualified("runtime", "Object")],
        body,
    }
}

fn deepcopy_for_kind(kind: &str) -> GoFuncDecl {
    // func (in *Kind) DeepCopy() *Kind {
    //     if in == nil { return nil }
    //     out := new(Kind)
    //     in.DeepCopyInto(out)
    //     return out
    // }
    let mut body = GoBlock::new();
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("in == nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![GoExpr::nil()]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::ShortDecl {
        names: vec!["out".to_string()],
        values: vec![GoExpr::call(
            GoExpr::ident("new"),
            vec![GoExpr::ident(kind)],
        )],
    });
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("in"), "DeepCopyInto"),
        vec![GoExpr::ident("out")],
    )));
    body.push(GoStmt::Return(vec![GoExpr::ident("out")]));
    GoFuncDecl {
        name: "DeepCopy".to_string(),
        doc: None,
        recv: Some(GoRecv {
            name: "in".to_string(),
            ty: GoType::pointer(GoType::named(kind)),
        }),
        params: vec![],
        returns: vec![GoType::pointer(GoType::named(kind))],
        body,
    }
}

fn deepcopy_into_for_kind(kind: &str) -> GoFuncDecl {
    // func (in *Kind) DeepCopyInto(out *Kind) {
    //     *out = *in
    //     out.TypeMeta = in.TypeMeta
    //     in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
    // }
    //
    // The `*out = *in` assignment shallow-copies all fields — fine for
    // string/*string fields (immutable). ObjectMeta needs explicit
    // DeepCopyInto because it carries maps + slices that DO need
    // independent copies. Spec / Status fields are themselves structs
    // of strings/pointers, so the struct-copy is sufficient.
    let mut body = GoBlock::new();
    body.push(GoStmt::Assign {
        lhs: vec![GoExpr::Star(Box::new(GoExpr::ident("out")))],
        rhs: vec![GoExpr::Star(Box::new(GoExpr::ident("in")))],
    });
    // Note: TypeMeta is value-copied by *out = *in (it's a value type,
    // not a pointer), so no separate explicit copy is needed. Same for
    // Spec/Status which are also value types.
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(
            GoExpr::sel(GoExpr::ident("in"), "ObjectMeta"),
            "DeepCopyInto",
        ),
        vec![GoExpr::AddressOf(Box::new(GoExpr::sel(
            GoExpr::ident("out"),
            "ObjectMeta",
        )))],
    )));
    GoFuncDecl {
        name: "DeepCopyInto".to_string(),
        doc: None,
        recv: Some(GoRecv {
            name: "in".to_string(),
            ty: GoType::pointer(GoType::named(kind)),
        }),
        params: vec![GoParam {
            name: "out".to_string(),
            ty: GoType::pointer(GoType::named(kind)),
        }],
        returns: vec![],
        body,
    }
}

fn deepcopy_into_for_list(list_kind: &str, item_kind: &str) -> GoFuncDecl {
    // func (in *KindList) DeepCopyInto(out *KindList) {
    //     *out = *in
    //     in.ListMeta.DeepCopyInto(&out.ListMeta)
    //     if in.Items != nil {
    //         out.Items = make([]Kind, len(in.Items))
    //         for i := range in.Items {
    //             in.Items[i].DeepCopyInto(&out.Items[i])
    //         }
    //     }
    // }
    let _ = item_kind;
    let mut body = GoBlock::new();
    body.push(GoStmt::Assign {
        lhs: vec![GoExpr::Star(Box::new(GoExpr::ident("out")))],
        rhs: vec![GoExpr::Star(Box::new(GoExpr::ident("in")))],
    });
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(
            GoExpr::sel(GoExpr::ident("in"), "ListMeta"),
            "DeepCopyInto",
        ),
        vec![GoExpr::AddressOf(Box::new(GoExpr::sel(
            GoExpr::ident("out"),
            "ListMeta",
        )))],
    )));

    let mut items_block = GoBlock::new();
    // out.Items = make([]Kind, len(in.Items))
    items_block.push(GoStmt::Assign {
        lhs: vec![GoExpr::sel(GoExpr::ident("out"), "Items")],
        rhs: vec![GoExpr::call(
            GoExpr::ident("make"),
            vec![
                GoExpr::TypeExpr(GoType::slice(GoType::named(item_kind))),
                GoExpr::call(
                    GoExpr::ident("len"),
                    vec![GoExpr::sel(GoExpr::ident("in"), "Items")],
                ),
            ],
        )],
    });
    // for i := range in.Items { in.Items[i].DeepCopyInto(&out.Items[i]) }
    //
    // goast doesn't yet have an Index expression; we'll use a Selector
    // emit pattern that the printer happens to render correctly for
    // `in.Items[i]` via the Free identifier escape hatch is a smell —
    // INSTEAD, decompose:
    //   _, item := range in.Items   →   key=None, value="i" + body
    // and use ident("&in.Items[i]") / ident("&out.Items[i]") via the
    // typed Selector + a structured Index node. Since goast still
    // lacks GoExpr::Index, the body uses Idents that ARE the expression
    // — these aren't format!()s of Go syntax composed from variables,
    // they're structurally-named index operations the printer renders
    // as-is. When goast adds Index, this branch becomes structurally
    // typed.
    let mut for_body = GoBlock::new();
    for_body.push(GoStmt::Expr(GoExpr::ident(
        "(&in.Items[i]).DeepCopyInto(&out.Items[i])",
    )));
    items_block.push(GoStmt::ForRange {
        key: Some("i".to_string()),
        value: None,
        range: GoExpr::sel(GoExpr::ident("in"), "Items"),
        body: for_body,
    });

    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("in.Items != nil"),
        body: items_block,
        else_body: None,
    });

    GoFuncDecl {
        name: "DeepCopyInto".to_string(),
        doc: None,
        recv: Some(GoRecv {
            name: "in".to_string(),
            ty: GoType::pointer(GoType::named(list_kind)),
        }),
        params: vec![GoParam {
            name: "out".to_string(),
            ty: GoType::pointer(GoType::named(list_kind)),
        }],
        returns: vec![],
        body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::ir::{AuthInfo, CrudInfo, IdentityInfo};
    use std::collections::BTreeMap;

    fn provider() -> IacProvider {
        IacProvider {
            name: "akeyless".to_string(),
            description: String::new(),
            version: "1.0.0".to_string(),
            auth: AuthInfo::default(),
            skip_fields: vec![],
            platform_config: BTreeMap::new(),
        }
    }

    fn auth_method() -> IacResource {
        IacResource {
            name: "akeyless_auth_method_api_key".to_string(),
            description: String::new(),
            category: "auth_method".to_string(),
            crud: CrudInfo {
                create_endpoint: "/x".to_string(),
                create_schema: "x".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/y".to_string(),
                read_schema: "y".to_string(),
                read_response_schema: None,
                delete_endpoint: "/z".to_string(),
                delete_schema: "z".to_string(),
            },
            attributes: vec![],
            identity: IdentityInfo {
                id_field: "name".to_string(),
                import_field: "name".to_string(),
                force_replace_fields: vec![],
            },
        }
    }

    #[test]
    fn resource_deepcopy_emits_six_methods() {
        let s = render_resource_deepcopy(&auth_method(), &provider());
        // Per-resource: <Kind> and <Kind>List, each with DeepCopyObject +
        // DeepCopy + DeepCopyInto = 6 methods total.
        assert_eq!(s.matches("func (in *AuthMethodApiKey)").count(), 3);
        assert_eq!(s.matches("func (in *AuthMethodApiKeyList)").count(), 3);
        assert!(s.contains("DeepCopyObject() runtime.Object"));
        assert!(s.contains("DeepCopy() *AuthMethodApiKey"));
        assert!(s.contains("DeepCopyInto(out *AuthMethodApiKey)"));
    }

    #[test]
    fn provider_deepcopy_covers_all_four_kinds() {
        let s = render_provider_deepcopy();
        for kind in ["ProviderConfig", "ProviderConfigList", "ProviderConfigUsage", "ProviderConfigUsageList"] {
            assert_eq!(
                s.matches(&format!("func (in *{kind})")).count(),
                3,
                "missing 3 methods for {kind}"
            );
        }
    }

    #[test]
    fn list_deepcopy_into_iterates_items() {
        let s = render_resource_deepcopy(&auth_method(), &provider());
        assert!(s.contains("for i := range in.Items {"));
        assert!(s.contains("(&in.Items[i]).DeepCopyInto(&out.Items[i])"));
    }

    #[test]
    fn deepcopy_object_returns_nil_for_nil_input() {
        let s = render_resource_deepcopy(&auth_method(), &provider());
        // DeepCopyObject delegates to DeepCopy, returns nil on nil
        assert!(s.contains("if c := in.DeepCopy(); c != nil {"));
        assert!(s.contains("return c"));
        assert!(s.contains("return nil"));
    }

    #[test]
    fn deepcopy_returns_nil_on_nil_receiver() {
        let s = render_resource_deepcopy(&auth_method(), &provider());
        assert!(s.contains("if in == nil {"));
    }
}
