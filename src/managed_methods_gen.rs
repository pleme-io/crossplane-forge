//! `zz_generated_managed.go` emitter — produces the
//! `crossplane-runtime/pkg/resource.Managed` interface methods that
//! every Crossplane-managed Kind has to expose. All methods are
//! mechanical delegations to the embedded `xpv1.ResourceSpec` /
//! `xpv1.ResourceStatus` fields.
//!
//! Industry equivalent is `angryjet` (the Crossplane code-generator
//! that emits these from kubebuilder annotations); we emit them
//! structurally instead because the substrate's substrate-hygiene
//! posture rules out external-tool-dependent code generation when
//! the generation can be made first-class.
//!
//! ## Methods emitted (per Kind)
//!
//! | interface | accessor pair |
//! |---|---|
//! | `xpv1.Conditioned`                  | `GetCondition` / `SetConditions` |
//! | `xpv1.ProviderConfigReferencer`     | `Get/SetProviderConfigReference` |
//! | `xpv1.ConnectionSecretWriterTo`     | `Get/SetWriteConnectionSecretToReference` |
//! | `xpv1.ConnectionDetailsPublisherTo` | `Get/SetPublishConnectionDetailsTo` |
//! | `xpv1.Orphanable`                   | `Get/SetDeletionPolicy` |
//! | `xpv1.ManagementPoliciesAccessor`   | `Get/SetManagementPolicies` |
//!
//! 12 methods per Kind.

use iac_forge::goast::{
    GoBlock, GoDecl, GoExpr, GoFile, GoFuncDecl, GoImport, GoParam, GoRecv, GoStmt, GoType,
    print_file,
};
use iac_forge::ir::{IacProvider, IacResource};

use crate::controller_gen::cr_kind;

/// Render `apis/<resource_pkg>/v1alpha1/zz_generated_managed.go` for
/// the resource's Kind.
#[must_use]
pub fn render_resource_managed_methods(resource: &IacResource, provider: &IacProvider) -> String {
    let kind = cr_kind(resource, provider);
    print_file(&build_managed_methods_file(&kind))
}

fn build_managed_methods_file(kind: &str) -> GoFile {
    let mut file = GoFile::new("v1alpha1");
    file.imports.push(GoImport::aliased(
        "xpv1",
        "github.com/crossplane/crossplane-runtime/apis/common/v1",
    ));

    // Each accessor pair is one Get + one Set method.
    for spec in accessor_specs() {
        file.decls.push(GoDecl::Func(getter(kind, &spec)));
        file.decls.push(GoDecl::Func(setter(kind, &spec)));
    }

    file
}

/// Single accessor specification — describes one Get/Set pair on a
/// Kind. Every entry maps to a delegated read/write of an embedded
/// xpv1.ResourceSpec or xpv1.ResourceStatus field.
struct AccessorSpec {
    /// Method name suffix shared by Get/Set (e.g. "Condition" → GetCondition + SetConditions).
    /// The Set method uses `set_name` because some pairs aren't symmetric (Conditions plural).
    get_name: &'static str,
    set_name: &'static str,
    /// Field path on the receiver: ("Spec", "ProviderConfigReference") or
    /// ("Status", "Conditions"). The first component is the embedded
    /// resource section.
    section: &'static str,
    field: &'static str,
    /// The Go type passed in/out of the accessors. Always xpv1-qualified
    /// because the embedded sections live in the xpv1 package.
    ty: &'static str,
    /// Param name for Set methods (typically "r" for reference, "p" for policy).
    set_param: &'static str,
    /// Special handling for variadic Set (Conditions takes `c ...xpv1.Condition`).
    variadic: bool,
    /// True if the parameter and return are pointer types (`*xpv1.Reference`).
    /// Conditions/DeletionPolicy/ManagementPolicies are values, not pointers.
    pointer: bool,
}

fn accessor_specs() -> Vec<AccessorSpec> {
    vec![
        // Conditions — special: variadic + value-typed (no pointer)
        AccessorSpec {
            get_name: "Condition",
            set_name: "Conditions",
            section: "Status",
            field: "Conditions",
            ty: "Condition",
            set_param: "c",
            variadic: true,
            pointer: false,
        },
        AccessorSpec {
            get_name: "ProviderConfigReference",
            set_name: "ProviderConfigReference",
            section: "Spec",
            field: "ProviderConfigReference",
            ty: "Reference",
            set_param: "r",
            variadic: false,
            pointer: true,
        },
        AccessorSpec {
            get_name: "WriteConnectionSecretToReference",
            set_name: "WriteConnectionSecretToReference",
            section: "Spec",
            field: "WriteConnectionSecretToReference",
            ty: "SecretReference",
            set_param: "r",
            variadic: false,
            pointer: true,
        },
        AccessorSpec {
            get_name: "PublishConnectionDetailsTo",
            set_name: "PublishConnectionDetailsTo",
            section: "Spec",
            field: "PublishConnectionDetailsTo",
            ty: "PublishConnectionDetailsTo",
            set_param: "r",
            variadic: false,
            pointer: true,
        },
        AccessorSpec {
            get_name: "DeletionPolicy",
            set_name: "DeletionPolicy",
            section: "Spec",
            field: "DeletionPolicy",
            ty: "DeletionPolicy",
            set_param: "r",
            variadic: false,
            pointer: false,
        },
        AccessorSpec {
            get_name: "ManagementPolicies",
            set_name: "ManagementPolicies",
            section: "Spec",
            field: "ManagementPolicies",
            ty: "ManagementPolicies",
            set_param: "r",
            variadic: false,
            pointer: false,
        },
    ]
}

fn getter(kind: &str, spec: &AccessorSpec) -> GoFuncDecl {
    // GetCondition is special — it takes a ConditionType arg and
    // returns a single Condition by delegating to mg.Status.GetCondition(ct).
    if spec.get_name == "Condition" {
        let mut body = GoBlock::new();
        body.push(GoStmt::Return(vec![GoExpr::call(
            GoExpr::sel(
                GoExpr::sel(GoExpr::ident("mg"), spec.section),
                "GetCondition",
            ),
            vec![GoExpr::ident("ct")],
        )]));
        return GoFuncDecl {
            name: "GetCondition".to_string(),
            doc: None,
            recv: Some(GoRecv {
                name: "mg".to_string(),
                ty: GoType::pointer(GoType::named(kind)),
            }),
            params: vec![GoParam {
                name: "ct".to_string(),
                ty: GoType::qualified("xpv1", "ConditionType"),
            }],
            returns: vec![GoType::qualified("xpv1", "Condition")],
            body,
        };
    }

    // Standard getter: return mg.Spec.<field>  (or *xpv1.Reference)
    let mut body = GoBlock::new();
    body.push(GoStmt::Return(vec![GoExpr::sel(
        GoExpr::sel(GoExpr::ident("mg"), spec.section),
        spec.field,
    )]));
    let ret_ty = if spec.pointer {
        GoType::pointer(GoType::qualified("xpv1", spec.ty))
    } else {
        GoType::qualified("xpv1", spec.ty)
    };
    GoFuncDecl {
        name: format!("Get{}", spec.get_name),
        doc: None,
        recv: Some(GoRecv {
            name: "mg".to_string(),
            ty: GoType::pointer(GoType::named(kind)),
        }),
        params: vec![],
        returns: vec![ret_ty],
        body,
    }
}

fn setter(kind: &str, spec: &AccessorSpec) -> GoFuncDecl {
    // SetConditions is variadic: takes c ...xpv1.Condition and delegates
    // to mg.Status.SetConditions(c...). goast doesn't have a typed
    // "variadic param" + "spread call" today, so this case uses an
    // ident-shaped parameter and a spread expression. The structural
    // intent is captured in the variadic flag; printer renders it
    // correctly.
    if spec.variadic {
        let mut body = GoBlock::new();
        // Body: mg.Status.SetConditions(c...)
        // Spread is currently unrepresented in goast — emit via a
        // structurally-named identifier `c...` that the printer renders
        // as a spread argument. Acceptable because (1) c is a local
        // bound by the param signature and (2) the spread is the ONLY
        // way to call SetConditions with a variadic.
        body.push(GoStmt::Expr(GoExpr::call(
            GoExpr::sel(
                GoExpr::sel(GoExpr::ident("mg"), spec.section),
                "SetConditions",
            ),
            vec![GoExpr::ident("c...")],
        )));
        return GoFuncDecl {
            name: "SetConditions".to_string(),
            doc: None,
            recv: Some(GoRecv {
                name: "mg".to_string(),
                ty: GoType::pointer(GoType::named(kind)),
            }),
            // Variadic params aren't yet a typed AST node — emit by
            // naming the param `c ...xpv1.Condition` via a typed
            // `...xpv1.Condition` slice convention. goast slice 2 may
            // formalise this; for now we use a Slice GoType which the
            // printer renders as `[]xpv1.Condition`. That doesn't match
            // Go's variadic syntax exactly, so we use a special-case
            // ident below — but we can't introduce a new GoParam shape
            // without extending goast. Compromise: since this is the
            // ONLY variadic-input site in the entire emission surface,
            // we accept a one-line structural printer escape via a
            // specialised goast extension — see managed_methods_gen
            // module doc.
            params: vec![GoParam {
                name: "c".to_string(),
                ty: GoType::Named(format!(
                    "...{}",
                    "xpv1.Condition"
                )),
            }],
            returns: vec![],
            body,
        };
    }

    // Standard setter: mg.Spec.<field> = r
    let mut body = GoBlock::new();
    body.push(GoStmt::Assign {
        lhs: vec![GoExpr::sel(
            GoExpr::sel(GoExpr::ident("mg"), spec.section),
            spec.field,
        )],
        rhs: vec![GoExpr::ident(spec.set_param)],
    });
    let param_ty = if spec.pointer {
        GoType::pointer(GoType::qualified("xpv1", spec.ty))
    } else {
        GoType::qualified("xpv1", spec.ty)
    };
    GoFuncDecl {
        name: format!("Set{}", spec.set_name),
        doc: None,
        recv: Some(GoRecv {
            name: "mg".to_string(),
            ty: GoType::pointer(GoType::named(kind)),
        }),
        params: vec![GoParam {
            name: spec.set_param.to_string(),
            ty: param_ty,
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
    fn emits_twelve_methods_per_kind() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        // 12 methods: 6 accessor pairs (Condition+Conditions, PCRef×2,
        // WriteConnSecretToRef×2, PublishConnDetailsTo×2, DeletionPolicy×2,
        // ManagementPolicies×2)
        let count = s.matches("func (mg *AuthMethodApiKey)").count();
        assert_eq!(count, 12, "expected 12 managed-resource methods, got {count}");
    }

    #[test]
    fn get_condition_takes_condition_type_and_returns_condition() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        assert!(s.contains(
            "func (mg *AuthMethodApiKey) GetCondition(ct xpv1.ConditionType) xpv1.Condition"
        ));
        assert!(s.contains("return mg.Status.GetCondition(ct)"));
    }

    #[test]
    fn set_conditions_is_variadic_and_spread_delegates() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        assert!(s.contains("SetConditions(c ...xpv1.Condition)"));
        assert!(s.contains("mg.Status.SetConditions(c...)"));
    }

    #[test]
    fn provider_config_ref_uses_pointer_xpv1_reference() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        assert!(s.contains(
            "func (mg *AuthMethodApiKey) GetProviderConfigReference() *xpv1.Reference"
        ));
        assert!(s.contains("return mg.Spec.ProviderConfigReference"));
        assert!(s.contains(
            "func (mg *AuthMethodApiKey) SetProviderConfigReference(r *xpv1.Reference)"
        ));
        assert!(s.contains("mg.Spec.ProviderConfigReference = r"));
    }

    #[test]
    fn deletion_policy_uses_value_type_no_pointer() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        // DeletionPolicy is a value type, not a pointer
        assert!(s.contains(
            "func (mg *AuthMethodApiKey) GetDeletionPolicy() xpv1.DeletionPolicy"
        ));
        assert!(!s.contains("GetDeletionPolicy() *xpv1.DeletionPolicy"));
    }

    #[test]
    fn management_policies_emitted() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        assert!(s.contains("GetManagementPolicies"));
        assert!(s.contains("SetManagementPolicies"));
    }

    #[test]
    fn write_connection_secret_to_reference_uses_pointer() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        assert!(s.contains(
            "func (mg *AuthMethodApiKey) GetWriteConnectionSecretToReference() *xpv1.SecretReference"
        ));
    }

    #[test]
    fn publish_connection_details_to_uses_pointer() {
        let s = render_resource_managed_methods(&auth_method(), &provider());
        assert!(s.contains(
            "func (mg *AuthMethodApiKey) GetPublishConnectionDetailsTo() *xpv1.PublishConnectionDetailsTo"
        ));
    }

    #[test]
    fn imports_xpv1_only() {
        let f = build_managed_methods_file("Foo");
        assert_eq!(f.imports.len(), 1);
        assert_eq!(
            f.imports[0].path,
            "github.com/crossplane/crossplane-runtime/apis/common/v1"
        );
        assert_eq!(f.imports[0].alias.as_deref(), Some("xpv1"));
    }
}
