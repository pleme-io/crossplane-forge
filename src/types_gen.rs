//! Crossplane-managed-resource Go-types emitter.
//!
//! For each `IacResource`, builds a [`GoFile`] for `<resource>_types.go`
//! containing the canonical Crossplane managed-resource type set:
//! `<Kind>Parameters`, `<Kind>Observation`, `<Kind>Spec` (with
//! `xpv1.ResourceSpec` embedded), `<Kind>Status` (with
//! `xpv1.ResourceStatus` embedded), `<Kind>` itself, and `<Kind>List`.
//! Plus a separate `GoFile` for the package's `groupversion_info.go`.
//!
//! Built entirely on top of [`iac_forge::goast`] — no `format!()`
//! strings of Go syntax. Every kubebuilder marker is a typed
//! [`KubeMarker`] variant, every JSON tag is a typed [`JsonTag`].

use iac_forge::goast::{
    GoBlock, GoDecl, GoExpr, GoField, GoFile, GoFuncDecl, GoImport, GoLit, GoStmt, GoStructTag,
    GoType, GoTypeBody, GoTypeDecl, JsonTag, KubeMarker, ResourceScope, SubresourceKind, print_file,
};
use iac_forge::ir::{IacAttribute, IacProvider, IacResource, IacType};
use iac_forge::naming::{strip_provider_prefix, to_snake_case};

use crate::controller_gen::{cr_kind, package_name};

/// Render the per-resource `<resource>_types.go` content.
#[must_use]
pub fn render_resource_types(resource: &IacResource, provider: &IacProvider) -> String {
    print_file(&build_resource_types_file(resource, provider))
}

/// Render the package-level `groupversion_info.go`.
#[must_use]
pub fn render_groupversion_info(
    resource: &IacResource,
    provider: &IacProvider,
    api_group: &str,
    api_version: &str,
) -> String {
    print_file(&build_groupversion_info_file(
        resource,
        provider,
        api_group,
        api_version,
    ))
}

// ── AST builders (the structurally typed core; rendering is a one-liner) ──

fn build_resource_types_file(resource: &IacResource, provider: &IacProvider) -> GoFile {
    let kind = cr_kind(resource, provider);
    let mut file = GoFile::new("v1alpha1");
    file.imports.push(GoImport::aliased(
        "metav1",
        "k8s.io/apimachinery/pkg/apis/meta/v1",
    ));
    file.imports.push(GoImport::aliased(
        "xpv1",
        "github.com/crossplane/crossplane-runtime/apis/common/v1",
    ));
    file.decls.push(GoDecl::Type(build_parameters_struct(
        resource, &kind,
    )));
    file.decls.push(GoDecl::Type(build_observation_struct(
        resource, &kind,
    )));
    file.decls.push(GoDecl::Type(build_spec_struct(&kind)));
    file.decls.push(GoDecl::Type(build_status_struct(&kind)));
    file.decls.push(GoDecl::Type(build_kind_struct(&kind)));
    file.decls.push(GoDecl::Type(build_kind_list_struct(&kind)));
    file
}

fn build_groupversion_info_file(
    resource: &IacResource,
    provider: &IacProvider,
    api_group: &str,
    api_version: &str,
) -> GoFile {
    let kind = cr_kind(resource, provider);
    let mut file = GoFile::new("v1alpha1");
    file.markers
        .push(KubeMarker::ObjectGenerate(true));
    file.markers
        .push(KubeMarker::GroupName(api_group.to_string()));
    file.imports.push(GoImport::plain(
        "k8s.io/apimachinery/pkg/runtime/schema",
    ));
    file.imports.push(GoImport::plain(
        "sigs.k8s.io/controller-runtime/pkg/scheme",
    ));

    // var ( GroupVersion = schema.GroupVersion{Group: "...", Version: "..."}
    //       SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
    //       AddToScheme   = SchemeBuilder.AddToScheme )
    //
    // GoVarDecl emits standalone vars; this is fine even though Go
    // idiom prefers `var ( ... )` blocks. Slice 2 of goast can add
    // GoVarBlock; the standalone form is canonical too.
    file.decls.push(GoDecl::Var(iac_forge::goast::GoVarDecl {
        name: "GroupVersion".to_string(),
        ty: None,
        value: Some(GoExpr::Composite {
            ty: GoType::qualified("schema", "GroupVersion"),
            fields: vec![
                (
                    Some("Group".to_string()),
                    GoExpr::Lit(GoLit::Str(api_group.to_string())),
                ),
                (
                    Some("Version".to_string()),
                    GoExpr::Lit(GoLit::Str(api_version.to_string())),
                ),
            ],
            addr_of: false,
        }),
        doc: None,
        block_id: None,
    }));
    file.decls.push(GoDecl::Var(iac_forge::goast::GoVarDecl {
        name: "SchemeBuilder".to_string(),
        ty: None,
        value: Some(GoExpr::Composite {
            ty: GoType::qualified("scheme", "Builder"),
            fields: vec![(
                Some("GroupVersion".to_string()),
                GoExpr::ident("GroupVersion"),
            )],
            addr_of: true,
        }),
        doc: None,
        block_id: None,
    }));
    file.decls.push(GoDecl::Var(iac_forge::goast::GoVarDecl {
        name: "AddToScheme".to_string(),
        ty: None,
        value: Some(GoExpr::Selector {
            recv: Box::new(GoExpr::ident("SchemeBuilder")),
            sel: "AddToScheme".to_string(),
        }),
        doc: None,
        block_id: None,
    }));

    // func init() { SchemeBuilder.Register(&Kind{}, &KindList{}) }
    let mut init_body = GoBlock::new();
    init_body.push(GoStmt::Expr(GoExpr::Call {
        fun: Box::new(GoExpr::Selector {
            recv: Box::new(GoExpr::ident("SchemeBuilder")),
            sel: "Register".to_string(),
        }),
        args: vec![
            GoExpr::Composite {
                ty: GoType::named(&kind),
                fields: vec![],
                addr_of: true,
            },
            GoExpr::Composite {
                ty: GoType::named(&format!("{kind}List")),
                fields: vec![],
                addr_of: true,
            },
        ],
    }));
    file.decls.push(GoDecl::Func(GoFuncDecl {
        name: "init".to_string(),
        doc: None,
        recv: None,
        params: vec![],
        returns: vec![],
        body: init_body,
    }));

    file
}

fn build_parameters_struct(resource: &IacResource, kind: &str) -> GoTypeDecl {
    let fields = resource
        .attributes
        .iter()
        .filter(|a| !a.computed)
        .map(attr_to_field)
        .collect();
    GoTypeDecl {
        name: format!("{kind}Parameters"),
        doc: Some(format!(
            "{kind}Parameters defines the desired state of the resource."
        )),
        markers: vec![],
        body: GoTypeBody::Struct(fields),
    }
}

fn build_observation_struct(resource: &IacResource, kind: &str) -> GoTypeDecl {
    let fields = resource
        .attributes
        .iter()
        .filter(|a| a.computed)
        .map(attr_to_field)
        .collect();
    GoTypeDecl {
        name: format!("{kind}Observation"),
        doc: Some(format!(
            "{kind}Observation reflects the observed state from the upstream provider."
        )),
        markers: vec![],
        body: GoTypeBody::Struct(fields),
    }
}

fn build_spec_struct(kind: &str) -> GoTypeDecl {
    let fields = vec![
        GoField {
            name: None,
            ty: GoType::qualified("xpv1", "ResourceSpec"),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: String::new(),
                omitempty: false,
                inline: true,
            })],
        },
        GoField {
            name: Some("ForProvider".to_string()),
            ty: GoType::named(&format!("{kind}Parameters")),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "forProvider".to_string(),
                omitempty: false,
                inline: false,
            })],
        },
    ];
    GoTypeDecl {
        name: format!("{kind}Spec"),
        doc: Some(format!("{kind}Spec is the desired state of the resource.")),
        markers: vec![],
        body: GoTypeBody::Struct(fields),
    }
}

fn build_status_struct(kind: &str) -> GoTypeDecl {
    let fields = vec![
        GoField {
            name: None,
            ty: GoType::qualified("xpv1", "ResourceStatus"),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: String::new(),
                omitempty: false,
                inline: true,
            })],
        },
        GoField {
            name: Some("AtProvider".to_string()),
            ty: GoType::named(&format!("{kind}Observation")),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "atProvider".to_string(),
                omitempty: true,
                inline: false,
            })],
        },
    ];
    GoTypeDecl {
        name: format!("{kind}Status"),
        doc: Some(format!(
            "{kind}Status reflects the observed state of the resource."
        )),
        markers: vec![],
        body: GoTypeBody::Struct(fields),
    }
}

fn build_kind_struct(kind: &str) -> GoTypeDecl {
    let markers = vec![
        KubeMarker::ObjectRoot,
        KubeMarker::Subresource(SubresourceKind::Status),
        KubeMarker::Resource {
            scope: ResourceScope::Cluster,
            categories: vec![
                "crossplane".to_string(),
                "akeyless".to_string(),
                "managed".to_string(),
            ],
        },
        KubeMarker::PrintColumn {
            name: "READY".to_string(),
            ty: "string".to_string(),
            json_path: ".status.conditions[?(@.type=='Ready')].status".to_string(),
            priority: None,
        },
        KubeMarker::PrintColumn {
            name: "SYNCED".to_string(),
            ty: "string".to_string(),
            json_path: ".status.conditions[?(@.type=='Synced')].status".to_string(),
            priority: None,
        },
        KubeMarker::PrintColumn {
            name: "EXTERNAL-NAME".to_string(),
            ty: "string".to_string(),
            json_path: ".metadata.annotations.crossplane\\.io/external-name".to_string(),
            priority: None,
        },
        KubeMarker::PrintColumn {
            name: "AGE".to_string(),
            ty: "date".to_string(),
            json_path: ".metadata.creationTimestamp".to_string(),
            priority: None,
        },
    ];
    let fields = vec![
        GoField {
            name: None,
            ty: GoType::qualified("metav1", "TypeMeta"),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: String::new(),
                omitempty: false,
                inline: true,
            })],
        },
        GoField {
            name: None,
            ty: GoType::qualified("metav1", "ObjectMeta"),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "metadata".to_string(),
                omitempty: true,
                inline: false,
            })],
        },
        GoField {
            name: Some("Spec".to_string()),
            ty: GoType::named(&format!("{kind}Spec")),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "spec".to_string(),
                omitempty: false,
                inline: false,
            })],
        },
        GoField {
            name: Some("Status".to_string()),
            ty: GoType::named(&format!("{kind}Status")),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "status".to_string(),
                omitempty: true,
                inline: false,
            })],
        },
    ];
    GoTypeDecl {
        name: kind.to_string(),
        doc: None,
        markers,
        body: GoTypeBody::Struct(fields),
    }
}

fn build_kind_list_struct(kind: &str) -> GoTypeDecl {
    let markers = vec![KubeMarker::ObjectRoot];
    let fields = vec![
        GoField {
            name: None,
            ty: GoType::qualified("metav1", "TypeMeta"),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: String::new(),
                omitempty: false,
                inline: true,
            })],
        },
        GoField {
            name: None,
            ty: GoType::qualified("metav1", "ListMeta"),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "metadata".to_string(),
                omitempty: true,
                inline: false,
            })],
        },
        GoField {
            name: Some("Items".to_string()),
            ty: GoType::slice(GoType::named(kind)),
            doc: None,
            markers: vec![],
            tags: vec![GoStructTag::Json(JsonTag {
                name: "items".to_string(),
                omitempty: false,
                inline: false,
            })],
        },
    ];
    GoTypeDecl {
        name: format!("{kind}List"),
        doc: None,
        markers,
        body: GoTypeBody::Struct(fields),
    }
}

fn attr_to_field(attr: &IacAttribute) -> GoField {
    let go_field_name = pascal_case_field(&attr.canonical_name);
    let ty = iac_type_to_go(&attr.iac_type, attr.required);
    let mut markers = Vec::new();
    if attr.required {
        markers.push(KubeMarker::Required);
    } else {
        markers.push(KubeMarker::Optional);
    }
    if attr.immutable {
        markers.push(KubeMarker::XValidationCEL {
            rule: "self == oldSelf".to_string(),
            message: "field is immutable".to_string(),
        });
    }
    let json_tag = JsonTag {
        name: attr.canonical_name.clone(),
        omitempty: !attr.required,
        inline: false,
    };
    let doc = if attr.description.is_empty() {
        None
    } else {
        Some(attr.description.replace('\n', " "))
    };
    GoField {
        name: Some(go_field_name),
        ty,
        doc,
        markers,
        tags: vec![GoStructTag::Json(json_tag)],
    }
}

fn pascal_case_field(canonical: &str) -> String {
    iac_forge::naming::to_pascal_case(&iac_forge::naming::to_snake_case(canonical))
}

fn iac_type_to_go(t: &IacType, required: bool) -> GoType {
    let base = match t {
        IacType::String => GoType::named("string"),
        IacType::Integer => GoType::named("int64"),
        IacType::Float | IacType::Numeric => GoType::named("float64"),
        IacType::Boolean => GoType::named("bool"),
        // Slice 1: collections + complex types collapse to opaque string.
        // Slice 2 (next iteration of types_gen) maps List(T)→[]T,
        // Map(K,V)→map[K]V structurally.
        IacType::List(_)
        | IacType::Set(_)
        | IacType::Map(_)
        | IacType::Object { .. }
        | IacType::Enum { .. }
        | IacType::Any => GoType::named("string"),
        _ => GoType::named("string"),
    };
    if required { base } else { GoType::pointer(base) }
}

// ── Path helpers ──────────────────────────────────────────────────────────

#[must_use]
pub fn types_package_name(resource: &IacResource, provider: &IacProvider) -> String {
    package_name(resource, provider)
}

#[must_use]
pub fn types_file_path(resource: &IacResource, provider: &IacProvider) -> String {
    let pkg = types_package_name(resource, provider);
    let stem = to_snake_case(&strip_provider_prefix(&resource.name, &provider.name));
    format!("apis/{pkg}/v1alpha1/{stem}_types.go")
}

#[must_use]
pub fn groupversion_file_path(resource: &IacResource, provider: &IacProvider) -> String {
    let pkg = types_package_name(resource, provider);
    format!("apis/{pkg}/v1alpha1/groupversion_info.go")
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::goast::{KubeMarker, ResourceScope};
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
            description: "Manages an API key auth method".to_string(),
            category: "auth_method".to_string(),
            crud: CrudInfo {
                create_endpoint: "/auth-method-create-api-key".to_string(),
                create_schema: "authMethodCreateApiKey".to_string(),
                update_endpoint: Some("/auth-method-update-api-key".to_string()),
                update_schema: Some("authMethodUpdateApiKey".to_string()),
                read_endpoint: "/get-auth-method".to_string(),
                read_schema: "getAuthMethod".to_string(),
                read_response_schema: Some("AuthMethod".to_string()),
                delete_endpoint: "/delete-auth-method".to_string(),
                delete_schema: "deleteAuthMethod".to_string(),
            },
            attributes: vec![
                IacAttribute {
                    api_name: "name".to_string(),
                    canonical_name: "name".to_string(),
                    description: "Auth method name".to_string(),
                    iac_type: IacType::String,
                    required: true,
                    optional: false,
                    computed: false,
                    sensitive: false,
                    json_encoded: false,
                    immutable: true,
                    default_value: None,
                    enum_values: None,
                    read_path: None,
                    update_only: false,
                },
                IacAttribute {
                    api_name: "access-id".to_string(),
                    canonical_name: "access_id".to_string(),
                    description: "The auth method access ID".to_string(),
                    iac_type: IacType::String,
                    required: false,
                    optional: false,
                    computed: true,
                    sensitive: false,
                    json_encoded: false,
                    immutable: false,
                    default_value: None,
                    enum_values: None,
                    read_path: None,
                    update_only: false,
                },
            ],
            identity: IdentityInfo {
                id_field: "name".to_string(),
                import_field: "name".to_string(),
                force_replace_fields: vec!["name".to_string()],
            },
        }
    }

    // ── AST-shape tests (assert on the GoFile AST, not on substrings) ────

    #[test]
    fn types_file_contains_six_type_decls() {
        let f = build_resource_types_file(&auth_method(), &provider());
        let type_decls: Vec<_> = f
            .decls
            .iter()
            .filter_map(|d| match d {
                GoDecl::Type(t) => Some(t.name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            type_decls,
            vec![
                "AuthMethodApiKeyParameters",
                "AuthMethodApiKeyObservation",
                "AuthMethodApiKeySpec",
                "AuthMethodApiKeyStatus",
                "AuthMethodApiKey",
                "AuthMethodApiKeyList",
            ]
        );
    }

    #[test]
    fn parameters_excludes_computed_fields_includes_required_marker() {
        let f = build_parameters_struct(&auth_method(), "AuthMethodApiKey");
        let GoTypeBody::Struct(fields) = &f.body else {
            panic!("expected struct body");
        };
        // Only the non-computed field (`name`) should be present
        assert_eq!(fields.len(), 1);
        let field = &fields[0];
        assert_eq!(field.name.as_deref(), Some("Name"));
        assert!(matches!(field.ty, GoType::Named(ref n) if n == "string"));
        // Required marker
        assert!(field.markers.iter().any(|m| matches!(m, KubeMarker::Required)));
        // Immutable marker (XValidation CEL)
        assert!(field.markers.iter().any(
            |m| matches!(m, KubeMarker::XValidationCEL { rule, .. } if rule == "self == oldSelf"),
        ));
    }

    #[test]
    fn observation_includes_only_computed_fields_with_pointer_type() {
        let f = build_observation_struct(&auth_method(), "AuthMethodApiKey");
        let GoTypeBody::Struct(fields) = &f.body else {
            panic!("expected struct body");
        };
        assert_eq!(fields.len(), 1);
        let field = &fields[0];
        assert_eq!(field.name.as_deref(), Some("AccessId"));
        // Computed → optional → pointer type
        assert!(matches!(field.ty, GoType::Pointer(_)));
        assert!(field.markers.iter().any(|m| matches!(m, KubeMarker::Optional)));
    }

    #[test]
    fn spec_struct_embeds_xpv1_resource_spec_inline() {
        let t = build_spec_struct("Foo");
        let GoTypeBody::Struct(fields) = &t.body else {
            panic!()
        };
        // First field is embedded xpv1.ResourceSpec with inline json tag
        let first = &fields[0];
        assert!(first.name.is_none());
        assert!(matches!(
            first.ty,
            GoType::Qualified { ref pkg, ref name } if pkg == "xpv1" && name == "ResourceSpec",
        ));
        assert!(first.tags.iter().any(|t| matches!(
            t,
            GoStructTag::Json(JsonTag { inline: true, .. })
        )));
    }

    #[test]
    fn kind_struct_carries_full_marker_set() {
        let t = build_kind_struct("Foo");
        // ObjectRoot, Subresource(Status), Resource, 4× PrintColumn
        let has_root = t.markers.iter().any(|m| matches!(m, KubeMarker::ObjectRoot));
        let has_status_sub = t
            .markers
            .iter()
            .any(|m| matches!(m, KubeMarker::Subresource(SubresourceKind::Status)));
        let has_cluster_resource = t.markers.iter().any(|m| matches!(
            m,
            KubeMarker::Resource { scope: ResourceScope::Cluster, .. },
        ));
        let print_col_count = t
            .markers
            .iter()
            .filter(|m| matches!(m, KubeMarker::PrintColumn { .. }))
            .count();
        assert!(has_root);
        assert!(has_status_sub);
        assert!(has_cluster_resource);
        assert_eq!(print_col_count, 4);
    }

    #[test]
    fn groupversion_info_registers_kind_and_list() {
        let f = build_groupversion_info_file(
            &auth_method(),
            &provider(),
            "akeyless.crossplane.io",
            "v1alpha1",
        );
        // Markers above package: ObjectGenerate(true), GroupName
        assert!(f.markers.iter().any(|m| matches!(m, KubeMarker::ObjectGenerate(true))));
        assert!(f.markers.iter().any(
            |m| matches!(m, KubeMarker::GroupName(g) if g == "akeyless.crossplane.io"),
        ));
        // 3 vars + 1 init() func
        let var_names: Vec<_> = f
            .decls
            .iter()
            .filter_map(|d| match d {
                GoDecl::Var(v) => Some(v.name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(var_names, vec!["GroupVersion", "SchemeBuilder", "AddToScheme"]);
        let has_init = f.decls.iter().any(|d| matches!(
            d,
            GoDecl::Func(GoFuncDecl { name, .. }) if name == "init",
        ));
        assert!(has_init);
    }

    #[test]
    fn paths_match_crossplane_layout() {
        assert_eq!(
            types_file_path(&auth_method(), &provider()),
            "apis/authmethodapikey/v1alpha1/auth_method_api_key_types.go"
        );
        assert_eq!(
            groupversion_file_path(&auth_method(), &provider()),
            "apis/authmethodapikey/v1alpha1/groupversion_info.go"
        );
    }

    // ── Integration test: rendered output stays valid Go (smoke-level) ───

    #[test]
    fn rendered_types_file_is_well_formed() {
        let s = render_resource_types(&auth_method(), &provider());
        // Header
        assert!(s.starts_with("// Code generated by iac-forge. DO NOT EDIT."));
        // Package decl
        assert!(s.contains("\npackage v1alpha1\n"));
        // No format!()-leakage telltales (curly-brace placeholders)
        assert!(!s.contains("{kind}"));
        assert!(!s.contains("{provider_name}"));
        assert!(!s.contains("{api_group}"));
    }

    #[test]
    fn deterministic_render_for_same_input() {
        let a = render_resource_types(&auth_method(), &provider());
        let b = render_resource_types(&auth_method(), &provider());
        assert_eq!(a, b);
    }
}
