//! Crossplane controller emitter — produces a per-resource controller
//! `.go` file implementing the `crossplane-runtime` `ExternalClient`
//! interface (Observe / Create / Update / Delete) plus the manager-
//! wiring `Setup` function and the `connector` that resolves a
//! `ProviderConfig` into an `external` client.
//!
//! Built entirely on top of [`iac_forge::goast`] — the file is
//! constructed as a typed [`GoFile`] tree, never as `format!()`
//! strings of Go syntax.

use std::collections::BTreeMap;

use iac_forge::goast::{
    GoBlock, GoDecl, GoExpr, GoField, GoFile, GoFuncDecl, GoImport, GoLit, GoParam, GoRecv,
    GoStmt, GoType, GoTypeBody, GoTypeDecl, print_file,
};
use iac_forge::ir::{IacProvider, IacResource};
use iac_forge::naming::{strip_provider_prefix, to_pascal_case};
use iac_forge::sdk_naming;

/// Configuration the emitter needs alongside the IR — primarily where
/// to import the SDK from and where to import the per-provider /
/// per-resource v1alpha1 types from.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ControllerConfig {
    /// Go module path for the SDK the controllers call into.
    pub sdk_module: String,
    /// Go module path for the generated CRD types this provider emits.
    pub provider_module: String,
    /// API group for the CRDs (Crossplane convention is
    /// `<provider>.crossplane.io`).
    pub api_group: String,
    /// API version for the emitted CRDs (e.g., `v1alpha1`).
    pub api_version: String,
    /// Per-resource shape overrides — keyed by the bare resource name
    /// (after `strip_provider_prefix`, before snake_case): e.g.
    /// `"esm"`, `"role_rule"`. Used by the controller emitter to
    /// override the default `Name string` body-field assumption.
    /// Resources not present in this map use `ResourceShape::default()`.
    pub resource_shapes: BTreeMap<String, ResourceShape>,
}

/// Per-resource shape configuration consumed by the controller
/// emitter. Captures the heterogeneity across SDK body types — some
/// use `Name string`, some `Name *string`, some a different field
/// entirely (e.g. `EsmName`, `UscName`, `Path`), and some use
/// composite keys / mixed-per-CRUD identifiers that don't fit the
/// single-identifier pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceShape {
    /// Default body-field name carrying the resource identifier
    /// (defaults to `"Name"`). Per-CRUD-method overrides in
    /// `per_method` win when present.
    pub identifier_field: String,
    /// Default for whether the identifier field is `*string` instead
    /// of `string`. Per-CRUD-method overrides in `per_method` win.
    pub identifier_pointer: bool,
    /// Per-CRUD-method overrides — populated for resources whose SDK
    /// body shape varies per method (e.g. `kmip_client.Name string`
    /// on Create vs `*string` on Read/Delete).
    pub per_method: BTreeMap<CrudMethod, MethodOverride>,
    /// Emit a stub controller (no SDK calls) for resources whose body
    /// shape requires composite keys / mixed-int64-vs-string IDs /
    /// singleton bodies that ResourceShape can't yet express. Stubs
    /// satisfy the ExternalClient interface but no-op every method.
    pub stub: bool,
}

/// Which CRUD method an override applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum CrudMethod {
    Read,
    Create,
    Update,
    Delete,
}

/// Per-method override of [`ResourceShape`]'s defaults.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MethodOverride {
    /// When `Some`, replaces `ResourceShape::identifier_field` for
    /// this CRUD method.
    pub identifier_field: Option<String>,
    /// When `Some`, replaces `ResourceShape::identifier_pointer` for
    /// this CRUD method.
    pub identifier_pointer: Option<bool>,
    /// When `Some`, the entire body construction goes through the
    /// template, ignoring `identifier_field` / `identifier_pointer`.
    /// Used for SDK body shapes that don't fit the
    /// single-string-identifier pattern.
    pub body_template: Option<BodyTemplate>,
}

/// Body-construction template for a CRUD method whose SDK body
/// shape doesn't fit the single-string-identifier pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BodyTemplate {
    /// `body := akeyless.X{Token: &e.token}` — no identifier field.
    /// Used by singleton-bodies (e.g. KmipDescribeServer / KmipDeleteServer).
    NoIdentifier,
    /// `id, _ := strconv.ParseInt(meta.GetExternalName(cr), 10, 64)` +
    /// `body := akeyless.X{<field>: id, Token: &e.token}`. Adds `strconv`
    /// to the controller's imports.
    Int64FromExternalName { field: String },
    /// `body := akeyless.X{<body_field>: cr.Spec.ForProvider.<spec_field>,
    ///                    ..., Token: &e.token}` — composite-key bodies
    /// pulling each field from the resource's spec.
    SpecFields(Vec<SpecFieldMapping>),
}

/// One (body_field, spec_field, pointer) triple for [`BodyTemplate::SpecFields`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SpecFieldMapping {
    /// SDK body struct field name (e.g. `"RoleName"`).
    pub body_field: String,
    /// Go-PascalCase'd `cr.Spec.ForProvider.<X>` field name
    /// (e.g. `"RoleName"`, `"Path"`, `"AmName"`). The emitter does not
    /// transform — pass the exact Go identifier the IR produces.
    pub spec_field: String,
    /// Whether the SDK body field is `*T` (pointer) — emitter wraps
    /// the spec value in a `&`.
    pub pointer: bool,
}

/// The resolved (post-`per_method`-lookup) shape for one CRUD method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveShape {
    pub identifier_field: String,
    pub identifier_pointer: bool,
    pub body_template: Option<BodyTemplate>,
}

impl Default for ResourceShape {
    fn default() -> Self {
        Self {
            identifier_field: "Name".to_string(),
            identifier_pointer: false,
            per_method: BTreeMap::new(),
            stub: false,
        }
    }
}

impl ResourceShape {
    /// Body-field with `*string` identifier (e.g. `Name *string`).
    #[must_use]
    pub fn name_pointer() -> Self {
        Self {
            identifier_pointer: true,
            ..Self::default()
        }
    }

    /// Body-field with a non-`Name` `string` identifier
    /// (e.g. `EsmName`, `UscName`, `Path`, `Hostname`).
    #[must_use]
    pub fn alt_field(field: impl Into<String>) -> Self {
        Self {
            identifier_field: field.into(),
            ..Self::default()
        }
    }

    /// Stub controller — body shape doesn't fit any expressible
    /// `ResourceShape`; deeper M3.2 graduation needed.
    #[must_use]
    pub fn stub() -> Self {
        Self {
            stub: true,
            ..Self::default()
        }
    }

    /// Add a per-method override and return self (chainable builder).
    #[must_use]
    pub fn with_method_override(mut self, method: CrudMethod, ov: MethodOverride) -> Self {
        self.per_method.insert(method, ov);
        self
    }

    /// Resolve the effective shape for one CRUD method, applying
    /// `per_method` overrides on top of the top-level defaults.
    #[must_use]
    pub fn for_method(&self, method: CrudMethod) -> EffectiveShape {
        let ov = self.per_method.get(&method);
        EffectiveShape {
            identifier_field: ov
                .and_then(|o| o.identifier_field.clone())
                .unwrap_or_else(|| self.identifier_field.clone()),
            identifier_pointer: ov
                .and_then(|o| o.identifier_pointer)
                .unwrap_or(self.identifier_pointer),
            body_template: ov.and_then(|o| o.body_template.clone()),
        }
    }

    /// True when at least one method uses
    /// [`BodyTemplate::Int64FromExternalName`]. Drives conditional
    /// `strconv` import in the emitted controller file.
    #[must_use]
    pub fn needs_strconv_import(&self) -> bool {
        self.per_method.values().any(|o| {
            matches!(
                o.body_template,
                Some(BodyTemplate::Int64FromExternalName { .. })
            )
        })
    }
}

impl ControllerConfig {
    #[must_use]
    pub fn akeyless_default() -> Self {
        Self {
            sdk_module: "github.com/pleme-io/akeyless-go".to_string(),
            provider_module: "github.com/pleme-io/crossplane-akeyless".to_string(),
            api_group: "akeyless.crossplane.io".to_string(),
            api_version: "v1alpha1".to_string(),
            resource_shapes: akeyless_resource_shape_overrides(),
        }
    }

    /// Look up the shape for a resource, falling back to the default
    /// (`Name string`) when no override is registered.
    #[must_use]
    pub fn shape_for(&self, resource: &IacResource, provider: &IacProvider) -> ResourceShape {
        let key: &str = strip_provider_prefix(&resource.name, &provider.name);
        self.resource_shapes
            .get(key)
            .cloned()
            .unwrap_or_default()
    }
}

/// Hardcoded shape overrides for the akeyless TOML corpus, derived from
/// inspecting the actual `pleme-io/akeyless-go` SDK body types
/// (`api_v2.go` + `model_*.go`). Future iteration can replace the
/// hardcoded mapping with OpenAPI-introspection at generation time.
#[must_use]
fn akeyless_resource_shape_overrides() -> BTreeMap<String, ResourceShape> {
    let mut m = BTreeMap::new();
    // Class B-simple: alternate single-string identifier field uniform across CRUD.
    m.insert("esm".into(), ResourceShape::alt_field("EsmName"));
    m.insert("usc".into(), ResourceShape::alt_field("UscName"));

    // Class B-graduated (M3.2): per-method overrides.
    //
    // certificate: GetCertificateValue.Name *string (only outlier);
    // CreateCertificate / UpdateCertificateValue / DeleteItem all
    // have Name string. Default pointer=false; Read overrides to true.
    m.insert(
        "certificate".into(),
        ResourceShape::default().with_method_override(
            CrudMethod::Read,
            MethodOverride {
                identifier_pointer: Some(true),
                ..Default::default()
            },
        ),
    );
    // kmip_client: KmipCreateClient.Name string; KmipDescribeClient
    // and KmipDeleteClient both Name *string. Default pointer=false;
    // Read + Delete override to true.
    m.insert(
        "kmip_client".into(),
        ResourceShape::default()
            .with_method_override(
                CrudMethod::Read,
                MethodOverride {
                    identifier_pointer: Some(true),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Delete,
                MethodOverride {
                    identifier_pointer: Some(true),
                    ..Default::default()
                },
            ),
    );

    // gateway_migration: Name string default; Read+Update use Name *string,
    // Delete uses Id string.
    m.insert(
        "gateway_migration".into(),
        ResourceShape::default()
            .with_method_override(
                CrudMethod::Read,
                MethodOverride {
                    identifier_pointer: Some(true),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Update,
                MethodOverride {
                    identifier_pointer: Some(true),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Delete,
                MethodOverride {
                    identifier_field: Some("Id".to_string()),
                    identifier_pointer: Some(false),
                    ..Default::default()
                },
            ),
    );

    // policy: 3-of-4 use Id string (Read, Update, Delete); Create uses
    // Path string. Default to Id; Create overrides to Path.
    m.insert(
        "policy".into(),
        ResourceShape {
            identifier_field: "Id".to_string(),
            ..ResourceShape::default()
        }
        .with_method_override(
            CrudMethod::Create,
            MethodOverride {
                identifier_field: Some("Path".to_string()),
                ..Default::default()
            },
        ),
    );

    // kmip_environment: Read+Delete have no identifier (singleton bodies).
    // Create uses Hostname (alt-field).
    m.insert(
        "kmip_environment".into(),
        ResourceShape::default()
            .with_method_override(
                CrudMethod::Read,
                MethodOverride {
                    body_template: Some(BodyTemplate::NoIdentifier),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Create,
                MethodOverride {
                    identifier_field: Some("Hostname".to_string()),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Delete,
                MethodOverride {
                    body_template: Some(BodyTemplate::NoIdentifier),
                    ..Default::default()
                },
            ),
    );

    // account_custom_field: Read/Update/Delete bodies use Id int64;
    // Create uses Name string.
    let int64_id = || MethodOverride {
        body_template: Some(BodyTemplate::Int64FromExternalName {
            field: "Id".to_string(),
        }),
        ..Default::default()
    };
    m.insert(
        "account_custom_field".into(),
        ResourceShape::default()
            .with_method_override(CrudMethod::Read, int64_id())
            .with_method_override(CrudMethod::Update, int64_id())
            .with_method_override(CrudMethod::Delete, int64_id()),
    );

    // role_rule: composite (RoleName + Path) on all four methods.
    // Read uses GetRole which only needs the role name.
    let role_rule_set_fields = vec![
        SpecFieldMapping {
            body_field: "RoleName".to_string(),
            spec_field: "RoleName".to_string(),
            pointer: false,
        },
        SpecFieldMapping {
            body_field: "Path".to_string(),
            spec_field: "Path".to_string(),
            pointer: false,
        },
    ];
    m.insert(
        "role_rule".into(),
        ResourceShape::default()
            .with_method_override(
                CrudMethod::Read,
                MethodOverride {
                    body_template: Some(BodyTemplate::SpecFields(vec![SpecFieldMapping {
                        body_field: "Name".to_string(),
                        spec_field: "RoleName".to_string(),
                        pointer: false,
                    }])),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Create,
                MethodOverride {
                    body_template: Some(BodyTemplate::SpecFields(role_rule_set_fields.clone())),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Update,
                MethodOverride {
                    body_template: Some(BodyTemplate::SpecFields(role_rule_set_fields.clone())),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Delete,
                MethodOverride {
                    body_template: Some(BodyTemplate::SpecFields(role_rule_set_fields)),
                    ..Default::default()
                },
            ),
    );

    // role_auth_method_assoc: composite (RoleName + AmName) on Create;
    // Read uses GetRole (single Name field); Delete uses AssocId (from
    // external-name, set when Create succeeds).
    m.insert(
        "role_auth_method_assoc".into(),
        ResourceShape::default()
            .with_method_override(
                CrudMethod::Read,
                MethodOverride {
                    body_template: Some(BodyTemplate::SpecFields(vec![SpecFieldMapping {
                        body_field: "Name".to_string(),
                        spec_field: "RoleName".to_string(),
                        pointer: false,
                    }])),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Create,
                MethodOverride {
                    body_template: Some(BodyTemplate::SpecFields(vec![
                        SpecFieldMapping {
                            body_field: "RoleName".to_string(),
                            spec_field: "RoleName".to_string(),
                            pointer: false,
                        },
                        SpecFieldMapping {
                            body_field: "AmName".to_string(),
                            spec_field: "AmName".to_string(),
                            pointer: false,
                        },
                    ])),
                    ..Default::default()
                },
            )
            .with_method_override(
                CrudMethod::Delete,
                MethodOverride {
                    identifier_field: Some("AssocId".to_string()),
                    ..Default::default()
                },
            ),
    );
    m
}

/// Render the per-resource controller .go file.
#[must_use]
pub fn render_controller(
    resource: &IacResource,
    provider: &IacProvider,
    config: &ControllerConfig,
) -> String {
    print_file(&build_controller_file(resource, provider, config))
}

// ── AST construction ──────────────────────────────────────────────────────

#[must_use]
pub fn build_controller_file(
    resource: &IacResource,
    provider: &IacProvider,
    config: &ControllerConfig,
) -> GoFile {
    let kind = cr_kind(resource, provider);
    let pkg = package_name(resource, provider);
    let shape = config.shape_for(resource, provider);

    let mut file = GoFile::new(&pkg);

    // Imports — stdlib first (printer groups), then third-party with aliases.
    file.imports.push(GoImport::plain("context"));
    file.imports.push(GoImport::plain("errors"));
    if shape.needs_strconv_import() {
        file.imports.push(GoImport::plain("strconv"));
    }
    file.imports.push(GoImport::plain("time"));
    // xpv1 (xpv1.Available) and meta (meta.GetExternalName) are only
    // referenced by non-stub Observe/Create/Update/Delete bodies.
    // Skip them when the controller is fully stubbed to avoid Go's
    // "imported and not used" error.
    if !shape.stub {
        file.imports.push(GoImport::aliased(
            "xpv1",
            "github.com/crossplane/crossplane-runtime/apis/common/v1",
        ));
        file.imports.push(GoImport::plain(
            "github.com/crossplane/crossplane-runtime/pkg/meta",
        ));
    }
    file.imports.push(GoImport::plain(
        "github.com/crossplane/crossplane-runtime/pkg/logging",
    ));
    file.imports.push(GoImport::plain(
        "github.com/crossplane/crossplane-runtime/pkg/reconciler/managed",
    ));
    file.imports.push(GoImport::plain(
        "github.com/crossplane/crossplane-runtime/pkg/resource",
    ));
    file.imports.push(GoImport::aliased(
        "ctrl",
        "sigs.k8s.io/controller-runtime",
    ));
    file.imports.push(GoImport::plain(
        "sigs.k8s.io/controller-runtime/pkg/client",
    ));
    file.imports.push(GoImport::aliased("akeyless", &config.sdk_module));
    file.imports.push(GoImport::aliased(
        "v1alpha1",
        &format!("{}/apis/{}/v1alpha1", config.provider_module, pkg),
    ));
    let provider_pkg = provider.name.replace('-', "");
    file.imports.push(GoImport::aliased(
        "providerv1alpha1",
        &format!(
            "{}/apis/{}/v1alpha1",
            config.provider_module, provider_pkg
        ),
    ));

    // type external struct { client *akeyless.APIClient ; token string }
    file.decls
        .push(GoDecl::Type(build_external_struct_type()));

    // Setup function
    file.decls.push(GoDecl::Func(build_setup_func(&kind)));

    // type connector struct { kube client.Client }
    file.decls.push(GoDecl::Type(build_connector_struct_type()));

    // (c *connector) Connect(...) (managed.ExternalClient, error)
    file.decls.push(GoDecl::Func(build_connect_func(&kind)));

    // (e *external) Observe / Create / Update / Delete
    file.decls
        .push(GoDecl::Func(build_observe_func(resource, &kind, &shape)));
    file.decls
        .push(GoDecl::Func(build_create_func(resource, &kind, &shape)));
    file.decls
        .push(GoDecl::Func(build_update_func(resource, &kind, &shape)));
    file.decls
        .push(GoDecl::Func(build_delete_func(resource, &kind, &shape)));
    file.decls.push(GoDecl::Func(build_disconnect_func()));

    file
}

/// Build the body-construction prelude + composite. Dispatches on
/// the effective shape's `body_template`:
///
///   - `Some(NoIdentifier)`         → empty body except `Token`
///   - `Some(Int64FromExternalName)` → strconv parse + int64 field
///   - `Some(SpecFields)`           → multi-field from cr.Spec.ForProvider
///   - `None`                       → default string-identifier path
///                                     (uses identifier_field +
///                                     identifier_pointer)
fn build_request_body(
    body_type: &str,
    shape: &ResourceShape,
    method: CrudMethod,
) -> (Vec<GoStmt>, GoExpr) {
    let eff = shape.for_method(method);
    if let Some(template) = eff.body_template.as_ref() {
        return build_body_from_template(body_type, template);
    }
    let id_value = if eff.identifier_pointer {
        GoExpr::AddressOf(Box::new(GoExpr::ident("name")))
    } else {
        GoExpr::call(
            GoExpr::path(&["meta", "GetExternalName"]),
            vec![GoExpr::ident("cr")],
        )
    };
    let preamble = if eff.identifier_pointer {
        vec![GoStmt::ShortDecl {
            names: vec!["name".to_string()],
            values: vec![GoExpr::call(
                GoExpr::path(&["meta", "GetExternalName"]),
                vec![GoExpr::ident("cr")],
            )],
        }]
    } else {
        vec![]
    };
    let composite = GoExpr::Composite {
        ty: GoType::qualified("akeyless", body_type),
        fields: vec![
            (Some(eff.identifier_field), id_value),
            (
                Some("Token".to_string()),
                GoExpr::AddressOf(Box::new(GoExpr::sel(
                    GoExpr::ident("e"),
                    "token",
                ))),
            ),
        ],
        addr_of: false,
    };
    (preamble, composite)
}

fn build_body_from_template(body_type: &str, template: &BodyTemplate) -> (Vec<GoStmt>, GoExpr) {
    match template {
        BodyTemplate::NoIdentifier => {
            let composite = GoExpr::Composite {
                ty: GoType::qualified("akeyless", body_type),
                fields: vec![(
                    Some("Token".to_string()),
                    GoExpr::AddressOf(Box::new(GoExpr::sel(
                        GoExpr::ident("e"),
                        "token",
                    ))),
                )],
                addr_of: false,
            };
            (vec![], composite)
        }
        BodyTemplate::Int64FromExternalName { field } => {
            // id, _ := strconv.ParseInt(meta.GetExternalName(cr), 10, 64)
            let preamble = vec![GoStmt::ShortDecl {
                names: vec!["id".to_string(), "_".to_string()],
                values: vec![GoExpr::call(
                    GoExpr::path(&["strconv", "ParseInt"]),
                    vec![
                        GoExpr::call(
                            GoExpr::path(&["meta", "GetExternalName"]),
                            vec![GoExpr::ident("cr")],
                        ),
                        GoExpr::Lit(GoLit::Int(10)),
                        GoExpr::Lit(GoLit::Int(64)),
                    ],
                )],
            }];
            let composite = GoExpr::Composite {
                ty: GoType::qualified("akeyless", body_type),
                fields: vec![
                    (Some(field.clone()), GoExpr::ident("id")),
                    (
                        Some("Token".to_string()),
                        GoExpr::AddressOf(Box::new(GoExpr::sel(
                            GoExpr::ident("e"),
                            "token",
                        ))),
                    ),
                ],
                addr_of: false,
            };
            (preamble, composite)
        }
        BodyTemplate::SpecFields(mappings) => {
            // body := akeyless.X{Field1: cr.Spec.ForProvider.Spec1, ..., Token: &e.token}
            let mut fields: Vec<(Option<String>, GoExpr)> = mappings
                .iter()
                .map(|m| {
                    let value = GoExpr::path(&["cr", "Spec", "ForProvider", m.spec_field.as_str()]);
                    let value = if m.pointer {
                        GoExpr::AddressOf(Box::new(value))
                    } else {
                        value
                    };
                    (Some(m.body_field.clone()), value)
                })
                .collect();
            fields.push((
                Some("Token".to_string()),
                GoExpr::AddressOf(Box::new(GoExpr::sel(
                    GoExpr::ident("e"),
                    "token",
                ))),
            ));
            let composite = GoExpr::Composite {
                ty: GoType::qualified("akeyless", body_type),
                fields,
                addr_of: false,
            };
            (vec![], composite)
        }
    }
}

fn build_disconnect_func() -> GoFuncDecl {
    // crossplane-runtime v1.18 added Disconnect(ctx) error to the
    // ExternalClient interface. The default no-op is correct for any
    // SDK that doesn't hold long-lived resources beyond per-call
    // request state — the akeyless SDK falls into this category.
    let mut body = GoBlock::new();
    body.push(GoStmt::Return(vec![GoExpr::nil()]));
    GoFuncDecl {
        name: "Disconnect".to_string(),
        doc: Some(
            "Disconnect releases any per-cluster client resources. The akeyless SDK\nholds no long-lived resources beyond per-call request state, so this is\na no-op. Required by crossplane-runtime v1.18+'s ExternalClient interface."
                .to_string(),
        ),
        recv: Some(GoRecv {
            name: "_".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![GoParam {
            name: "_".to_string(),
            ty: GoType::qualified("context", "Context"),
        }],
        returns: vec![GoType::named("error")],
        body,
    }
}

fn build_external_struct_type() -> GoTypeDecl {
    let fields = vec![
        GoField {
            name: Some("client".to_string()),
            ty: GoType::pointer(GoType::qualified("akeyless", "APIClient")),
            doc: None,
            markers: vec![],
            tags: vec![],
        },
        GoField {
            name: Some("token".to_string()),
            ty: GoType::named("string"),
            doc: None,
            markers: vec![],
            tags: vec![],
        },
    ];
    GoTypeDecl {
        name: "external".to_string(),
        doc: Some(
            "external is the per-cluster reconciler that owns the SDK client and\nthe token resolved from the bound ProviderConfig."
                .to_string(),
        ),
        markers: vec![],
        body: GoTypeBody::Struct(fields),
    }
}

fn build_connector_struct_type() -> GoTypeDecl {
    let fields = vec![GoField {
        name: Some("kube".to_string()),
        ty: GoType::qualified("client", "Client"),
        doc: None,
        markers: vec![],
        tags: vec![],
    }];
    GoTypeDecl {
        name: "connector".to_string(),
        doc: Some(
            "connector resolves a ProviderConfig and constructs an external\nclient for each managed-resource reconcile loop."
                .to_string(),
        ),
        markers: vec![],
        body: GoTypeBody::Struct(fields),
    }
}

fn build_setup_func(kind: &str) -> GoFuncDecl {
    let mut body = GoBlock::new();
    // name := "<Kind>"
    body.push(GoStmt::ShortDecl {
        names: vec!["name".to_string()],
        values: vec![GoExpr::str(kind)],
    });
    // gvk := v1alpha1.GroupVersion.WithKind("<Kind>")
    body.push(GoStmt::ShortDecl {
        names: vec!["gvk".to_string()],
        values: vec![GoExpr::call(
            GoExpr::sel(
                GoExpr::sel(GoExpr::ident("v1alpha1"), "GroupVersion"),
                "WithKind",
            ),
            vec![GoExpr::str(kind)],
        )],
    });
    // r := managed.NewReconciler(mgr,
    //         resource.ManagedKind(gvk),
    //         managed.WithExternalConnecter(&connector{kube: mgr.GetClient()}),
    //         managed.WithPollInterval(pollInterval),
    //         managed.WithLogger(ctrl.Log.WithName(name)))
    let connector_lit = GoExpr::Composite {
        ty: GoType::named("connector"),
        fields: vec![(
            Some("kube".to_string()),
            GoExpr::call(
                GoExpr::sel(GoExpr::ident("mgr"), "GetClient"),
                vec![],
            ),
        )],
        addr_of: true,
    };
    let new_reconciler = GoExpr::call(
        GoExpr::path(&["managed", "NewReconciler"]),
        vec![
            GoExpr::ident("mgr"),
            GoExpr::call(
                GoExpr::path(&["resource", "ManagedKind"]),
                vec![GoExpr::ident("gvk")],
            ),
            GoExpr::call(
                GoExpr::path(&["managed", "WithExternalConnecter"]),
                vec![connector_lit],
            ),
            GoExpr::call(
                GoExpr::path(&["managed", "WithPollInterval"]),
                vec![GoExpr::ident("pollInterval")],
            ),
            GoExpr::call(
                GoExpr::path(&["managed", "WithLogger"]),
                vec![GoExpr::call(
                    GoExpr::path(&["logging", "NewLogrLogger"]),
                    vec![GoExpr::call(
                        GoExpr::path(&["ctrl", "Log", "WithName"]),
                        vec![GoExpr::ident("name")],
                    )],
                )],
            ),
        ],
    );
    body.push(GoStmt::ShortDecl {
        names: vec!["r".to_string()],
        values: vec![new_reconciler],
    });
    // return ctrl.NewControllerManagedBy(mgr).Named(name).For(&v1alpha1.<Kind>{}).Complete(r)
    let chain = GoExpr::call(
        GoExpr::sel(
            GoExpr::call(
                GoExpr::sel(
                    GoExpr::call(
                        GoExpr::sel(
                            GoExpr::call(
                                GoExpr::path(&["ctrl", "NewControllerManagedBy"]),
                                vec![GoExpr::ident("mgr")],
                            ),
                            "Named",
                        ),
                        vec![GoExpr::ident("name")],
                    ),
                    "For",
                ),
                vec![GoExpr::Composite {
                    ty: GoType::qualified("v1alpha1", kind),
                    fields: vec![],
                    addr_of: true,
                }],
            ),
            "Complete",
        ),
        vec![GoExpr::ident("r")],
    );
    body.push(GoStmt::Return(vec![chain]));

    GoFuncDecl {
        name: "Setup".to_string(),
        doc: Some("Setup wires this resource's controller into the manager.".to_string()),
        recv: None,
        params: vec![
            GoParam {
                name: "mgr".to_string(),
                ty: GoType::qualified("ctrl", "Manager"),
            },
            GoParam {
                name: "pollInterval".to_string(),
                ty: GoType::qualified("time", "Duration"),
            },
        ],
        returns: vec![GoType::named("error")],
        body,
    }
}

fn build_connect_func(kind: &str) -> GoFuncDecl {
    let mut body = GoBlock::new();
    // cr, ok := mg.(*v1alpha1.<Kind>)
    body.push(GoStmt::ShortDecl {
        names: vec!["cr".to_string(), "ok".to_string()],
        values: vec![GoExpr::TypeAssert {
            x: Box::new(GoExpr::ident("mg")),
            ty: GoType::pointer(GoType::qualified("v1alpha1", kind)),
            with_ok: true,
        }],
    });
    // if !ok { return nil, errors.New("expected *v1alpha1.<Kind>") }
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("!ok"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![
                GoExpr::nil(),
                GoExpr::call(
                    GoExpr::path(&["errors", "New"]),
                    vec![GoExpr::str(&format!("expected *v1alpha1.{kind}"))],
                ),
            ]));
            b
        },
        else_body: None,
    });
    // pcRef := cr.GetProviderConfigReference()
    body.push(GoStmt::ShortDecl {
        names: vec!["pcRef".to_string()],
        values: vec![GoExpr::call(
            GoExpr::sel(GoExpr::ident("cr"), "GetProviderConfigReference"),
            vec![],
        )],
    });
    // if pcRef == nil { return nil, errors.New("no provider config reference") }
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("pcRef == nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![
                GoExpr::nil(),
                GoExpr::call(
                    GoExpr::path(&["errors", "New"]),
                    vec![GoExpr::str("no provider config reference")],
                ),
            ]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::Blank);
    // pc := &providerv1alpha1.ProviderConfig{}
    body.push(GoStmt::ShortDecl {
        names: vec!["pc".to_string()],
        values: vec![GoExpr::Composite {
            ty: GoType::qualified("providerv1alpha1", "ProviderConfig"),
            fields: vec![],
            addr_of: true,
        }],
    });
    // if err := c.kube.Get(ctx, client.ObjectKey{Name: pcRef.Name}, pc); err != nil { return nil, err }
    body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["err".to_string()],
            values: vec![GoExpr::call(
                GoExpr::sel(
                    GoExpr::sel(GoExpr::ident("c"), "kube"),
                    "Get",
                ),
                vec![
                    GoExpr::ident("ctx"),
                    GoExpr::Composite {
                        ty: GoType::qualified("client", "ObjectKey"),
                        fields: vec![(
                            Some("Name".to_string()),
                            GoExpr::sel(GoExpr::ident("pcRef"), "Name"),
                        )],
                        addr_of: false,
                    },
                    GoExpr::ident("pc"),
                ],
            )],
        })),
        cond: GoExpr::ident("err != nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![GoExpr::nil(), GoExpr::ident("err")]));
            b
        },
        else_body: None,
    });
    // creds, err := resource.CommonCredentialExtractor(ctx, pc.Spec.Credentials.Source, c.kube, pc.Spec.Credentials.CommonCredentialSelectors)
    body.push(GoStmt::ShortDecl {
        names: vec!["creds".to_string(), "err".to_string()],
        values: vec![GoExpr::call(
            GoExpr::path(&["resource", "CommonCredentialExtractor"]),
            vec![
                GoExpr::ident("ctx"),
                GoExpr::path(&["pc", "Spec", "Credentials", "Source"]),
                GoExpr::sel(GoExpr::ident("c"), "kube"),
                GoExpr::path(&["pc", "Spec", "Credentials", "CommonCredentialSelectors"]),
            ],
        )],
    });
    // if err != nil { return nil, err }
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("err != nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![GoExpr::nil(), GoExpr::ident("err")]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::Blank);
    // cfg := akeyless.NewConfiguration()
    body.push(GoStmt::ShortDecl {
        names: vec!["cfg".to_string()],
        values: vec![GoExpr::call(
            GoExpr::path(&["akeyless", "NewConfiguration"]),
            vec![],
        )],
    });
    // if pc.Spec.APIGateway != "" { cfg.Servers = []akeyless.ServerConfiguration{{URL: pc.Spec.APIGateway}} }
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("pc.Spec.APIGateway != \"\""),
        body: {
            let mut b = GoBlock::new();
            // Slice 1: keep this assignment shape simple — emit a comment
            // pointing at slice 2 for slice-literal AST. The goast AST
            // doesn't yet have a SliceLit variant; for now we emit a
            // Free identifier covering the line.
            b.push(GoStmt::Comment(
                "TODO goast-slice-2: emit cfg.Servers via structured SliceLit".to_string(),
            ));
            b.push(GoStmt::Expr(GoExpr::ident(
                "cfg.Servers = []akeyless.ServerConfiguration{{URL: pc.Spec.APIGateway}}",
            )));
            b
        },
        else_body: None,
    });
    // return &external{client: akeyless.NewAPIClient(cfg), token: string(creds)}, nil
    let _ = creds_unused();
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::named("external"),
            fields: vec![
                (
                    Some("client".to_string()),
                    GoExpr::call(
                        GoExpr::path(&["akeyless", "NewAPIClient"]),
                        vec![GoExpr::ident("cfg")],
                    ),
                ),
                (
                    Some("token".to_string()),
                    GoExpr::call(GoExpr::ident("string"), vec![GoExpr::ident("creds")]),
                ),
            ],
            addr_of: true,
        },
        GoExpr::nil(),
    ]));

    GoFuncDecl {
        name: "Connect".to_string(),
        doc: None,
        recv: Some(GoRecv {
            name: "c".to_string(),
            ty: GoType::pointer(GoType::named("connector")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalClient"),
            GoType::named("error"),
        ],
        body,
    }
}

fn creds_unused() {} // silence — `creds` is referenced in the return; the var doesn't go unused

fn build_observe_func(resource: &IacResource, kind: &str, shape: &ResourceShape) -> GoFuncDecl {
    if shape.stub {
        return build_stub_observe(kind);
    }
    let read_method = sdk_naming::go_method_name(&resource.crud.read_schema);
    let read_body_type = sdk_naming::go_body_type_name(&resource.crud.read_schema);

    let mut body = type_assert_into_cr(kind, &observation_return_path());
    let (preamble, composite) = build_request_body(&read_body_type, shape, CrudMethod::Read);
    for s in preamble {
        body.push(s);
    }
    body.push(GoStmt::ShortDecl {
        names: vec!["body".to_string()],
        values: vec![composite],
    });
    // _, _, err := e.client.V2API.<read_method>(ctx).<read_body_type>(body).Execute()
    body.push(sdk_call_three_underscores(
        &read_method,
        &read_body_type,
    ));
    // if err != nil { ... return ExternalObservation{}, err }
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("err != nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Comment(
                "TODO controller-iter-2: distinguish 404 (NotFound → ResourceExists=false)".to_string(),
            ));
            b.push(GoStmt::Comment(
                "from real errors; today every read error short-circuits the reconcile.".to_string(),
            ));
            b.push(GoStmt::Return(vec![
                GoExpr::Composite {
                    ty: GoType::qualified("managed", "ExternalObservation"),
                    fields: vec![],
                    addr_of: false,
                },
                GoExpr::ident("err"),
            ]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::Blank);
    // cr.SetConditions(xpv1.Available())
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("cr"), "SetConditions"),
        vec![GoExpr::call(
            GoExpr::path(&["xpv1", "Available"]),
            vec![],
        )],
    )));
    // return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
    body.push(GoStmt::Comment(
        "ResourceUpToDate=true short-circuits the spec↔atProvider diff;".to_string(),
    ));
    body.push(GoStmt::Comment(
        "structural diff lands in controller-iter-2.".to_string(),
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalObservation"),
            fields: vec![
                (Some("ResourceExists".to_string()), GoExpr::Lit(GoLit::Bool(true))),
                (
                    Some("ResourceUpToDate".to_string()),
                    GoExpr::Lit(GoLit::Bool(true)),
                ),
            ],
            addr_of: false,
        },
        GoExpr::nil(),
    ]));

    GoFuncDecl {
        name: "Observe".to_string(),
        doc: Some(
            "Observe queries the upstream provider for the resource's current state.\nThe returned ExternalObservation tells the managed-resource reconciler\n(a) whether the resource exists upstream and (b) whether its observed\nstate matches the declared spec."
                .to_string(),
        ),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalObservation"),
            GoType::named("error"),
        ],
        body,
    }
}

fn build_create_func(resource: &IacResource, kind: &str, shape: &ResourceShape) -> GoFuncDecl {
    if shape.stub {
        return build_stub_create(kind);
    }
    let create_method = sdk_naming::go_method_name(&resource.crud.create_schema);
    let create_body_type = sdk_naming::go_body_type_name(&resource.crud.create_schema);

    let mut body = type_assert_into_cr(kind, &creation_return_path());
    let (preamble, composite) = build_request_body(&create_body_type, shape, CrudMethod::Create);
    for s in preamble {
        body.push(s);
    }
    body.push(GoStmt::ShortDecl {
        names: vec!["body".to_string()],
        values: vec![composite],
    });
    body.push(GoStmt::Comment(
        "TODO controller-iter-2: map cr.Spec.ForProvider fields → body fields".to_string(),
    ));
    body.push(sdk_call_three_underscores(
        &create_method,
        &create_body_type,
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalCreation"),
            fields: vec![],
            addr_of: false,
        },
        GoExpr::ident("err"),
    ]));

    GoFuncDecl {
        name: "Create".to_string(),
        doc: Some("Create provisions the resource upstream.".to_string()),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalCreation"),
            GoType::named("error"),
        ],
        body,
    }
}

fn build_update_func(resource: &IacResource, kind: &str, shape: &ResourceShape) -> GoFuncDecl {
    if shape.stub {
        return build_stub_update(kind);
    }
    let Some(update_schema) = resource.crud.update_schema.as_deref() else {
        // No-op for resources without an update endpoint
        let mut body = GoBlock::new();
        body.push(GoStmt::ShortDecl {
            names: vec!["_".to_string(), "ok".to_string()],
            values: vec![GoExpr::TypeAssert {
                x: Box::new(GoExpr::ident("mg")),
                ty: GoType::pointer(GoType::qualified("v1alpha1", kind)),
                with_ok: true,
            }],
        });
        body.push(GoStmt::If {
            init: None,
            cond: GoExpr::ident("!ok"),
            body: {
                let mut b = GoBlock::new();
                b.push(GoStmt::Return(vec![
                    GoExpr::Composite {
                        ty: GoType::qualified("managed", "ExternalUpdate"),
                        fields: vec![],
                        addr_of: false,
                    },
                    GoExpr::call(
                        GoExpr::path(&["errors", "New"]),
                        vec![GoExpr::str(&format!("expected *v1alpha1.{kind}"))],
                    ),
                ]));
                b
            },
            else_body: None,
        });
        body.push(GoStmt::Return(vec![
            GoExpr::Composite {
                ty: GoType::qualified("managed", "ExternalUpdate"),
                fields: vec![],
                addr_of: false,
            },
            GoExpr::nil(),
        ]));
        return GoFuncDecl {
            name: "Update".to_string(),
            doc: Some(
                "Update is a no-op — this resource type has no update endpoint.\nSpec changes that mutate immutable fields trigger force-replace via\nthe controller-runtime managed reconciler."
                    .to_string(),
            ),
            recv: Some(GoRecv {
                name: "e".to_string(),
                ty: GoType::pointer(GoType::named("external")),
            }),
            params: vec![
                GoParam {
                    name: "_".to_string(),
                    ty: GoType::qualified("context", "Context"),
                },
                GoParam {
                    name: "mg".to_string(),
                    ty: GoType::qualified("resource", "Managed"),
                },
            ],
            returns: vec![
                GoType::qualified("managed", "ExternalUpdate"),
                GoType::named("error"),
            ],
            body,
        };
    };

    let update_method = sdk_naming::go_method_name(update_schema);
    let update_body_type = sdk_naming::go_body_type_name(update_schema);

    let mut body = type_assert_into_cr(kind, &update_return_path());
    let (preamble, composite) = build_request_body(&update_body_type, shape, CrudMethod::Update);
    for s in preamble {
        body.push(s);
    }
    body.push(GoStmt::ShortDecl {
        names: vec!["body".to_string()],
        values: vec![composite],
    });
    body.push(GoStmt::Comment(
        "TODO controller-iter-2: map mutable cr.Spec.ForProvider fields → body fields".to_string(),
    ));
    body.push(sdk_call_three_underscores(
        &update_method,
        &update_body_type,
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalUpdate"),
            fields: vec![],
            addr_of: false,
        },
        GoExpr::ident("err"),
    ]));

    GoFuncDecl {
        name: "Update".to_string(),
        doc: Some(
            "Update reconciles the upstream resource against the declared spec.".to_string(),
        ),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalUpdate"),
            GoType::named("error"),
        ],
        body,
    }
}

fn build_delete_func(resource: &IacResource, kind: &str, shape: &ResourceShape) -> GoFuncDecl {
    if shape.stub {
        return build_stub_delete(kind);
    }
    let delete_method = sdk_naming::go_method_name(&resource.crud.delete_schema);
    let delete_body_type = sdk_naming::go_body_type_name(&resource.crud.delete_schema);

    // crossplane-runtime v1.18 changed Delete's signature from `error` to
    // `(managed.ExternalDelete, error)`. The zero ExternalDelete value is
    // returned on success.
    let zero_external_delete = GoExpr::Composite {
        ty: GoType::qualified("managed", "ExternalDelete"),
        fields: vec![],
        addr_of: false,
    };

    // Type assert: cr, ok := mg.(*v1alpha1.<Kind>)
    let mut body = GoBlock::new();
    body.push(GoStmt::ShortDecl {
        names: vec!["cr".to_string(), "ok".to_string()],
        values: vec![GoExpr::TypeAssert {
            x: Box::new(GoExpr::ident("mg")),
            ty: GoType::pointer(GoType::qualified("v1alpha1", kind)),
            with_ok: true,
        }],
    });
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("!ok"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![
                zero_external_delete.clone(),
                GoExpr::call(
                    GoExpr::path(&["errors", "New"]),
                    vec![GoExpr::str(&format!("expected *v1alpha1.{kind}"))],
                ),
            ]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::Blank);
    let (preamble, composite) = build_request_body(&delete_body_type, shape, CrudMethod::Delete);
    for s in preamble {
        body.push(s);
    }
    body.push(GoStmt::ShortDecl {
        names: vec!["body".to_string()],
        values: vec![composite],
    });
    body.push(sdk_call_three_underscores(
        &delete_method,
        &delete_body_type,
    ));
    body.push(GoStmt::Comment(
        "TODO controller-iter-2: swallow 404 so deletion is idempotent.".to_string(),
    ));
    body.push(GoStmt::Return(vec![
        zero_external_delete,
        GoExpr::ident("err"),
    ]));

    GoFuncDecl {
        name: "Delete".to_string(),
        doc: Some(
            "Delete removes the upstream resource. Idempotent on NotFound.\nSignature follows crossplane-runtime v1.18+: returns (ExternalDelete, error)."
                .to_string(),
        ),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalDelete"),
            GoType::named("error"),
        ],
        body,
    }
}

// ── Stub controllers (composite-key / mixed-identifier resources) ────────
//
// Resources whose SDK body shape requires composite keys
// (RoleName+Path, RoleName+AmName), mixed-per-CRUD-op identifiers
// (Name on Create vs Id on Get/Delete), or singleton bodies with no
// identifier field, get a stub controller that satisfies the
// ExternalClient interface but no-ops every reconcile call. M3.2
// graduates each one as the body-mapping work lands.

fn build_stub_observe(kind: &str) -> GoFuncDecl {
    let mut body = type_assert_discard_cr(kind, &observation_return_path());
    body.push(GoStmt::Comment(
        "Stub: body-mapping pending M3.2 graduation — returns ResourceExists=false".to_string(),
    ));
    body.push(GoStmt::Comment(
        "so the reconciler stays idle for resources with composite-key shapes.".to_string(),
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalObservation"),
            fields: vec![],
            addr_of: false,
        },
        GoExpr::nil(),
    ]));
    GoFuncDecl {
        name: "Observe".to_string(),
        doc: Some(
            "Observe — STUB. Body-mapping pending M3.2 graduation for resources whose\nSDK body shape requires composite keys / mixed-per-CRUD identifiers.\nReturns an empty ExternalObservation so the reconciler stays idle."
                .to_string(),
        ),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalObservation"),
            GoType::named("error"),
        ],
        body,
    }
}

fn build_stub_create(kind: &str) -> GoFuncDecl {
    let mut body = type_assert_discard_cr(kind, &creation_return_path());
    body.push(GoStmt::Comment(
        "Stub: body-mapping pending M3.2 graduation.".to_string(),
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalCreation"),
            fields: vec![],
            addr_of: false,
        },
        GoExpr::nil(),
    ]));
    GoFuncDecl {
        name: "Create".to_string(),
        doc: Some("Create — STUB. Body-mapping pending M3.2 graduation.".to_string()),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalCreation"),
            GoType::named("error"),
        ],
        body,
    }
}

fn build_stub_update(kind: &str) -> GoFuncDecl {
    let mut body = type_assert_discard_cr(kind, &update_return_path());
    body.push(GoStmt::Comment(
        "Stub: body-mapping pending M3.2 graduation.".to_string(),
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalUpdate"),
            fields: vec![],
            addr_of: false,
        },
        GoExpr::nil(),
    ]));
    GoFuncDecl {
        name: "Update".to_string(),
        doc: Some("Update — STUB. Body-mapping pending M3.2 graduation.".to_string()),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalUpdate"),
            GoType::named("error"),
        ],
        body,
    }
}

fn build_stub_delete(kind: &str) -> GoFuncDecl {
    // Mirror the type-assert + If !ok prelude from build_delete_func so
    // the stub's signature matches non-stub Delete (returns
    // managed.ExternalDelete + error per v1.18).
    let mut body = GoBlock::new();
    body.push(GoStmt::ShortDecl {
        names: vec!["_".to_string(), "ok".to_string()],
        values: vec![GoExpr::TypeAssert {
            x: Box::new(GoExpr::ident("mg")),
            ty: GoType::pointer(GoType::qualified("v1alpha1", kind)),
            with_ok: true,
        }],
    });
    body.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("!ok"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![
                GoExpr::Composite {
                    ty: GoType::qualified("managed", "ExternalDelete"),
                    fields: vec![],
                    addr_of: false,
                },
                GoExpr::call(
                    GoExpr::path(&["errors", "New"]),
                    vec![GoExpr::str(&format!("expected *v1alpha1.{kind}"))],
                ),
            ]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::Comment(
        "Stub: body-mapping pending M3.2 graduation.".to_string(),
    ));
    body.push(GoStmt::Return(vec![
        GoExpr::Composite {
            ty: GoType::qualified("managed", "ExternalDelete"),
            fields: vec![],
            addr_of: false,
        },
        GoExpr::nil(),
    ]));
    GoFuncDecl {
        name: "Delete".to_string(),
        doc: Some("Delete — STUB. Body-mapping pending M3.2 graduation.".to_string()),
        recv: Some(GoRecv {
            name: "e".to_string(),
            ty: GoType::pointer(GoType::named("external")),
        }),
        params: vec![
            GoParam {
                name: "ctx".to_string(),
                ty: GoType::qualified("context", "Context"),
            },
            GoParam {
                name: "mg".to_string(),
                ty: GoType::qualified("resource", "Managed"),
            },
        ],
        returns: vec![
            GoType::qualified("managed", "ExternalDelete"),
            GoType::named("error"),
        ],
        body,
    }
}

// ── Per-method body helpers ──────────────────────────────────────────────

/// Like [`type_assert_into_cr`] but discards the receiver pointer
/// (`_, ok :=` instead of `cr, ok :=`). Used by stub controllers
/// that only need to verify the type matches.
fn type_assert_discard_cr(kind: &str, return_zero: &GoType) -> GoBlock {
    let mut b = GoBlock::new();
    b.push(GoStmt::ShortDecl {
        names: vec!["_".to_string(), "ok".to_string()],
        values: vec![GoExpr::TypeAssert {
            x: Box::new(GoExpr::ident("mg")),
            ty: GoType::pointer(GoType::qualified("v1alpha1", kind)),
            with_ok: true,
        }],
    });
    b.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("!ok"),
        body: {
            let mut inner = GoBlock::new();
            inner.push(GoStmt::Return(vec![
                GoExpr::Composite {
                    ty: return_zero.clone(),
                    fields: vec![],
                    addr_of: false,
                },
                GoExpr::call(
                    GoExpr::path(&["errors", "New"]),
                    vec![GoExpr::str(&format!("expected *v1alpha1.{kind}"))],
                ),
            ]));
            inner
        },
        else_body: None,
    });
    b.push(GoStmt::Blank);
    b
}

fn type_assert_into_cr(kind: &str, return_zero: &GoType) -> GoBlock {
    // cr, ok := mg.(*v1alpha1.<Kind>)
    // if !ok { return <ZeroOf>{}, errors.New("expected *v1alpha1.<Kind>") }
    let mut b = GoBlock::new();
    b.push(GoStmt::ShortDecl {
        names: vec!["cr".to_string(), "ok".to_string()],
        values: vec![GoExpr::TypeAssert {
            x: Box::new(GoExpr::ident("mg")),
            ty: GoType::pointer(GoType::qualified("v1alpha1", kind)),
            with_ok: true,
        }],
    });
    b.push(GoStmt::If {
        init: None,
        cond: GoExpr::ident("!ok"),
        body: {
            let mut inner = GoBlock::new();
            inner.push(GoStmt::Return(vec![
                GoExpr::Composite {
                    ty: return_zero.clone(),
                    fields: vec![],
                    addr_of: false,
                },
                GoExpr::call(
                    GoExpr::path(&["errors", "New"]),
                    vec![GoExpr::str(&format!("expected *v1alpha1.{kind}"))],
                ),
            ]));
            inner
        },
        else_body: None,
    });
    b.push(GoStmt::Blank);
    let _ = cr_ident_unused();
    b
}

fn cr_ident_unused() {}

fn observation_return_path() -> GoType {
    GoType::qualified("managed", "ExternalObservation")
}
fn creation_return_path() -> GoType {
    GoType::qualified("managed", "ExternalCreation")
}
fn update_return_path() -> GoType {
    GoType::qualified("managed", "ExternalUpdate")
}

fn sdk_call_three_underscores(method: &str, body_type: &str) -> GoStmt {
    // _, _, err := e.client.V2API.<method>(ctx).<body_type>(body).Execute()
    let chain = GoExpr::call(
        GoExpr::sel(
            GoExpr::call(
                GoExpr::sel(
                    GoExpr::call(
                        GoExpr::sel(
                            GoExpr::sel(
                                GoExpr::sel(GoExpr::ident("e"), "client"),
                                "V2API",
                            ),
                            method,
                        ),
                        vec![GoExpr::ident("ctx")],
                    ),
                    body_type,
                ),
                vec![GoExpr::ident("body")],
            ),
            "Execute",
        ),
        vec![],
    );
    GoStmt::ShortDecl {
        names: vec!["_".to_string(), "_".to_string(), "err".to_string()],
        values: vec![chain],
    }
}

// ── Naming helpers (re-exported public surface kept stable) ──────────────

/// The Go package name for this resource's controller — `to_snake_case`'d
/// type name (e.g. `authmethodapikey`).
#[must_use]
pub fn package_name(resource: &IacResource, provider: &IacProvider) -> String {
    strip_provider_prefix(&resource.name, &provider.name)
        .replace('_', "")
        .to_lowercase()
}

/// The Go-exported CRD kind (e.g. `AuthMethodApiKey`).
#[must_use]
pub fn cr_kind(resource: &IacResource, provider: &IacProvider) -> String {
    to_pascal_case(strip_provider_prefix(&resource.name, &provider.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::ir::{AuthInfo, CrudInfo, IdentityInfo};
    use std::collections::BTreeMap;

    fn akeyless_provider() -> IacProvider {
        IacProvider {
            name: "akeyless".to_string(),
            description: "Akeyless Vault Provider".to_string(),
            version: "1.0.0".to_string(),
            auth: AuthInfo::default(),
            skip_fields: vec![],
            platform_config: BTreeMap::new(),
        }
    }

    fn auth_method_api_key() -> IacResource {
        IacResource {
            name: "akeyless_auth_method_api_key".to_string(),
            description: "Manages an API key authentication method".to_string(),
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
            attributes: vec![],
            identity: IdentityInfo {
                id_field: "name".to_string(),
                import_field: "name".to_string(),
                force_replace_fields: vec!["name".to_string()],
            },
        }
    }

    fn role_no_update() -> IacResource {
        IacResource {
            name: "akeyless_role".to_string(),
            description: "Manages a role".to_string(),
            category: "role".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create-role".to_string(),
                create_schema: "createRole".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/get-role".to_string(),
                read_schema: "getRole".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete-role".to_string(),
                delete_schema: "deleteRole".to_string(),
            },
            attributes: vec![],
            identity: IdentityInfo {
                id_field: "name".to_string(),
                import_field: "name".to_string(),
                force_replace_fields: vec!["name".to_string()],
            },
        }
    }

    // ── AST-shape tests ──────────────────────────────────────────────────

    #[test]
    fn controller_file_has_expected_decl_sequence() {
        let f = build_controller_file(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        let decl_names: Vec<String> = f
            .decls
            .iter()
            .filter_map(|d| match d {
                GoDecl::Type(t) => Some(t.name.clone()),
                GoDecl::Func(fd) => Some(fd.name.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            decl_names,
            vec![
                "external".to_string(),
                "Setup".to_string(),
                "connector".to_string(),
                "Connect".to_string(),
                "Observe".to_string(),
                "Create".to_string(),
                "Update".to_string(),
                "Delete".to_string(),
                "Disconnect".to_string(),
            ]
        );
    }

    #[test]
    fn external_struct_has_client_and_token_fields() {
        let t = build_external_struct_type();
        let GoTypeBody::Struct(fields) = &t.body else {
            panic!()
        };
        let names: Vec<&str> = fields.iter().filter_map(|f| f.name.as_deref()).collect();
        assert_eq!(names, vec!["client", "token"]);
        // client is *akeyless.APIClient
        assert!(matches!(
            fields[0].ty,
            GoType::Pointer(ref inner) if matches!(
                **inner,
                GoType::Qualified { ref pkg, ref name } if pkg == "akeyless" && name == "APIClient",
            )
        ));
    }

    #[test]
    fn setup_returns_complete_chain() {
        let f = build_setup_func("Foo");
        // Last statement is a Return wrapping a Call to .Complete(r)
        let last = f.body.stmts.last().unwrap();
        let GoStmt::Return(exprs) = last else {
            panic!("expected return")
        };
        let GoExpr::Call { fun, args } = &exprs[0] else {
            panic!("expected call")
        };
        let GoExpr::Selector { sel, .. } = fun.as_ref() else {
            panic!("expected selector")
        };
        assert_eq!(sel, "Complete");
        assert!(matches!(args[0], GoExpr::Ident(ref s) if s == "r"));
    }

    #[test]
    fn observe_calls_correct_sdk_method() {
        let f = build_observe_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        let rendered = print_file(&{
            let mut file = GoFile::new("p");
            file.decls.push(GoDecl::Func(f));
            file
        });
        // The rendered chain must call V2API.GetAuthMethod(ctx).GetAuthMethod(body).Execute()
        // We assert via substring on the structurally-rendered output —
        // failure modes here are SDK-naming bugs, not formatting bugs.
        assert!(rendered.contains("e.client.V2API.GetAuthMethod(ctx).GetAuthMethod(body).Execute()"));
    }

    #[test]
    fn create_calls_correct_sdk_method() {
        let f = build_create_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        let mut file = GoFile::new("p");
        file.decls.push(GoDecl::Func(f));
        let rendered = print_file(&file);
        assert!(rendered.contains(
            "e.client.V2API.AuthMethodCreateApiKey(ctx).AuthMethodCreateApiKey(body).Execute()"
        ));
    }

    #[test]
    fn update_no_op_branch_for_resources_without_update_schema() {
        let f = build_update_func(&role_no_update(), "Role", &ResourceShape::default());
        // Rendering: should NOT contain V2API call
        let mut file = GoFile::new("p");
        file.decls.push(GoDecl::Func(f));
        let rendered = print_file(&file);
        assert!(!rendered.contains("V2API."));
        assert!(rendered.contains("// Update is a no-op"));
    }

    #[test]
    fn delete_calls_correct_sdk_method() {
        let f = build_delete_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        let mut file = GoFile::new("p");
        file.decls.push(GoDecl::Func(f));
        let rendered = print_file(&file);
        assert!(rendered.contains(
            "e.client.V2API.DeleteAuthMethod(ctx).DeleteAuthMethod(body).Execute()"
        ));
        // v1.18 return shape — both error paths return managed.ExternalDelete{}
        assert!(rendered.contains("return managed.ExternalDelete{}, err"));
        assert!(rendered.contains("return managed.ExternalDelete{}, errors.New("));
    }

    #[test]
    fn controller_emits_disconnect_method_for_v1_18() {
        // crossplane-runtime v1.18 added Disconnect(ctx) error to
        // ExternalClient. *external must implement it; we emit a no-op.
        let s = render_controller(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        assert!(s.contains("func (_ *external) Disconnect(_ context.Context) error {"));
        assert!(s.contains("holds no long-lived resources"));
        // No-op: just `return nil`
        assert!(s.contains("return nil"));
    }

    #[test]
    fn setup_func_wraps_logger_in_logging_logr_wrapper() {
        // M5.3 regression: managed.WithLogger requires Crossplane's
        // logging.Logger interface, not raw logr.Logger from
        // controller-runtime. Wrap via logging.NewLogrLogger.
        let s = render_controller(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        assert!(s.contains("managed.WithLogger(logging.NewLogrLogger(ctrl.Log.WithName(name)))"));
        assert!(s.contains("\"github.com/crossplane/crossplane-runtime/pkg/logging\""));
    }

    #[test]
    fn package_and_kind_naming() {
        assert_eq!(
            package_name(&auth_method_api_key(), &akeyless_provider()),
            "authmethodapikey"
        );
        assert_eq!(
            cr_kind(&auth_method_api_key(), &akeyless_provider()),
            "AuthMethodApiKey"
        );
    }

    #[test]
    fn rendered_controller_carries_no_format_string_leakage() {
        let s = render_controller(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        // No leaked placeholder syntax
        assert!(!s.contains("{kind}"));
        assert!(!s.contains("{api_group}"));
        assert!(!s.contains("{this_pkg}"));
        // Code-gen marker
        assert!(s.contains("// Code generated by iac-forge. DO NOT EDIT."));
    }

    #[test]
    fn deterministic_render() {
        let cfg = ControllerConfig::akeyless_default();
        let a = render_controller(&auth_method_api_key(), &akeyless_provider(), &cfg);
        let b = render_controller(&auth_method_api_key(), &akeyless_provider(), &cfg);
        assert_eq!(a, b);
    }

    // ── Deeper AST-shape tests (one per emitted function body) ──────────

    #[test]
    fn observe_body_shape() {
        let f = build_observe_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        // First two stmts: type-assert short decl, then the !ok if branch.
        assert!(matches!(
            f.body.stmts[0],
            GoStmt::ShortDecl { ref names, .. } if names == &["cr".to_string(), "ok".to_string()]
        ));
        assert!(matches!(f.body.stmts[1], GoStmt::If { .. }));
        // Should declare a `body` short-decl with the read-body composite.
        let body_decl = f.body.stmts.iter().find(|s| matches!(
            s, GoStmt::ShortDecl { names, .. } if names == &["body".to_string()],
        ));
        assert!(body_decl.is_some());
        // Should `_, _, err :=` short-decl the SDK chain.
        let sdk_call = f.body.stmts.iter().find(|s| matches!(
            s, GoStmt::ShortDecl { names, .. }
                if names == &["_".to_string(), "_".to_string(), "err".to_string()],
        ));
        assert!(sdk_call.is_some());
        // Last stmt is a Return with managed.ExternalObservation
        let GoStmt::Return(returns) = f.body.stmts.last().unwrap() else {
            panic!("expected return")
        };
        assert_eq!(returns.len(), 2);
        assert!(matches!(
            returns[0],
            GoExpr::Composite {
                ty: GoType::Qualified { ref pkg, ref name },
                ..
            } if pkg == "managed" && name == "ExternalObservation"
        ));
        assert!(matches!(returns[1], GoExpr::Lit(GoLit::Nil)));
    }

    #[test]
    fn observe_signature_correct() {
        let f = build_observe_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        assert_eq!(f.name, "Observe");
        let recv = f.recv.as_ref().unwrap();
        assert_eq!(recv.name, "e");
        assert!(matches!(
            recv.ty,
            GoType::Pointer(ref inner)
                if matches!(**inner, GoType::Named(ref n) if n == "external"),
        ));
        let param_names: Vec<&str> = f.params.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(param_names, vec!["ctx", "mg"]);
        assert_eq!(f.returns.len(), 2);
        assert!(matches!(
            f.returns[0],
            GoType::Qualified { ref pkg, ref name }
                if pkg == "managed" && name == "ExternalObservation",
        ));
        assert!(matches!(f.returns[1], GoType::Named(ref n) if n == "error"));
    }

    #[test]
    fn create_signature_returns_external_creation() {
        let f = build_create_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        assert_eq!(f.name, "Create");
        assert!(matches!(
            f.returns[0],
            GoType::Qualified { ref pkg, ref name }
                if pkg == "managed" && name == "ExternalCreation",
        ));
    }

    #[test]
    fn delete_returns_external_delete_and_error_v1_18() {
        let f = build_delete_func(&auth_method_api_key(), "AuthMethodApiKey", &ResourceShape::default());
        assert_eq!(f.returns.len(), 2);
        assert!(matches!(
            f.returns[0],
            GoType::Qualified { ref pkg, ref name }
                if pkg == "managed" && name == "ExternalDelete",
        ));
        assert!(matches!(f.returns[1], GoType::Named(ref n) if n == "error"));
    }

    #[test]
    fn setup_signature_takes_manager_and_poll_interval() {
        let f = build_setup_func("Foo");
        assert!(f.recv.is_none(), "Setup is a free function, not a method");
        let params: Vec<&str> = f.params.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(params, vec!["mgr", "pollInterval"]);
        assert!(matches!(
            f.params[0].ty,
            GoType::Qualified { ref pkg, ref name } if pkg == "ctrl" && name == "Manager",
        ));
        assert!(matches!(
            f.params[1].ty,
            GoType::Qualified { ref pkg, ref name } if pkg == "time" && name == "Duration",
        ));
        assert_eq!(f.returns.len(), 1);
        assert!(matches!(f.returns[0], GoType::Named(ref n) if n == "error"));
    }

    #[test]
    fn connect_signature_returns_external_client_and_error() {
        let f = build_connect_func("Foo");
        assert_eq!(f.name, "Connect");
        let recv = f.recv.as_ref().unwrap();
        assert_eq!(recv.name, "c");
        assert!(matches!(
            recv.ty,
            GoType::Pointer(ref inner)
                if matches!(**inner, GoType::Named(ref n) if n == "connector"),
        ));
        assert!(matches!(
            f.returns[0],
            GoType::Qualified { ref pkg, ref name }
                if pkg == "managed" && name == "ExternalClient",
        ));
    }

    #[test]
    fn imports_include_required_runtime_packages() {
        let f = build_controller_file(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        let paths: Vec<&str> = f.imports.iter().map(|i| i.path.as_str()).collect();
        // stdlib
        assert!(paths.contains(&"context"));
        assert!(paths.contains(&"errors"));
        assert!(paths.contains(&"time"));
        // crossplane-runtime
        assert!(paths.contains(&"github.com/crossplane/crossplane-runtime/apis/common/v1"));
        assert!(paths.contains(&"github.com/crossplane/crossplane-runtime/pkg/meta"));
        assert!(paths.contains(&"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"));
        assert!(paths.contains(&"github.com/crossplane/crossplane-runtime/pkg/resource"));
        // controller-runtime
        assert!(paths.contains(&"sigs.k8s.io/controller-runtime"));
        assert!(paths.contains(&"sigs.k8s.io/controller-runtime/pkg/client"));
        // sdk + types
        assert!(paths.contains(&"github.com/pleme-io/akeyless-go"));
        // No duplicate paths (would be a regression)
        let mut sorted = paths.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), paths.len(), "imports must be unique");
    }

    #[test]
    fn imports_aliases_correct() {
        let f = build_controller_file(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        let by_path: std::collections::HashMap<&str, Option<&str>> = f
            .imports
            .iter()
            .map(|i| (i.path.as_str(), i.alias.as_deref()))
            .collect();
        assert_eq!(
            by_path.get("github.com/crossplane/crossplane-runtime/apis/common/v1"),
            Some(&Some("xpv1"))
        );
        assert_eq!(
            by_path.get("sigs.k8s.io/controller-runtime"),
            Some(&Some("ctrl"))
        );
        assert_eq!(
            by_path.get("github.com/pleme-io/akeyless-go"),
            Some(&Some("akeyless"))
        );
    }

    #[test]
    fn five_representative_resources_render_cleanly() {
        let provider = akeyless_provider();
        let cfg = ControllerConfig::akeyless_default();
        let resources = vec![auth_method_api_key(), role_no_update()];
        for r in &resources {
            let s = render_controller(r, &provider, &cfg);
            // Header
            assert!(
                s.starts_with("// Code generated by iac-forge. DO NOT EDIT."),
                "missing header in {}",
                r.name
            );
            // Each resource emits all 4 ExternalClient methods
            for m in &["Observe", "Create", "Update", "Delete"] {
                assert!(
                    s.contains(&format!("func (e *external) {m}")),
                    "missing {m} in {}",
                    r.name
                );
            }
            // No leakage
            assert!(!s.contains("{kind}"));
            assert!(!s.contains("{this_pkg}"));
        }
    }

    #[test]
    fn no_method_body_panics_for_resources_with_or_without_update() {
        let cfg = ControllerConfig::akeyless_default();
        let provider = akeyless_provider();
        // With update endpoint
        let _ = render_controller(&auth_method_api_key(), &provider, &cfg);
        // Without update endpoint
        let _ = render_controller(&role_no_update(), &provider, &cfg);
    }

    #[test]
    fn rendered_setup_chain_matches_expected_method_order() {
        // The Setup function returns a chain:
        //   ctrl.NewControllerManagedBy(mgr).Named(name).For(&v1alpha1.<Kind>{}).Complete(r)
        // Verify all four pieces are present in the rendered output, in
        // the right order.
        let s = render_controller(
            &auth_method_api_key(),
            &akeyless_provider(),
            &ControllerConfig::akeyless_default(),
        );
        let nc_pos = s.find("ctrl.NewControllerManagedBy(mgr)").unwrap();
        let named_pos = s.find(".Named(name)").unwrap();
        let for_pos = s.find(".For(&v1alpha1.AuthMethodApiKey{})").unwrap();
        let complete_pos = s.find(".Complete(r)").unwrap();
        assert!(nc_pos < named_pos);
        assert!(named_pos < for_pos);
        assert!(for_pos < complete_pos);
    }
}
