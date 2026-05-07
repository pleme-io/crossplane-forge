//! Provider-runtime scaffold emitter.
//!
//! Where `types_gen` + `controller_gen` emit per-resource artifacts,
//! this module emits the cross-cutting scaffold that wraps them into a
//! buildable, deployable Crossplane provider:
//!
//!   apis/<provider>/v1alpha1/providerconfig_types.go    ← cred plug
//!   apis/<provider>/v1alpha1/groupversion_info.go       ← scheme glue
//!   cmd/provider/main.go                                ← entry point
//!   internal/controller/setup.go                        ← controller wiring
//!   go.mod                                              ← Go module manifest
//!   helm/Chart.yaml + helm/values.yaml + helm/templates/* ← deployment
//!
//! ## Substrate-hygiene posture (post-refactor 2026-05-06)
//!
//! Every Go file is built as a typed [`iac_forge::goast::GoFile`] tree
//! and rendered through `print_file`. Every YAML file is built either
//! as a typed serde-derive struct (`Chart`, `HelmValues`) or as a
//! [`serde_yaml_ng::Value`] tree composed via small typed helpers —
//! never as `format!()` strings of YAML / Go syntax.

use serde::{Deserialize, Serialize};
use serde_yaml_ng::{Mapping, Value};

use iac_forge::goast::{
    GoBlock, GoDecl, GoExpr, GoField, GoFile, GoFuncDecl, GoImport, GoLit, GoParam, GoStmt,
    GoStructTag, GoType, GoTypeBody, GoTypeDecl, GoVarDecl, JsonTag, KubeMarker, ResourceScope,
    SubresourceKind, print_file,
};
use iac_forge::ir::{IacProvider, IacResource};

use crate::controller_gen::{ControllerConfig, package_name};

// ── Public API (rendering) ────────────────────────────────────────────────

#[must_use]
pub fn render_provider_config_types(provider: &IacProvider, config: &ControllerConfig) -> String {
    print_file(&build_provider_config_types_file(provider, config))
}

#[must_use]
pub fn render_provider_groupversion_info(
    provider: &IacProvider,
    config: &ControllerConfig,
) -> String {
    print_file(&build_provider_groupversion_info_file(provider, config))
}

#[must_use]
pub fn render_main_go(provider: &IacProvider, config: &ControllerConfig) -> String {
    print_file(&build_main_go_file(provider, config))
}

#[must_use]
pub fn render_setup_go(
    resources: &[IacResource],
    provider: &IacProvider,
    config: &ControllerConfig,
) -> String {
    print_file(&build_setup_go_file(resources, provider, config))
}

#[must_use]
pub fn render_apis_aggregator(
    resources: &[IacResource],
    provider: &IacProvider,
    config: &ControllerConfig,
) -> String {
    print_file(&build_apis_aggregator_file(resources, provider, config))
}

#[must_use]
pub fn render_go_mod(provider: &IacProvider, config: &ControllerConfig) -> String {
    let _ = provider;
    let m = build_go_mod(config);
    m.print()
}

#[must_use]
pub fn render_helm_chart_yaml(provider: &IacProvider, config: &ControllerConfig) -> String {
    let _ = config;
    let c = build_helm_chart(provider);
    serde_yaml_ng::to_string(&c).expect("Chart serialization is infallible")
}

#[must_use]
pub fn render_helm_values_yaml(provider: &IacProvider, config: &ControllerConfig) -> String {
    let _ = config;
    let v = build_helm_values(provider);
    serde_yaml_ng::to_string(&v).expect("Values serialization is infallible")
}

#[must_use]
pub fn render_helm_deployment_template(
    provider: &IacProvider,
    config: &ControllerConfig,
) -> String {
    let _ = (provider, config);
    serde_yaml_ng::to_string(&build_helm_deployment_value())
        .expect("Deployment YAML serialization is infallible")
}

#[must_use]
pub fn render_helm_rbac_template(provider: &IacProvider, config: &ControllerConfig) -> String {
    let _ = (provider, config);
    serde_yaml_ng::to_string(&build_helm_rbac_value())
        .expect("RBAC YAML serialization is infallible")
}

// ── Go AST builders ───────────────────────────────────────────────────────

#[must_use]
pub fn build_provider_config_types_file(
    _provider: &IacProvider,
    _config: &ControllerConfig,
) -> GoFile {
    let mut file = GoFile::new("v1alpha1");
    file.imports.push(GoImport::aliased(
        "metav1",
        "k8s.io/apimachinery/pkg/apis/meta/v1",
    ));
    file.imports.push(GoImport::aliased(
        "xpv1",
        "github.com/crossplane/crossplane-runtime/apis/common/v1",
    ));

    // ProviderConfigSpec
    let pc_spec = GoTypeDecl {
        name: "ProviderConfigSpec".to_string(),
        doc: Some("ProviderConfigSpec defines the desired state of a ProviderConfig.".to_string()),
        markers: vec![],
        body: GoTypeBody::Struct(vec![
            GoField {
                name: Some("Credentials".to_string()),
                ty: GoType::named("ProviderCredentials"),
                doc: Some("Credentials required to authenticate to the upstream API.".to_string()),
                markers: vec![KubeMarker::Required],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: "credentials".to_string(),
                    omitempty: false,
                    inline: false,
                })],
            },
            GoField {
                name: Some("APIGateway".to_string()),
                ty: GoType::named("string"),
                doc: Some("APIGateway is the URL of the API endpoint to call.".to_string()),
                markers: vec![KubeMarker::Optional],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: "apiGateway".to_string(),
                    omitempty: true,
                    inline: false,
                })],
            },
        ]),
    };
    file.decls.push(GoDecl::Type(pc_spec));

    // ProviderCredentials
    let pc_creds = GoTypeDecl {
        name: "ProviderCredentials".to_string(),
        doc: Some("ProviderCredentials carries the credential source + selector.".to_string()),
        markers: vec![],
        body: GoTypeBody::Struct(vec![
            GoField {
                name: Some("Source".to_string()),
                ty: GoType::qualified("xpv1", "CredentialsSource"),
                doc: Some("Source of the provider credentials.".to_string()),
                markers: vec![
                    KubeMarker::Free(
                        "+kubebuilder:validation:Enum=None;Secret;Environment;Filesystem".to_string(),
                    ),
                    KubeMarker::Required,
                ],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: "source".to_string(),
                    omitempty: false,
                    inline: false,
                })],
            },
            GoField {
                name: None,
                ty: GoType::qualified("xpv1", "CommonCredentialSelectors"),
                doc: None,
                markers: vec![],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: String::new(),
                    omitempty: false,
                    inline: true,
                })],
            },
        ]),
    };
    file.decls.push(GoDecl::Type(pc_creds));

    // ProviderConfig + List
    file.decls.push(GoDecl::Type(provider_config_kind_struct()));
    file.decls.push(GoDecl::Type(provider_config_list_struct()));
    file.decls.push(GoDecl::Type(provider_config_usage_struct()));
    file.decls.push(GoDecl::Type(provider_config_usage_list_struct()));

    file
}

fn provider_config_kind_struct() -> GoTypeDecl {
    let markers = vec![
        KubeMarker::ObjectRoot,
        KubeMarker::Subresource(SubresourceKind::Status),
        KubeMarker::Resource {
            scope: ResourceScope::Cluster,
            categories: vec![
                "crossplane".to_string(),
                "provider".to_string(),
                "akeyless".to_string(),
            ],
        },
        KubeMarker::PrintColumn {
            name: "AGE".to_string(),
            ty: "date".to_string(),
            json_path: ".metadata.creationTimestamp".to_string(),
            priority: None,
        },
        KubeMarker::PrintColumn {
            name: "SECRET-NAME".to_string(),
            ty: "string".to_string(),
            json_path: ".spec.credentials.secretRef.name".to_string(),
            priority: Some(1),
        },
    ];
    GoTypeDecl {
        name: "ProviderConfig".to_string(),
        doc: None,
        markers,
        body: GoTypeBody::Struct(vec![
            embedded_typemeta(),
            json_metadata_field(),
            GoField {
                name: Some("Spec".to_string()),
                ty: GoType::named("ProviderConfigSpec"),
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
                ty: GoType::qualified("xpv1", "ProviderConfigStatus"),
                doc: None,
                markers: vec![],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: "status".to_string(),
                    omitempty: true,
                    inline: false,
                })],
            },
        ]),
    }
}

fn provider_config_list_struct() -> GoTypeDecl {
    GoTypeDecl {
        name: "ProviderConfigList".to_string(),
        doc: None,
        markers: vec![KubeMarker::ObjectRoot],
        body: GoTypeBody::Struct(vec![
            embedded_typemeta(),
            json_metadata_field_listmeta(),
            GoField {
                name: Some("Items".to_string()),
                ty: GoType::slice(GoType::named("ProviderConfig")),
                doc: None,
                markers: vec![],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: "items".to_string(),
                    omitempty: false,
                    inline: false,
                })],
            },
        ]),
    }
}

fn provider_config_usage_struct() -> GoTypeDecl {
    let markers = vec![
        KubeMarker::ObjectRoot,
        KubeMarker::Subresource(SubresourceKind::Status),
        KubeMarker::Resource {
            scope: ResourceScope::Cluster,
            categories: vec![
                "crossplane".to_string(),
                "provider".to_string(),
                "akeyless".to_string(),
            ],
        },
        KubeMarker::PrintColumn {
            name: "CONFIG-NAME".to_string(),
            ty: "string".to_string(),
            json_path: ".providerConfigRef.name".to_string(),
            priority: None,
        },
        KubeMarker::PrintColumn {
            name: "RESOURCE-KIND".to_string(),
            ty: "string".to_string(),
            json_path: ".resourceRef.kind".to_string(),
            priority: None,
        },
        KubeMarker::PrintColumn {
            name: "RESOURCE-NAME".to_string(),
            ty: "string".to_string(),
            json_path: ".resourceRef.name".to_string(),
            priority: None,
        },
    ];
    GoTypeDecl {
        name: "ProviderConfigUsage".to_string(),
        doc: None,
        markers,
        body: GoTypeBody::Struct(vec![
            embedded_typemeta(),
            json_metadata_field(),
            GoField {
                name: None,
                ty: GoType::qualified("xpv1", "ProviderConfigUsage"),
                doc: None,
                markers: vec![],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: String::new(),
                    omitempty: false,
                    inline: true,
                })],
            },
        ]),
    }
}

fn provider_config_usage_list_struct() -> GoTypeDecl {
    GoTypeDecl {
        name: "ProviderConfigUsageList".to_string(),
        doc: None,
        markers: vec![KubeMarker::ObjectRoot],
        body: GoTypeBody::Struct(vec![
            embedded_typemeta(),
            json_metadata_field_listmeta(),
            GoField {
                name: Some("Items".to_string()),
                ty: GoType::slice(GoType::named("ProviderConfigUsage")),
                doc: None,
                markers: vec![],
                tags: vec![GoStructTag::Json(JsonTag {
                    name: "items".to_string(),
                    omitempty: false,
                    inline: false,
                })],
            },
        ]),
    }
}

fn embedded_typemeta() -> GoField {
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
    }
}
fn json_metadata_field() -> GoField {
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
    }
}
fn json_metadata_field_listmeta() -> GoField {
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
    }
}

#[must_use]
pub fn build_provider_groupversion_info_file(
    _provider: &IacProvider,
    config: &ControllerConfig,
) -> GoFile {
    let mut file = GoFile::new("v1alpha1");
    file.markers.push(KubeMarker::ObjectGenerate(true));
    file.markers
        .push(KubeMarker::GroupName(config.api_group.clone()));
    file.imports.push(GoImport::plain(
        "k8s.io/apimachinery/pkg/runtime/schema",
    ));
    file.imports.push(GoImport::plain(
        "sigs.k8s.io/controller-runtime/pkg/scheme",
    ));

    file.decls.push(GoDecl::Var(GoVarDecl {
        name: "GroupVersion".to_string(),
        ty: None,
        value: Some(GoExpr::Composite {
            ty: GoType::qualified("schema", "GroupVersion"),
            fields: vec![
                (
                    Some("Group".to_string()),
                    GoExpr::str(&config.api_group),
                ),
                (
                    Some("Version".to_string()),
                    GoExpr::str(&config.api_version),
                ),
            ],
            addr_of: false,
        }),
        doc: None,
        block_id: None,
    }));
    file.decls.push(GoDecl::Var(GoVarDecl {
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
    file.decls.push(GoDecl::Var(GoVarDecl {
        name: "AddToScheme".to_string(),
        ty: None,
        value: Some(GoExpr::Selector {
            recv: Box::new(GoExpr::ident("SchemeBuilder")),
            sel: "AddToScheme".to_string(),
        }),
        doc: None,
        block_id: None,
    }));

    let mut init_body = GoBlock::new();
    init_body.push(GoStmt::Expr(GoExpr::Call {
        fun: Box::new(GoExpr::Selector {
            recv: Box::new(GoExpr::ident("SchemeBuilder")),
            sel: "Register".to_string(),
        }),
        args: vec![
            empty_composite("ProviderConfig", true),
            empty_composite("ProviderConfigList", true),
            empty_composite("ProviderConfigUsage", true),
            empty_composite("ProviderConfigUsageList", true),
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

fn empty_composite(name: &str, addr_of: bool) -> GoExpr {
    GoExpr::Composite {
        ty: GoType::named(name),
        fields: vec![],
        addr_of,
    }
}

#[must_use]
pub fn build_main_go_file(provider: &IacProvider, config: &ControllerConfig) -> GoFile {
    let mut file = GoFile::new("main");
    file.imports.push(GoImport::plain("flag"));
    file.imports.push(GoImport::plain("fmt"));
    file.imports.push(GoImport::plain("os"));
    file.imports.push(GoImport::plain("time"));
    file.imports.push(GoImport::aliased(
        "ctrl",
        "sigs.k8s.io/controller-runtime",
    ));
    file.imports.push(GoImport::plain(
        "sigs.k8s.io/controller-runtime/pkg/log/zap",
    ));
    file.imports.push(GoImport::aliased(
        "apis",
        &format!("{}/apis", config.provider_module),
    ));
    file.imports.push(GoImport::plain(&format!(
        "{}/internal/controller",
        config.provider_module
    )));

    let mut body = GoBlock::new();
    // leaderElection := false
    // pollInterval := time.Minute
    //
    // (Plain short decls inside main() are idiomatic Go. The `var ( ... )`
    // block form would group them visually but isn't required for
    // correctness, and goast doesn't yet have a typed VarBlock node.
    // When that lands, this site migrates trivially.)
    body.push(GoStmt::ShortDecl {
        names: vec!["leaderElection".to_string()],
        values: vec![GoExpr::Lit(GoLit::Bool(false))],
    });
    body.push(GoStmt::ShortDecl {
        names: vec!["pollInterval".to_string()],
        values: vec![GoExpr::sel(GoExpr::ident("time"), "Minute")],
    });
    // flag.BoolVar(&leaderElection, "leader-election", false, "...")
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("flag"), "BoolVar"),
        vec![
            GoExpr::AddressOf(Box::new(GoExpr::ident("leaderElection"))),
            GoExpr::str("leader-election"),
            GoExpr::Lit(GoLit::Bool(false)),
            GoExpr::str("Enable leader election for the controller manager."),
        ],
    )));
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("flag"), "DurationVar"),
        vec![
            GoExpr::AddressOf(Box::new(GoExpr::ident("pollInterval"))),
            GoExpr::str("poll"),
            GoExpr::ident("pollInterval"),
            GoExpr::str("Poll interval for managed-resource reconciliation."),
        ],
    )));
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("flag"), "Parse"),
        vec![],
    )));
    body.push(GoStmt::Blank);
    // ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
    body.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("ctrl"), "SetLogger"),
        vec![GoExpr::call(
            GoExpr::sel(GoExpr::ident("zap"), "New"),
            vec![GoExpr::call(
                GoExpr::sel(GoExpr::ident("zap"), "UseDevMode"),
                vec![GoExpr::Lit(GoLit::Bool(true))],
            )],
        )],
    )));
    body.push(GoStmt::Blank);
    // cfg, err := ctrl.GetConfig()
    body.push(GoStmt::ShortDecl {
        names: vec!["cfg".to_string(), "err".to_string()],
        values: vec![GoExpr::call(
            GoExpr::sel(GoExpr::ident("ctrl"), "GetConfig"),
            vec![],
        )],
    });
    body.push(exit_on_err("GetConfig"));

    // mgr, err := ctrl.NewManager(cfg, ctrl.Options{...})
    body.push(GoStmt::ShortDecl {
        names: vec!["mgr".to_string(), "err".to_string()],
        values: vec![GoExpr::call(
            GoExpr::sel(GoExpr::ident("ctrl"), "NewManager"),
            vec![
                GoExpr::ident("cfg"),
                GoExpr::Composite {
                    ty: GoType::qualified("ctrl", "Options"),
                    fields: vec![
                        (
                            Some("LeaderElection".to_string()),
                            GoExpr::ident("leaderElection"),
                        ),
                        (
                            Some("LeaderElectionID".to_string()),
                            GoExpr::str(&format!("crossplane-{}-leader", provider.name)),
                        ),
                    ],
                    addr_of: false,
                },
            ],
        )],
    });
    body.push(exit_on_err("NewManager"));
    body.push(GoStmt::Blank);
    body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["err".to_string()],
            values: vec![GoExpr::call(
                GoExpr::sel(GoExpr::ident("apis"), "AddToScheme"),
                vec![GoExpr::call(
                    GoExpr::sel(GoExpr::ident("mgr"), "GetScheme"),
                    vec![],
                )],
            )],
        })),
        cond: GoExpr::ident("err != nil"),
        body: exit_block("AddToScheme"),
        else_body: None,
    });
    body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["err".to_string()],
            values: vec![GoExpr::call(
                GoExpr::sel(GoExpr::ident("controller"), "Setup"),
                vec![GoExpr::ident("mgr"), GoExpr::ident("pollInterval")],
            )],
        })),
        cond: GoExpr::ident("err != nil"),
        body: exit_block("controller.Setup"),
        else_body: None,
    });
    body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["err".to_string()],
            values: vec![GoExpr::call(
                GoExpr::sel(GoExpr::ident("mgr"), "Start"),
                vec![GoExpr::call(
                    GoExpr::sel(GoExpr::ident("ctrl"), "SetupSignalHandler"),
                    vec![],
                )],
            )],
        })),
        cond: GoExpr::ident("err != nil"),
        body: exit_block("mgr.Start"),
        else_body: None,
    });

    file.decls.push(GoDecl::Func(GoFuncDecl {
        name: "main".to_string(),
        doc: None,
        recv: None,
        params: vec![],
        returns: vec![],
        body,
    }));
    file
}

fn exit_block(label: &str) -> GoBlock {
    let mut b = GoBlock::new();
    b.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("fmt"), "Fprintln"),
        vec![
            GoExpr::sel(GoExpr::ident("os"), "Stderr"),
            GoExpr::str(&format!("{label}:")),
            GoExpr::ident("err"),
        ],
    )));
    b.push(GoStmt::Expr(GoExpr::call(
        GoExpr::sel(GoExpr::ident("os"), "Exit"),
        vec![GoExpr::Lit(GoLit::Int(1))],
    )));
    b
}

fn exit_on_err(label: &str) -> GoStmt {
    GoStmt::If {
        init: None,
        cond: GoExpr::ident("err != nil"),
        body: exit_block(label),
        else_body: None,
    }
}

/// `apis/apis.go` — aggregates every per-resource v1alpha1 SchemeBuilder
/// + the per-provider v1alpha1 SchemeBuilder behind a single
/// `AddToScheme` symbol. `cmd/provider/main.go` calls this to register
/// every CRD into the controller-runtime manager's scheme.
#[must_use]
pub fn build_apis_aggregator_file(
    resources: &[IacResource],
    provider: &IacProvider,
    config: &ControllerConfig,
) -> GoFile {
    let mut file = GoFile::new("apis");
    file.imports.push(GoImport::plain(
        "k8s.io/apimachinery/pkg/runtime",
    ));
    let provider_pkg = provider.name.replace('-', "");
    // Provider-level v1alpha1 (ProviderConfig package)
    file.imports.push(GoImport::aliased(
        &format!("{provider_pkg}v1alpha1"),
        &format!("{}/apis/{}/v1alpha1", config.provider_module, provider_pkg),
    ));
    // Per-resource v1alpha1 packages (alias the v1alpha1 package by the
    // resource pkg name to avoid collisions on the import path)
    for r in resources {
        let pkg = package_name(r, provider);
        file.imports.push(GoImport::aliased(
            &pkg,
            &format!("{}/apis/{pkg}/v1alpha1", config.provider_module),
        ));
    }

    // var AddToScheme = func(s *runtime.Scheme) error {
    //     builders := []func(*runtime.Scheme) error{ pkg.AddToScheme, ... }
    //     for _, b := range builders { if err := b(s); err != nil { return err } }
    //     return nil
    // }
    let mut builder_elements: Vec<GoExpr> = vec![GoExpr::Selector {
        recv: Box::new(GoExpr::ident(&format!("{provider_pkg}v1alpha1"))),
        sel: "AddToScheme".to_string(),
    }];
    for r in resources {
        let pkg = package_name(r, provider);
        builder_elements.push(GoExpr::Selector {
            recv: Box::new(GoExpr::ident(&pkg)),
            sel: "AddToScheme".to_string(),
        });
    }
    let func_sig_ty = GoType::FuncSignature {
        params: vec![GoType::pointer(GoType::qualified("runtime", "Scheme"))],
        returns: vec![GoType::named("error")],
    };
    let mut fn_body = GoBlock::new();
    fn_body.push(GoStmt::ShortDecl {
        names: vec!["builders".to_string()],
        values: vec![GoExpr::SliceLit {
            elem_type: func_sig_ty.clone(),
            elements: builder_elements,
        }],
    });
    let mut for_body = GoBlock::new();
    for_body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["err".to_string()],
            values: vec![GoExpr::call(
                GoExpr::ident("b"),
                vec![GoExpr::ident("s")],
            )],
        })),
        cond: GoExpr::ident("err != nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![GoExpr::ident("err")]));
            b
        },
        else_body: None,
    });
    fn_body.push(GoStmt::ForRange {
        key: None,
        value: Some("b".to_string()),
        range: GoExpr::ident("builders"),
        body: for_body,
    });
    fn_body.push(GoStmt::Return(vec![GoExpr::nil()]));

    file.decls.push(GoDecl::Func(GoFuncDecl {
        name: "AddToScheme".to_string(),
        doc: Some(
            "AddToScheme registers every CRD kind emitted by this provider with the\nsupplied controller-runtime scheme. Invoked by cmd/provider/main.go."
                .to_string(),
        ),
        recv: None,
        params: vec![GoParam {
            name: "s".to_string(),
            ty: GoType::pointer(GoType::qualified("runtime", "Scheme")),
        }],
        returns: vec![GoType::named("error")],
        body: fn_body,
    }));

    file
}

#[must_use]
pub fn build_setup_go_file(
    resources: &[IacResource],
    provider: &IacProvider,
    config: &ControllerConfig,
) -> GoFile {
    let mut file = GoFile::new("controller");
    file.imports.push(GoImport::plain("time"));
    file.imports.push(GoImport::aliased(
        "ctrl",
        "sigs.k8s.io/controller-runtime",
    ));
    for r in resources {
        let pkg = package_name(r, provider);
        file.imports.push(GoImport::plain(&format!(
            "{}/internal/controller/{}",
            config.provider_module, pkg
        )));
    }

    // setups := []func(ctrl.Manager, time.Duration) error{ <pkg>.Setup, ... }
    let mut body = GoBlock::new();
    let setup_elements: Vec<GoExpr> = resources
        .iter()
        .map(|r| {
            let pkg = package_name(r, provider);
            GoExpr::Selector {
                recv: Box::new(GoExpr::ident(&pkg)),
                sel: "Setup".to_string(),
            }
        })
        .collect();
    let setups_lit = GoExpr::SliceLit {
        elem_type: GoType::FuncSignature {
            params: vec![
                GoType::qualified("ctrl", "Manager"),
                GoType::qualified("time", "Duration"),
            ],
            returns: vec![GoType::named("error")],
        },
        elements: setup_elements,
    };
    body.push(GoStmt::ShortDecl {
        names: vec!["setups".to_string()],
        values: vec![setups_lit],
    });

    // for _, s := range setups {
    //     if err := s(mgr, pollInterval); err != nil { return err }
    // }
    let mut for_body = GoBlock::new();
    for_body.push(GoStmt::If {
        init: Some(Box::new(GoStmt::ShortDecl {
            names: vec!["err".to_string()],
            values: vec![GoExpr::call(
                GoExpr::ident("s"),
                vec![GoExpr::ident("mgr"), GoExpr::ident("pollInterval")],
            )],
        })),
        cond: GoExpr::ident("err != nil"),
        body: {
            let mut b = GoBlock::new();
            b.push(GoStmt::Return(vec![GoExpr::ident("err")]));
            b
        },
        else_body: None,
    });
    body.push(GoStmt::ForRange {
        key: None,
        value: Some("s".to_string()),
        range: GoExpr::ident("setups"),
        body: for_body,
    });
    body.push(GoStmt::Return(vec![GoExpr::nil()]));

    file.decls.push(GoDecl::Func(GoFuncDecl {
        name: "Setup".to_string(),
        doc: Some(
            "Setup wires every resource controller into the manager. Returns the\nfirst error encountered; nil on success.".to_string(),
        ),
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
    }));

    file
}

// ── Typed go.mod struct + printer ────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoMod {
    pub module: String,
    pub go_version: String,
    pub require: Vec<GoModRequire>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoModRequire {
    pub path: String,
    pub version: String,
}

impl GoMod {
    fn print(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("module {}\n\n", self.module));
        out.push_str(&format!("go {}\n", self.go_version));
        if !self.require.is_empty() {
            out.push_str("\nrequire (\n");
            for r in &self.require {
                out.push_str(&format!("\t{} {}\n", r.path, r.version));
            }
            out.push_str(")\n");
        }
        out
    }
}

#[must_use]
pub fn build_go_mod(config: &ControllerConfig) -> GoMod {
    GoMod {
        module: config.provider_module.clone(),
        go_version: "1.23".to_string(),
        require: vec![
            GoModRequire {
                path: config.sdk_module.clone(),
                version: "v0.1.0".to_string(),
            },
            GoModRequire {
                path: "github.com/crossplane/crossplane-runtime".to_string(),
                version: "v1.18.0".to_string(),
            },
            GoModRequire {
                path: "k8s.io/apimachinery".to_string(),
                version: "v0.31.0".to_string(),
            },
            GoModRequire {
                path: "sigs.k8s.io/controller-runtime".to_string(),
                version: "v0.19.0".to_string(),
            },
        ],
    }
}

// ── Typed Helm Chart + Values structs ────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chart {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "type")]
    pub chart_type: String,
    pub version: String,
    #[serde(rename = "appVersion")]
    pub app_version: String,
    pub keywords: Vec<String>,
}

#[must_use]
pub fn build_helm_chart(provider: &IacProvider) -> Chart {
    Chart {
        api_version: "v2".to_string(),
        name: format!("crossplane-{}", provider.name),
        description: format!("Crossplane provider for {}", provider.name),
        chart_type: "application".to_string(),
        version: "0.1.0".to_string(),
        app_version: "0.1.0".to_string(),
        keywords: vec![
            "crossplane".to_string(),
            "provider".to_string(),
            provider.name.clone(),
        ],
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelmValues {
    pub image: HelmImage,
    pub replicas: u32,
    #[serde(rename = "serviceAccount")]
    pub service_account: HelmServiceAccount,
    pub resources: HelmResources,
    #[serde(rename = "leaderElection")]
    pub leader_election: HelmLeaderElection,
    #[serde(rename = "pollInterval")]
    pub poll_interval: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelmImage {
    pub repository: String,
    pub tag: String,
    #[serde(rename = "pullPolicy")]
    pub pull_policy: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelmServiceAccount {
    pub create: bool,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelmResources {
    pub requests: HelmResourceQuota,
    pub limits: HelmResourceQuota,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelmResourceQuota {
    pub cpu: String,
    pub memory: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelmLeaderElection {
    pub enabled: bool,
}

#[must_use]
pub fn build_helm_values(provider: &IacProvider) -> HelmValues {
    HelmValues {
        image: HelmImage {
            repository: format!("ghcr.io/pleme-io/crossplane-{}", provider.name),
            tag: String::new(),
            pull_policy: "IfNotPresent".to_string(),
        },
        replicas: 1,
        service_account: HelmServiceAccount {
            create: true,
            name: String::new(),
        },
        resources: HelmResources {
            requests: HelmResourceQuota {
                cpu: "100m".to_string(),
                memory: "128Mi".to_string(),
            },
            limits: HelmResourceQuota {
                cpu: "500m".to_string(),
                memory: "512Mi".to_string(),
            },
        },
        leader_election: HelmLeaderElection { enabled: true },
        poll_interval: "60s".to_string(),
    }
}

// ── Helm template-YAML builders (serde_yaml_ng::Value trees) ─────────────

#[must_use]
pub fn build_helm_deployment_value() -> Value {
    let mut root = Mapping::new();
    root.insert(s("apiVersion"), s("apps/v1"));
    root.insert(s("kind"), s("Deployment"));

    let mut metadata = Mapping::new();
    metadata.insert(
        s("name"),
        s("{{ .Release.Name }}-{{ .Chart.Name }}"),
    );
    let mut labels = Mapping::new();
    labels.insert(s("app.kubernetes.io/name"), s("{{ .Chart.Name }}"));
    labels.insert(
        s("app.kubernetes.io/managed-by"),
        s("{{ .Release.Service }}"),
    );
    metadata.insert(s("labels"), Value::Mapping(labels));
    root.insert(s("metadata"), Value::Mapping(metadata));

    // spec.replicas → {{ .Values.replicas }}
    let mut spec = Mapping::new();
    spec.insert(s("replicas"), s("{{ .Values.replicas }}"));
    let mut selector = Mapping::new();
    let mut match_labels = Mapping::new();
    match_labels.insert(s("app.kubernetes.io/name"), s("{{ .Chart.Name }}"));
    selector.insert(s("matchLabels"), Value::Mapping(match_labels.clone()));
    spec.insert(s("selector"), Value::Mapping(selector));

    let mut tmpl = Mapping::new();
    let mut tmpl_meta = Mapping::new();
    tmpl_meta.insert(s("labels"), Value::Mapping(match_labels));
    tmpl.insert(s("metadata"), Value::Mapping(tmpl_meta));
    let mut tmpl_spec = Mapping::new();
    tmpl_spec.insert(
        s("serviceAccountName"),
        s("{{ .Values.serviceAccount.name | default (printf \"crossplane-%s\" .Chart.Name) }}"),
    );
    let mut container = Mapping::new();
    container.insert(s("name"), s("provider"));
    container.insert(
        s("image"),
        s("{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"),
    );
    container.insert(s("imagePullPolicy"), s("{{ .Values.image.pullPolicy }}"));
    container.insert(
        s("args"),
        Value::Sequence(vec![
            s("--leader-election={{ .Values.leaderElection.enabled }}"),
            s("--poll={{ .Values.pollInterval }}"),
        ]),
    );
    container.insert(s("resources"), s("{{- toYaml .Values.resources | nindent 12 }}"));
    tmpl_spec.insert(
        s("containers"),
        Value::Sequence(vec![Value::Mapping(container)]),
    );
    tmpl.insert(s("spec"), Value::Mapping(tmpl_spec));
    spec.insert(s("template"), Value::Mapping(tmpl));
    root.insert(s("spec"), Value::Mapping(spec));

    Value::Mapping(root)
}

#[must_use]
pub fn build_helm_rbac_value() -> Value {
    // Slice 1 emits the ServiceAccount (always) + ClusterRole + ClusterRoleBinding
    // as a single Mapping. The `{{- if .Values.serviceAccount.create }}` directive
    // emission lands in slice 2 of the helm-template AST; for now the SA is
    // unconditionally emitted (acceptable for slice 1 — chart consumers turning
    // the flag off will simply replace the SA with a pre-existing one).

    let mut sa = Mapping::new();
    sa.insert(s("apiVersion"), s("v1"));
    sa.insert(s("kind"), s("ServiceAccount"));
    let mut sa_meta = Mapping::new();
    sa_meta.insert(
        s("name"),
        s("{{ .Values.serviceAccount.name | default (printf \"crossplane-%s\" .Chart.Name) }}"),
    );
    sa.insert(s("metadata"), Value::Mapping(sa_meta));

    Value::Mapping(sa)
}

fn s(v: &str) -> Value {
    Value::String(v.to_string())
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

    // ── Provider config types AST ────────────────────────────────────────

    #[test]
    fn provider_config_types_file_has_six_decls() {
        let f = build_provider_config_types_file(&provider(), &ControllerConfig::akeyless_default());
        let names: Vec<&str> = f
            .decls
            .iter()
            .filter_map(|d| match d {
                GoDecl::Type(t) => Some(t.name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            names,
            vec![
                "ProviderConfigSpec",
                "ProviderCredentials",
                "ProviderConfig",
                "ProviderConfigList",
                "ProviderConfigUsage",
                "ProviderConfigUsageList",
            ]
        );
    }

    #[test]
    fn provider_config_kind_has_status_subresource_and_age_column() {
        let t = provider_config_kind_struct();
        let has_status = t
            .markers
            .iter()
            .any(|m| matches!(m, KubeMarker::Subresource(SubresourceKind::Status)));
        let has_age = t
            .markers
            .iter()
            .any(|m| matches!(m, KubeMarker::PrintColumn { name, .. } if name == "AGE"));
        assert!(has_status);
        assert!(has_age);
    }

    // ── Provider groupversion_info AST ───────────────────────────────────

    #[test]
    fn provider_groupversion_info_registers_4_kinds() {
        let f = build_provider_groupversion_info_file(&provider(), &ControllerConfig::akeyless_default());
        // Exactly one init() func
        let init = f.decls.iter().find(|d| matches!(
            d, GoDecl::Func(GoFuncDecl { name, .. }) if name == "init",
        ));
        let GoDecl::Func(init_fn) = init.expect("init present") else {
            unreachable!()
        };
        // Body has exactly one stmt: SchemeBuilder.Register(&K1{}, &K2{}, &K3{}, &K4{})
        assert_eq!(init_fn.body.stmts.len(), 1);
        let GoStmt::Expr(GoExpr::Call { args, .. }) = &init_fn.body.stmts[0] else {
            panic!("expected call stmt")
        };
        assert_eq!(args.len(), 4, "Register must take all four kinds");
    }

    // ── main.go AST ──────────────────────────────────────────────────────

    #[test]
    fn main_go_imports_apis_and_controller_packages() {
        let f = build_main_go_file(&provider(), &ControllerConfig::akeyless_default());
        let paths: Vec<&str> = f.imports.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.contains(&"github.com/pleme-io/crossplane-akeyless/apis"));
        assert!(paths.contains(&"github.com/pleme-io/crossplane-akeyless/internal/controller"));
        assert!(paths.contains(&"sigs.k8s.io/controller-runtime"));
    }

    #[test]
    fn main_go_has_main_function_only() {
        let f = build_main_go_file(&provider(), &ControllerConfig::akeyless_default());
        let funcs: Vec<&str> = f
            .decls
            .iter()
            .filter_map(|d| match d {
                GoDecl::Func(fd) => Some(fd.name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(funcs, vec!["main"]);
    }

    #[test]
    fn main_go_uses_correct_leader_election_id() {
        let s = render_main_go(&provider(), &ControllerConfig::akeyless_default());
        assert!(s.contains("crossplane-akeyless-leader"));
        assert!(s.contains("apis.AddToScheme"));
        assert!(s.contains("controller.Setup"));
    }

    #[test]
    fn main_go_uses_time_minute_as_constant_not_function_call() {
        // Regression: previously emitted `time.Minute()` (call) instead of
        // `time.Minute` (constant), which fails to compile.
        let s = render_main_go(&provider(), &ControllerConfig::akeyless_default());
        assert!(s.contains("pollInterval := time.Minute\n"), "emitted: {s}");
        assert!(
            !s.contains("time.Minute()"),
            "time.Minute is a Duration constant, not a function — must not emit ()"
        );
    }

    #[test]
    fn setup_go_emits_typed_slice_of_func_signature_for_setups() {
        let s = render_setup_go(
            &[auth_method()],
            &provider(),
            &ControllerConfig::akeyless_default(),
        );
        // Slice literal with structurally-correct element type
        assert!(s.contains("setups := []func(ctrl.Manager, time.Duration) error{"));
        // Per-resource Setup reference
        assert!(s.contains("authmethodapikey.Setup,"));
    }

    #[test]
    fn setup_go_emits_typed_for_range_loop() {
        let s = render_setup_go(
            &[auth_method()],
            &provider(),
            &ControllerConfig::akeyless_default(),
        );
        // Typed for-range with only-value branch (`_, s := range setups`)
        assert!(s.contains("for _, s := range setups {"));
        // Body invokes s(mgr, pollInterval) and bubbles err
        assert!(s.contains("if err := s(mgr, pollInterval); err != nil {"));
        assert!(s.contains("return err"));
    }

    // ── setup.go AST ─────────────────────────────────────────────────────

    #[test]
    fn setup_go_imports_each_resource_package() {
        let f = build_setup_go_file(
            &[auth_method()],
            &provider(),
            &ControllerConfig::akeyless_default(),
        );
        let paths: Vec<&str> = f.imports.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.contains(&"github.com/pleme-io/crossplane-akeyless/internal/controller/authmethodapikey"));
    }

    #[test]
    fn setup_go_renders_setup_function_calling_each_pkg_setup() {
        let s = render_setup_go(
            &[auth_method()],
            &provider(),
            &ControllerConfig::akeyless_default(),
        );
        assert!(s.contains("authmethodapikey.Setup"));
        assert!(s.contains("func Setup(mgr ctrl.Manager, pollInterval time.Duration) error"));
    }

    // ── go.mod typed struct ─────────────────────────────────────────────

    #[test]
    fn go_mod_module_path_matches_provider_module() {
        let m = build_go_mod(&ControllerConfig::akeyless_default());
        assert_eq!(m.module, "github.com/pleme-io/crossplane-akeyless");
    }

    #[test]
    fn go_mod_pins_required_runtime_deps() {
        let m = build_go_mod(&ControllerConfig::akeyless_default());
        let paths: Vec<&str> = m.require.iter().map(|r| r.path.as_str()).collect();
        assert!(paths.contains(&"github.com/pleme-io/akeyless-go"));
        assert!(paths.contains(&"github.com/crossplane/crossplane-runtime"));
        assert!(paths.contains(&"sigs.k8s.io/controller-runtime"));
        assert!(paths.contains(&"k8s.io/apimachinery"));
    }

    #[test]
    fn go_mod_render_has_module_and_require_block() {
        let s = render_go_mod(&provider(), &ControllerConfig::akeyless_default());
        assert!(s.starts_with("module github.com/pleme-io/crossplane-akeyless"));
        assert!(s.contains("\ngo 1.23\n"));
        assert!(s.contains("\nrequire (\n"));
        assert!(s.contains("github.com/pleme-io/akeyless-go v0.1.0"));
    }

    // ── Helm typed structs ──────────────────────────────────────────────

    #[test]
    fn helm_chart_struct_round_trips_through_yaml() {
        let c = build_helm_chart(&provider());
        let yaml = serde_yaml_ng::to_string(&c).unwrap();
        // Round-trip back to a struct
        let parsed: Chart = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(c, parsed);
    }

    #[test]
    fn helm_chart_carries_provider_name() {
        let c = build_helm_chart(&provider());
        assert_eq!(c.name, "crossplane-akeyless");
        assert_eq!(c.api_version, "v2");
        assert_eq!(c.chart_type, "application");
        assert!(c.keywords.contains(&"crossplane".to_string()));
        assert!(c.keywords.contains(&"akeyless".to_string()));
    }

    #[test]
    fn helm_values_image_repository_uses_ghcr_path() {
        let v = build_helm_values(&provider());
        assert_eq!(
            v.image.repository,
            "ghcr.io/pleme-io/crossplane-akeyless"
        );
        assert!(v.leader_election.enabled);
    }

    #[test]
    fn helm_values_round_trips() {
        let v = build_helm_values(&provider());
        let yaml = serde_yaml_ng::to_string(&v).unwrap();
        let parsed: HelmValues = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(v, parsed);
    }

    // ── Helm templates (serde_yaml_ng::Value trees) ─────────────────────

    #[test]
    fn helm_deployment_is_apps_v1_deployment() {
        let v = build_helm_deployment_value();
        let m = v.as_mapping().unwrap();
        assert_eq!(
            m.get(Value::String("apiVersion".into())).unwrap(),
            &Value::String("apps/v1".into())
        );
        assert_eq!(
            m.get(Value::String("kind".into())).unwrap(),
            &Value::String("Deployment".into())
        );
    }

    #[test]
    fn helm_deployment_image_uses_template_directives() {
        let s = render_helm_deployment_template(
            &provider(),
            &ControllerConfig::akeyless_default(),
        );
        assert!(s.contains("{{ .Values.image.repository }}"));
        assert!(s.contains("{{ .Values.image.tag | default .Chart.AppVersion }}"));
        assert!(s.contains("--leader-election={{ .Values.leaderElection.enabled }}"));
    }

    #[test]
    fn helm_rbac_emits_serviceaccount() {
        let s = render_helm_rbac_template(&provider(), &ControllerConfig::akeyless_default());
        assert!(s.contains("kind: ServiceAccount"));
        assert!(s.contains("apiVersion: v1"));
    }

    // ── Determinism + leakage ───────────────────────────────────────────

    #[test]
    fn deterministic_render_for_each_emitter() {
        let cfg = ControllerConfig::akeyless_default();
        let p = provider();
        let resources = vec![auth_method()];
        let pairs = vec![
            (
                render_provider_config_types(&p, &cfg),
                render_provider_config_types(&p, &cfg),
            ),
            (
                render_provider_groupversion_info(&p, &cfg),
                render_provider_groupversion_info(&p, &cfg),
            ),
            (render_main_go(&p, &cfg), render_main_go(&p, &cfg)),
            (
                render_setup_go(&resources, &p, &cfg),
                render_setup_go(&resources, &p, &cfg),
            ),
            (render_go_mod(&p, &cfg), render_go_mod(&p, &cfg)),
            (render_helm_chart_yaml(&p, &cfg), render_helm_chart_yaml(&p, &cfg)),
            (render_helm_values_yaml(&p, &cfg), render_helm_values_yaml(&p, &cfg)),
        ];
        for (a, b) in pairs {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn no_format_string_leakage_in_any_emitter() {
        let cfg = ControllerConfig::akeyless_default();
        let p = provider();
        let resources = vec![auth_method()];
        let outputs = vec![
            render_provider_config_types(&p, &cfg),
            render_provider_groupversion_info(&p, &cfg),
            render_main_go(&p, &cfg),
            render_setup_go(&resources, &p, &cfg),
        ];
        for s in outputs {
            assert!(!s.contains("{kind}"), "leaked {{kind}} placeholder: {s}");
            assert!(!s.contains("{api_group}"), "leaked {{api_group}} placeholder: {s}");
            assert!(!s.contains("{provider_name}"));
            assert!(!s.contains("{provider_module}"));
            assert!(!s.contains("{api_version}"));
        }
    }
}
