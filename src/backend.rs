use std::collections::BTreeMap;
use std::fmt;

use iac_forge::backend::{ArtifactKind, Backend, GeneratedArtifact, NamingConvention};
use iac_forge::error::IacForgeError;
use iac_forge::ir::{IacDataSource, IacProvider, IacResource};
use iac_forge::naming::{strip_provider_prefix, to_kebab_case, to_pascal_case, to_snake_case};

use crate::controller_gen::{self, ControllerConfig, package_name};
use crate::{crd, deepcopy_gen, managed_methods_gen, provider_gen, types_gen};

// ── Backend ──────────────────────────────────────────────────────────────

/// Crossplane backend — emits a complete provider tree from `iac-forge` IR:
///
///   apis/<resource_pkg>/v1alpha1/<resource>_types.go
///   apis/<resource_pkg>/v1alpha1/groupversion_info.go
///   apis/<provider>/v1alpha1/providerconfig_types.go
///   apis/<provider>/v1alpha1/groupversion_info.go
///   internal/controller/<resource_pkg>/controller.go
///   internal/controller/setup.go
///   cmd/provider/main.go
///   go.mod
///   helm/Chart.yaml
///   helm/values.yaml
///   helm/templates/deployment.yaml
///   helm/templates/rbac.yaml
///   package/crds/<resource>-crd.yaml          ← legacy CRD YAML
///   package/crds/providerconfig-crd.yaml      ← legacy CRD YAML
///
/// All Go output is built through `iac_forge::goast`; all Helm chart
/// metadata + values are built through typed serde structs; templated
/// YAML is composed via `serde_yaml_ng::Value` trees. No `format!()` of
/// emitted syntax anywhere — see the post-2026-05-06 substrate-hygiene
/// reset notes in [`crate::types_gen`], [`crate::controller_gen`],
/// [`crate::provider_gen`].
#[derive(Debug, Clone, Copy, Default)]
pub struct CrossplaneBackend;

impl fmt::Display for CrossplaneBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("crossplane")
    }
}

/// Naming convention. Only used for the legacy CRD YAML paths today;
/// every Go file path is computed by the relevant `*_gen.rs` module
/// because the layout (apis/<pkg>/v1alpha1/, internal/controller/<pkg>/,
/// cmd/provider/, etc.) is structured beyond what a single `file_name`
/// hook can encode.
#[derive(Debug, Clone, Copy)]
struct CrossplaneNaming;

impl NamingConvention for CrossplaneNaming {
    fn resource_type_name(&self, resource_name: &str, provider_name: &str) -> String {
        to_pascal_case(strip_provider_prefix(resource_name, provider_name))
    }

    fn file_name(&self, resource_name: &str, kind: &ArtifactKind) -> String {
        let base = to_kebab_case(&to_snake_case(resource_name));
        match kind {
            ArtifactKind::Resource => format!("package/crds/{base}-crd.yaml"),
            ArtifactKind::Provider => "package/crds/providerconfig-crd.yaml".to_string(),
            _ => format!("{base}.yaml"),
        }
    }

    fn field_name(&self, api_name: &str) -> String {
        to_snake_case(api_name)
    }
}

impl Backend for CrossplaneBackend {
    #[allow(clippy::unnecessary_literal_bound)]
    fn platform(&self) -> &str {
        "crossplane"
    }

    fn generate_resource(
        &self,
        resource: &IacResource,
        provider: &IacProvider,
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        let cfg = controller_config_for(provider);
        let pkg = package_name(resource, provider);
        let stem = to_snake_case(&strip_provider_prefix(&resource.name, &provider.name));

        // Legacy CRD YAML — kept until controller-gen-generated CRDs replace it.
        let group = crd::derive_group(&provider.name, &provider.platform_config);
        let api_version = crd::derive_api_version(&provider.platform_config);
        let crd_yaml = crd::generate_resource_crd_with_config(
            resource,
            &provider.name,
            &group,
            &api_version,
            &provider.platform_config,
        )?;
        let crd_path = self.naming().file_name(&resource.name, &ArtifactKind::Resource);

        // Per-resource Go types
        let types_go = types_gen::render_resource_types(resource, provider);
        let types_path = format!("apis/{pkg}/v1alpha1/{stem}_types.go");

        // Per-resource groupversion_info
        let gvi_go = types_gen::render_groupversion_info(
            resource,
            provider,
            &resource_api_group(&pkg, &cfg),
            &cfg.api_version,
        );
        let gvi_path = format!("apis/{pkg}/v1alpha1/groupversion_info.go");

        // Per-resource controller
        let controller_go = controller_gen::render_controller(resource, provider, &cfg);
        let controller_path = format!("internal/controller/{pkg}/controller.go");

        // zz_generated_deepcopy.go for the resource's package
        let deepcopy_go = deepcopy_gen::render_resource_deepcopy(resource, provider);
        let deepcopy_path = format!("apis/{pkg}/v1alpha1/zz_generated_deepcopy.go");

        // zz_generated_managed.go — managed.Resource interface accessor methods
        let managed_go = managed_methods_gen::render_resource_managed_methods(resource, provider);
        let managed_path = format!("apis/{pkg}/v1alpha1/zz_generated_managed.go");

        Ok(vec![
            GeneratedArtifact::new(crd_path, crd_yaml, ArtifactKind::Resource),
            GeneratedArtifact::new(types_path, types_go, ArtifactKind::Resource),
            GeneratedArtifact::new(gvi_path, gvi_go, ArtifactKind::Module),
            GeneratedArtifact::new(deepcopy_path, deepcopy_go, ArtifactKind::Module),
            GeneratedArtifact::new(managed_path, managed_go, ArtifactKind::Module),
            GeneratedArtifact::new(controller_path, controller_go, ArtifactKind::Controller),
        ])
    }

    fn generate_data_source(
        &self,
        _ds: &IacDataSource,
        _provider: &IacProvider,
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        // Crossplane has no data-source concept.
        Ok(vec![])
    }

    fn generate_provider(
        &self,
        provider: &IacProvider,
        resources: &[IacResource],
        _data_sources: &[IacDataSource],
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        let cfg = controller_config_for(provider);
        let provider_pkg = provider.name.replace('-', "");

        // Legacy ProviderConfig CRD YAML
        let group = crd::derive_group(&provider.name, &provider.platform_config);
        let api_version = crd::derive_api_version(&provider.platform_config);
        let pc_crd_yaml =
            crd::generate_provider_config_crd(&provider.name, &group, &api_version)?;
        let pc_crd_path = self
            .naming()
            .file_name(&provider.name, &ArtifactKind::Provider);

        // ProviderConfig Go types + groupversion_info
        let pc_types_go = provider_gen::render_provider_config_types(provider, &cfg);
        let pc_types_path =
            format!("apis/{provider_pkg}/v1alpha1/providerconfig_types.go");

        let pc_gvi_go = provider_gen::render_provider_groupversion_info(provider, &cfg);
        let pc_gvi_path = format!("apis/{provider_pkg}/v1alpha1/groupversion_info.go");

        let pc_deepcopy_go = deepcopy_gen::render_provider_deepcopy();
        let pc_deepcopy_path =
            format!("apis/{provider_pkg}/v1alpha1/zz_generated_deepcopy.go");

        // apis/apis.go — aggregates every per-resource SchemeBuilder
        // behind a single AddToScheme that cmd/provider/main.go calls.
        let apis_aggregator_go = provider_gen::render_apis_aggregator(resources, provider, &cfg);
        let apis_aggregator_path = "apis/apis.go".to_string();

        // main.go + setup.go
        let main_go = provider_gen::render_main_go(provider, &cfg);
        let main_path = "cmd/provider/main.go".to_string();

        let setup_go = provider_gen::render_setup_go(resources, provider, &cfg);
        let setup_path = "internal/controller/setup.go".to_string();

        // go.mod
        let go_mod = provider_gen::render_go_mod(provider, &cfg);

        // Helm chart
        let chart_yaml = provider_gen::render_helm_chart_yaml(provider, &cfg);
        let values_yaml = provider_gen::render_helm_values_yaml(provider, &cfg);
        let deployment_yaml = provider_gen::render_helm_deployment_template(provider, &cfg);
        let rbac_yaml = provider_gen::render_helm_rbac_template(provider, &cfg);

        Ok(vec![
            GeneratedArtifact::new(pc_crd_path, pc_crd_yaml, ArtifactKind::Provider),
            GeneratedArtifact::new(
                pc_types_path,
                pc_types_go,
                ArtifactKind::ProviderConfig,
            ),
            GeneratedArtifact::new(pc_gvi_path, pc_gvi_go, ArtifactKind::Module),
            GeneratedArtifact::new(pc_deepcopy_path, pc_deepcopy_go, ArtifactKind::Module),
            GeneratedArtifact::new(
                apis_aggregator_path,
                apis_aggregator_go,
                ArtifactKind::Module,
            ),
            GeneratedArtifact::new(main_path, main_go, ArtifactKind::Provider),
            GeneratedArtifact::new(setup_path, setup_go, ArtifactKind::Module),
            GeneratedArtifact::new("go.mod".to_string(), go_mod, ArtifactKind::Metadata),
            GeneratedArtifact::new(
                "helm/Chart.yaml".to_string(),
                chart_yaml,
                ArtifactKind::HelmChart,
            ),
            GeneratedArtifact::new(
                "helm/values.yaml".to_string(),
                values_yaml,
                ArtifactKind::HelmChart,
            ),
            GeneratedArtifact::new(
                "helm/templates/deployment.yaml".to_string(),
                deployment_yaml,
                ArtifactKind::HelmChart,
            ),
            GeneratedArtifact::new(
                "helm/templates/rbac.yaml".to_string(),
                rbac_yaml,
                ArtifactKind::HelmChart,
            ),
        ])
    }

    fn generate_test(
        &self,
        _resource: &IacResource,
        _provider: &IacProvider,
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        // Crossplane CRDs don't have test artifacts emitted by this backend.
        // The reconcile-loop test surface is exercised by integration tests
        // (M6 cluster smoke) rather than per-resource unit tests today.
        Ok(vec![])
    }

    fn naming(&self) -> &dyn NamingConvention {
        &CrossplaneNaming
    }
}

// ── ControllerConfig derivation ──────────────────────────────────────────

/// Build a [`ControllerConfig`] for the given provider, reading optional
/// overrides from the provider's `platform_config[crossplane]` table.
///
/// Defaults:
///   sdk_module      = `github.com/pleme-io/<provider>-go`
///   provider_module = `github.com/pleme-io/crossplane-<provider>`
///   api_group       = `<provider>.crossplane.io`  (or platform_config override)
///   api_version     = `v1alpha1`                  (or platform_config override)
///
/// The `<provider>-go` SDK convention matches the akeyless-go autogen
/// shipped 2026-05-06.
#[must_use]
pub fn controller_config_for(provider: &IacProvider) -> ControllerConfig {
    let group = crossplane_str(&provider.platform_config, "group")
        .unwrap_or_else(|| format!("{}.crossplane.io", provider.name.replace('_', "-")));
    let api_version = crossplane_str(&provider.platform_config, "api_version")
        .unwrap_or_else(|| "v1alpha1".to_string());
    let sdk_module = crossplane_str(&provider.platform_config, "sdk_module")
        .unwrap_or_else(|| format!("github.com/pleme-io/{}-go", provider.name));
    let provider_module = crossplane_str(&provider.platform_config, "provider_module")
        .unwrap_or_else(|| format!("github.com/pleme-io/crossplane-{}", provider.name));

    ControllerConfig {
        sdk_module,
        provider_module,
        api_group: group,
        api_version,
    }
}

fn crossplane_str(
    platform_config: &BTreeMap<String, toml::Value>,
    key: &str,
) -> Option<String> {
    platform_config
        .get("crossplane")
        .and_then(|v| v.as_table())
        .and_then(|t| t.get(key))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Per-resource API group: defaults to the provider-level group.
/// Future slice can split per-category (e.g. `authmethod.akeyless.crossplane.io`).
fn resource_api_group(_resource_pkg: &str, cfg: &ControllerConfig) -> String {
    cfg.api_group.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::ir::{AuthInfo, CrudInfo, IacAttribute, IacType, IdentityInfo};
    use std::collections::BTreeMap;

    fn make_test_provider() -> IacProvider {
        IacProvider {
            name: "akeyless".to_string(),
            description: "Akeyless Vault Provider".to_string(),
            version: "1.0.0".to_string(),
            auth: AuthInfo {
                token_field: "token".to_string(),
                env_var: "AKEYLESS_ACCESS_TOKEN".to_string(),
                gateway_url_field: "api_gateway_address".to_string(),
                gateway_env_var: "AKEYLESS_GATEWAY".to_string(),
            },
            skip_fields: vec!["token".to_string()],
            platform_config: BTreeMap::new(),
        }
    }

    fn make_test_resource() -> IacResource {
        IacResource {
            name: "akeyless_static_secret".to_string(),
            description: "A static secret".to_string(),
            category: "secret".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create-secret".to_string(),
                create_schema: "createSecret".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/get-secret-value".to_string(),
                read_schema: "getSecretValue".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete-item".to_string(),
                delete_schema: "deleteItem".to_string(),
            },
            attributes: vec![
                IacAttribute {
                    api_name: "name".to_string(),
                    canonical_name: "name".to_string(),
                    description: "Secret name".to_string(),
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
                    api_name: "value".to_string(),
                    canonical_name: "value".to_string(),
                    description: "Secret value".to_string(),
                    iac_type: IacType::String,
                    required: true,
                    optional: false,
                    computed: false,
                    sensitive: true,
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

    #[test]
    fn platform_name() {
        assert_eq!(CrossplaneBackend.platform(), "crossplane");
    }

    #[test]
    fn generate_resource_emits_full_artifact_set() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resource = make_test_resource();
        let arts = backend
            .generate_resource(&resource, &provider)
            .expect("generate_resource");
        // 6 artifacts per resource: legacy CRD + types.go + groupversion +
        // deepcopy + managed-methods + controller
        assert_eq!(arts.len(), 6, "expected 6 artifacts per resource");
        let paths: Vec<&str> = arts.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.iter().any(|p| p.ends_with("-crd.yaml")));
        assert!(paths
            .iter()
            .any(|p| p.starts_with("apis/") && p.ends_with("_types.go")));
        assert!(paths
            .iter()
            .any(|p| p.starts_with("apis/") && p.ends_with("/groupversion_info.go")));
        assert!(paths
            .iter()
            .any(|p| p.starts_with("internal/controller/") && p.ends_with("/controller.go")));
    }

    #[test]
    fn generate_resource_paths_use_resource_package_name() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resource = make_test_resource(); // akeyless_static_secret → "staticsecret"
        let arts = backend.generate_resource(&resource, &provider).unwrap();
        let types = arts.iter().find(|a| a.path.ends_with("_types.go")).unwrap();
        assert!(types.path.starts_with("apis/staticsecret/v1alpha1/"));
        let ctrl = arts
            .iter()
            .find(|a| a.path.ends_with("/controller.go"))
            .unwrap();
        assert!(ctrl.path.starts_with("internal/controller/staticsecret/"));
    }

    #[test]
    fn generate_resource_artifact_kinds_set_correctly() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resource = make_test_resource();
        let arts = backend.generate_resource(&resource, &provider).unwrap();
        // Find each artifact by path pattern and verify its kind
        for a in &arts {
            let expected = if a.path.ends_with("-crd.yaml") || a.path.ends_with("_types.go") {
                ArtifactKind::Resource
            } else if a.path.ends_with("/groupversion_info.go")
                || a.path.ends_with("/zz_generated_deepcopy.go")
                || a.path.ends_with("/zz_generated_managed.go")
            {
                ArtifactKind::Module
            } else if a.path.ends_with("/controller.go") {
                ArtifactKind::Controller
            } else {
                panic!("unexpected artifact path: {}", a.path);
            };
            assert_eq!(a.kind, expected, "wrong kind for {}", a.path);
        }
    }

    #[test]
    fn generate_provider_emits_full_scaffold() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resources = vec![make_test_resource()];
        let arts = backend
            .generate_provider(&provider, &resources, &[])
            .expect("generate_provider");
        let paths: std::collections::BTreeSet<&str> =
            arts.iter().map(|a| a.path.as_str()).collect();
        // Required scaffold artifacts
        for p in [
            "package/crds/providerconfig-crd.yaml",
            "apis/akeyless/v1alpha1/providerconfig_types.go",
            "apis/akeyless/v1alpha1/groupversion_info.go",
            "apis/akeyless/v1alpha1/zz_generated_deepcopy.go",
            "apis/apis.go",
            "cmd/provider/main.go",
            "internal/controller/setup.go",
            "go.mod",
            "helm/Chart.yaml",
            "helm/values.yaml",
            "helm/templates/deployment.yaml",
            "helm/templates/rbac.yaml",
        ] {
            assert!(paths.contains(p), "missing scaffold artifact: {p}");
        }
    }

    #[test]
    fn apis_aggregator_imports_provider_pkg_and_each_resource_pkg() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resources = vec![make_test_resource()]; // staticsecret
        let arts = backend
            .generate_provider(&provider, &resources, &[])
            .unwrap();
        let agg = arts
            .iter()
            .find(|a| a.path == "apis/apis.go")
            .expect("apis/apis.go present");
        // Provider package import (akeylessv1alpha1)
        assert!(agg
            .content
            .contains("akeylessv1alpha1 \"github.com/pleme-io/crossplane-akeyless/apis/akeyless/v1alpha1\""));
        // Per-resource package import
        assert!(agg
            .content
            .contains("staticsecret \"github.com/pleme-io/crossplane-akeyless/apis/staticsecret/v1alpha1\""));
        // AddToScheme aggregator
        assert!(agg.content.contains("func AddToScheme(s *runtime.Scheme) error {"));
        // Builders slice references each pkg's AddToScheme
        assert!(agg.content.contains("akeylessv1alpha1.AddToScheme,"));
        assert!(agg.content.contains("staticsecret.AddToScheme,"));
    }

    #[test]
    fn generate_provider_artifact_kind_distribution() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resources = vec![make_test_resource()];
        let arts = backend
            .generate_provider(&provider, &resources, &[])
            .unwrap();
        // Count by kind
        let mut by_kind: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
        for a in &arts {
            *by_kind.entry(a.kind.to_string()).or_insert(0) += 1;
        }
        // Provider CRD + main.go = 2 Provider artifacts
        assert_eq!(by_kind.get("provider").copied().unwrap_or(0), 2);
        // ProviderConfig types = 1
        assert_eq!(by_kind.get("provider_config").copied().unwrap_or(0), 1);
        // groupversion_info + zz_deepcopy + setup.go + apis.go = 4 Module artifacts
        assert_eq!(by_kind.get("module").copied().unwrap_or(0), 4);
        // go.mod = 1 Metadata
        assert_eq!(by_kind.get("metadata").copied().unwrap_or(0), 1);
        // helm/* = 4 HelmChart
        assert_eq!(by_kind.get("helm_chart").copied().unwrap_or(0), 4);
    }

    #[test]
    fn data_source_is_no_op() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let ds = IacDataSource {
            name: "test_ds".to_string(),
            description: "A data source".to_string(),
            read_endpoint: "/read".to_string(),
            read_schema: "read".to_string(),
            read_response_schema: None,
            attributes: vec![],
        };
        let arts = backend.generate_data_source(&ds, &provider).unwrap();
        assert!(arts.is_empty());
    }

    #[test]
    fn controller_config_defaults_from_provider_name() {
        let cfg = controller_config_for(&make_test_provider());
        assert_eq!(cfg.sdk_module, "github.com/pleme-io/akeyless-go");
        assert_eq!(cfg.provider_module, "github.com/pleme-io/crossplane-akeyless");
        assert_eq!(cfg.api_group, "akeyless.crossplane.io");
        assert_eq!(cfg.api_version, "v1alpha1");
    }

    #[test]
    fn controller_config_overrides_from_platform_config() {
        let mut p = make_test_provider();
        let mut crossplane = toml::map::Map::new();
        crossplane.insert("group".into(), toml::Value::String("custom.example.io".into()));
        crossplane.insert(
            "sdk_module".into(),
            toml::Value::String("github.com/example/custom-go".into()),
        );
        p.platform_config
            .insert("crossplane".into(), toml::Value::Table(crossplane));

        let cfg = controller_config_for(&p);
        assert_eq!(cfg.api_group, "custom.example.io");
        assert_eq!(cfg.sdk_module, "github.com/example/custom-go");
        // Unset values still default
        assert_eq!(cfg.provider_module, "github.com/pleme-io/crossplane-akeyless");
        assert_eq!(cfg.api_version, "v1alpha1");
    }

    #[test]
    fn legacy_naming_resource_type_pascal_cases() {
        let n = CrossplaneNaming;
        assert_eq!(n.resource_type_name("akeyless_static_secret", "akeyless"), "StaticSecret");
    }

    #[test]
    fn legacy_naming_file_name_routes_by_kind() {
        let n = CrossplaneNaming;
        assert_eq!(
            n.file_name("akeyless_static_secret", &ArtifactKind::Resource),
            "package/crds/akeyless-static-secret-crd.yaml"
        );
        assert_eq!(
            n.file_name("akeyless", &ArtifactKind::Provider),
            "package/crds/providerconfig-crd.yaml"
        );
    }
}
