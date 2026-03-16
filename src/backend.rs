use iac_forge::backend::{ArtifactKind, Backend, GeneratedArtifact, NamingConvention};
use iac_forge::error::IacForgeError;
use iac_forge::ir::{IacDataSource, IacProvider, IacResource};
use iac_forge::naming::{strip_provider_prefix, to_kebab_case, to_pascal_case, to_snake_case};

use crate::crd;

/// Crossplane backend — generates Kubernetes CRD YAML from IaC forge IR.
pub struct CrossplaneBackend;

/// Naming convention for Crossplane CRD resources.
struct CrossplaneNaming;

impl NamingConvention for CrossplaneNaming {
    fn resource_type_name(&self, resource_name: &str, provider_name: &str) -> String {
        to_pascal_case(strip_provider_prefix(resource_name, provider_name))
    }

    fn file_name(&self, resource_name: &str, kind: &ArtifactKind) -> String {
        let base = to_kebab_case(&to_snake_case(resource_name));
        match kind {
            ArtifactKind::Resource => format!("{base}-crd.yaml"),
            ArtifactKind::Provider => format!("{base}-providerconfig-crd.yaml"),
            _ => format!("{base}.yaml"),
        }
    }

    fn field_name(&self, api_name: &str) -> String {
        to_snake_case(api_name)
    }
}

impl Backend for CrossplaneBackend {
    fn platform(&self) -> &str {
        "crossplane"
    }

    fn generate_resource(
        &self,
        resource: &IacResource,
        provider: &IacProvider,
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        let group = crd::derive_group(&provider.name, &provider.platform_config);
        let api_version = crd::derive_api_version(&provider.platform_config);

        let yaml = crd::generate_resource_crd_with_config(
            resource,
            &provider.name,
            &group,
            &api_version,
            &provider.platform_config,
        )
        .map_err(|e| IacForgeError::BackendError(format!("YAML serialization error: {e}")))?;

        let path = self.naming().file_name(&resource.name, &ArtifactKind::Resource);

        Ok(vec![GeneratedArtifact {
            path,
            content: yaml,
            kind: ArtifactKind::Resource,
        }])
    }

    fn generate_data_source(
        &self,
        _ds: &IacDataSource,
        _provider: &IacProvider,
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        // Crossplane does not have a data source concept — no-op.
        Ok(vec![])
    }

    fn generate_provider(
        &self,
        provider: &IacProvider,
        _resources: &[IacResource],
        _data_sources: &[IacDataSource],
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        let group = crd::derive_group(&provider.name, &provider.platform_config);
        let api_version = crd::derive_api_version(&provider.platform_config);

        let yaml =
            crd::generate_provider_config_crd(&provider.name, &group, &api_version).map_err(
                |e| IacForgeError::BackendError(format!("YAML serialization error: {e}")),
            )?;

        let path = self
            .naming()
            .file_name(&provider.name, &ArtifactKind::Provider);

        Ok(vec![GeneratedArtifact {
            path,
            content: yaml,
            kind: ArtifactKind::Provider,
        }])
    }

    fn generate_test(
        &self,
        _resource: &IacResource,
        _provider: &IacProvider,
    ) -> Result<Vec<GeneratedArtifact>, IacForgeError> {
        // Crossplane CRDs don't have test artifacts — no-op.
        Ok(vec![])
    }

    fn naming(&self) -> &dyn NamingConvention {
        &CrossplaneNaming
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::ir::{AuthInfo, CrudInfo, IacAttribute, IacType, IdentityInfo};
    use std::collections::HashMap;

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
            platform_config: HashMap::new(),
        }
    }

    fn make_test_resource() -> IacResource {
        IacResource {
            name: "akeyless_static_secret".to_string(),
            description: "A static secret".to_string(),
            category: "secret".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create-secret".to_string(),
                create_schema: "CreateSecret".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/get-secret-value".to_string(),
                read_schema: "GetSecretValue".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete-item".to_string(),
                delete_schema: "DeleteItem".to_string(),
            },
            attributes: vec![
                IacAttribute {
                    api_name: "name".to_string(),
                    canonical_name: "name".to_string(),
                    description: "Secret name".to_string(),
                    iac_type: IacType::String,
                    required: true,
                    computed: false,
                    sensitive: false,
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
                    computed: false,
                    sensitive: true,
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
        let backend = CrossplaneBackend;
        assert_eq!(backend.platform(), "crossplane");
    }

    #[test]
    fn generate_resource_produces_artifact() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resource = make_test_resource();

        let artifacts = backend
            .generate_resource(&resource, &provider)
            .expect("generate");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].kind, ArtifactKind::Resource);
        assert!(artifacts[0].path.ends_with("-crd.yaml"));
        assert!(artifacts[0].content.contains("apiextensions.k8s.io/v1"));
    }

    #[test]
    fn generate_data_source_is_noop() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let ds = IacDataSource {
            name: "test_ds".to_string(),
            description: "test".to_string(),
            read_endpoint: "/read".to_string(),
            read_schema: "Read".to_string(),
            read_response_schema: None,
            attributes: vec![],
        };

        let artifacts = backend
            .generate_data_source(&ds, &provider)
            .expect("generate");
        assert!(artifacts.is_empty());
    }

    #[test]
    fn generate_provider_produces_providerconfig() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();

        let artifacts = backend
            .generate_provider(&provider, &[], &[])
            .expect("generate");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].kind, ArtifactKind::Provider);
        assert!(artifacts[0].content.contains("ProviderConfig"));
    }

    #[test]
    fn generate_test_is_noop() {
        let backend = CrossplaneBackend;
        let provider = make_test_provider();
        let resource = make_test_resource();

        let artifacts = backend
            .generate_test(&resource, &provider)
            .expect("generate");
        assert!(artifacts.is_empty());
    }

    #[test]
    fn naming_convention() {
        let naming = CrossplaneNaming;
        assert_eq!(
            naming.resource_type_name("akeyless_static_secret", "akeyless"),
            "StaticSecret"
        );
        assert_eq!(
            naming.file_name("akeyless_static_secret", &ArtifactKind::Resource),
            "akeyless-static-secret-crd.yaml"
        );
        assert_eq!(
            naming.file_name("akeyless", &ArtifactKind::Provider),
            "akeyless-providerconfig-crd.yaml"
        );
        assert_eq!(naming.field_name("bound-aws-account-id"), "bound_aws_account_id");
    }

    #[test]
    fn group_from_platform_config() {
        let mut provider = make_test_provider();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "group".to_string(),
            toml::Value::String("vault.akeyless.io".to_string()),
        );
        provider.platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );

        let backend = CrossplaneBackend;
        let resource = make_test_resource();

        let artifacts = backend
            .generate_resource(&resource, &provider)
            .expect("generate");
        assert!(artifacts[0].content.contains("vault.akeyless.io"));
    }
}
