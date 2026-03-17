use std::collections::BTreeMap;

use iac_forge::ir::{IacAttribute, IacResource, IacType};
use serde_json::{Map, Value, json};

/// Convert an `IacType` to an OpenAPI v3 JSON schema fragment.
#[must_use]
pub fn iac_type_to_schema(iac_type: &IacType) -> Value {
    match iac_type {
        IacType::String => json!({ "type": "string" }),
        IacType::Integer => json!({ "type": "integer", "format": "int64" }),
        IacType::Float => json!({ "type": "number", "format": "double" }),
        IacType::Boolean => json!({ "type": "boolean" }),
        IacType::List(inner) => {
            json!({
                "type": "array",
                "items": iac_type_to_schema(inner)
            })
        }
        IacType::Set(inner) => {
            let mut schema = json!({
                "type": "array",
                "items": iac_type_to_schema(inner)
            });
            schema["uniqueItems"] = Value::Bool(true);
            schema
        }
        IacType::Map(inner) => {
            json!({
                "type": "object",
                "additionalProperties": iac_type_to_schema(inner)
            })
        }
        IacType::Object { fields, .. } => {
            let mut properties = Map::new();
            let mut required = Vec::new();
            for field in fields {
                let mut schema = iac_type_to_schema(&field.iac_type);
                if !field.description.is_empty() {
                    schema["description"] = Value::String(field.description.clone());
                }
                properties.insert(field.canonical_name.clone(), schema);
                if field.required {
                    required.push(Value::String(field.canonical_name.clone()));
                }
            }
            let mut obj = json!({
                "type": "object",
                "properties": properties
            });
            if !required.is_empty() {
                obj["required"] = Value::Array(required);
            }
            obj
        }
        IacType::Enum {
            values, underlying, ..
        } => {
            let mut schema = iac_type_to_schema(underlying);
            let enum_vals: Vec<Value> = values.iter().map(|v| Value::String(v.clone())).collect();
            schema["enum"] = Value::Array(enum_vals);
            schema
        }
        IacType::Any => json!({
            "x-kubernetes-preserve-unknown-fields": true
        }),
    }
}

/// Build the `forProvider` schema properties from resource attributes.
///
/// Only includes non-computed (mutable) fields.
fn build_for_provider(attributes: &[IacAttribute]) -> (Map<String, Value>, Vec<Value>) {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for attr in attributes {
        if attr.computed {
            continue;
        }

        let mut schema = iac_type_to_schema(&attr.iac_type);
        let mut desc = attr.description.clone();
        if attr.immutable {
            if desc.is_empty() {
                desc = "(immutable)".to_string();
            } else {
                desc = format!("{desc} (immutable)");
            }
        }
        if !desc.is_empty() {
            schema["description"] = Value::String(desc);
        }
        if attr.sensitive {
            let existing_desc = schema
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let sensitive_desc = if existing_desc.is_empty() {
                "[sensitive]".to_string()
            } else {
                format!("{existing_desc} [sensitive]")
            };
            schema["description"] = Value::String(sensitive_desc);
        }

        properties.insert(attr.canonical_name.clone(), schema);

        if attr.required {
            required.push(Value::String(attr.canonical_name.clone()));
        }
    }

    (properties, required)
}

/// Build the `atProvider` schema properties from resource attributes.
///
/// Includes all fields (both mutable and computed) for status observation.
fn build_at_provider(attributes: &[IacAttribute]) -> Map<String, Value> {
    let mut properties = Map::new();

    for attr in attributes {
        let mut schema = iac_type_to_schema(&attr.iac_type);
        if !attr.description.is_empty() {
            schema["description"] = Value::String(attr.description.clone());
        }

        properties.insert(attr.canonical_name.clone(), schema);
    }

    properties
}

/// Derive the CRD group from provider platform config or provider name.
///
/// Checks `platform_config["crossplane"]` for a `group` key, falling back
/// to `{provider_name}.crossplane.io`.
#[must_use]
pub fn derive_group(
    provider_name: &str,
    platform_config: &std::collections::HashMap<String, toml::Value>,
) -> String {
    if let Some(crossplane) = platform_config.get("crossplane") {
        if let Some(table) = crossplane.as_table() {
            if let Some(group) = table.get("group") {
                if let Some(s) = group.as_str() {
                    return s.to_string();
                }
            }
        }
    }
    format!("{provider_name}.crossplane.io")
}

/// Derive the CRD API version from provider platform config.
///
/// Checks `platform_config["crossplane"]` for an `api_version` key,
/// falling back to `v1alpha1`.
#[must_use]
pub fn derive_api_version(
    platform_config: &std::collections::HashMap<String, toml::Value>,
) -> String {
    if let Some(crossplane) = platform_config.get("crossplane") {
        if let Some(table) = crossplane.as_table() {
            if let Some(version) = table.get("api_version") {
                if let Some(s) = version.as_str() {
                    return s.to_string();
                }
            }
        }
    }
    "v1alpha1".to_string()
}

/// Generate a full CRD YAML document for a resource.
///
/// Produces a Kubernetes `CustomResourceDefinition` with:
/// - `spec.forProvider`: mutable (non-computed) fields
/// - `status.atProvider`: all fields for observation
///
/// # Errors
///
/// Returns an error if YAML serialization fails.
pub fn generate_resource_crd(
    resource: &IacResource,
    provider_name: &str,
    group: &str,
    api_version: &str,
) -> Result<String, serde_yaml_ng::Error> {
    generate_resource_crd_with_config(
        resource,
        provider_name,
        group,
        api_version,
        &std::collections::HashMap::new(),
    )
}

/// Generate a full CRD YAML document for a resource with platform config.
///
/// This variant accepts platform config for scope overrides.
///
/// # Errors
///
/// Returns an error if YAML serialization fails.
pub fn generate_resource_crd_with_config(
    resource: &IacResource,
    provider_name: &str,
    group: &str,
    api_version: &str,
    platform_config: &std::collections::HashMap<String, toml::Value>,
) -> Result<String, serde_yaml_ng::Error> {
    let kind = iac_forge::to_pascal_case(iac_forge::strip_provider_prefix(
        &resource.name,
        provider_name,
    ));
    let singular = kind.to_lowercase();
    let plural = format!("{singular}s");

    let (for_provider_props, for_provider_required) = build_for_provider(&resource.attributes);
    let at_provider_props = build_at_provider(&resource.attributes);

    let mut for_provider_schema = json!({
        "type": "object",
        "properties": for_provider_props
    });
    if !for_provider_required.is_empty() {
        for_provider_schema["required"] = Value::Array(for_provider_required);
    }

    let at_provider_schema = json!({
        "type": "object",
        "properties": at_provider_props
    });

    let conditions_schema = json!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "type": { "type": "string" },
                "status": { "type": "string" },
                "lastTransitionTime": { "type": "string", "format": "date-time" },
                "reason": { "type": "string" },
                "message": { "type": "string" }
            },
            "required": ["type", "status"]
        }
    });

    let scope = platform_config
        .get("crossplane")
        .and_then(|v| v.as_table())
        .and_then(|t| t.get("scope"))
        .and_then(|v| v.as_str())
        .unwrap_or("Cluster");

    let printer_columns = json!([
        {
            "name": "READY",
            "type": "string",
            "jsonPath": ".status.conditions[?(@.type=='Ready')].status"
        },
        {
            "name": "SYNCED",
            "type": "string",
            "jsonPath": ".status.conditions[?(@.type=='Synced')].status"
        },
        {
            "name": "AGE",
            "type": "date",
            "jsonPath": ".metadata.creationTimestamp"
        }
    ]);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": format!("{plural}.{group}")
        },
        "spec": {
            "group": group,
            "names": {
                "kind": kind,
                "plural": plural,
                "singular": singular,
                "categories": ["crossplane", "managed", provider_name]
            },
            "scope": scope,
            "versions": [{
                "name": api_version,
                "served": true,
                "storage": true,
                "additionalPrinterColumns": printer_columns,
                "subresources": {
                    "status": {}
                },
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "forProvider": for_provider_schema
                                },
                                "required": ["forProvider"]
                            },
                            "status": {
                                "type": "object",
                                "properties": {
                                    "atProvider": at_provider_schema,
                                    "conditions": conditions_schema
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    // Serialize with sorted keys for deterministic output.
    let sorted = sort_json_keys(&crd);
    serde_yaml_ng::to_string(&sorted)
}

/// Generate a `ProviderConfig` CRD YAML for the provider.
///
/// # Errors
///
/// Returns an error if YAML serialization fails.
pub fn generate_provider_config_crd(
    provider_name: &str,
    group: &str,
    api_version: &str,
) -> Result<String, serde_yaml_ng::Error> {
    let kind = "ProviderConfig";
    let singular = "providerconfig";
    let plural = "providerconfigs";

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": format!("{plural}.{group}")
        },
        "spec": {
            "group": group,
            "names": {
                "kind": kind,
                "plural": plural,
                "singular": singular,
                "categories": ["crossplane", "provider", provider_name]
            },
            "scope": "Cluster",
            "versions": [{
                "name": api_version,
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "credentials": {
                                        "type": "object",
                                        "properties": {
                                            "source": {
                                                "type": "string",
                                                "enum": ["None", "Secret"]
                                            },
                                            "secretRef": {
                                                "type": "object",
                                                "properties": {
                                                    "name": { "type": "string" },
                                                    "namespace": { "type": "string" },
                                                    "key": { "type": "string" }
                                                },
                                                "required": ["name", "namespace", "key"]
                                            }
                                        },
                                        "required": ["source"]
                                    }
                                },
                                "required": ["credentials"]
                            }
                        }
                    }
                }
            }]
        }
    });

    let sorted = sort_json_keys(&crd);
    serde_yaml_ng::to_string(&sorted)
}

/// Recursively sort JSON object keys for deterministic output.
fn sort_json_keys(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), sort_json_keys(v)))
                .collect();
            serde_json::to_value(sorted).unwrap_or_else(|_| value.clone())
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sort_json_keys).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::ir::{CrudInfo, IacAttribute, IacResource, IacType, IdentityInfo};
    use std::collections::HashMap;

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
                IacAttribute {
                    api_name: "tags".to_string(),
                    canonical_name: "tags".to_string(),
                    description: "Tags".to_string(),
                    iac_type: IacType::List(Box::new(IacType::String)),
                    required: false,
                    computed: false,
                    sensitive: false,
                    immutable: false,
                    default_value: None,
                    enum_values: None,
                    read_path: None,
                    update_only: false,
                },
                IacAttribute {
                    api_name: "version".to_string(),
                    canonical_name: "version".to_string(),
                    description: "Version number".to_string(),
                    iac_type: IacType::Integer,
                    required: false,
                    computed: true,
                    sensitive: false,
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
    fn simple_resource_generates_valid_yaml() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        assert_eq!(doc["apiVersion"], "apiextensions.k8s.io/v1");
        assert_eq!(doc["kind"], "CustomResourceDefinition");
        assert_eq!(
            doc["metadata"]["name"],
            "staticsecrets.akeyless.crossplane.io"
        );
        assert_eq!(doc["spec"]["group"], "akeyless.crossplane.io");
        assert_eq!(doc["spec"]["names"]["kind"], "StaticSecret");
        assert_eq!(doc["spec"]["names"]["plural"], "staticsecrets");
        assert_eq!(doc["spec"]["names"]["singular"], "staticsecret");
    }

    #[test]
    fn fields_map_to_correct_schema_types() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        // String field
        assert_eq!(for_provider["name"]["type"], "string");

        // List<String> field
        assert_eq!(for_provider["tags"]["type"], "array");
        assert_eq!(for_provider["tags"]["items"]["type"], "string");

        // Computed field should NOT be in forProvider
        assert!(for_provider.get("version").is_none());

        // Computed field SHOULD be in atProvider
        let at_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"]["atProvider"]["properties"];
        assert_eq!(at_provider["version"]["type"], "integer");
        assert_eq!(at_provider["version"]["format"], "int64");
    }

    #[test]
    fn required_fields_in_required_list() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"];

        let required = for_provider["required"]
            .as_array()
            .expect("required array");
        let required_names: Vec<&str> = required.iter().filter_map(Value::as_str).collect();

        assert!(required_names.contains(&"name"));
        assert!(required_names.contains(&"value"));
        assert!(!required_names.contains(&"tags"));
    }

    #[test]
    fn sensitive_fields_annotated() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        // Sensitive fields should NOT have x-kubernetes-preserve-unknown-fields
        assert!(
            for_provider["value"].get("x-kubernetes-preserve-unknown-fields").is_none(),
            "sensitive fields must not set x-kubernetes-preserve-unknown-fields"
        );
        let desc = for_provider["value"]["description"]
            .as_str()
            .expect("description");
        assert!(desc.contains("[sensitive]"));
    }

    #[test]
    fn group_derived_from_platform_config() {
        let mut platform_config = HashMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "group".to_string(),
            toml::Value::String("custom.example.io".to_string()),
        );
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );

        assert_eq!(
            derive_group("mycloud", &platform_config),
            "custom.example.io"
        );
    }

    #[test]
    fn group_defaults_to_provider_name() {
        let platform_config = HashMap::new();
        assert_eq!(
            derive_group("mycloud", &platform_config),
            "mycloud.crossplane.io"
        );
    }

    #[test]
    fn api_version_from_platform_config() {
        let mut platform_config = HashMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "api_version".to_string(),
            toml::Value::String("v1beta1".to_string()),
        );
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );

        assert_eq!(derive_api_version(&platform_config), "v1beta1");
    }

    #[test]
    fn api_version_defaults() {
        let platform_config = HashMap::new();
        assert_eq!(derive_api_version(&platform_config), "v1alpha1");
    }

    #[test]
    fn immutable_field_description() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        let name_desc = for_provider["name"]["description"]
            .as_str()
            .expect("description");
        assert!(name_desc.contains("(immutable)"));
    }

    #[test]
    fn type_mappings_comprehensive() {
        assert_eq!(iac_type_to_schema(&IacType::String), json!({"type": "string"}));
        assert_eq!(
            iac_type_to_schema(&IacType::Integer),
            json!({"type": "integer", "format": "int64"})
        );
        assert_eq!(
            iac_type_to_schema(&IacType::Float),
            json!({"type": "number", "format": "double"})
        );
        assert_eq!(iac_type_to_schema(&IacType::Boolean), json!({"type": "boolean"}));

        let list_schema = iac_type_to_schema(&IacType::List(Box::new(IacType::Integer)));
        assert_eq!(list_schema["type"], "array");
        assert_eq!(list_schema["items"]["type"], "integer");

        let map_schema = iac_type_to_schema(&IacType::Map(Box::new(IacType::String)));
        assert_eq!(map_schema["type"], "object");
        assert_eq!(map_schema["additionalProperties"]["type"], "string");

        let enum_schema = iac_type_to_schema(&IacType::Enum {
            values: vec!["a".into(), "b".into()],
            underlying: Box::new(IacType::String),
        });
        assert_eq!(enum_schema["type"], "string");
        assert_eq!(enum_schema["enum"], json!(["a", "b"]));

        let any_schema = iac_type_to_schema(&IacType::Any);
        assert_eq!(any_schema["x-kubernetes-preserve-unknown-fields"], true);
    }

    #[test]
    fn provider_config_crd() {
        let yaml =
            generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
                .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        assert_eq!(doc["apiVersion"], "apiextensions.k8s.io/v1");
        assert_eq!(doc["kind"], "CustomResourceDefinition");
        assert_eq!(
            doc["metadata"]["name"],
            "providerconfigs.akeyless.crossplane.io"
        );
        assert_eq!(doc["spec"]["names"]["kind"], "ProviderConfig");
    }

    #[test]
    fn sensitive_fields_no_preserve_unknown() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        // The sensitive "value" field must NOT have x-kubernetes-preserve-unknown-fields
        assert!(
            for_provider["value"].get("x-kubernetes-preserve-unknown-fields").is_none(),
            "sensitive fields must not have x-kubernetes-preserve-unknown-fields"
        );
        // The description should contain [sensitive]
        let desc = for_provider["value"]["description"].as_str().expect("desc");
        assert!(desc.contains("[sensitive]"));
    }

    #[test]
    fn status_subresource_present() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let version = &doc["spec"]["versions"][0];
        assert!(
            version.get("subresources").is_some(),
            "subresources key must be present"
        );
        assert!(
            version["subresources"].get("status").is_some(),
            "status subresource must be present"
        );
    }

    #[test]
    fn conditions_schema_in_status() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let status = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"];
        let conditions = &status["properties"]["conditions"];

        assert_eq!(conditions["type"], "array");
        let items = &conditions["items"];
        assert_eq!(items["type"], "object");
        assert!(items["properties"].get("type").is_some());
        assert!(items["properties"].get("status").is_some());
        assert!(items["properties"].get("lastTransitionTime").is_some());
        assert!(items["properties"].get("reason").is_some());
        assert!(items["properties"].get("message").is_some());

        let required = items["required"].as_array().expect("required array");
        let required_names: Vec<&str> = required.iter().filter_map(Value::as_str).collect();
        assert!(required_names.contains(&"type"));
        assert!(required_names.contains(&"status"));
    }

    #[test]
    fn set_type_has_unique_items() {
        let schema = iac_type_to_schema(&IacType::Set(Box::new(IacType::String)));
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["items"]["type"], "string");
        assert_eq!(
            schema["uniqueItems"], true,
            "Set type must have uniqueItems: true"
        );

        // List type should NOT have uniqueItems
        let list_schema = iac_type_to_schema(&IacType::List(Box::new(IacType::String)));
        assert!(
            list_schema.get("uniqueItems").is_none(),
            "List type must not have uniqueItems"
        );
    }

    #[test]
    fn printer_columns_present() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let columns = doc["spec"]["versions"][0]["additionalPrinterColumns"]
            .as_array()
            .expect("printer columns array");

        assert_eq!(columns.len(), 3);

        let names: Vec<&str> = columns
            .iter()
            .filter_map(|c| c["name"].as_str())
            .collect();
        assert!(names.contains(&"READY"));
        assert!(names.contains(&"SYNCED"));
        assert!(names.contains(&"AGE"));
    }

    #[test]
    fn scope_defaults_to_cluster() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        assert_eq!(doc["spec"]["scope"], "Cluster");
    }

    #[test]
    fn scope_from_platform_config() {
        let resource = make_test_resource();
        let mut platform_config = HashMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "scope".to_string(),
            toml::Value::String("Namespaced".to_string()),
        );
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );

        let yaml = generate_resource_crd_with_config(
            &resource,
            "akeyless",
            "akeyless.crossplane.io",
            "v1alpha1",
            &platform_config,
        )
        .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        assert_eq!(doc["spec"]["scope"], "Namespaced");
    }

    /// Build a resource with ALL IacType variants.
    fn resource_with_all_types() -> IacResource {
        IacResource {
            name: "akeyless_all_types".to_string(),
            description: "All type variants".to_string(),
            category: "test".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create".to_string(),
                create_schema: "Create".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/read".to_string(),
                read_schema: "Read".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete".to_string(),
                delete_schema: "Delete".to_string(),
            },
            attributes: vec![
                IacAttribute {
                    api_name: "str_field".to_string(),
                    canonical_name: "str_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "int_field".to_string(),
                    canonical_name: "int_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Integer,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "float_field".to_string(),
                    canonical_name: "float_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Float,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "bool_field".to_string(),
                    canonical_name: "bool_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Boolean,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "list_field".to_string(),
                    canonical_name: "list_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::List(Box::new(IacType::String)),
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "set_field".to_string(),
                    canonical_name: "set_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Set(Box::new(IacType::String)),
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "map_field".to_string(),
                    canonical_name: "map_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Map(Box::new(IacType::String)),
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "object_field".to_string(),
                    canonical_name: "object_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Object {
                        name: "Inner".to_string(),
                        fields: vec![IacAttribute {
                            api_name: "sub".to_string(),
                            canonical_name: "sub".to_string(),
                            description: "sub field".to_string(),
                            iac_type: IacType::String,
                            required: true, computed: false, sensitive: false, immutable: false,
                            default_value: None, enum_values: None, read_path: None, update_only: false,
                        }],
                    },
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "enum_field".to_string(),
                    canonical_name: "enum_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Enum {
                        values: vec!["a".into(), "b".into()],
                        underlying: Box::new(IacType::String),
                    },
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "any_field".to_string(),
                    canonical_name: "any_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Any,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
            ],
            identity: IdentityInfo {
                id_field: "str_field".to_string(),
                import_field: "str_field".to_string(),
                force_replace_fields: vec![],
            },
        }
    }

    #[test]
    fn resource_with_all_iac_type_variants() {
        let resource = resource_with_all_types();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        assert_eq!(for_provider["str_field"]["type"], "string");
        assert_eq!(for_provider["int_field"]["type"], "integer");
        assert_eq!(for_provider["int_field"]["format"], "int64");
        assert_eq!(for_provider["float_field"]["type"], "number");
        assert_eq!(for_provider["float_field"]["format"], "double");
        assert_eq!(for_provider["bool_field"]["type"], "boolean");
        assert_eq!(for_provider["list_field"]["type"], "array");
        assert_eq!(for_provider["set_field"]["type"], "array");
        assert_eq!(for_provider["set_field"]["uniqueItems"], true);
        assert_eq!(for_provider["map_field"]["type"], "object");
        assert!(for_provider["map_field"]["additionalProperties"].is_object());
        assert_eq!(for_provider["object_field"]["type"], "object");
        assert!(for_provider["object_field"]["properties"].is_object());
        assert_eq!(for_provider["enum_field"]["type"], "string");
        assert!(for_provider["enum_field"]["enum"].is_array());
        assert_eq!(for_provider["any_field"]["x-kubernetes-preserve-unknown-fields"], true);
    }

    #[test]
    fn crd_metadata_has_correct_plural_name() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        // metadata.name should be "staticsecrets.akeyless.crossplane.io"
        let meta_name = doc["metadata"]["name"].as_str().unwrap();
        assert_eq!(meta_name, "staticsecrets.akeyless.crossplane.io");
        // spec.names.plural should be "staticsecrets"
        assert_eq!(doc["spec"]["names"]["plural"], "staticsecrets");
        assert_eq!(doc["spec"]["names"]["singular"], "staticsecret");
    }

    #[test]
    fn resource_with_no_attributes() {
        let resource = IacResource {
            name: "akeyless_empty".to_string(),
            description: "".to_string(),
            category: "test".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create".to_string(),
                create_schema: "Create".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/read".to_string(),
                read_schema: "Read".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete".to_string(),
                delete_schema: "Delete".to_string(),
            },
            attributes: vec![],
            identity: IdentityInfo {
                id_field: "id".to_string(),
                import_field: "id".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"];
        // properties should be empty object
        assert!(for_provider["properties"].as_object().unwrap().is_empty());
        // required should not be present (empty list is omitted)
        assert!(for_provider.get("required").is_none());
    }

    #[test]
    fn object_type_has_nested_properties_and_required() {
        let schema = iac_type_to_schema(&IacType::Object {
            name: "Config".to_string(),
            fields: vec![
                IacAttribute {
                    api_name: "key".to_string(),
                    canonical_name: "key".to_string(),
                    description: "Config key".to_string(),
                    iac_type: IacType::String,
                    required: true, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "value".to_string(),
                    canonical_name: "value".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Integer,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
            ],
        });

        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["key"].is_object());
        assert_eq!(schema["properties"]["key"]["type"], "string");
        assert_eq!(schema["properties"]["key"]["description"], "Config key");
        assert_eq!(schema["properties"]["value"]["type"], "integer");
        // Required should contain "key" but not "value"
        let req = schema["required"].as_array().unwrap();
        let req_names: Vec<&str> = req.iter().filter_map(Value::as_str).collect();
        assert!(req_names.contains(&"key"));
        assert!(!req_names.contains(&"value"));
    }

    #[test]
    fn generate_all_produces_crd_files() {
        use super::super::backend::CrossplaneBackend;
        use iac_forge::backend::Backend;
        use iac_forge::ir::{AuthInfo, IacDataSource, IacProvider};

        let backend = CrossplaneBackend;
        let provider = IacProvider {
            name: "akeyless".to_string(),
            description: "Akeyless".to_string(),
            version: "1.0.0".to_string(),
            auth: AuthInfo::default(),
            skip_fields: vec![],
            platform_config: HashMap::new(),
        };

        let resources = vec![make_test_resource(), IacResource {
            name: "akeyless_auth_method".to_string(),
            description: "Auth method".to_string(),
            category: "auth".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create".to_string(),
                create_schema: "Create".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/read".to_string(),
                read_schema: "Read".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete".to_string(),
                delete_schema: "Delete".to_string(),
            },
            attributes: vec![IacAttribute {
                api_name: "method".to_string(),
                canonical_name: "method".to_string(),
                description: "Method".to_string(),
                iac_type: IacType::String,
                required: true, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "method".to_string(),
                import_field: "method".to_string(),
                force_replace_fields: vec![],
            },
        }];
        let data_sources: Vec<IacDataSource> = vec![];

        let artifacts = backend
            .generate_all(&provider, &resources, &data_sources)
            .expect("generate_all should succeed");

        // 2 resource CRDs + 1 provider CRD = 3 (data sources are no-op, tests are no-op)
        assert_eq!(artifacts.len(), 3);
        assert!(artifacts.iter().any(|a| a.path.contains("static-secret")));
        assert!(artifacts.iter().any(|a| a.path.contains("auth-method")));
        assert!(artifacts.iter().any(|a| a.path.contains("providerconfig")));

        // Verify each CRD is valid YAML
        for artifact in &artifacts {
            let _: Value = serde_yaml_ng::from_str(&artifact.content)
                .unwrap_or_else(|e| panic!("Invalid YAML in {}: {e}", artifact.path));
        }
    }

    #[test]
    fn categories_include_provider_name() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let categories = doc["spec"]["names"]["categories"].as_array().unwrap();
        let cat_strs: Vec<&str> = categories.iter().filter_map(Value::as_str).collect();
        assert!(cat_strs.contains(&"crossplane"));
        assert!(cat_strs.contains(&"managed"));
        assert!(cat_strs.contains(&"akeyless"));
    }

    #[test]
    fn sort_json_keys_deterministic() {
        let input = json!({"z": 1, "a": 2, "m": {"c": 3, "b": 4}});
        let sorted = sort_json_keys(&input);
        let keys: Vec<&String> = sorted.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
        let inner_keys: Vec<&String> = sorted["m"].as_object().unwrap().keys().collect();
        assert_eq!(inner_keys, vec!["b", "c"]);
    }

    #[test]
    fn immutable_field_with_empty_description() {
        let resource = IacResource {
            name: "akeyless_nodesc".to_string(),
            description: "".to_string(),
            category: "test".to_string(),
            crud: CrudInfo {
                create_endpoint: "/create".to_string(),
                create_schema: "Create".to_string(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/read".to_string(),
                read_schema: "Read".to_string(),
                read_response_schema: None,
                delete_endpoint: "/delete".to_string(),
                delete_schema: "Delete".to_string(),
            },
            attributes: vec![IacAttribute {
                api_name: "id".to_string(),
                canonical_name: "id".to_string(),
                description: "".to_string(),
                iac_type: IacType::String,
                required: true, computed: false, sensitive: false, immutable: true,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "id".to_string(),
                import_field: "id".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        // When description is empty, immutable field should still get "(immutable)"
        let desc = for_provider["id"]["description"].as_str().unwrap();
        assert_eq!(desc, "(immutable)");
    }
}
