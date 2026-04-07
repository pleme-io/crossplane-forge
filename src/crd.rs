use std::collections::BTreeMap;

use iac_forge::ir::{IacAttribute, IacResource, IacType};
use serde_json::{Map, Value, json};

use crate::error::CrdError;

/// Convert an `IacType` to an `OpenAPI` v3 JSON schema fragment.
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

/// Build an annotated description for `forProvider` fields.
///
/// Appends `(immutable)` and/or `[sensitive]` markers as appropriate.
fn annotated_description(base: &str, immutable: bool, sensitive: bool) -> Option<String> {
    let mut desc = base.to_string();

    if immutable {
        if desc.is_empty() {
            desc = "(immutable)".to_string();
        } else {
            desc = format!("{desc} (immutable)");
        }
    }

    if sensitive {
        if desc.is_empty() {
            desc = "[sensitive]".to_string();
        } else {
            desc = format!("{desc} [sensitive]");
        }
    }

    if desc.is_empty() { None } else { Some(desc) }
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

        if let Some(desc) = annotated_description(&attr.description, attr.immutable, attr.sensitive)
        {
            schema["description"] = Value::String(desc);
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

/// Look up a string value inside `platform_config["crossplane"][key]`.
fn crossplane_config_str<'a>(
    platform_config: &'a BTreeMap<String, toml::Value>,
    key: &str,
) -> Option<&'a str> {
    platform_config
        .get("crossplane")
        .and_then(toml::Value::as_table)
        .and_then(|t| t.get(key))
        .and_then(toml::Value::as_str)
}

/// Derive the CRD group from provider platform config or provider name.
///
/// Checks `platform_config["crossplane"]` for a `group` key, falling back
/// to `{provider_name}.crossplane.io`.
#[must_use]
pub fn derive_group(
    provider_name: &str,
    platform_config: &BTreeMap<String, toml::Value>,
) -> String {
    crossplane_config_str(platform_config, "group")
        .map_or_else(|| format!("{provider_name}.crossplane.io"), String::from)
}

/// Derive the CRD API version from provider platform config.
///
/// Checks `platform_config["crossplane"]` for an `api_version` key,
/// falling back to `v1alpha1`.
#[must_use]
pub fn derive_api_version(platform_config: &BTreeMap<String, toml::Value>) -> String {
    crossplane_config_str(platform_config, "api_version")
        .unwrap_or("v1alpha1")
        .to_string()
}

/// Generate a full CRD YAML document for a resource.
///
/// Produces a Kubernetes `CustomResourceDefinition` with:
/// - `spec.forProvider`: mutable (non-computed) fields
/// - `status.atProvider`: all fields for observation
///
/// # Errors
///
/// Returns a [`CrdError`] if YAML serialization or JSON conversion fails.
pub fn generate_resource_crd(
    resource: &IacResource,
    provider_name: &str,
    group: &str,
    api_version: &str,
) -> Result<String, CrdError> {
    generate_resource_crd_with_config(
        resource,
        provider_name,
        group,
        api_version,
        &BTreeMap::new(),
    )
}

/// Derive the CRD scope from platform config.
///
/// Checks `platform_config["crossplane"]` for a `scope` key,
/// falling back to `Cluster`.
fn derive_scope(platform_config: &BTreeMap<String, toml::Value>) -> &str {
    crossplane_config_str(platform_config, "scope").unwrap_or("Cluster")
}

/// Build the Crossplane standard conditions JSON schema.
fn conditions_schema() -> Value {
    json!({
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
    })
}

/// Build the standard Crossplane printer columns (READY, SYNCED, AGE).
fn printer_columns() -> Value {
    json!([
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
    ])
}

/// Generate a full CRD YAML document for a resource with platform config.
///
/// This variant accepts platform config for scope overrides.
///
/// # Errors
///
/// Returns a [`CrdError`] if YAML serialization or JSON conversion fails.
pub fn generate_resource_crd_with_config(
    resource: &IacResource,
    provider_name: &str,
    group: &str,
    api_version: &str,
    platform_config: &BTreeMap<String, toml::Value>,
) -> Result<String, CrdError> {
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

    let scope = derive_scope(platform_config);

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
                "additionalPrinterColumns": printer_columns(),
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
                                    "conditions": conditions_schema()
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    let sorted = sort_json_keys(&crd)?;
    Ok(serde_yaml_ng::to_string(&sorted)?)
}

/// Generate a `ProviderConfig` CRD YAML for the provider.
///
/// # Errors
///
/// Returns a [`CrdError`] if YAML serialization or JSON conversion fails.
pub fn generate_provider_config_crd(
    provider_name: &str,
    group: &str,
    api_version: &str,
) -> Result<String, CrdError> {
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

    let sorted = sort_json_keys(&crd)?;
    Ok(serde_yaml_ng::to_string(&sorted)?)
}

/// Recursively sort JSON object keys for deterministic output.
///
/// # Errors
///
/// Returns [`CrdError::JsonConversion`] if a `BTreeMap` cannot be converted
/// back into a `serde_json::Value` (should not happen in practice).
fn sort_json_keys(value: &Value) -> Result<Value, CrdError> {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| Ok((k.clone(), sort_json_keys(v)?)))
                .collect::<Result<_, CrdError>>()?;
            Ok(serde_json::to_value(sorted)?)
        }
        Value::Array(arr) => Ok(Value::Array(
            arr.iter().map(sort_json_keys).collect::<Result<_, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iac_forge::ir::{CrudInfo, IacAttribute, IacResource, IacType, IdentityInfo};
    use std::collections::BTreeMap;

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
        let mut platform_config = BTreeMap::new();
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
        let platform_config = BTreeMap::new();
        assert_eq!(
            derive_group("mycloud", &platform_config),
            "mycloud.crossplane.io"
        );
    }

    #[test]
    fn api_version_from_platform_config() {
        let mut platform_config = BTreeMap::new();
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
        let platform_config = BTreeMap::new();
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
        let mut platform_config = BTreeMap::new();
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
            platform_config: BTreeMap::new(),
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
        let sorted = sort_json_keys(&input).unwrap();
        let keys: Vec<&String> = sorted.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
        let inner_keys: Vec<&String> = sorted["m"].as_object().unwrap().keys().collect();
        assert_eq!(inner_keys, vec!["b", "c"]);
    }

    #[test]
    fn sensitive_field_with_empty_description() {
        let resource = IacResource {
            name: "akeyless_sensitive".to_string(),
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
                api_name: "secret".to_string(),
                canonical_name: "secret".to_string(),
                description: "".to_string(),
                iac_type: IacType::String,
                required: false, computed: false, sensitive: true, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "secret".to_string(),
                import_field: "secret".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        let desc = for_provider["secret"]["description"].as_str().unwrap();
        assert_eq!(desc, "[sensitive]");
    }

    #[test]
    fn immutable_and_sensitive_field() {
        let resource = IacResource {
            name: "akeyless_both".to_string(),
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
                api_name: "key".to_string(),
                canonical_name: "key".to_string(),
                description: "API key".to_string(),
                iac_type: IacType::String,
                required: true, computed: false, sensitive: true, immutable: true,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "key".to_string(),
                import_field: "key".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        let desc = for_provider["key"]["description"].as_str().unwrap();
        assert!(desc.contains("(immutable)"), "should contain immutable marker");
        assert!(desc.contains("[sensitive]"), "should contain sensitive marker");
    }

    #[test]
    fn immutable_sensitive_empty_description() {
        let resource = IacResource {
            name: "akeyless_immsens".to_string(),
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
                api_name: "token".to_string(),
                canonical_name: "token".to_string(),
                description: "".to_string(),
                iac_type: IacType::String,
                required: false, computed: false, sensitive: true, immutable: true,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "token".to_string(),
                import_field: "token".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];

        let desc = for_provider["token"]["description"].as_str().unwrap();
        assert!(desc.contains("(immutable)"));
        assert!(desc.contains("[sensitive]"));
    }

    #[test]
    fn derive_group_non_table_crossplane_config() {
        let mut platform_config = BTreeMap::new();
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::String("not-a-table".to_string()),
        );
        assert_eq!(
            derive_group("mycloud", &platform_config),
            "mycloud.crossplane.io",
            "should fall back to default when crossplane config is not a table"
        );
    }

    #[test]
    fn derive_group_missing_group_key() {
        let mut platform_config = BTreeMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "api_version".to_string(),
            toml::Value::String("v1beta1".to_string()),
        );
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );
        assert_eq!(
            derive_group("mycloud", &platform_config),
            "mycloud.crossplane.io",
            "should fall back to default when group key is absent"
        );
    }

    #[test]
    fn derive_group_non_string_group_value() {
        let mut platform_config = BTreeMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "group".to_string(),
            toml::Value::Integer(42),
        );
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );
        assert_eq!(
            derive_group("mycloud", &platform_config),
            "mycloud.crossplane.io",
            "should fall back to default when group value is not a string"
        );
    }

    #[test]
    fn derive_api_version_non_table_crossplane_config() {
        let mut platform_config = BTreeMap::new();
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Boolean(true),
        );
        assert_eq!(
            derive_api_version(&platform_config),
            "v1alpha1",
            "should fall back to default when crossplane config is not a table"
        );
    }

    #[test]
    fn derive_api_version_missing_key() {
        let mut platform_config = BTreeMap::new();
        let crossplane_table = toml::map::Map::new();
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );
        assert_eq!(
            derive_api_version(&platform_config),
            "v1alpha1",
            "should fall back to default when api_version key is absent"
        );
    }

    #[test]
    fn derive_api_version_non_string_value() {
        let mut platform_config = BTreeMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "api_version".to_string(),
            toml::Value::Integer(1),
        );
        platform_config.insert(
            "crossplane".to_string(),
            toml::Value::Table(crossplane_table),
        );
        assert_eq!(
            derive_api_version(&platform_config),
            "v1alpha1",
            "should fall back to default when api_version value is not a string"
        );
    }

    #[test]
    fn nested_list_of_lists() {
        let schema = iac_type_to_schema(&IacType::List(Box::new(
            IacType::List(Box::new(IacType::Integer)),
        )));
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["items"]["type"], "array");
        assert_eq!(schema["items"]["items"]["type"], "integer");
    }

    #[test]
    fn map_of_sets() {
        let schema = iac_type_to_schema(&IacType::Map(Box::new(
            IacType::Set(Box::new(IacType::String)),
        )));
        assert_eq!(schema["type"], "object");
        let inner = &schema["additionalProperties"];
        assert_eq!(inner["type"], "array");
        assert_eq!(inner["uniqueItems"], true);
        assert_eq!(inner["items"]["type"], "string");
    }

    #[test]
    fn list_of_objects() {
        let schema = iac_type_to_schema(&IacType::List(Box::new(IacType::Object {
            name: "Item".to_string(),
            fields: vec![IacAttribute {
                api_name: "id".to_string(),
                canonical_name: "id".to_string(),
                description: "Item ID".to_string(),
                iac_type: IacType::Integer,
                required: true, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
        })));
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["items"]["type"], "object");
        assert_eq!(schema["items"]["properties"]["id"]["type"], "integer");
        let req = schema["items"]["required"].as_array().unwrap();
        assert!(req.iter().any(|v| v == "id"));
    }

    #[test]
    fn object_with_no_required_fields() {
        let schema = iac_type_to_schema(&IacType::Object {
            name: "Config".to_string(),
            fields: vec![
                IacAttribute {
                    api_name: "opt_a".to_string(),
                    canonical_name: "opt_a".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "opt_b".to_string(),
                    canonical_name: "opt_b".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::Boolean,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
            ],
        });
        assert_eq!(schema["type"], "object");
        assert!(schema.get("required").is_none(), "required should be omitted when no fields are required");
        assert!(schema["properties"]["opt_a"].is_object());
        assert!(schema["properties"]["opt_b"].is_object());
    }

    #[test]
    fn object_with_empty_fields() {
        let schema = iac_type_to_schema(&IacType::Object {
            name: "Empty".to_string(),
            fields: vec![],
        });
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"].as_object().unwrap().is_empty());
        assert!(schema.get("required").is_none());
    }

    #[test]
    fn enum_with_integer_underlying() {
        let schema = iac_type_to_schema(&IacType::Enum {
            values: vec!["1".into(), "2".into(), "3".into()],
            underlying: Box::new(IacType::Integer),
        });
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["format"], "int64");
        let enum_vals = schema["enum"].as_array().unwrap();
        assert_eq!(enum_vals.len(), 3);
    }

    #[test]
    fn enum_with_empty_values() {
        let schema = iac_type_to_schema(&IacType::Enum {
            values: vec![],
            underlying: Box::new(IacType::String),
        });
        assert_eq!(schema["type"], "string");
        let enum_vals = schema["enum"].as_array().unwrap();
        assert!(enum_vals.is_empty());
    }

    #[test]
    fn sort_json_keys_with_null_and_scalars() {
        let input = json!({
            "z": null,
            "a": true,
            "m": 42,
            "b": "hello"
        });
        let sorted = sort_json_keys(&input).unwrap();
        let keys: Vec<&String> = sorted.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["a", "b", "m", "z"]);
        assert_eq!(sorted["z"], Value::Null);
        assert_eq!(sorted["a"], true);
        assert_eq!(sorted["m"], 42);
        assert_eq!(sorted["b"], "hello");
    }

    #[test]
    fn sort_json_keys_with_nested_arrays_of_objects() {
        let input = json!({
            "items": [
                {"z_key": 1, "a_key": 2},
                {"c": 3}
            ]
        });
        let sorted = sort_json_keys(&input).unwrap();
        let first = &sorted["items"][0];
        let keys: Vec<&String> = first.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["a_key", "z_key"], "nested objects inside arrays should also be sorted");
    }

    #[test]
    fn sort_json_keys_scalar_passthrough() {
        assert_eq!(sort_json_keys(&json!(42)).unwrap(), json!(42));
        assert_eq!(sort_json_keys(&json!("str")).unwrap(), json!("str"));
        assert_eq!(sort_json_keys(&json!(true)).unwrap(), json!(true));
        assert_eq!(sort_json_keys(&json!(null)).unwrap(), json!(null));
    }

    #[test]
    fn sort_json_keys_empty_object() {
        let input = json!({});
        let sorted = sort_json_keys(&input).unwrap();
        assert!(sorted.as_object().unwrap().is_empty());
    }

    #[test]
    fn sort_json_keys_empty_array() {
        let input = json!([]);
        let sorted = sort_json_keys(&input).unwrap();
        assert!(sorted.as_array().unwrap().is_empty());
    }

    #[test]
    fn at_provider_includes_all_fields() {
        let resource = IacResource {
            name: "akeyless_mixed".to_string(),
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
            attributes: vec![
                IacAttribute {
                    api_name: "name".to_string(),
                    canonical_name: "name".to_string(),
                    description: "The name".to_string(),
                    iac_type: IacType::String,
                    required: true, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "id".to_string(),
                    canonical_name: "id".to_string(),
                    description: "The id".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: true, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "secret_val".to_string(),
                    canonical_name: "secret_val".to_string(),
                    description: "A secret".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: false, sensitive: true, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
            ],
            identity: IdentityInfo {
                id_field: "id".to_string(),
                import_field: "id".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let at_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"]["atProvider"]["properties"];

        assert!(at_provider.get("name").is_some(), "atProvider should include mutable fields");
        assert!(at_provider.get("id").is_some(), "atProvider should include computed fields");
        assert!(at_provider.get("secret_val").is_some(), "atProvider should include sensitive fields");

        let at_desc = at_provider["name"]["description"].as_str().unwrap();
        assert_eq!(at_desc, "The name", "atProvider should preserve description");
        assert!(
            !at_desc.contains("(immutable)"),
            "atProvider should NOT add immutable marker"
        );
    }

    #[test]
    fn at_provider_no_sensitive_annotation() {
        let resource = IacResource {
            name: "akeyless_atsens".to_string(),
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
                api_name: "secret".to_string(),
                canonical_name: "secret".to_string(),
                description: "Secret value".to_string(),
                iac_type: IacType::String,
                required: false, computed: false, sensitive: true, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "secret".to_string(),
                import_field: "secret".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let at_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"]["atProvider"]["properties"];

        let desc = at_provider["secret"]["description"].as_str().unwrap();
        assert_eq!(desc, "Secret value", "atProvider should NOT add [sensitive] annotation");
    }

    #[test]
    fn at_provider_field_with_empty_description() {
        let resource = IacResource {
            name: "akeyless_nodesc2".to_string(),
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
                api_name: "count".to_string(),
                canonical_name: "count".to_string(),
                description: "".to_string(),
                iac_type: IacType::Integer,
                required: false, computed: true, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "count".to_string(),
                import_field: "count".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let at_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"]["atProvider"]["properties"];

        assert!(
            at_provider["count"].get("description").is_none(),
            "should not add description key when description is empty"
        );
        assert_eq!(at_provider["count"]["type"], "integer");
    }

    #[test]
    fn provider_config_crd_structure() {
        let yaml = generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let spec_schema = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
        let credentials = &spec_schema["properties"]["credentials"];

        assert_eq!(credentials["type"], "object");

        let source = &credentials["properties"]["source"];
        assert_eq!(source["type"], "string");
        let source_enum = source["enum"].as_array().unwrap();
        let enum_vals: Vec<&str> = source_enum.iter().filter_map(Value::as_str).collect();
        assert!(enum_vals.contains(&"None"));
        assert!(enum_vals.contains(&"Secret"));

        let secret_ref = &credentials["properties"]["secretRef"];
        assert_eq!(secret_ref["type"], "object");
        let sr_required = secret_ref["required"].as_array().unwrap();
        let sr_names: Vec<&str> = sr_required.iter().filter_map(Value::as_str).collect();
        assert!(sr_names.contains(&"name"));
        assert!(sr_names.contains(&"namespace"));
        assert!(sr_names.contains(&"key"));

        let cred_required = credentials["required"].as_array().unwrap();
        let cred_names: Vec<&str> = cred_required.iter().filter_map(Value::as_str).collect();
        assert!(cred_names.contains(&"source"));

        let spec_required = spec_schema["required"].as_array().unwrap();
        let spec_names: Vec<&str> = spec_required.iter().filter_map(Value::as_str).collect();
        assert!(spec_names.contains(&"credentials"));
    }

    #[test]
    fn provider_config_crd_scope_is_cluster() {
        let yaml = generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        assert_eq!(doc["spec"]["scope"], "Cluster");
    }

    #[test]
    fn provider_config_crd_categories() {
        let yaml = generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let categories = doc["spec"]["names"]["categories"].as_array().unwrap();
        let cat_strs: Vec<&str> = categories.iter().filter_map(Value::as_str).collect();
        assert!(cat_strs.contains(&"crossplane"));
        assert!(cat_strs.contains(&"provider"));
        assert!(cat_strs.contains(&"akeyless"));
    }

    #[test]
    fn generate_resource_crd_with_config_ignores_non_crossplane_keys() {
        let resource = make_test_resource();
        let mut platform_config = BTreeMap::new();
        let mut terraform_table = toml::map::Map::new();
        terraform_table.insert(
            "scope".to_string(),
            toml::Value::String("Namespaced".to_string()),
        );
        platform_config.insert(
            "terraform".to_string(),
            toml::Value::Table(terraform_table),
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
        assert_eq!(
            doc["spec"]["scope"], "Cluster",
            "non-crossplane config keys should not affect scope"
        );
    }

    #[test]
    fn scope_non_string_falls_back_to_cluster() {
        let resource = make_test_resource();
        let mut platform_config = BTreeMap::new();
        let mut crossplane_table = toml::map::Map::new();
        crossplane_table.insert(
            "scope".to_string(),
            toml::Value::Integer(0),
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
        assert_eq!(
            doc["spec"]["scope"], "Cluster",
            "non-string scope should fall back to Cluster"
        );
    }

    #[test]
    fn crd_version_served_and_storage() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let version = &doc["spec"]["versions"][0];
        assert_eq!(version["served"], true);
        assert_eq!(version["storage"], true);
        assert_eq!(version["name"], "v1alpha1");
    }

    #[test]
    fn crd_spec_requires_for_provider() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");

        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");
        let spec = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
        let required = spec["required"].as_array().unwrap();
        let names: Vec<&str> = required.iter().filter_map(Value::as_str).collect();
        assert!(names.contains(&"forProvider"));
    }

    #[test]
    fn computed_only_field_excluded_from_for_provider() {
        let resource = IacResource {
            name: "akeyless_componly".to_string(),
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
            attributes: vec![
                IacAttribute {
                    api_name: "server_id".to_string(),
                    canonical_name: "server_id".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: true, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "user_input".to_string(),
                    canonical_name: "user_input".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
            ],
            identity: IdentityInfo {
                id_field: "server_id".to_string(),
                import_field: "server_id".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];
        assert!(for_provider.get("server_id").is_none(), "computed-only field should be excluded from forProvider");
        assert!(for_provider.get("user_input").is_some(), "non-computed field should be included");

        let at_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["status"]["properties"]["atProvider"]["properties"];
        assert!(at_provider.get("server_id").is_some(), "computed field should be in atProvider");
        assert!(at_provider.get("user_input").is_some(), "all fields should be in atProvider");
    }

    #[test]
    fn only_one_version_entry() {
        let resource = make_test_resource();
        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let versions = doc["spec"]["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 1);
    }

    #[test]
    fn resource_crd_yaml_is_deterministic() {
        let resource = make_test_resource();
        let yaml1 = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("first gen");
        let yaml2 = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("second gen");
        assert_eq!(yaml1, yaml2, "repeated generation should produce identical output");
    }

    #[test]
    fn provider_config_crd_yaml_is_deterministic() {
        let yaml1 = generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("first gen");
        let yaml2 = generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("second gen");
        assert_eq!(yaml1, yaml2, "repeated generation should produce identical output");
    }

    #[test]
    fn map_of_objects() {
        let schema = iac_type_to_schema(&IacType::Map(Box::new(IacType::Object {
            name: "Entry".to_string(),
            fields: vec![IacAttribute {
                api_name: "val".to_string(),
                canonical_name: "val".to_string(),
                description: "".to_string(),
                iac_type: IacType::String,
                required: false, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
        })));
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"]["type"], "object");
        assert!(schema["additionalProperties"]["properties"]["val"].is_object());
    }

    #[test]
    fn set_of_integers() {
        let schema = iac_type_to_schema(&IacType::Set(Box::new(IacType::Integer)));
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["items"]["type"], "integer");
        assert_eq!(schema["items"]["format"], "int64");
        assert_eq!(schema["uniqueItems"], true);
    }

    #[test]
    fn object_field_description_empty_omits_key() {
        let schema = iac_type_to_schema(&IacType::Object {
            name: "NoDesc".to_string(),
            fields: vec![IacAttribute {
                api_name: "x".to_string(),
                canonical_name: "x".to_string(),
                description: "".to_string(),
                iac_type: IacType::Boolean,
                required: false, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
        });
        assert!(
            schema["properties"]["x"].get("description").is_none(),
            "should not add description when empty"
        );
    }

    #[test]
    fn multiple_required_fields_ordering() {
        let resource = IacResource {
            name: "akeyless_multi_req".to_string(),
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
            attributes: vec![
                IacAttribute {
                    api_name: "z_field".to_string(),
                    canonical_name: "z_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: true, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "a_field".to_string(),
                    canonical_name: "a_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: true, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
                IacAttribute {
                    api_name: "m_field".to_string(),
                    canonical_name: "m_field".to_string(),
                    description: "".to_string(),
                    iac_type: IacType::String,
                    required: false, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                },
            ],
            identity: IdentityInfo {
                id_field: "z_field".to_string(),
                import_field: "z_field".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"];
        let required = for_provider["required"].as_array().unwrap();
        let names: Vec<&str> = required.iter().filter_map(Value::as_str).collect();
        assert!(names.contains(&"z_field"));
        assert!(names.contains(&"a_field"));
        assert!(!names.contains(&"m_field"));
    }

    #[test]
    fn non_immutable_non_sensitive_field_has_plain_description() {
        let resource = IacResource {
            name: "akeyless_plain".to_string(),
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
                api_name: "tags".to_string(),
                canonical_name: "tags".to_string(),
                description: "Resource tags".to_string(),
                iac_type: IacType::List(Box::new(IacType::String)),
                required: false, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "tags".to_string(),
                import_field: "tags".to_string(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "akeyless", "akeyless.crossplane.io", "v1alpha1")
            .expect("yaml generation");
        let doc: Value = serde_yaml_ng::from_str(&yaml).expect("parse yaml");

        let for_provider = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["forProvider"]["properties"];
        let desc = for_provider["tags"]["description"].as_str().unwrap();
        assert_eq!(desc, "Resource tags");
        assert!(!desc.contains("(immutable)"));
        assert!(!desc.contains("[sensitive]"));
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

    #[test]
    fn conditions_schema_structure() {
        let schema = conditions_schema();
        assert_eq!(schema["type"], "array");
        let items = &schema["items"];
        assert_eq!(items["type"], "object");

        let props = items["properties"].as_object().unwrap();
        assert!(props.contains_key("type"));
        assert!(props.contains_key("status"));
        assert!(props.contains_key("lastTransitionTime"));
        assert!(props.contains_key("reason"));
        assert!(props.contains_key("message"));

        assert_eq!(items["properties"]["lastTransitionTime"]["format"], "date-time");

        let req = items["required"].as_array().unwrap();
        let req_names: Vec<&str> = req.iter().filter_map(Value::as_str).collect();
        assert_eq!(req_names, vec!["type", "status"]);
    }

    #[test]
    fn printer_columns_structure() {
        let cols = printer_columns();
        let arr = cols.as_array().unwrap();
        assert_eq!(arr.len(), 3);

        assert_eq!(arr[0]["name"], "READY");
        assert_eq!(arr[0]["type"], "string");
        assert!(arr[0]["jsonPath"].as_str().unwrap().contains("Ready"));

        assert_eq!(arr[1]["name"], "SYNCED");
        assert_eq!(arr[1]["type"], "string");
        assert!(arr[1]["jsonPath"].as_str().unwrap().contains("Synced"));

        assert_eq!(arr[2]["name"], "AGE");
        assert_eq!(arr[2]["type"], "date");
        assert!(arr[2]["jsonPath"].as_str().unwrap().contains("creationTimestamp"));
    }

    #[test]
    fn derive_scope_defaults_to_cluster() {
        let config = BTreeMap::new();
        assert_eq!(derive_scope(&config), "Cluster");
    }

    #[test]
    fn derive_scope_from_config() {
        let mut config = BTreeMap::new();
        let mut table = toml::map::Map::new();
        table.insert("scope".into(), toml::Value::String("Namespaced".into()));
        config.insert("crossplane".into(), toml::Value::Table(table));
        assert_eq!(derive_scope(&config), "Namespaced");
    }

    #[test]
    fn derive_scope_non_string_falls_back() {
        let mut config = BTreeMap::new();
        let mut table = toml::map::Map::new();
        table.insert("scope".into(), toml::Value::Integer(42));
        config.insert("crossplane".into(), toml::Value::Table(table));
        assert_eq!(derive_scope(&config), "Cluster");
    }

    #[test]
    fn derive_scope_non_table_falls_back() {
        let mut config = BTreeMap::new();
        config.insert("crossplane".into(), toml::Value::Boolean(true));
        assert_eq!(derive_scope(&config), "Cluster");
    }

    #[test]
    fn generate_resource_crd_delegates_to_with_config() {
        let resource = make_test_resource();
        let yaml_without = generate_resource_crd(
            &resource,
            "akeyless",
            "akeyless.crossplane.io",
            "v1alpha1",
        )
        .unwrap();
        let yaml_with = generate_resource_crd_with_config(
            &resource,
            "akeyless",
            "akeyless.crossplane.io",
            "v1alpha1",
            &BTreeMap::new(),
        )
        .unwrap();
        pretty_assertions::assert_eq!(yaml_without, yaml_with);
    }

    #[test]
    fn build_for_provider_excludes_computed() {
        let attrs = vec![
            IacAttribute {
                api_name: "input".into(),
                canonical_name: "input".into(),
                description: "User input".into(),
                iac_type: IacType::String,
                required: true, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            },
            IacAttribute {
                api_name: "computed_id".into(),
                canonical_name: "computed_id".into(),
                description: "Auto-generated".into(),
                iac_type: IacType::String,
                required: false, computed: true, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            },
        ];
        let (props, required) = build_for_provider(&attrs);
        assert!(props.contains_key("input"));
        assert!(!props.contains_key("computed_id"));
        assert_eq!(required.len(), 1);
        assert_eq!(required[0], "input");
    }

    #[test]
    fn build_at_provider_includes_all() {
        let attrs = vec![
            IacAttribute {
                api_name: "input".into(),
                canonical_name: "input".into(),
                description: "User input".into(),
                iac_type: IacType::String,
                required: true, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            },
            IacAttribute {
                api_name: "computed_id".into(),
                canonical_name: "computed_id".into(),
                description: "Auto-generated".into(),
                iac_type: IacType::String,
                required: false, computed: true, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            },
        ];
        let props = build_at_provider(&attrs);
        assert!(props.contains_key("input"));
        assert!(props.contains_key("computed_id"));
        assert_eq!(props["input"]["description"], "User input");
        assert_eq!(props["computed_id"]["description"], "Auto-generated");
    }

    #[test]
    fn build_for_provider_empty_attrs() {
        let (props, required) = build_for_provider(&[]);
        assert!(props.is_empty());
        assert!(required.is_empty());
    }

    #[test]
    fn build_at_provider_empty_attrs() {
        let props = build_at_provider(&[]);
        assert!(props.is_empty());
    }

    #[test]
    fn deeply_nested_type_schema() {
        let deep = IacType::Map(Box::new(IacType::List(Box::new(IacType::Set(
            Box::new(IacType::Object {
                name: "Inner".into(),
                fields: vec![IacAttribute {
                    api_name: "val".into(),
                    canonical_name: "val".into(),
                    description: "".into(),
                    iac_type: IacType::Enum {
                        values: vec!["x".into()],
                        underlying: Box::new(IacType::String),
                    },
                    required: true, computed: false, sensitive: false, immutable: false,
                    default_value: None, enum_values: None, read_path: None, update_only: false,
                }],
            }),
        )))));
        let schema = iac_type_to_schema(&deep);
        assert_eq!(schema["type"], "object");
        let inner = &schema["additionalProperties"];
        assert_eq!(inner["type"], "array");
        let set = &inner["items"];
        assert_eq!(set["type"], "array");
        assert_eq!(set["uniqueItems"], true);
        let obj = &set["items"];
        assert_eq!(obj["type"], "object");
        assert_eq!(obj["properties"]["val"]["type"], "string");
        assert!(obj["properties"]["val"]["enum"].is_array());
    }

    #[test]
    fn provider_config_crd_version_served_and_storage() {
        let yaml =
            generate_provider_config_crd("akeyless", "akeyless.crossplane.io", "v1alpha1")
                .unwrap();
        let doc: Value = serde_yaml_ng::from_str(&yaml).unwrap();
        let version = &doc["spec"]["versions"][0];
        assert_eq!(version["served"], true);
        assert_eq!(version["storage"], true);
        assert_eq!(version["name"], "v1alpha1");
    }

    #[test]
    fn provider_config_crd_with_different_group() {
        let yaml =
            generate_provider_config_crd("mycloud", "custom.example.io", "v1beta1")
                .unwrap();
        let doc: Value = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(doc["metadata"]["name"], "providerconfigs.custom.example.io");
        assert_eq!(doc["spec"]["group"], "custom.example.io");
        assert_eq!(doc["spec"]["versions"][0]["name"], "v1beta1");
        let cats = doc["spec"]["names"]["categories"].as_array().unwrap();
        let cat_strs: Vec<&str> = cats.iter().filter_map(Value::as_str).collect();
        assert!(cat_strs.contains(&"mycloud"));
    }

    #[test]
    fn resource_crd_different_provider_names() {
        let resource = IacResource {
            name: "aws_s3_bucket".into(),
            description: "S3 bucket".into(),
            category: "storage".into(),
            crud: CrudInfo {
                create_endpoint: "/create".into(),
                create_schema: "Create".into(),
                update_endpoint: None,
                update_schema: None,
                read_endpoint: "/read".into(),
                read_schema: "Read".into(),
                read_response_schema: None,
                delete_endpoint: "/delete".into(),
                delete_schema: "Delete".into(),
            },
            attributes: vec![IacAttribute {
                api_name: "bucket".into(),
                canonical_name: "bucket".into(),
                description: "Bucket name".into(),
                iac_type: IacType::String,
                required: true, computed: false, sensitive: false, immutable: false,
                default_value: None, enum_values: None, read_path: None, update_only: false,
            }],
            identity: IdentityInfo {
                id_field: "bucket".into(),
                import_field: "bucket".into(),
                force_replace_fields: vec![],
            },
        };

        let yaml = generate_resource_crd(&resource, "aws", "aws.crossplane.io", "v1alpha1")
            .unwrap();
        let doc: Value = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(doc["spec"]["names"]["kind"], "S3Bucket");
        assert_eq!(doc["spec"]["names"]["singular"], "s3bucket");
        assert_eq!(doc["spec"]["names"]["plural"], "s3buckets");
        assert_eq!(doc["metadata"]["name"], "s3buckets.aws.crossplane.io");
        let cats = doc["spec"]["names"]["categories"].as_array().unwrap();
        let cat_strs: Vec<&str> = cats.iter().filter_map(Value::as_str).collect();
        assert!(cat_strs.contains(&"aws"));
    }

    #[test]
    fn sort_json_keys_deeply_nested() {
        let input = json!({
            "z": {
                "b": {
                    "d": 1,
                    "a": 2
                },
                "a": 3
            },
            "a": [{"z": 1, "a": 2}]
        });
        let sorted = sort_json_keys(&input).unwrap();
        let top_keys: Vec<&String> = sorted.as_object().unwrap().keys().collect();
        assert_eq!(top_keys, vec!["a", "z"]);
        let z_keys: Vec<&String> = sorted["z"].as_object().unwrap().keys().collect();
        assert_eq!(z_keys, vec!["a", "b"]);
        let b_keys: Vec<&String> = sorted["z"]["b"].as_object().unwrap().keys().collect();
        assert_eq!(b_keys, vec!["a", "d"]);
    }

    #[test]
    fn annotated_description_plain() {
        assert_eq!(
            annotated_description("Some field", false, false),
            Some("Some field".into())
        );
    }

    #[test]
    fn annotated_description_empty_no_flags() {
        assert_eq!(annotated_description("", false, false), None);
    }

    #[test]
    fn annotated_description_immutable_only() {
        assert_eq!(
            annotated_description("A field", true, false),
            Some("A field (immutable)".into())
        );
    }

    #[test]
    fn annotated_description_sensitive_only() {
        assert_eq!(
            annotated_description("A field", false, true),
            Some("A field [sensitive]".into())
        );
    }

    #[test]
    fn annotated_description_both_flags() {
        assert_eq!(
            annotated_description("A field", true, true),
            Some("A field (immutable) [sensitive]".into())
        );
    }

    #[test]
    fn annotated_description_empty_immutable() {
        assert_eq!(
            annotated_description("", true, false),
            Some("(immutable)".into())
        );
    }

    #[test]
    fn annotated_description_empty_sensitive() {
        assert_eq!(
            annotated_description("", false, true),
            Some("[sensitive]".into())
        );
    }

    #[test]
    fn annotated_description_empty_both() {
        assert_eq!(
            annotated_description("", true, true),
            Some("(immutable) [sensitive]".into())
        );
    }
}
