//! Schema-driven body-field signatures pulled from an OpenAPI 3.x spec.
//!
//! M6.4 — without this, M6.2's broad-walk push of
//! `cr.Spec.ForProvider.X` onto SDK body composites breaks ~62% of the
//! akeyless 119-resource corpus because the akeyless TOML's IacType
//! disagrees with the akeyless OpenAPI body schema's actual Go type
//! (DeleteProtection bool/string, RotationHour int64/int32, UseTls
//! bool/string, plus Update bodies omitting Create-only fields). With
//! this module, `build_request_body` consults the OpenAPI spec at emit
//! time and skips any push whose name+type doesn't match the SDK body
//! schema, restoring 100% coverage.

use iac_forge::ir::IacType;
use iac_forge::naming::{to_pascal_case, to_snake_case};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// Lookup result for one (body_type, field) probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldMatch {
    /// The SDK body has this field with a matching scalar type. Safe
    /// to push `body.X = cr.Spec.ForProvider.X`.
    Match,
    /// The SDK body lacks this field, OR the field exists but its
    /// scalar type / required-ness disagrees with the IacAttribute.
    /// Caller should skip the push.
    Skip,
}

/// One field's signature on an OpenAPI body schema.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldSig {
    /// JSON property name as it appears in the spec
    /// (e.g. `"delete-protection"`).
    json_name: String,
    /// `type:` from the OpenAPI schema (`string`, `integer`,
    /// `boolean`, `number`, `array`, `object`).
    openapi_type: String,
    /// `format:` (`int32`, `int64`, etc.) or empty.
    openapi_format: String,
    /// Whether the property is in the schema's `required` array.
    required: bool,
}

/// Per-body-schema field map keyed by Go-PascalCase field name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BodySchema {
    fields: BTreeMap<String, FieldSig>,
}

/// Map from SDK-body-type-name (PascalCase, e.g.
/// `"AuthMethodCreateApiKey"`) → its field-signature table.
///
/// Constructed once from the OpenAPI spec, shared across all
/// resource renders via `Arc`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BodySigMap {
    schemas: BTreeMap<String, BodySchema>,
}

impl BodySigMap {
    /// Load and parse the OpenAPI spec at `path` (JSON-encoded).
    ///
    /// # Errors
    ///
    /// Returns an error if the file can't be read or parsed as JSON.
    pub fn from_openapi_json_path(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = std::fs::read(path.as_ref())?;
        let v: Value = serde_json::from_slice(&bytes)?;
        Ok(Self::from_value(&v))
    }

    /// Build a sig map from an already-loaded OpenAPI JSON value.
    ///
    /// Walks `components.schemas.*`, extracts each schema's
    /// `properties` and `required` array, and stores fields by
    /// PascalCase name.
    #[must_use]
    pub fn from_value(spec: &Value) -> Self {
        let mut schemas = BTreeMap::new();
        let Some(components_schemas) = spec
            .get("components")
            .and_then(|c| c.get("schemas"))
            .and_then(|s| s.as_object())
        else {
            return Self { schemas };
        };
        for (schema_name, schema_value) in components_schemas {
            // Only object schemas with `properties` carry body fields.
            let Some(props) = schema_value
                .get("properties")
                .and_then(|p| p.as_object())
            else {
                continue;
            };
            let required: Vec<String> = schema_value
                .get("required")
                .and_then(|r| r.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let mut fields = BTreeMap::new();
            for (json_name, prop_value) in props {
                let pascal = pascal_case_from_json(json_name);
                let openapi_type = prop_value
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let openapi_format = prop_value
                    .get("format")
                    .and_then(|f| f.as_str())
                    .unwrap_or("")
                    .to_string();
                fields.insert(
                    pascal,
                    FieldSig {
                        json_name: json_name.clone(),
                        openapi_type,
                        openapi_format,
                        required: required.iter().any(|r| r == json_name),
                    },
                );
            }
            // Schema name in the OpenAPI spec is camelCase
            // (`authMethodCreateApiKey`); the akeyless-go SDK uses
            // PascalCase (`AuthMethodCreateApiKey`). Index by both.
            let pascal_schema = pascal_case_from_json(schema_name);
            schemas.insert(
                pascal_schema,
                BodySchema {
                    fields: fields.clone(),
                },
            );
            schemas.insert(schema_name.clone(), BodySchema { fields });
        }
        Self { schemas }
    }

    /// Probe whether (body_type, pascal_field) is safe to push given
    /// the IacAttribute's `iac_type` and `required` flag.
    ///
    /// `Skip` means the field is missing from the body schema OR the
    /// types disagree — caller should NOT emit the push to avoid a
    /// Go compile error.
    #[must_use]
    pub fn check_field(
        &self,
        body_type: &str,
        pascal_field: &str,
        attr_type: &IacType,
        attr_required: bool,
    ) -> FieldMatch {
        let Some(schema) = self.schemas.get(body_type) else {
            // No schema info → conservative skip. This will hit if the
            // OpenAPI doesn't carry the body type (unusual) or if the
            // body_type name was passed through a different naming
            // convention than the spec uses.
            return FieldMatch::Skip;
        };
        let Some(sig) = schema.fields.get(pascal_field) else {
            return FieldMatch::Skip;
        };
        if !openapi_matches_iac_type(&sig.openapi_type, &sig.openapi_format, attr_type) {
            return FieldMatch::Skip;
        }
        if sig.required != attr_required {
            // Required-ness disagreement → pointer-vs-value mismatch
            // between the Parameters struct and the SDK body. Skip.
            return FieldMatch::Skip;
        }
        FieldMatch::Match
    }

    /// Number of schemas indexed (for debugging + tests).
    #[must_use]
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }
}

/// Wraps a `BodySigMap` in `Arc` for cheap sharing across the per-
/// resource render loop. Optional — `None` disables M6.2 broad walk
/// (resources fall back to identifier+token only).
pub type SharedBodySigMap = Option<Arc<BodySigMap>>;

fn pascal_case_from_json(s: &str) -> String {
    to_pascal_case(&to_snake_case(s))
}

fn openapi_matches_iac_type(otype: &str, oformat: &str, iac: &IacType) -> bool {
    match iac {
        IacType::String => otype == "string",
        IacType::Boolean => otype == "boolean",
        IacType::Float | IacType::Numeric => {
            otype == "number" || (otype == "integer" && (oformat == "double" || oformat == "float"))
        }
        IacType::Integer => {
            // IacType::Integer maps to Go int64. OpenAPI integer with
            // format=int64 matches; format=int32 does NOT (the SDK
            // emits `*int32` and Parameters has `*int64`).
            otype == "integer" && oformat != "int32"
        }
        // List / Map / Object: types_gen still collapses these to
        // opaque `string` (slice 1). Mismatch on purpose for now —
        // caller skips. Slice 2 graduates types_gen + this matcher.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture_spec() -> Value {
        json!({
            "components": {
                "schemas": {
                    "authMethodCreateApiKey": {
                        "type": "object",
                        "required": ["name"],
                        "properties": {
                            "name": { "type": "string" },
                            "access-expires": { "type": "integer", "format": "int64" },
                            "force-sub-claims": { "type": "boolean" },
                            "delete-protection": { "type": "string" },
                            "token": { "type": "string" }
                        }
                    },
                    "rotatedSecretCreateWindows": {
                        "type": "object",
                        "required": ["name", "target-name"],
                        "properties": {
                            "name": { "type": "string" },
                            "target-name": { "type": "string" },
                            "rotation-hour": { "type": "integer", "format": "int32" },
                            "delete-protection": { "type": "string" }
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn parses_components_schemas_with_pascal_index() {
        let m = BodySigMap::from_value(&fixture_spec());
        // Each schema is indexed by both camelCase + PascalCase.
        assert_eq!(m.schema_count(), 4);
    }

    #[test]
    fn matches_aligned_scalar_field() {
        let m = BodySigMap::from_value(&fixture_spec());
        // AccessExpires: openapi int64 + IacType::Integer + optional → match.
        assert_eq!(
            m.check_field(
                "AuthMethodCreateApiKey",
                "AccessExpires",
                &IacType::Integer,
                false,
            ),
            FieldMatch::Match,
        );
        // ForceSubClaims: openapi boolean + IacType::Boolean + optional → match.
        assert_eq!(
            m.check_field(
                "AuthMethodCreateApiKey",
                "ForceSubClaims",
                &IacType::Boolean,
                false,
            ),
            FieldMatch::Match,
        );
    }

    #[test]
    fn skips_type_mismatch_delete_protection_bool_vs_string() {
        let m = BodySigMap::from_value(&fixture_spec());
        // TOML claims Boolean but openapi says string → SKIP.
        assert_eq!(
            m.check_field(
                "AuthMethodCreateApiKey",
                "DeleteProtection",
                &IacType::Boolean,
                false,
            ),
            FieldMatch::Skip,
        );
    }

    #[test]
    fn skips_type_mismatch_rotation_hour_int64_vs_int32() {
        let m = BodySigMap::from_value(&fixture_spec());
        // TOML claims Integer (→ int64) but openapi says int32 → SKIP.
        assert_eq!(
            m.check_field(
                "RotatedSecretCreateWindows",
                "RotationHour",
                &IacType::Integer,
                false,
            ),
            FieldMatch::Skip,
        );
    }

    #[test]
    fn skips_missing_field() {
        let m = BodySigMap::from_value(&fixture_spec());
        // Field doesn't exist on the body → SKIP.
        assert_eq!(
            m.check_field(
                "AuthMethodCreateApiKey",
                "NonexistentField",
                &IacType::String,
                false,
            ),
            FieldMatch::Skip,
        );
    }

    #[test]
    fn skips_unknown_body_type() {
        let m = BodySigMap::from_value(&fixture_spec());
        assert_eq!(
            m.check_field(
                "TotallyMadeUpBody",
                "Name",
                &IacType::String,
                true,
            ),
            FieldMatch::Skip,
        );
    }

    #[test]
    fn skips_required_disagreement() {
        let m = BodySigMap::from_value(&fixture_spec());
        // openapi says target-name is required, Parameters might say
        // optional → mismatch.
        assert_eq!(
            m.check_field(
                "RotatedSecretCreateWindows",
                "TargetName",
                &IacType::String,
                false, // saying NOT required, but spec says required → skip
            ),
            FieldMatch::Skip,
        );
        // Aligned: both required.
        assert_eq!(
            m.check_field(
                "RotatedSecretCreateWindows",
                "TargetName",
                &IacType::String,
                true,
            ),
            FieldMatch::Match,
        );
    }
}
