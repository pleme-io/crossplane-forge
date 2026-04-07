/// Errors produced during CRD generation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CrdError {
    /// YAML serialization failed.
    #[error("YAML serialization failed: {0}")]
    YamlSerialization(#[from] serde_yaml_ng::Error),

    /// JSON-to-Value conversion failed (e.g. inside `sort_json_keys`).
    #[error("JSON conversion failed: {0}")]
    JsonConversion(#[from] serde_json::Error),
}
