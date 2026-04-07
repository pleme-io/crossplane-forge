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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_error_display() {
        let yaml_err: Result<serde_json::Value, _> = serde_yaml_ng::from_str("{{bad");
        let crd_err = CrdError::from(yaml_err.unwrap_err());
        let msg = crd_err.to_string();
        assert!(msg.starts_with("YAML serialization failed:"), "got: {msg}");
    }

    #[test]
    fn json_error_display() {
        let json_err: Result<serde_json::Value, _> = serde_json::from_str("not json");
        let crd_err = CrdError::from(json_err.unwrap_err());
        let msg = crd_err.to_string();
        assert!(msg.starts_with("JSON conversion failed:"), "got: {msg}");
    }

    #[test]
    fn yaml_error_from_conversion() {
        let yaml_err: Result<serde_json::Value, _> = serde_yaml_ng::from_str("{{bad");
        let crd_err: CrdError = yaml_err.unwrap_err().into();
        assert!(matches!(crd_err, CrdError::YamlSerialization(_)));
    }

    #[test]
    fn json_error_from_conversion() {
        let json_err: Result<serde_json::Value, _> = serde_json::from_str("not json");
        let crd_err: CrdError = json_err.unwrap_err().into();
        assert!(matches!(crd_err, CrdError::JsonConversion(_)));
    }
}
