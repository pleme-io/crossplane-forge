//! Crossplane CRD generator from `IaC` forge IR.
//!
//! Implements [`iac_forge::Backend`] to produce Kubernetes
//! `CustomResourceDefinition` YAML from the `iac-forge` intermediate
//! representation. Each [`IacResource`](iac_forge::ir::IacResource) becomes a
//! namespaced CRD with `forProvider` (input) and `atProvider` (output) specs,
//! plus standard Crossplane conditions and printer columns.

/// Backend trait implementation and naming convention.
pub mod backend;
/// Crossplane controller Go-code emitter (per-resource ExternalClient impl).
pub mod controller_gen;
/// CRD YAML generation helpers.
pub mod crd;
/// Typed errors for CRD generation.
pub mod error;
/// Provider-runtime scaffold emitter (ProviderConfig types, main.go,
/// setup.go, go.mod, Helm chart files).
pub mod provider_gen;
/// Crossplane managed-resource Go-types emitter.
pub mod types_gen;

pub use backend::CrossplaneBackend;
pub use controller_gen::{ControllerConfig, render_controller};
pub use error::CrdError;
pub use provider_gen::{
    render_go_mod, render_helm_chart_yaml, render_helm_deployment_template, render_helm_rbac_template,
    render_helm_values_yaml, render_main_go, render_provider_config_types,
    render_provider_groupversion_info, render_setup_go,
};
pub use types_gen::{render_groupversion_info, render_resource_types};
