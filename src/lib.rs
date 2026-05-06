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
/// Crossplane managed-resource Go-types emitter.
pub mod types_gen;

pub use backend::CrossplaneBackend;
pub use controller_gen::{ControllerConfig, render_controller};
pub use error::CrdError;
pub use types_gen::{render_groupversion_info, render_resource_types};
