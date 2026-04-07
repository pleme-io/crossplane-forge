//! Crossplane CRD generator from `IaC` forge IR.
//!
//! Implements [`iac_forge::Backend`] to produce Kubernetes
//! `CustomResourceDefinition` YAML from the `iac-forge` intermediate
//! representation. Each [`IacResource`](iac_forge::ir::IacResource) becomes a
//! namespaced CRD with `forProvider` (input) and `atProvider` (output) specs,
//! plus standard Crossplane conditions and printer columns.

/// Backend trait implementation and naming convention.
pub mod backend;
/// CRD YAML generation helpers.
pub mod crd;
/// Typed errors for CRD generation.
pub mod error;

pub use backend::CrossplaneBackend;
pub use error::CrdError;
