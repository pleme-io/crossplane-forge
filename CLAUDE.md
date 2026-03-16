# crossplane-forge

Crossplane provider code generator. Implements `iac_forge::Backend` to produce
Kubernetes CustomResourceDefinition (CRD) YAML from the iac-forge IR.

## Architecture

Takes `IacResource` and `IacProvider` from iac-forge IR and generates complete
CRD YAML files conforming to `apiextensions.k8s.io/v1`. Each resource becomes
a namespaced CRD with `forProvider` (input) and `atProvider` (output) specs,
plus standard Crossplane conditions and printer columns.

Crossplane has no concept of data sources or test artifacts -- those Backend
methods are no-ops.

## CRD Structure

Generated CRDs follow the Crossplane managed resource pattern:

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: <plural>.<group>
spec:
  group: <group>                    # e.g. vault.akeyless.io
  names:
    kind: <PascalCase>              # e.g. StaticSecret
    plural: <lowercase-plural>
    singular: <lowercase>
    categories: [crossplane, managed, <provider>]
  scope: Namespaced
  versions:
    - name: <apiVersion>            # e.g. v1alpha1
      served: true
      storage: true
      subresources:
        status: {}
      additionalPrinterColumns:
        - name: READY, SYNCED, AGE
      schema:
        openAPIV3Schema:
          spec:
            forProvider: { ... }    # input attributes
            atProvider: { ... }     # computed attributes
          status:
            conditions: [...]       # standard Crossplane conditions
```

## Key Types

- `CrossplaneBackend` -- implements `iac_forge::Backend` trait (unit struct)
- `CrossplaneNaming` -- naming convention: PascalCase types, kebab-case files, snake_case fields

## Type Mappings (IacType -> OpenAPI v3 JSON Schema)

```
IacType::String       -> { "type": "string" }
IacType::Integer      -> { "type": "integer", "format": "int64" }
IacType::Float        -> { "type": "number", "format": "double" }
IacType::Boolean      -> { "type": "boolean" }
IacType::List(T)      -> { "type": "array", "items": <T> }
IacType::Set(T)       -> { "type": "array", "items": <T>, "uniqueItems": true }
IacType::Map(T)       -> { "type": "object", "additionalProperties": <T> }
IacType::Object       -> { "type": "object", "properties": {...}, "required": [...] }
IacType::Enum         -> { "type": <underlying>, "enum": [...] }
IacType::Any          -> { "type": "object", "x-kubernetes-preserve-unknown-fields": true }
```

## Configuration

Platform-specific config via `provider.platform_config["crossplane"]`:
- `group` -- CRD API group (default: `<provider>.io`)
- `api_version` -- CRD version (default: `v1alpha1`)
- `scope` -- CRD scope (default: `Namespaced`)

## Source Layout

```
src/
  lib.rs        # Public API re-exports (CrossplaneBackend)
  backend.rs    # Backend trait implementation + naming convention
  crd.rs        # CRD YAML generation (iac_type_to_schema, generate_resource_crd, etc.)
```

## Usage

```rust
use crossplane_forge::CrossplaneBackend;
use iac_forge::Backend;

let backend = CrossplaneBackend;
let artifacts = backend.generate_resource(&resource, &provider)?;
// artifacts[0].content is the CRD YAML string
// artifacts[0].path is e.g. "static-secret-crd.yaml"
```

## File Naming

- Resources: `{kebab-case-name}-crd.yaml`
- Provider: `{name}-providerconfig-crd.yaml`

## Testing

Run: `cargo test`
