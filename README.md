# crossplane-forge

Crossplane CRD generator from IaC forge IR.

Implements [`iac_forge::Backend`] to produce Kubernetes
`CustomResourceDefinition` YAML from the `iac-forge` intermediate
representation. Each [`IacResource`](iac_forge::ir::IacResource) becomes a
namespaced CRD with `forProvider` (input) and `atProvider` (output) specs,
plus standard Crossplane conditions and printer columns.

Part of the pleme-io code-generation pipeline:

```
sekkei -> takumi -> openapi-forge / iac-forge -> backend renderers
```

## License

MIT — see [LICENSE](./LICENSE).
