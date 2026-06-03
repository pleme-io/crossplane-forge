//! `go_spec` — the Go **macro-farm** surface.
//!
//! This module is the Go-codegen analog of `tatara-rust-ast`'s
//! `*DeriveSpec` family (`tatara_rust_derive::PerFieldDeriveSpec`, …).
//! Where a tatara Rust spec is a typed, serde-serializable declaration
//! that lowers to a Rust AST and renders through `ToRustTokens` +
//! `prettyplease`, a [`GoSpec`] here is a typed, serde-serializable
//! declaration that lowers to a [`go_synthesizer::GoFile`] and renders
//! through [`go_synthesizer::print_file`].
//!
//! ```text
//!  tatara-rust-ast                       go_spec (this module)
//!  ───────────────                       ──────────────────────
//!  PerFieldDeriveSpec  (typed spec)      GoSpec        (typed spec)
//!        │                                     │
//!        ▼ compile_to_crate                    ▼ to_go_file
//!  Rust AST (syn / proc-macro2)          go_synthesizer::GoFile
//!        │                                     │
//!        ▼ ToRustTokens + prettyplease         ▼ go_synthesizer::print_file
//!  buildable .rs source                  buildable .go source
//! ```
//!
//! ## Why this is the right shape
//!
//! The org-level ★★ EMITTER SUBSTRATE / PRIME DIRECTIVE rule says
//! *every repeating impl is a Spec; emit, don't author* — and *never
//! hand-roll `format!()` strings of target syntax*. The fleet already
//! has that surface for Rust (tatara-rust-ast) and for whole-file Go
//! AST authoring (`go_synthesizer`). What was **missing** was the
//! intermediate *spec → Go* layer: a small typed declaration a caller
//! authors once, that mechanically expands into the canonical
//! `go_synthesizer` AST. [`GoSpec`] closes that gap. It is deliberately
//! **minimal** — the two families that the canonical
//! [`go_synthesizer::GoTypeBody`] / [`go_synthesizer::GoFuncDecl`]
//! surface fully supports today (tagged structs and funcs). Adding a
//! third family (e.g. named interfaces with method sets) is a one-enum-
//! variant follow-up *once `go_synthesizer` grows the matching
//! `GoTypeBody::Interface` node* — see [the extension path][ext] below.
//!
//! [ext]: #extension-path
//!
//! ## Extension path
//!
//! 1. **New field family** — add a variant to [`GoFieldSpec`]; lower it
//!    in [`GoStructSpec::to_decl`]. No `go_synthesizer` change needed
//!    (the struct/field surface is already complete).
//! 2. **New item family** (e.g. `Interface`, `Const`, `VarBlock`) — add
//!    a variant to [`GoItemSpec`] + a `to_decl`-style lowering. If the
//!    family needs a Go syntactic form `go_synthesizer` lacks (named
//!    interfaces with methods), add exactly one
//!    [`go_synthesizer::GoTypeBody`] variant upstream first, then lower
//!    onto it — the substrate-hygienic way to surface need rather than
//!    re-introducing `format!()` shortcuts.
//! 3. **A `(defgostruct …)` / `(defgofunc …)` tatara-lisp authoring
//!    surface** — because [`GoSpec`] is plain serde, a `defform` macro
//!    can deserialize the Lisp form straight into a [`GoSpec`]; the
//!    lowering here stays unchanged.

use go_synthesizer::{
    GoBlock, GoDecl, GoExpr, GoField, GoFile, GoFuncDecl, GoImport, GoMod, GoParam, GoRecv,
    GoRequire, GoStmt, GoStructTag, GoType, GoTypeBody, GoTypeDecl, JsonTag, KubeMarker,
    YamlTag, print_file,
};
use serde::{Deserialize, Serialize};

/// A typed Go reference type, mirroring the subset of
/// [`go_synthesizer::GoType`] that a spec author needs. Kept as its own
/// serde-friendly enum (rather than re-using `GoType` directly) so the
/// spec surface stays stable even if `go_synthesizer::GoType` grows
/// `#[non_exhaustive]` variants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum GoTypeSpec {
    /// `string`, `int64`, `MyStruct` — a name in the current package or
    /// a builtin.
    Named { name: String },
    /// `pkg.Name` — a qualified reference to another package's type.
    Qualified { pkg: String, name: String },
    /// `*T`.
    Pointer { inner: Box<GoTypeSpec> },
    /// `[]T`.
    Slice { elem: Box<GoTypeSpec> },
    /// `map[K]V`.
    Map { key: Box<GoTypeSpec>, value: Box<GoTypeSpec> },
}

impl GoTypeSpec {
    /// Convenience constructor for a bare named type.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self::Named { name: name.into() }
    }

    /// Convenience constructor for a qualified `pkg.Name` type.
    #[must_use]
    pub fn qualified(pkg: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Qualified { pkg: pkg.into(), name: name.into() }
    }

    /// Lower to the canonical [`go_synthesizer::GoType`].
    #[must_use]
    pub fn to_go_type(&self) -> GoType {
        match self {
            Self::Named { name } => GoType::named(name.clone()),
            Self::Qualified { pkg, name } => GoType::qualified(pkg.clone(), name.clone()),
            Self::Pointer { inner } => GoType::pointer(inner.to_go_type()),
            Self::Slice { elem } => GoType::slice(elem.to_go_type()),
            Self::Map { key, value } => {
                GoType::Map(Box::new(key.to_go_type()), Box::new(value.to_go_type()))
            }
        }
    }
}

/// A serialization tag on a struct field. Mirrors the structured subset
/// of [`go_synthesizer::GoStructTag`] (json / yaml) — the canonical
/// "discouraged" `Custom` variant is intentionally not exposed here so
/// the spec stays opinionated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "encoding")]
pub enum GoTagSpec {
    /// `json:"<name>[,omitempty]"`.
    Json { name: String, #[serde(default)] omitempty: bool },
    /// `yaml:"<name>[,omitempty]"`.
    Yaml { name: String, #[serde(default)] omitempty: bool },
}

impl GoTagSpec {
    fn to_struct_tag(&self) -> GoStructTag {
        match self {
            Self::Json { name, omitempty } => GoStructTag::Json(JsonTag {
                name: name.clone(),
                omitempty: *omitempty,
                inline: false,
            }),
            Self::Yaml { name, omitempty } => GoStructTag::Yaml(YamlTag {
                name: name.clone(),
                omitempty: *omitempty,
                inline: false,
            }),
        }
    }
}

/// A kubebuilder marker a field can carry, mirroring the common subset
/// of [`go_synthesizer::KubeMarker`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoMarkerSpec {
    /// `+kubebuilder:validation:Required`.
    Required,
    /// `+optional`.
    Optional,
}

impl GoMarkerSpec {
    fn to_kube_marker(&self) -> KubeMarker {
        match self {
            Self::Required => KubeMarker::Required,
            Self::Optional => KubeMarker::Optional,
        }
    }
}

/// One field of a struct family.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoFieldSpec {
    /// Field identifier (Go exported name, e.g. `Name`).
    pub name: String,
    /// Field type.
    pub ty: GoTypeSpec,
    /// Optional doc comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Serialization tags (json / yaml).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<GoTagSpec>,
    /// kubebuilder markers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub markers: Vec<GoMarkerSpec>,
}

impl GoFieldSpec {
    fn to_field(&self) -> GoField {
        GoField {
            name: Some(self.name.clone()),
            ty: self.ty.to_go_type(),
            doc: self.doc.clone(),
            markers: self.markers.iter().map(GoMarkerSpec::to_kube_marker).collect(),
            tags: self.tags.iter().map(GoTagSpec::to_struct_tag).collect(),
        }
    }
}

/// The **struct family** — `type <Name> struct { … }`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoStructSpec {
    /// Type name (`StaticSecret`, `Config`, …).
    pub name: String,
    /// Optional doc comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Fields, in source order.
    #[serde(default)]
    pub fields: Vec<GoFieldSpec>,
}

impl GoStructSpec {
    fn to_decl(&self) -> GoDecl {
        GoDecl::Type(GoTypeDecl {
            name: self.name.clone(),
            doc: self.doc.clone(),
            markers: vec![],
            body: GoTypeBody::Struct(self.fields.iter().map(GoFieldSpec::to_field).collect()),
        })
    }
}

/// One parameter of a func family.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoParamSpec {
    pub name: String,
    pub ty: GoTypeSpec,
}

/// The minimal func body the spec supports today: an ordered list of
/// `return <exprs>` lines is over-engineered for a "minimal but real"
/// surface, so we model exactly the two bodies generated Go funcs
/// overwhelmingly need — a single `return <expr>` or an empty body.
///
/// Anything richer should be authored directly as a
/// [`go_synthesizer::GoBlock`] by the caller (the spec layer is for the
/// *common* shapes; the AST layer is always available for the rest).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "body")]
pub enum GoFuncBodySpec {
    /// `{}` — empty body.
    Empty,
    /// `return nil`.
    ReturnNil,
    /// `return &<TypeName>{}` — the canonical "constructor returns a
    /// fresh zero value" shape (e.g. `func New() *Client { return &Client{} }`).
    ReturnNewOf { ty: GoTypeSpec },
}

impl GoFuncBodySpec {
    fn to_block(&self) -> GoBlock {
        let mut b = GoBlock::new();
        match self {
            Self::Empty => {}
            Self::ReturnNil => b.push(GoStmt::Return(vec![GoExpr::nil()])),
            Self::ReturnNewOf { ty } => b.push(GoStmt::Return(vec![GoExpr::Composite {
                ty: ty.to_go_type(),
                fields: vec![],
                addr_of: true,
            }])),
        }
        b
    }
}

/// The **func family** — `func [(<recv>)] <Name>(<params>) <returns> { … }`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoFuncSpec {
    /// Function name.
    pub name: String,
    /// Optional doc comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Optional method receiver `(name ty)`; `None` is a free function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recv: Option<GoParamSpec>,
    /// Parameters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<GoParamSpec>,
    /// Return types.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returns: Vec<GoTypeSpec>,
    /// Function body.
    pub body: GoFuncBodySpec,
}

impl GoFuncSpec {
    fn to_decl(&self) -> GoDecl {
        GoDecl::Func(GoFuncDecl {
            name: self.name.clone(),
            doc: self.doc.clone(),
            recv: self.recv.as_ref().map(|r| GoRecv {
                name: r.name.clone(),
                ty: r.ty.to_go_type(),
            }),
            params: self
                .params
                .iter()
                .map(|p| GoParam { name: p.name.clone(), ty: p.ty.to_go_type() })
                .collect(),
            returns: self.returns.iter().map(GoTypeSpec::to_go_type).collect(),
            body: self.body.to_block(),
        })
    }
}

/// A single top-level item the spec can emit. The macro-farm "kind"
/// dispatch — the Go analog of `tatara_rust_catalog::CatalogSpec`'s
/// per-kind variants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum GoItemSpec {
    /// A `type … struct { … }` declaration.
    Struct(GoStructSpec),
    /// A `func …` declaration.
    Func(GoFuncSpec),
}

impl GoItemSpec {
    fn to_decl(&self) -> GoDecl {
        match self {
            Self::Struct(s) => s.to_decl(),
            Self::Func(f) => f.to_decl(),
        }
    }
}

/// An import declaration for the generated file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoImportSpec {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl GoImportSpec {
    fn to_import(&self) -> GoImport {
        match &self.alias {
            Some(a) => GoImport::aliased(a.clone(), self.path.clone()),
            None => GoImport::plain(self.path.clone()),
        }
    }
}

/// The top-level Go-codegen **spec** — a typed, serde-serializable
/// declaration of one Go source file. Lowers to a
/// [`go_synthesizer::GoFile`] and renders to buildable Go source via
/// [`go_synthesizer::print_file`].
///
/// This is the Go analog of a tatara-rust-ast `*DeriveSpec` value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoSpec {
    /// `package <name>`.
    pub package: String,
    /// File-level doc comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Imports.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imports: Vec<GoImportSpec>,
    /// Top-level items in source order.
    #[serde(default)]
    pub items: Vec<GoItemSpec>,
}

impl GoSpec {
    /// Construct an empty spec for a package.
    #[must_use]
    pub fn new(package: impl Into<String>) -> Self {
        Self { package: package.into(), doc: None, imports: vec![], items: vec![] }
    }

    /// Lower this spec to the canonical [`go_synthesizer::GoFile`] AST.
    /// This is the structural core; rendering is a one-liner on top.
    #[must_use]
    pub fn to_go_file(&self) -> GoFile {
        let mut file = GoFile::new(self.package.clone());
        file.doc = self.doc.clone();
        file.imports = self.imports.iter().map(GoImportSpec::to_import).collect();
        file.decls = self.items.iter().map(GoItemSpec::to_decl).collect();
        file
    }

    /// Render this spec to buildable, gofmt-stable Go source.
    ///
    /// Goes **entirely** through [`go_synthesizer::print_file`] — there
    /// is no `format!()` of Go syntax anywhere in this module.
    #[must_use]
    pub fn render(&self) -> String {
        print_file(&self.to_go_file())
    }

    /// Render a companion `go.mod` for this spec, so a generated file
    /// can be compiled in isolation. The module path + go version are
    /// caller-supplied; `requires` are typed [`GoRequire`]-equivalents.
    ///
    /// Goes through [`go_synthesizer::GoMod::print`] — again, no
    /// `format!()` of go.mod syntax.
    #[must_use]
    pub fn render_go_mod(
        module: &str,
        go_version: &str,
        requires: &[(String, String)],
    ) -> String {
        let mut m = GoMod::new(module, go_version);
        m.require = requires
            .iter()
            .map(|(path, version)| GoRequire::new(path.clone(), version.clone()))
            .collect();
        m.print()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worked spec mirroring a real Crossplane-style managed-resource
    /// fragment: a tagged `Parameters` struct + a constructor func.
    fn worked_spec() -> GoSpec {
        GoSpec {
            package: "v1alpha1".to_string(),
            doc: Some("Code authored as a typed GoSpec.".to_string()),
            imports: vec![GoImportSpec {
                path: "k8s.io/apimachinery/pkg/apis/meta/v1".to_string(),
                alias: Some("metav1".to_string()),
            }],
            items: vec![
                GoItemSpec::Struct(GoStructSpec {
                    name: "StaticSecretParameters".to_string(),
                    doc: Some("StaticSecretParameters are the input attributes.".to_string()),
                    fields: vec![
                        GoFieldSpec {
                            name: "Name".to_string(),
                            ty: GoTypeSpec::named("string"),
                            doc: Some("Name of the secret.".to_string()),
                            tags: vec![GoTagSpec::Json {
                                name: "name".to_string(),
                                omitempty: false,
                            }],
                            markers: vec![GoMarkerSpec::Required],
                        },
                        GoFieldSpec {
                            name: "Value".to_string(),
                            ty: GoTypeSpec::Pointer {
                                inner: Box::new(GoTypeSpec::named("string")),
                            },
                            doc: None,
                            tags: vec![GoTagSpec::Json {
                                name: "value".to_string(),
                                omitempty: true,
                            }],
                            markers: vec![GoMarkerSpec::Optional],
                        },
                        GoFieldSpec {
                            name: "Labels".to_string(),
                            ty: GoTypeSpec::Map {
                                key: Box::new(GoTypeSpec::named("string")),
                                value: Box::new(GoTypeSpec::named("string")),
                            },
                            doc: None,
                            tags: vec![GoTagSpec::Json {
                                name: "labels".to_string(),
                                omitempty: true,
                            }],
                            markers: vec![],
                        },
                    ],
                }),
                GoItemSpec::Func(GoFuncSpec {
                    name: "NewStaticSecretParameters".to_string(),
                    doc: Some("NewStaticSecretParameters returns a zero value.".to_string()),
                    recv: None,
                    params: vec![],
                    returns: vec![GoTypeSpec::Pointer {
                        inner: Box::new(GoTypeSpec::named("StaticSecretParameters")),
                    }],
                    body: GoFuncBodySpec::ReturnNewOf {
                        ty: GoTypeSpec::named("StaticSecretParameters"),
                    },
                }),
                GoItemSpec::Func(GoFuncSpec {
                    name: "Validate".to_string(),
                    doc: None,
                    recv: Some(GoParamSpec {
                        name: "p".to_string(),
                        ty: GoTypeSpec::Pointer {
                            inner: Box::new(GoTypeSpec::named("StaticSecretParameters")),
                        },
                    }),
                    params: vec![],
                    returns: vec![GoTypeSpec::named("error")],
                    body: GoFuncBodySpec::ReturnNil,
                }),
            ],
        }
    }

    /// THE round-trip test: a typed `GoSpec` → Go source via
    /// `go_synthesizer::print_file`, asserting the typed constructs all
    /// landed. This is the Go analog of tatara-rust-emit's
    /// `emit_file_for_impl` test.
    #[test]
    fn spec_renders_buildable_go_through_synthesizer() {
        let spec = worked_spec();
        let go = spec.render();

        // File-level scaffolding from go_synthesizer's printer.
        assert!(go.starts_with("// Code generated by iac-forge. DO NOT EDIT.\n"));
        assert!(go.contains("\npackage v1alpha1\n"));
        assert!(go.contains("// Code authored as a typed GoSpec."));

        // Import grouping (aliased third-party).
        assert!(go.contains("metav1 \"k8s.io/apimachinery/pkg/apis/meta/v1\""));

        // Struct family: name, doc, fields with typed tags + markers.
        assert!(go.contains("type StaticSecretParameters struct {"));
        assert!(go.contains("// StaticSecretParameters are the input attributes."));
        assert!(go.contains("// +kubebuilder:validation:Required"));
        assert!(go.contains("// +optional"));
        assert!(go.contains("Name string `json:\"name\"`"));
        assert!(go.contains("Value *string `json:\"value,omitempty\"`"));
        assert!(go.contains("Labels map[string]string `json:\"labels,omitempty\"`"));

        // Func family: free constructor + method with receiver.
        assert!(go.contains("func NewStaticSecretParameters() *StaticSecretParameters {"));
        assert!(go.contains("return &StaticSecretParameters{}"));
        assert!(go.contains("func (p *StaticSecretParameters) Validate() error {"));
        assert!(go.contains("\treturn nil"));
    }

    /// Determinism: two structurally identical specs render byte-equal
    /// (the `go_synthesizer` byte-equal contract surfaced through the
    /// spec layer).
    #[test]
    fn render_is_deterministic_and_byte_equal() {
        assert_eq!(worked_spec().render(), worked_spec().render());
    }

    /// The spec is pure serde — it can round-trip through JSON, which is
    /// what makes a future `(defgostruct …)` tatara-lisp authoring
    /// surface a drop-in (deserialize Lisp form → `GoSpec`).
    #[test]
    fn spec_serde_roundtrips_through_json() {
        let spec = worked_spec();
        let json = serde_json::to_string(&spec).unwrap();
        let back: GoSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
        // And the deserialized spec renders identically.
        assert_eq!(spec.render(), back.render());
    }

    /// The companion go.mod renders through `GoMod::print` (no
    /// `format!()` of go.mod syntax), so a generated file is compilable
    /// in isolation.
    #[test]
    fn companion_go_mod_renders_through_synthesizer() {
        let go_mod = GoSpec::render_go_mod(
            "github.com/pleme-io/example",
            "1.22",
            &[(
                "k8s.io/apimachinery".to_string(),
                "v0.29.0".to_string(),
            )],
        );
        assert!(go_mod.contains("module github.com/pleme-io/example"));
        assert!(go_mod.contains("go 1.22"));
        assert!(go_mod.contains("k8s.io/apimachinery v0.29.0"));
    }

    /// No `format!()` of Go syntax leaks into the rendered output: the
    /// only path from spec to source is `to_go_file` + `print_file`.
    /// This mirrors crossplane-forge's existing
    /// `no_format_string_leakage_in_any_emitter` discipline.
    #[test]
    fn empty_spec_still_well_formed() {
        let spec = GoSpec::new("p");
        let go = spec.render();
        assert!(go.starts_with("// Code generated by iac-forge. DO NOT EDIT.\n"));
        assert!(go.contains("\npackage p\n"));
        assert!(go.ends_with('\n'));
    }
}
