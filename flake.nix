{
  description = "crossplane-forge — Crossplane CRD generator";

  # substrate.rust.library dispatches over the committed Cargo.gen.lock (the
  # slim gen delta, reconstructed to a full BuildSpec in PURE NIX) rather than
  # through crate2nix.
  #
  # ── WHY NOT `import "${substrate}/lib/rust-library.nix"` ──────────────────
  #
  # That entry point routes through crate2nix, which GENERATES its Cargo.nix by
  # building a derivation and reading the result back during evaluation — an
  # import-from-derivation. Measured here 2026-08-18, before this change:
  #
  #   error: cannot build '…-crossplane-forge-crate2nix.drv^out' during
  #   evaluation because the option 'allow-import-from-derivation' is disabled
  #
  # This is a SECOND, independent IFD mechanism from the one substrate's
  # gen-pin bump removed: that one lived in gen's git-source handling, this one
  # lives in crate2nix's own generate step, so it survived that fix untouched.
  # Two distinct sub-mechanisms were measured across the fleet — this
  # generate-path one, and `mkGitHash` (crate2nix tools.nix:293) hashing a git
  # dependency by building a runCommand. Both disappear with the delta path,
  # which reads what it needs out of an artifact this repo already tracks.
  inputs.substrate.url = "github:pleme-io/substrate";

  outputs =
    { substrate, ... }:
    substrate.rust.library {
      src = ./.;
    };
}
