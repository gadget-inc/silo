{ inputs, ... }:
{
  perSystem = { config, self', pkgs, lib, system, ... }:
    let
      # Set up rust overlay for the toolchain from rust-toolchain.toml
      rustPkgs = import inputs.nixpkgs {
        inherit system;
        overlays = [ inputs.rust-overlay.overlays.default ];
      };
      
      # Get rust toolchain from rust-toolchain.toml
      rustToolchain = rustPkgs.rust-bin.fromRustupToolchainFile ../../rust-toolchain.toml;
      
      # Create crane lib with our toolchain
      craneLib = (inputs.crane.mkLib pkgs).overrideToolchain rustToolchain;
      
      # Source filter that includes proto files, FlatBuffers schemas and templates alongside Rust sources
      extraFilter = path: _type:
        # Proto files
        (lib.hasSuffix ".proto" path) ||
        (builtins.match ".*/proto$" path != null) ||
        (builtins.match ".*/proto/.*" path != null) ||
        # FlatBuffers schema files
        (lib.hasSuffix ".fbs" path) ||
        (builtins.match ".*/schema$" path != null) ||
        (builtins.match ".*/schema/.*" path != null) ||
        # Askama templates
        (lib.hasSuffix ".html" path) ||
        (builtins.match ".*/templates$" path != null) ||
        (builtins.match ".*/templates/.*" path != null);
      sourceFilter = path: type:
        (extraFilter path type) || (craneLib.filterCargoSources path type);
      
      src = lib.cleanSourceWith {
        src = inputs.self;
        filter = sourceFilter;
      };
      
      # Common args for crane builds
      commonArgs = {
        inherit src;
        strictDeps = true;
        nativeBuildInputs = [ pkgs.protobuf pkgs.flatbuffers ];
        # Enable frame pointers for profiling support - pprof uses frame-pointer
        # based unwinding, which requires the compiler to actually preserve them
        CARGO_PROFILE_RELEASE_CODEGEN_UNITS = "1";  # Better inlining visibility
        RUSTFLAGS = "-C force-frame-pointers=yes";
      };
      
      # Build dependencies separately for caching
      cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      
      # Main package
      silo = craneLib.buildPackage (commonArgs // {
        inherit cargoArtifacts;
        doCheck = false; # Tests require external services, CI runs them separately
      });
    in
    {
      packages.default = silo;
      packages.silo = silo;
    };
}
