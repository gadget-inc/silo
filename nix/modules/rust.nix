{ inputs, ... }:
{
  imports = [
    inputs.rust-flake.flakeModules.default
    inputs.rust-flake.flakeModules.nixpkgs
  ];
  perSystem = { config, self', pkgs, lib, ... }:
    let
      # Get crane lib for the filter function
      craneLib = config.rust-project.crane.lib;
      
      # Custom source filter that includes proto files alongside Rust sources
      protoFilter = path: _type:
        (builtins.match ".*\\.proto$" path != null) ||  # .proto files
        (builtins.match ".*/proto$" path != null) ||    # proto directory
        (builtins.match ".*/proto/.*" path != null);    # files inside proto/
      sourceFilter = path: type:
        (protoFilter path type) || (craneLib.filterCargoSources path type);
      
      # Source with proto files included
      srcWithProto = lib.cleanSourceWith {
        src = craneLib.cleanCargoSource inputs.self;
        filter = sourceFilter;
      };
    in
    {
      packages.default = self'.packages.silo;

      # Production build with k8s feature enabled
      rust-project.crates.silo.crane.args = {
        cargoExtraArgs = "--features k8s";
        # protoc is needed for etcd-client and tonic-build
        nativeBuildInputs = [ pkgs.protobuf ];
        # Include proto files in the source
        src = srcWithProto;
      };
    };
}
