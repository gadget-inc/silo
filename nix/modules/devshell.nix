{ inputs, ... }:
{
  perSystem = { config, self', pkgs, lib, ... }: {
    devShells.default = pkgs.mkShell {
      name = "silo-shell";
      inputsFrom = [
        self'.devShells.rust
      ];
      packages = with pkgs; [
        just
        nixd # Nix language server
        bacon
        etcd
        protobuf # provides protoc binary
        bzip2 # required by slatedb dependencies for compression
        alloy6 # Alloy verification language for formal specs
      ];
    };
  };
}
