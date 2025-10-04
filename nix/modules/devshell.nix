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
      ];
    };
  };
}
