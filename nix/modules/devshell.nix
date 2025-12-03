{ inputs, ... }:
{
  perSystem = { config, self', pkgs, lib, ... }:
    let
      # Custom packages not in nixpkgs
      gubernator = pkgs.buildGoModule rec {
        pname = "gubernator";
        version = "2.13.0";

        src = pkgs.fetchFromGitHub {
          owner = "gubernator-io";
          repo = pname;
          rev = "v${version}";
          hash = "sha256-SsIVqXwOZ55/lo8cIlTWQ0qlSRYsd4pBLfbiOooGf+Q=";
        };

        vendorHash = "sha256-gNUYe6IWMvr1laqP8yFmKX87RdvLnWaXB1L5GEbAUQI=";

        # don't run the tests when building for speed
        doCheck = false;

        meta = with lib; {
          description = "Rate limiting service";
          homepage = "https://github.com/gubernator-io/gubernator";
          license = licenses.free;
          platforms = platforms.all;
        };
      };

      # Script to run development services via process-compose
      devScript = pkgs.writeShellScriptBin "dev" ''
        # Find the project root by looking for the process-compose.yml file
        PROJECT_ROOT="''${SILO_PROJECT_ROOT:-$(pwd)}"
        if [ ! -f "$PROJECT_ROOT/process-compose.yml" ]; then
          echo "Error: process-compose.yml not found in $PROJECT_ROOT"
          echo "Run this command from the silo project root."
          exit 1
        fi
        # Pass PATH via process substitution so process-compose subprocesses can find nix binaries
        ${pkgs.process-compose}/bin/process-compose up -f "$PROJECT_ROOT/process-compose.yml" -e <(echo "PATH=$PATH") "$@"
      '';
    in
    {
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
          process-compose
          gubernator
          devScript
          nodejs_24 # For validation scripts
        ];

        # Set the project root for the dev script
        shellHook = ''
          export SILO_PROJECT_ROOT="$(pwd)"
        '';
      };
    };
}
