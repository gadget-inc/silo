{ inputs, ... }:
{
  perSystem = { config, self', pkgs, lib, system, ... }:
    let
      # Set up rust overlay for the toolchain
      rustPkgs = import inputs.nixpkgs {
        inherit system;
        overlays = [ inputs.rust-overlay.overlays.default ];
      };
      
      # Get rust toolchain from rust-toolchain.toml
      rustToolchain = rustPkgs.rust-bin.fromRustupToolchainFile ../../rust-toolchain.toml;

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

      # Toxiproxy proxy definitions for dev (silo servers behind proxy ports)
      toxiproxyConfig = pkgs.writeText "toxiproxy-config.json" (builtins.toJSON [
        { name = "silo-1"; listen = "127.0.0.1:17450"; upstream = "127.0.0.1:7450"; enabled = true; }
        { name = "silo-2"; listen = "127.0.0.1:17451"; upstream = "127.0.0.1:7451"; enabled = true; }
      ]);

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
        packages = [
          rustToolchain
        ] ++ (with pkgs; [
          git
          just
          nixd # Nix language server
          bacon
          etcd
          protobuf # provides protoc binary
          flatbuffers # provides flatc binary for FlatBuffers schema compilation
          bzip2 # required by slatedb dependencies for compression
          alloy6 # Alloy verification language for formal specs
          process-compose
          gubernator
          devScript
          nodejs_24 # For validation scripts
          zx
          pnpm # Package manager for TypeScript client
          kubectl # for interacting with local K8S API server
          watchexec # Auto-reload for development
          grpcurl # For health checking gRPC servers
          toxiproxy # Network fault injection proxy for testing
        ]);

        # Set the project root for the dev script
        # Match RUSTFLAGS with rust.nix to avoid cache invalidation between nix build and local dev
        shellHook = ''
          export SILO_PROJECT_ROOT="$(pwd)"
          export RUSTFLAGS="-C force-frame-pointers=yes"
          export TOXIPROXY_CONFIG="${toxiproxyConfig}"
        '';
      };
    };
}
