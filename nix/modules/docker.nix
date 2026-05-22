{ inputs, ... }:
{
  perSystem = { config, self', pkgs, lib, system, ... }:
    let
      # Extract just the Rust gdb pretty-printer Python files from the toolchain
      # so the prod image can ship `rust-gdb` without dragging in the entire
      # Rust toolchain (~hundreds of MB of rustc + stdlib sources).
      rust-gdb-printers = pkgs.runCommand "rust-gdb-printers" { } ''
        mkdir -p $out/share/rust-gdb
        cp ${self'.packages.rust-toolchain}/lib/rustlib/etc/*.py $out/share/rust-gdb/
      '';
      rust-gdb = pkgs.writeShellScriptBin "rust-gdb" ''
        PYTHONPATH="''${PYTHONPATH:+$PYTHONPATH:}${rust-gdb-printers}/share/rust-gdb" \
        exec ${pkgs.gdb}/bin/gdb \
          -d "${rust-gdb-printers}/share/rust-gdb" \
          -iex "add-auto-load-safe-path ${rust-gdb-printers}/share/rust-gdb" \
          "$@"
      '';
    in
    {
    packages.silo-docker = pkgs.dockerTools.buildLayeredImage {
      name = "silo";
      tag = "latest";

      contents = [
        self'.packages.silo
        # Include CA certificates for TLS
        pkgs.cacert
        # Debugger + Rust pretty-printer wrapper for attaching to silo in prod.
        # Adds ~50MB for gdb; the rust-gdb wrapper itself is tiny.
        pkgs.gdb
        rust-gdb
      ];

      # pprof CPU profiler needs /tmp for temporary files during profile collection
      fakeRootCommands = ''
        mkdir -p ./tmp
        chmod 1777 ./tmp
      '';

      config = {
        Entrypoint = [ "${self'.packages.silo}/bin/silo" ];
        ExposedPorts = {
          "7450/tcp" = { };
          "8080/tcp" = { };
        };
        Env = [
          "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
        ];
      };
    };

    # Dev/debug image with utilities for load testing and troubleshooting
    packages.silo-docker-dev =
      let
        devEnv = pkgs.buildEnv {
          name = "silo-dev-env";
          paths = [
            # use debug silo for faster builds
            self'.packages.silo-debug
            self'.packages.siloctl
            pkgs.cacert
            # Shell and core utilities
            pkgs.bashInteractive
            pkgs.coreutils
            pkgs.findutils
            pkgs.gnugrep
            pkgs.gnused
            pkgs.gawk
            # Networking tools
            pkgs.curl
            pkgs.wget
            pkgs.dnsutils
            pkgs.netcat
            pkgs.iproute2
            # Editors
            pkgs.vim
            # Process inspection
            pkgs.procps
            pkgs.htop
            # Debugging
            pkgs.strace
            pkgs.less
            pkgs.jq
            # grpcurl for testing gRPC endpoints
            pkgs.grpcurl
            # tokio-console for inspecting silo's async runtime
            pkgs.tokio-console
            # Archive tools
            pkgs.gnutar
            pkgs.gzip
          ];
          pathsToLink = [ "/bin" "/etc" "/share" ];
        };
      in
      pkgs.dockerTools.buildLayeredImage {
        name = "silo-dev";
        tag = "latest";

        contents = [ devEnv ];

        config = {
          Entrypoint = [ "${devEnv}/bin/bash" ];
          Cmd = [ "-l" ];
          Env = [
            "SSL_CERT_FILE=${devEnv}/etc/ssl/certs/ca-bundle.crt"
            "PATH=${devEnv}/bin"
          ];
        };
      };

    packages.silo-autoscaler-docker = pkgs.dockerTools.buildLayeredImage {
      name = "silo-autoscaler";
      tag = "latest";

      contents = [
        self'.packages.silo-autoscaler
        pkgs.cacert
      ];

      config = {
        Entrypoint = [ "${self'.packages.silo-autoscaler}/bin/silo-autoscaler" ];
        Env = [
          "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
        ];
      };
    };

    packages.silo-compactor-docker = pkgs.dockerTools.buildLayeredImage {
      name = "silo-compactor";
      tag = "latest";

      contents = [
        self'.packages.silo-compactor
        pkgs.cacert
      ];

      config = {
        Entrypoint = [ "${self'.packages.silo-compactor}/bin/silo-compactor" ];
        Env = [
          "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
        ];
      };
    };
  };
}
