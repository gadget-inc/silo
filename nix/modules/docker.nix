{ inputs, ... }:
{
  perSystem = { config, self', pkgs, lib, system, ... }: {
    packages.silo-docker = pkgs.dockerTools.buildLayeredImage {
      name = "silo";
      tag = "latest";

      contents = [
        self'.packages.silo
        # Include CA certificates for TLS
        pkgs.cacert
      ];

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
  };
}

