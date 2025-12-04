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
          "50051/tcp" = { };
          "8080/tcp" = { };
        };
        Env = [
          "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
        ];
      };
    };
  };
}

