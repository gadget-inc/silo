FROM mcr.microsoft.com/vscode/devcontainers/base:bookworm

# use bash to run commands in the dockerfile so we can easily source the nix environment
SHELL ["/bin/bash", "-c"]

# install dependencies nix needs to install itself plus systemd
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends xz-utils acl systemd systemd-sysv

# setup the default vscode user that codespaces uses as a passwordless sudoer
RUN usermod -aG sudo vscode && echo "vscode   ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers
USER vscode
ENV USER=vscode

# Install nix (requires non-root user)
RUN curl -L https://nixos.org/nix/install | sh -s -- --no-daemon && \
  echo 'source ~/.nix-profile/etc/profile.d/nix.sh' >> ~/.bashrc && \
  echo 'source ~/.nix-profile/etc/profile.d/nix.sh' >> ~/.zshrc

# Enable required Nix experimental features, avoid permission denied errors from nix sandboxing & pin the nixpkgs flake
ENV NIXPKGS_COMMIT=02032da4af073d0f6110540c8677f16d4be0117f
RUN source ~/.nix-profile/etc/profile.d/nix.sh && \
  nix-env -f https://github.com/NixOS/nixpkgs/archive/$NIXPKGS_COMMIT.tar.gz -iA nix && \
  mkdir -p ~/.config/nix && \
  echo 'experimental-features = nix-command flakes' >> ~/.config/nix/nix.conf && \
  echo 'trusted-users = root $USER' >> ~/.config/nix/nix.conf && \
  echo 'sandbox = false' >> ~/.config/nix/nix.conf && \
  nix registry pin nixpkgs github:NixOS/nixpkgs/$NIXPKGS_COMMIT

# Install & setup direnv
RUN source ~/.nix-profile/etc/profile.d/nix.sh && \
  nix profile install nixpkgs#direnv nixpkgs#nix-direnv && \
  echo 'eval "$(direnv hook bash)"' >> ~/.bashrc && \
  echo 'eval "$(direnv hook zsh)"' >> ~/.zshrc && \
  echo 'source ~/.nix-profile/share/nix-direnv/direnvrc' >> ~/.direnvrc

# Install docker and setup buildx as default docker builder
RUN source ~/.nix-profile/etc/profile.d/nix.sh && \
  nix profile install nixpkgs#docker && \
  docker buildx install && \
  sudo setfacl --remove-default /tmp && \
  sudo locale-gen en_US.UTF-8 || true && \
  nix profile install nixpkgs#cachix && \
  sudo usermod -aG docker vscode

# Install k3s (lightweight Kubernetes)
RUN curl -sfL https://get.k3s.io | sudo INSTALL_K3S_SKIP_START=true INSTALL_K3S_SKIP_ENABLE=true sh -
