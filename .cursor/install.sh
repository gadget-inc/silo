#!/bin/bash
set -e

# Make direnv work which will setup dev env and download all the nix stuff
direnv allow

# Build once to download and cache dependencies
direnv exec . cargo build

# Install TypeScript client dependencies
cd typescript_client && direnv exec . pnpm install
