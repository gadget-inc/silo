#!/bin/bash
set -e

# This script starts the background services and waits for the databases to be ready

direnv allow

# start background services
direnv exec . dev 2>&1 > /tmp/background.log &
