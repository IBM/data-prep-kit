#!/usr/bin/env bash

set -euo pipefail

# Start cland
clamd

# Run commands
exec "$@"
