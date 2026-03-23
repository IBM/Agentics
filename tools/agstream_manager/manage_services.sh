#!/bin/bash

# Wrapper script for manage_services_full.sh
# This allows calling from the root directory while the actual script is in scripts/

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Forward all arguments to the actual script
exec "$SCRIPT_DIR/scripts/manage_services_full.sh" "$@"

# Made with Bob
