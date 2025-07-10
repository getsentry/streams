#!/bin/bash
set -e

# Function to print usage
usage() {
    echo "Usage: $0 {runner|profile} [args...]"
    echo "  runner  - Execute bin/runner with provided arguments"
    echo "  profile - Execute bin/pyspy_runner with provided arguments"
    exit 1
}

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    usage
fi

# Get the command (first argument)
COMMAND="$1"
shift  # Remove the command from arguments, leaving only the args

case "$COMMAND" in
    "runner")
        echo "Executing bin/runner with arguments: $@"
        exec bin/runner "$@"
        ;;
    "profile")
        echo "Executing bin/pyspy_runner with arguments: $@"
        exec bin/pyspy_runner "$@"
        ;;
    *)
        echo "Unknown command: $COMMAND"
        usage
        ;;
esac
