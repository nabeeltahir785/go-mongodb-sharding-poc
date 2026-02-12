#!/bin/bash
# Generate a keyfile for MongoDB internal authentication.
# The keyfile must be owned by the mongodb user (999) and have permissions 400.

set -e

KEYFILE_DIR="$(cd "$(dirname "$0")/.." && pwd)/keyfile"
KEYFILE_PATH="${KEYFILE_DIR}/mongo-keyfile"

if [ -f "${KEYFILE_PATH}" ]; then
    echo "Keyfile already exists at ${KEYFILE_PATH}, skipping generation."
    exit 0
fi

mkdir -p "${KEYFILE_DIR}"

echo "Generating MongoDB keyfile..."
openssl rand -base64 756 > "${KEYFILE_PATH}"
chmod 400 "${KEYFILE_PATH}"

echo "Keyfile generated at ${KEYFILE_PATH}"
