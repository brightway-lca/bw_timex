#!/usr/bin/env bash
#
# Build the bw_timex conda package and upload it to the `diepers` channel.
#
# Prerequisites (once):
#   conda install -n base conda-build anaconda-client
#   anaconda login                       # authenticate uploads
#
# Usage (from anywhere):
#   bash conda/build_upload.sh
#
# The recipe reads its version from bw_timex/__init__.py, so bump that
# before building a new release.

set -euo pipefail

# Resolve paths so the script works regardless of the calling directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)/conda-out"

CHANNELS=(-c conda-forge -c cmutel -c diepers)

echo "==> Building bw_timex conda package"
conda build "${SCRIPT_DIR}" "${CHANNELS[@]}" --output-folder "${OUTPUT_DIR}"

# Ask conda-build for the artifact path without rebuilding.
PACKAGE_PATH="$(conda build "${SCRIPT_DIR}" "${CHANNELS[@]}" \
    --output-folder "${OUTPUT_DIR}" --output)"

echo "==> Uploading ${PACKAGE_PATH} to the 'diepers' channel"
anaconda upload --user diepers "${PACKAGE_PATH}"

echo "==> Done"
