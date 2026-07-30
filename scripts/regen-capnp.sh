#!/usr/bin/env bash

# Regenerate Cap'n Proto schema files
#
# This script regenerates Rust bindings from Cap'n Proto schema files.
# Run this whenever you modify any .capnp schema files.
#
# Requirements:
#   - Cap'n Proto compiler (capnp) must be installed
#   - capnpc-rust (installed via cargo build-dependencies)
#
# Usage:
#   ./scripts/regen-capnp.sh

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Read required version from tools.toml (single source of truth)
REQUIRED_VERSION="$(bash "$SCRIPT_DIR/tool-version.sh" capnp)"
CHECK_ONLY="${CAPNP_CHECK:-0}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
RUST_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.96.0}"
OUT_DIR_FILE="$(mktemp "${TMPDIR:-/tmp}/nautilus_out_dir.XXXXXX")"
BUILD_MESSAGES_FILE="$(mktemp "${TMPDIR:-/tmp}/nautilus_build_messages.XXXXXX")"
trap 'rm -f "$OUT_DIR_FILE" "$BUILD_MESSAGES_FILE"' EXIT

echo -e "${YELLOW}Regenerating Cap'n Proto schemas...${NC}"

# Check if capnp is installed
if ! command -v capnp &> /dev/null; then
  echo -e "${RED}Error: capnp compiler not found${NC}"
  echo "Please install Cap'n Proto ${REQUIRED_VERSION}:"
  echo "  - macOS: brew install capnp"
  echo "  - Linux: Install from source (Ubuntu's package is too old):"
  echo "      ./scripts/install-capnp.sh"
  echo "    Or manually: https://capnproto.org/install.html"
  exit 1
fi

# Verify installed version matches required version
INSTALLED_VERSION=$(capnp --version | awk '{print $NF}')
if [[ "$INSTALLED_VERSION" != "$REQUIRED_VERSION" ]]; then
  echo -e "${RED}Error: capnp version mismatch${NC}"
  echo "  Required: ${REQUIRED_VERSION} (from tools.toml)"
  echo "  Installed: ${INSTALLED_VERSION}"
  echo "Please install the correct version using: ./scripts/install-capnp.sh"
  exit 1
fi

echo "Using capnp: $(command -v capnp)"
echo "Version: $(capnp --version)"

# Navigate to project root
cd "${PROJECT_ROOT}"

# Force a clean rebuild of the serialization crate with capnp feature
echo -e "${YELLOW}Rebuilding serialization crate to regenerate schemas...${NC}"
RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN" cargo clean -p stratneo-nautilus-serialization
if ! RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN" cargo build \
  -p stratneo-nautilus-serialization \
  --features capnp \
  --message-format=json-render-diagnostics \
  > "$BUILD_MESSAGES_FILE"; then
  echo -e "${RED}Error: stratneo-nautilus-serialization build failed${NC}"
  exit 1
fi

jq -r '
  select(
    .reason == "build-script-executed"
    and (.package_id | contains("stratneo-nautilus-serialization"))
  )
  | .out_dir
' "$BUILD_MESSAGES_FILE" > "$OUT_DIR_FILE"

OUT_DIR=$(head -n 1 "$OUT_DIR_FILE")

# Fallback: search target/debug/build if json parsing failed
if [ -z "$OUT_DIR" ] || [ ! -d "$OUT_DIR" ]; then
  echo -e "${YELLOW}JSON parse failed, searching ${TARGET_DIR}/debug/build...${NC}"
  OUT_DIR=$(find "${TARGET_DIR}/debug/build" -type d -name "stratneo-nautilus-serialization-*" -path "*/out" | head -1)
fi

if [ -z "$OUT_DIR" ] || [ ! -d "$OUT_DIR" ]; then
  echo -e "${RED}Error: Could not find OUT_DIR for stratneo-nautilus-serialization${NC}"
  echo "Searched for: ${TARGET_DIR}/debug/build/stratneo-nautilus-serialization-*/out"
  exit 1
fi

echo "Found OUT_DIR: $OUT_DIR"

# Copy generated files to the repo
echo -e "${YELLOW}Cleaning existing generated files...${NC}"
rm -rf crates/serialization/generated/capnp/*
echo -e "${YELLOW}Copying generated files to repository...${NC}"
mkdir -p crates/serialization/generated/capnp
cp -r "${OUT_DIR}/"* crates/serialization/generated/capnp/

# Format the generated files (manual regen only, requires nightly)
if [ "$CHECK_ONLY" = "1" ]; then
  echo -e "${YELLOW}Skipping formatting during schema check...${NC}"
elif cargo +nightly fmt --version > /dev/null 2>&1; then
  echo -e "${YELLOW}Formatting generated files...${NC}"
  cargo +nightly fmt --manifest-path crates/serialization/Cargo.toml --all
else
  echo -e "${YELLOW}Warning: Nightly toolchain not found. Skipping formatting.${NC}"
  echo "Please run 'cargo +nightly fmt --manifest-path crates/serialization/Cargo.toml --all' manually after installing Rust nightly."
fi
# Show what was generated
echo -e "${GREEN}Successfully regenerated Cap'n Proto schemas!${NC}"
echo ""
echo "Generated files:"
find crates/serialization/generated/capnp -name "*.rs" | sort

# Check if there are any changes
if git diff --quiet crates/serialization/generated/capnp; then
  echo -e "${GREEN}No changes detected - schemas are up to date${NC}"
else
  echo -e "${YELLOW}Changes detected in generated files${NC}"
  echo "Review the changes with: git diff crates/serialization/generated/capnp"
fi
