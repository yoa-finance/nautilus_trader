#!/usr/bin/env bash
set -euo pipefail

# Build and publish the StratNeo Nautilus wheel to the private PyPI index.
#
# This script intentionally publishes only the package needed by backtest-runner:
#   nautilus-trader-stratneo, Python 3.12, Linux.
#
# Upload and download verification use the same convention as
# backtest-service's SDK publish workflow:
#   PYPI_SIMPLE_INDEX_URL
#   PYPI_USERNAME
#   PYPI_PASSWORD

PACKAGE_NAME="${PACKAGE_NAME:-nautilus-trader-stratneo}"
VERSION="${VERSION:-}"
DIST_DIR="${DIST_DIR:-dist-stratneo-publish}"
WHEEL_GLOB="${WHEEL_GLOB:-*.whl}"
BUILD_IN_DOCKER="${BUILD_IN_DOCKER:-1}"
SKIP_BUILD="${SKIP_BUILD:-0}"
UPLOAD="${UPLOAD:-1}"
VERIFY_DOWNLOAD="${VERIFY_DOWNLOAD:-1}"
PRECHECK_ONLY="${PRECHECK_ONLY:-0}"
UPLOAD_WITH_DOCKER="${UPLOAD_WITH_DOCKER:-1}"
UPLOAD_IN_POD="${UPLOAD_IN_POD:-0}"
K8S_NAMESPACE="${K8S_NAMESPACE:-trading}"
K8S_POD="${K8S_POD:-}"
K8S_POD_SELECTOR="${K8S_POD_SELECTOR:-app=backtest-api}"
K8S_CONTAINER="${K8S_CONTAINER:-}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"
ALLOW_NON_MASTER="${ALLOW_NON_MASTER:-0}"
DOCKER_IMAGE="${DOCKER_IMAGE:-python:3.12-slim}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"
RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-stable}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

info() {
  echo "==> $*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 not found in PATH"
}

project_version() {
  python3 - <<'PY'
import tomllib
from pathlib import Path

raw = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
print(str(raw["project"]["version"]).strip())
PY
}

validate_versions() {
  local expected="$1"
  python3 - "$expected" <<'PY'
import json
import re
import sys
import tomllib
from pathlib import Path

expected = sys.argv[1]
raw = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

checks = {
    "project.version": str(raw.get("project", {}).get("version", "")).strip(),
    "tool.poetry.version": str(raw.get("tool", {}).get("poetry", {}).get("version", "")).strip(),
}

build_rs = Path("crates/core/build.rs").read_text(encoding="utf-8")
match = re.search(r'let nautilus_version = "([^"]+)"', build_rs)
checks["crates/core/build.rs"] = match.group(1) if match else ""

version_json = json.loads(Path("version.json").read_text(encoding="utf-8"))
checks["version.json"] = str(version_json.get("message", "")).removeprefix("v")

failed = False
for name, actual in checks.items():
    if actual != expected:
        print(f"{name}: expected {expected!r}, got {actual!r}", file=sys.stderr)
        failed = True

if failed:
    raise SystemExit(1)

print(f"validated version {expected}")
PY
}

validate_git_state() {
  local branch
  branch="$(git branch --show-current)"
  if [[ "$branch" != "master" && "$ALLOW_NON_MASTER" != "1" ]]; then
    die "must run from master, current branch is ${branch}. Set ALLOW_NON_MASTER=1 to override."
  fi

  if [[ -n "$(git status --short)" && "$ALLOW_DIRTY" != "1" ]]; then
    git status --short >&2
    die "working tree is not clean. Commit first, or set ALLOW_DIRTY=1 for an emergency local publish."
  fi
}

clean_dist_dir() {
  [[ -n "$DIST_DIR" && "$DIST_DIR" != "/" && "$DIST_DIR" != "." ]] || die "unsafe DIST_DIR: ${DIST_DIR}"
  mkdir -p "$DIST_DIR"
  find "$DIST_DIR" -maxdepth 1 -type f -name "*.whl" -delete
}

build_wheel_docker() {
  require_command docker

  local host_uid host_gid
  host_uid="$(id -u)"
  host_gid="$(id -g)"

  local platform_args=()
  if [[ -n "$DOCKER_PLATFORM" ]]; then
    platform_args=(--platform "$DOCKER_PLATFORM")
  fi

  info "building ${PACKAGE_NAME}==${VERSION} in Docker (${DOCKER_IMAGE}, ${DOCKER_PLATFORM:-default platform})"
  docker run --rm \
    "${platform_args[@]}" \
    -v "${REPO_ROOT}:/work" \
    -v stratneo-nautilus-cargo:/cargo \
    -v stratneo-nautilus-rustup:/rustup \
    -w /work \
    -e CARGO_HOME=/cargo \
    -e RUSTUP_HOME=/rustup \
    -e RUSTUP_TOOLCHAIN="$RUSTUP_TOOLCHAIN" \
    -e BUILD_MODE=release \
    -e COPY_TO_SOURCE=true \
    -e CARGO_TARGET_DIR=/work/target/stratneo-publish-py312 \
    -e DIST_DIR="$DIST_DIR" \
    -e HOST_UID="$host_uid" \
    -e HOST_GID="$host_gid" \
    "$DOCKER_IMAGE" \
    bash -lc '
      set -euo pipefail
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        curl \
        file \
        gcc \
        git \
        libcurl4-openssl-dev \
        libssl-dev \
        pkg-config
      rm -rf /var/lib/apt/lists/*

      if ! command -v rustup >/dev/null 2>&1; then
        curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain "${RUSTUP_TOOLCHAIN}"
      fi
      . "${CARGO_HOME}/env"
      rustup default "${RUSTUP_TOOLCHAIN}"

      python -m pip install --upgrade pip
      uv_version="$(bash scripts/uv-version.sh)"
      python -m pip install "uv==${uv_version}"

      uv build --wheel --out-dir "${DIST_DIR}"
      chown -R "${HOST_UID}:${HOST_GID}" "${DIST_DIR}" target/stratneo-publish-py312 nautilus_trader/core || true
    '
}

build_wheel_local() {
  require_command uv
  info "building ${PACKAGE_NAME}==${VERSION} locally"
  BUILD_MODE=release \
    COPY_TO_SOURCE=true \
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target/stratneo-publish-py312}" \
    uv build --wheel --out-dir "$DIST_DIR"
}

validate_wheel_output() {
  local expected_prefix
  expected_prefix="${PACKAGE_NAME//-/_}-${VERSION}-"

  mapfile -t wheels < <(find "$DIST_DIR" -maxdepth 1 -type f -name "$WHEEL_GLOB" | sort)
  if [[ "${#wheels[@]}" -eq 0 ]]; then
    die "no wheel produced in ${DIST_DIR}"
  fi

  info "built wheels"
  printf '  %s\n' "${wheels[@]}"

  local wheel_name
  for wheel in "${wheels[@]}"; do
    wheel_name="$(basename "$wheel")"
    if [[ "$wheel_name" != "$expected_prefix"* ]]; then
      die "unexpected wheel name ${wheel_name}; expected prefix ${expected_prefix}"
    fi
  done
}

require_pypi_env() {
  [[ -n "${PYPI_SIMPLE_INDEX_URL:-}" ]] || die "PYPI_SIMPLE_INDEX_URL is required"
  [[ -n "${PYPI_USERNAME:-}" ]] || die "PYPI_USERNAME is required"
  [[ -n "${PYPI_PASSWORD:-}" ]] || die "PYPI_PASSWORD is required"
}

private_pypi_upload_url() {
  python3 - <<'PY'
import os

url = os.environ.get("PYPI_SIMPLE_INDEX_URL", "").strip()
if not url:
    raise SystemExit("PYPI_SIMPLE_INDEX_URL is required")
url = url.rstrip("/")
if url.endswith("/simple"):
    url = url[:-len("/simple")]
print(url)
PY
}

upload_artifacts() {
  local upload_url="$1"
  require_command python3

  python3 -m pip install --upgrade pip twine
  mapfile -t artifacts < <(find "$DIST_DIR" -maxdepth 1 -type f -name "$WHEEL_GLOB" | sort)
  if [[ "${#artifacts[@]}" -eq 0 ]]; then
    die "no wheels found in ${DIST_DIR}"
  fi

  local artifact output status
  for artifact in "${artifacts[@]}"; do
    info "uploading $(basename "$artifact")"
    set +e
    output="$(python3 -m twine upload --non-interactive \
      --repository-url "$upload_url" \
      -u "$PYPI_USERNAME" \
      -p "$PYPI_PASSWORD" \
      "$artifact" 2>&1)"
    status=$?
    set -e
    printf '%s\n' "$output"
    if [[ "$status" -eq 0 ]]; then
      continue
    fi
    case "$output" in
      *"409 Conflict"*|*"already exists"*)
        echo "[skip] $(basename "$artifact") already exists in private index"
        ;;
      *)
        exit "$status"
        ;;
    esac
  done
}

resolve_k8s_pod() {
  if [[ -n "$K8S_POD" ]]; then
    echo "$K8S_POD"
    return
  fi

  kubectl -n "$K8S_NAMESPACE" get pod \
    -l "$K8S_POD_SELECTOR" \
    -o jsonpath='{.items[0].metadata.name}'
}

kubectl_exec() {
  local pod="$1"
  shift

  if [[ -n "$K8S_CONTAINER" ]]; then
    kubectl -n "$K8S_NAMESPACE" exec "$pod" -c "$K8S_CONTAINER" -- "$@"
  else
    kubectl -n "$K8S_NAMESPACE" exec "$pod" -- "$@"
  fi
}

kubectl_cp_to_pod() {
  local src="$1"
  local pod="$2"
  local dst="$3"

  if [[ -n "$K8S_CONTAINER" ]]; then
    kubectl -n "$K8S_NAMESPACE" cp "$src" "$pod:$dst" -c "$K8S_CONTAINER"
  else
    kubectl -n "$K8S_NAMESPACE" cp "$src" "$pod:$dst"
  fi
}

upload_wheels_in_pod() {
  require_command kubectl

  local pod
  pod="$(resolve_k8s_pod)"
  [[ -n "$pod" ]] || die "no pod found for selector ${K8S_POD_SELECTOR} in namespace ${K8S_NAMESPACE}"

  mapfile -t wheels < <(find "$DIST_DIR" -maxdepth 1 -type f -name "$WHEEL_GLOB" | sort)
  if [[ "${#wheels[@]}" -eq 0 ]]; then
    die "no wheels found in ${DIST_DIR}"
  fi

  info "uploading wheels from pod ${K8S_NAMESPACE}/${pod}"
  kubectl_exec "$pod" python - <<'PY'
import os
import sys

missing = [
    name
    for name in ("PYPI_SIMPLE_INDEX_URL", "PYPI_USERNAME", "PYPI_PASSWORD")
    if not os.getenv(name)
]
if missing:
    raise SystemExit("missing pod env: " + ", ".join(missing))
print("validated pod private PyPI env")
PY

  local remote_dir upload_script
  remote_dir="/tmp/stratneo-wheel-publish-${VERSION}-$(date +%s)"
  upload_script="$(mktemp)"

  cat > "$upload_script" <<'REMOTE'
#!/usr/bin/env sh
set -eu

package_name="$1"
version="$2"
remote_dir="$3"
verify_download="$4"

python -m pip install --upgrade pip twine

upload_url="$(python - <<'PY'
import os

url = os.environ["PYPI_SIMPLE_INDEX_URL"].strip().rstrip("/")
if url.endswith("/simple"):
    url = url[:-len("/simple")]
print(url)
PY
)"

for artifact in "$remote_dir"/*.whl; do
  [ -f "$artifact" ] || continue
  echo "Uploading $(basename "$artifact")"
  set +e
  output="$(python -m twine upload --non-interactive \
    --repository-url "$upload_url" \
    -u "$PYPI_USERNAME" \
    -p "$PYPI_PASSWORD" \
    "$artifact" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output"
  if [ "$status" -eq 0 ]; then
    continue
  fi
  case "$output" in
    *"409 Conflict"*|*"already exists"*)
      echo "[skip] $(basename "$artifact") already exists in private index"
      ;;
    *)
      exit "$status"
      ;;
  esac
done

if [ "$verify_download" = "1" ]; then
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir" "$remote_dir"' EXIT
  pip_conf="$tmp_dir/pip.conf"
  python - "$pip_conf" <<'PY'
import os
import stat
import sys
import urllib.parse
from pathlib import Path

output = Path(sys.argv[1])
simple_url = os.environ["PYPI_SIMPLE_INDEX_URL"].strip()
username = os.environ["PYPI_USERNAME"].strip()
password = os.environ["PYPI_PASSWORD"].strip()
trusted_host = os.environ.get("PYPI_TRUSTED_HOST", "").strip()

parsed = urllib.parse.urlsplit(simple_url)
if parsed.scheme not in {"http", "https"} or not parsed.netloc:
    raise SystemExit("PYPI_SIMPLE_INDEX_URL must be an http(s) URL with a host")

netloc = parsed.netloc.rsplit("@", 1)[-1]
netloc = (
    f"{urllib.parse.quote(username, safe='')}:"
    f"{urllib.parse.quote(password, safe='')}@{netloc}"
)
index_url = urllib.parse.urlunsplit(
    (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
)

def escape(value: str) -> str:
    return value.replace("%", "%%")

lines = ["[global]", f"index-url = {escape(index_url)}"]
if parsed.scheme == "http":
    host = trusted_host or parsed.netloc.split(":", 1)[0]
    lines.append(f"trusted-host = {escape(host)}")

output.write_text("\n".join(lines) + "\n", encoding="utf-8")
output.chmod(stat.S_IRUSR | stat.S_IWUSR)
PY

  mkdir -p "$tmp_dir/download"
  PIP_CONFIG_FILE="$pip_conf" python -m pip download \
    --disable-pip-version-check \
    --no-cache-dir \
    "--only-binary=${package_name}" \
    --no-deps \
    --dest "$tmp_dir/download" \
    "${package_name}==${version}"
else
  rm -rf "$remote_dir"
fi
REMOTE

  kubectl_exec "$pod" sh -lc "rm -rf '$remote_dir' && mkdir -p '$remote_dir'"
  local wheel
  for wheel in "${wheels[@]}"; do
    kubectl_cp_to_pod "$wheel" "$pod" "$remote_dir/$(basename "$wheel")"
  done
  kubectl_cp_to_pod "$upload_script" "$pod" "$remote_dir/upload.sh"
  rm -f "$upload_script"

  kubectl_exec "$pod" sh "$remote_dir/upload.sh" "$PACKAGE_NAME" "$VERSION" "$remote_dir" "$VERIFY_DOWNLOAD"
}

upload_wheels() {
  [[ "$UPLOAD" == "1" ]] || {
    info "UPLOAD=0, skipping private PyPI upload"
    return
  }

  if [[ "$UPLOAD_IN_POD" == "1" ]]; then
    upload_wheels_in_pod
    return
  fi

  require_pypi_env
  local upload_url
  upload_url="$(private_pypi_upload_url)"

  if [[ "$UPLOAD_WITH_DOCKER" != "1" ]]; then
    info "uploading wheels to private PyPI at ${upload_url}"
    upload_artifacts "$upload_url"
    return
  fi

  require_command docker
  info "uploading wheels to private PyPI at ${upload_url}"

  local env_args=(
    -e "DIST_DIR=${DIST_DIR}"
    -e "PYPI_USERNAME=${PYPI_USERNAME}"
    -e "PYPI_PASSWORD=${PYPI_PASSWORD}"
    -e "PYPI_UPLOAD_URL=${upload_url}"
  )

  docker run --rm \
    -v "${REPO_ROOT}:/work" \
    -w /work \
    "${env_args[@]}" \
    "$DOCKER_IMAGE" \
    bash -lc '
      set -euo pipefail
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y --no-install-recommends ca-certificates
      rm -rf /var/lib/apt/lists/*
      python -m pip install --upgrade pip twine
      shopt -s nullglob
      artifacts=("${DIST_DIR}"/*.whl)
      if [ "${#artifacts[@]}" -eq 0 ]; then
        echo "ERROR: no wheels found in ${DIST_DIR}" >&2
        exit 1
      fi
      for artifact in "${artifacts[@]}"; do
        echo "Uploading $(basename "$artifact")"
        set +e
        output="$(python -m twine upload --non-interactive \
          --repository-url "$PYPI_UPLOAD_URL" \
          -u "$PYPI_USERNAME" \
          -p "$PYPI_PASSWORD" \
          "$artifact" 2>&1)"
        status=$?
        set -e
        printf "%s\n" "$output"
        if [ "$status" -eq 0 ]; then
          continue
        fi
        case "$output" in
          *"409 Conflict"*|*"already exists"*)
            echo "[skip] $(basename "$artifact") already exists in private index"
            ;;
          *)
            exit "$status"
            ;;
        esac
      done
    '
}

render_pip_conf_from_env() {
  local output="$1"
  python3 - "$output" <<'PY'
import os
import stat
import sys
import urllib.parse
from pathlib import Path

output = Path(sys.argv[1])
simple_url = os.environ.get("PYPI_SIMPLE_INDEX_URL", "").strip()
if not simple_url:
    raise SystemExit("PYPI_SIMPLE_INDEX_URL is required")

username = os.environ.get("PYPI_USERNAME", "").strip()
password = os.environ.get("PYPI_PASSWORD", "").strip()
trusted_host = os.environ.get("PYPI_TRUSTED_HOST", "").strip()

parsed = urllib.parse.urlsplit(simple_url)
if parsed.scheme not in {"http", "https"} or not parsed.netloc:
    raise SystemExit("PYPI_SIMPLE_INDEX_URL must be an http(s) URL with a host")

netloc = parsed.netloc.rsplit("@", 1)[-1]
if username or password:
    if not username or not password:
        raise SystemExit("PYPI_USERNAME and PYPI_PASSWORD must be set together")
    netloc = (
        f"{urllib.parse.quote(username, safe='')}:"
        f"{urllib.parse.quote(password, safe='')}@{netloc}"
    )

index_url = urllib.parse.urlunsplit(
    (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
)

def escape(value: str) -> str:
    return value.replace("%", "%%")

lines = ["[global]", f"index-url = {escape(index_url)}"]
if parsed.scheme == "http":
    host = trusted_host or parsed.netloc.split(":", 1)[0]
    lines.append(f"trusted-host = {escape(host)}")

output.write_text("\n".join(lines) + "\n", encoding="utf-8")
output.chmod(stat.S_IRUSR | stat.S_IWUSR)
PY
}

verify_download() {
  [[ "$VERIFY_DOWNLOAD" == "1" ]] || {
    info "VERIFY_DOWNLOAD=0, skipping pip download verification"
    return
  }

  require_command python3

  local tmp_dir pip_conf
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN

  require_pypi_env

  if [[ -n "${PIP_CONFIG_FILE:-}" ]]; then
    pip_conf="$PIP_CONFIG_FILE"
  else
    pip_conf="${tmp_dir}/pip.conf"
    render_pip_conf_from_env "$pip_conf"
  fi

  mkdir -p "${tmp_dir}/download"
  info "verifying ${PACKAGE_NAME}==${VERSION} is downloadable from private index"
  PIP_CONFIG_FILE="$pip_conf" python3 -m pip download \
    --disable-pip-version-check \
    --no-cache-dir \
    "--only-binary=${PACKAGE_NAME}" \
    --no-deps \
    --dest "${tmp_dir}/download" \
    "${PACKAGE_NAME}==${VERSION}"
}

main() {
  cd "$REPO_ROOT"
  require_command git
  require_command python3

  if [[ -z "$VERSION" ]]; then
    VERSION="$(project_version)"
  fi

  validate_git_state
  validate_versions "$VERSION"

  if [[ "$PRECHECK_ONLY" == "1" ]]; then
    info "precheck complete for ${PACKAGE_NAME}==${VERSION}"
    return
  fi

  if [[ "$SKIP_BUILD" == "1" ]]; then
    info "SKIP_BUILD=1, using existing wheels in ${DIST_DIR}"
  else
    clean_dist_dir

    if [[ "$BUILD_IN_DOCKER" == "1" ]]; then
      build_wheel_docker
    else
      build_wheel_local
    fi
  fi

  validate_wheel_output
  upload_wheels
  if [[ "$UPLOAD_IN_POD" == "1" && "$UPLOAD" == "1" ]]; then
    info "pod upload path handled download verification"
  else
    verify_download
  fi

  info "published ${PACKAGE_NAME}==${VERSION}"
}

main "$@"
