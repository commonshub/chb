#!/usr/bin/env bash
set -euo pipefail

REPO="CommonsHub/chb"
DEFAULT_BIN_DIR="/usr/local/bin"
FALLBACK_BIN_DIR="${HOME}/.local/bin"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

detect_os() {
  local os
  os="$(uname -s)"
  case "$os" in
    Linux) echo "linux" ;;
    *)
      echo "Unsupported OS: $os" >&2
      exit 1
      ;;
  esac
}

detect_arch() {
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *)
      echo "Unsupported architecture: $arch" >&2
      exit 1
      ;;
  esac
}

fetch_latest_tag() {
  curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" |
    sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' |
    head -n1
}

verify_checksum() {
  local file="$1"
  local checksums="$2"
  local basename
  basename="$(basename "$file")"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$file")" && sha256sum -c "$checksums" --ignore-missing --status)
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    local expected actual
    expected="$(grep " ${basename}\$" "$checksums" | awk '{print $1}')"
    if [[ -z "$expected" ]]; then
      echo "Checksum entry not found for ${basename}" >&2
      exit 1
    fi
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"
    [[ "$expected" == "$actual" ]] || {
      echo "Checksum mismatch for ${basename}" >&2
      exit 1
    }
    return
  fi

  echo "No sha256 tool found; skipping checksum verification" >&2
}

pick_bin_dir() {
  if [[ -n "${BIN_DIR:-}" ]]; then
    echo "$BIN_DIR"
    return
  fi

  if [[ -w "$DEFAULT_BIN_DIR" ]] || [[ ! -e "$DEFAULT_BIN_DIR" && -w "$(dirname "$DEFAULT_BIN_DIR")" ]]; then
    echo "$DEFAULT_BIN_DIR"
    return
  fi

  echo "$FALLBACK_BIN_DIR"
}

main() {
  need_cmd curl
  need_cmd tar
  need_cmd install

  local os arch version version_no_v asset_name base_url tmpdir archive checksums bin_dir extracted

  os="$(detect_os)"
  arch="$(detect_arch)"

  version="${VERSION:-}"
  if [[ -z "$version" ]]; then
    version="$(fetch_latest_tag)"
  fi
  if [[ -z "$version" ]]; then
    echo "Could not determine latest release tag" >&2
    exit 1
  fi

  version_no_v="${version#v}"
  asset_name="chb_${version_no_v}_${os}_${arch}"
  base_url="https://github.com/${REPO}/releases/download/${version}"

  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT

  archive="${tmpdir}/${asset_name}.tar.gz"
  checksums="${tmpdir}/checksums.txt"

  echo "Downloading ${asset_name}.tar.gz from ${version}"
  curl -fsSL -o "$archive" "${base_url}/${asset_name}.tar.gz"

  if curl -fsSL -o "$checksums" "${base_url}/checksums.txt"; then
    verify_checksum "$archive" "$checksums"
  else
    echo "checksums.txt not found; continuing without verification" >&2
  fi

  tar -xzf "$archive" -C "$tmpdir"
  extracted="${tmpdir}/${asset_name}"
  [[ -f "$extracted" ]] || {
    echo "Downloaded archive did not contain ${asset_name}" >&2
    exit 1
  }

  bin_dir="$(pick_bin_dir)"
  mkdir -p "$bin_dir"
  install "$extracted" "${bin_dir}/chb"

  echo "Installed chb to ${bin_dir}/chb"
  "${bin_dir}/chb" --version || true

  if [[ "$bin_dir" == "$FALLBACK_BIN_DIR" ]]; then
    case ":${PATH}:" in
      *":${FALLBACK_BIN_DIR}:"*) ;;
      *)
        echo
        echo "Add ${FALLBACK_BIN_DIR} to your PATH:"
        echo "  export PATH=\"${FALLBACK_BIN_DIR}:\$PATH\""
        ;;
    esac
  fi
}

main "$@"
