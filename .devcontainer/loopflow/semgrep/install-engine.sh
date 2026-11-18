#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/engine-version.txt"
DESTINATION="/usr/local/bin/opengrep"

if [ ! -f "$VERSION_FILE" ]; then
    echo "Missing pinned engine version file: ${VERSION_FILE}" >&2
    exit 1
fi

version="$(tr -d '[:space:]' < "$VERSION_FILE")"

if command -v opengrep >/dev/null 2>&1; then
    actual="$(opengrep --version | tr -d '[:space:]')"
    if [ "$actual" = "$version" ]; then
        exit 0
    fi
fi

case "$(dpkg --print-architecture)" in
    amd64)
        asset="opengrep_linux_x86"
        sha256="37e93f4da5fd04a18c5c1febbdd12f7e98dfccd4d0c9cf2593bcb25eeb462da0"
        ;;
    arm64)
        asset="opengrep_linux_aarch64"
        sha256="6257b65423a744b704af2ff9beeb8f222c2f6925236af386b56b031d4c06d6d8"
        ;;
    *)
        echo "Unsupported architecture: $(dpkg --print-architecture)" >&2
        exit 1
        ;;
esac

run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

if ! command -v curl >/dev/null 2>&1; then
    run_as_root apt-get update
    run_as_root apt-get install -y --no-install-recommends curl ca-certificates
    run_as_root rm -rf /var/lib/apt/lists/*
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

curl -fsSL -o "$tmp" \
    "https://github.com/opengrep/opengrep/releases/download/v${version}/${asset}"
echo "${sha256}  ${tmp}" | sha256sum -c -
run_as_root install -m 0755 "$tmp" "$DESTINATION"

actual="$(opengrep --version | tr -d '[:space:]')"
if [ "$actual" != "$version" ]; then
    echo "Opengrep version mismatch: expected ${version}, got ${actual}" >&2
    exit 1
fi
