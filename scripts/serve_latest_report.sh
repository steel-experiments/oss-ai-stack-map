#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS_DIR="${ROOT_DIR}/docs"

if [[ ! -f "${DOCS_DIR}/index.html" ]]; then
  echo "missing ${DOCS_DIR}/index.html; render a report first" >&2
  exit 1
fi

if [[ "${EUID}" -eq 0 ]]; then
  tailscale serve --bg "${DOCS_DIR}" >/dev/null
  tailscale serve status
else
  sudo tailscale serve --bg "${DOCS_DIR}" >/dev/null
  sudo tailscale serve status
fi
