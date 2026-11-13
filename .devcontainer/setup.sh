#!/usr/bin/env bash
# One-time devcontainer setup: corporate CAs, Rust toolchain/targets, pre-commit.
set -euxo pipefail

# Trust corporate TLS-inspection CAs, if the host provides any.
# Host dir ~/.devcontainer-ca is mounted at /opt/host-ca; every *.pem/*.crt in it is installed.
# Must run before anything else that downloads over HTTPS.
shopt -s nullglob
for f in /opt/host-ca/*.pem /opt/host-ca/*.crt; do
  [ -s "$f" ] && sudo cp "$f" "/usr/local/share/ca-certificates/host-$(basename "${f%.*}").crt"
done
sudo update-ca-certificates

# MSRV toolchain (matches Cargo.toml / CI) plus the WASM target used by CI
rustup toolchain install 1.90 --profile default --target wasm32-unknown-unknown
rustup default 1.90
rustup component add rustfmt clippy

# uv for Python tooling, then pre-commit hooks (best effort: never fail creation over these)
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh || echo "WARNING: uv install failed"
  export PATH="$HOME/.local/bin:$PATH"
fi
uvx pre-commit install || echo "WARNING: pre-commit install skipped"

echo "Devcontainer ready."
