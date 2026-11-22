#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
out="${TMPDIR:-/tmp}/otlp2records-duckdb"
rm -rf "$out"
cargo run --quiet --example export_for_duckdb --features parquet -- "$out"
uv run --quiet --no-project --with 'duckdb==1.5.*' --with 'pyarrow>=20' \
    python -I tests/duckdb/check.py "$out"
