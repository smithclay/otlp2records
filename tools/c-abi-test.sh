#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
features="grpc,otap-zstd"
cargo build --features "$features"
native_libs="$(cargo rustc --lib --features "$features" --crate-type staticlib -- --print native-static-libs 2>&1 \
    | sed -n 's/.*native-static-libs: //p' | tail -1)"
out="${TMPDIR:-/tmp}/otlp2records-c-abi"
mkdir -p "$out"
${CC:-cc} -std=c11 -Wall -Wextra -Werror -g -fsanitize=address,undefined -fno-omit-frame-pointer \
    -I include tests/c_abi/abi_test.c target/debug/libotlp2records.a $native_libs -o "$out/abi_test"
ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 "$out/abi_test" "$root"
