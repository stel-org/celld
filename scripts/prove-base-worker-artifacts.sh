#!/usr/bin/env bash
# Run the reusable Base Worker artifact preparation and celld V8 execution proof.
set -euo pipefail

fail() {
  printf 'Reusable Base Worker artifact proof failed: %s\n' "$*" >&2
  exit 1
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
celld_repo=$(cd -- "$script_dir/.." && pwd)
base_worker_repo=${BASE_WORKER_REPO:-}

[[ -n "$base_worker_repo" && "$base_worker_repo" = /* ]] \
  || fail "BASE_WORKER_REPO must be an absolute Base Worker checkout path"
git -C "$base_worker_repo" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || fail "BASE_WORKER_REPO is not a Git checkout: $base_worker_repo"

if [[ -n ${BASE_WORKER_BIN:-} ]]; then
  base_worker_bin=$BASE_WORKER_BIN
  [[ "$base_worker_bin" = /* && -x "$base_worker_bin" ]] \
    || fail "Base Worker executable unavailable: $base_worker_bin"
else
  base_worker_bin="$base_worker_repo/target/debug/deno"
  if [[ ! -x "$base_worker_bin" ]]; then
    (cd "$base_worker_repo" && cargo build -p deno --bin deno) \
      || fail "Base Worker fork build failed: $base_worker_bin"
  fi
fi

printf 'Base Worker revision: %s\n' "$(git -C "$base_worker_repo" rev-parse HEAD)"
printf 'Base Worker executable: %s\n' "$base_worker_bin"
"$base_worker_bin" --version

(cd "$celld_repo" && BASE_WORKER_BIN="$base_worker_bin" \
  cargo test -p celld --test base_worker_artifact_integration -- --ignored --nocapture)

printf 'Reusable Base Worker artifact proof passed\n'
