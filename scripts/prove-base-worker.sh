#!/usr/bin/env bash
# Prove that a TypeScript artifact produced by the Base Worker fork executes
# through celld's existing V8 host. This script never uses a system Deno.
set -euo pipefail

fail() {
  printf 'Base Worker integration proof failed: %s\n' "$*" >&2
  exit 1
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
celld_repo=$(cd -- "$script_dir/.." && pwd)
base_worker_repo=${BASE_WORKER_REPO:-}

[[ -n "$base_worker_repo" ]] || fail "BASE_WORKER_REPO must be an absolute Base Worker checkout path"
[[ "$base_worker_repo" = /* ]] || fail "BASE_WORKER_REPO must be absolute: $base_worker_repo"
[[ -d "$base_worker_repo" ]] || fail "Base Worker repository is unavailable: $base_worker_repo"
git -C "$base_worker_repo" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || fail "BASE_WORKER_REPO is not a Git checkout: $base_worker_repo"

if [[ -n ${BASE_WORKER_BIN:-} ]]; then
  base_worker_bin=$BASE_WORKER_BIN
  [[ "$base_worker_bin" = /* ]] || fail "BASE_WORKER_BIN must be absolute: $base_worker_bin"
  [[ -x "$base_worker_bin" ]] || fail "Base Worker executable unavailable: repository=$base_worker_repo binary=$base_worker_bin"
else
  base_worker_bin="$base_worker_repo/target/debug/deno"
  if [[ ! -x "$base_worker_bin" ]]; then
    (cd "$base_worker_repo" && cargo build -p deno --bin deno) \
      || fail "Base Worker executable unavailable after fork build: repository=$base_worker_repo binary=$base_worker_bin"
  fi
fi

if [[ ${1:-} == --check-worker ]]; then
  "$base_worker_bin" --version
  exit 0
fi

temporary_dir=$(mktemp -d "${TMPDIR:-/tmp}/celld-base-worker.XXXXXX")
cleanup() {
  rm -rf -- "$temporary_dir"
}
trap cleanup EXIT

# Ensure the unavailable-binary path remains a hard failure and cannot fall
# back to a system `deno` executable.
if BASE_WORKER_REPO="$base_worker_repo" BASE_WORKER_BIN="$temporary_dir/missing-deno" \
  "$0" --check-worker >/dev/null 2>&1; then
  fail "a missing Base Worker executable was accepted"
fi
printf 'Missing Base Worker executable: rejected as expected\n'

fixture_dir="$celld_repo/crates/celld/tests/fixtures/base-worker"
artifact="$temporary_dir/base-worker-bridge.js"
bundle_command=("$base_worker_bin" bundle --no-config --no-lock --no-remote --no-npm --platform=browser --format=esm -o "$artifact" "$fixture_dir/index.ts")

printf 'Base Worker repository: %s\n' "$base_worker_repo"
printf 'Base Worker revision: %s\n' "$(git -C "$base_worker_repo" rev-parse HEAD)"
printf 'Base Worker executable: %s\n' "$base_worker_bin"
"$base_worker_bin" --version
printf 'Fixture entrypoint: %s\n' "$fixture_dir/index.ts"
printf 'Bundle command:'
printf ' %q' "${bundle_command[@]}"
printf '\n'
"${bundle_command[@]}"

[[ -s "$artifact" ]] || fail "Base Worker did not produce a JavaScript artifact"
grep -Fq 'Hello from Base Worker' "$artifact" || fail "bundle omitted the local TypeScript dependency"
if grep -Fq 'interface WorkerMessage' "$artifact"; then
  fail "bundle retained TypeScript-only interface syntax"
fi
printf 'Bundle SHA-256: %s\n' "$(sha256sum "$artifact" | awk '{print $1}')"
printf 'Bundle size: %s bytes\n' "$(wc -c < "$artifact")"

for negative_fixture in invalid-syntax.ts missing-import.ts; do
  if "$base_worker_bin" bundle --no-config --no-lock --no-remote --no-npm --platform=browser --format=esm \
    -o "$temporary_dir/$negative_fixture.js" "$fixture_dir/$negative_fixture"; then
    fail "Base Worker unexpectedly bundled $negative_fixture"
  fi
  printf 'Base Worker rejected %s as expected\n' "$negative_fixture"
done

(cd "$celld_repo" && BASE_WORKER_ARTIFACT="$artifact" \
  cargo test -p celld --test base_worker_bridge -- --ignored --nocapture)
printf 'Base Worker to celld V8 integration proof passed\n'
