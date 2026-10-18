#!/usr/bin/env bash
# Prove locked JSR resolution by the Base Worker fork and execution by celld.
set -euo pipefail

fail() {
  printf 'Base Worker JSR integration proof failed: %s\n' "$*" >&2
  exit 1
}

expect_failure() {
  local description=$1
  shift
  if "$@" >"$temporary_dir/$description.log" 2>&1; then
    fail "$description unexpectedly succeeded"
  fi
  printf '%s: rejected as expected\n' "$description"
  sed -n '1,8p' "$temporary_dir/$description.log"
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

temporary_dir=$(mktemp -d "${TMPDIR:-/tmp}/celld-base-worker-jsr.XXXXXX")
cleanup() {
  rm -rf -- "$temporary_dir"
}
trap cleanup EXIT

fixture_dir="$celld_repo/crates/celld/tests/fixtures/base-worker-jsr"
lockfile="$fixture_dir/deno.lock"
artifact="$temporary_dir/jsr-worker.js"
repeated_artifact="$temporary_dir/jsr-worker-repeat.js"
cold_cache="$temporary_dir/cold-cache"
local_fixture="$celld_repo/crates/celld/tests/fixtures/base-worker-reusable/typescript-worker.ts"

bundle_command=(
  "$base_worker_bin" bundle
  --no-config
  --no-npm
  --allow-import=jsr.io
  --lock "$lockfile"
  --frozen-lockfile
  --platform=browser
  --format=esm
  -o "$artifact"
  "$fixture_dir/index.ts"
)

printf 'Base Worker revision: %s\n' "$(git -C "$base_worker_repo" rev-parse HEAD)"
printf 'Base Worker executable: %s\n' "$base_worker_bin"
"$base_worker_bin" --version
printf 'JSR fixture: %s\n' "$fixture_dir/index.ts"
printf 'JSR lockfile: %s\n' "$lockfile"
printf 'Bundle command:'
printf ' %q' "${bundle_command[@]}"
printf '\n'

DENO_DIR="$cold_cache" "${bundle_command[@]}"
[[ -s "$artifact" ]] || fail "Base Worker did not produce a JSR JavaScript artifact"
grep -Fq 'function concat' "$artifact" \
  || fail "the JSR package functionality was not bundled"
if grep -Fq 'jsr:@std/bytes@1.0.6' "$artifact"; then
  fail "the generated artifact retained an unresolved JSR import"
fi
printf 'Cold-cache bundle SHA-256: %s\n' "$(sha256sum "$artifact" | awk '{print $1}')"
printf 'Cold-cache bundle size: %s bytes\n' "$(wc -c < "$artifact")"

repeat_command=("${bundle_command[@]}")
for index in "${!repeat_command[@]}"; do
  if [[ ${repeat_command[$index]} == "$artifact" ]]; then
    repeat_command[$index]=$repeated_artifact
  fi
done
DENO_DIR="$cold_cache" "${repeat_command[@]}"
cmp -- "$artifact" "$repeated_artifact" \
  || fail "a repeated preparation produced a different JSR artifact"
printf 'Repeated locked preparation: identical artifact\n'

seed_esbuild_cache() {
  local cache_directory=$1
  local artifact_path=$2
  DENO_DIR="$cache_directory" "$base_worker_bin" bundle \
    --no-config --no-lock --no-remote --no-npm --platform=browser --format=esm \
    -o "$artifact_path" "$local_fixture" >/dev/null
}

expect_failure missing-jsr-package \
  env DENO_DIR="$temporary_dir/missing-package-cache" "$base_worker_bin" bundle \
  --no-config --no-lock --no-npm --allow-import=jsr.io --platform=browser --format=esm \
  -o "$temporary_dir/missing-package.js" "$fixture_dir/missing-package.ts"
expect_failure unavailable-jsr-version \
  env DENO_DIR="$temporary_dir/unavailable-version-cache" "$base_worker_bin" bundle \
  --no-config --no-lock --no-npm --allow-import=jsr.io --platform=browser --format=esm \
  -o "$temporary_dir/unavailable-version.js" "$fixture_dir/unavailable-version.ts"

offline_env=(
  env
  HTTPS_PROXY=http://127.0.0.1:9
  https_proxy=http://127.0.0.1:9
  HTTP_PROXY=http://127.0.0.1:9
  http_proxy=http://127.0.0.1:9
  ALL_PROXY=http://127.0.0.1:9
  all_proxy=http://127.0.0.1:9
  NO_PROXY=
  no_proxy=
)
registry_only_cache="$temporary_dir/offline-registry-cache"
missing_jsr_cache="$temporary_dir/offline-cold-cache"
seed_esbuild_cache "$registry_only_cache" "$temporary_dir/registry-cache-seed.js"
seed_esbuild_cache "$missing_jsr_cache" "$temporary_dir/missing-jsr-cache-seed.js"
expect_failure unavailable-jsr-registry \
  "${offline_env[@]}" DENO_DIR="$registry_only_cache" "$base_worker_bin" bundle \
  --no-config --no-lock --no-npm --allow-import=jsr.io --platform=browser --format=esm \
  -o "$temporary_dir/offline-registry.js" "$fixture_dir/index.ts"
expect_failure missing-cached-jsr-dependency \
  "${offline_env[@]}" DENO_DIR="$missing_jsr_cache" "$base_worker_bin" bundle \
  --no-config --no-npm --allow-import=jsr.io --lock "$lockfile" --frozen-lockfile \
  --platform=browser --format=esm -o "$temporary_dir/offline-cold.js" "$fixture_dir/index.ts"

(cd "$celld_repo" && DENO_DIR="$cold_cache" BASE_WORKER_BIN="$base_worker_bin" \
  cargo test -p celld --test base_worker_artifact_integration \
  locked_jsr_dependency_prepares_and_executes_through_the_existing_host -- --ignored --nocapture)

BASE_WORKER_REPO="$base_worker_repo" BASE_WORKER_BIN="$base_worker_bin" \
  "$script_dir/prove-base-worker.sh"
BASE_WORKER_REPO="$base_worker_repo" BASE_WORKER_BIN="$base_worker_bin" \
  "$script_dir/prove-base-worker-artifacts.sh"

printf 'Base Worker JSR integration proof passed\n'
