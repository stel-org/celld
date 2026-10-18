# Base Worker artifact execution integration

## Scope

B3.1.1 proves one narrow cross-repository boundary: a JavaScript ES module
artifact produced by the `stel-org/base-worker` fork can run in celld's
existing JavaScript host. The proof uses Base Worker revision
`a18ce33715e30cd2b0d99c7e322ef11a65490e4d` and celld revision
`153071ce97aed598687d00b72057d8829fe9c52a` as its starting point.

This is an artifact boundary, not an attempt to link the two Rust runtimes.
Base Worker currently uses its own Deno V8 facade; celld keeps its existing V8
engine and does not depend on Base Worker's Cargo workspace.

## Verified B3.1.1 proof

The initial proof used the debug executable at
`target/debug/deno` built from that Base Worker revision. It reported Base
Worker `deno 2.9.7`, V8 `15.0.245.2-rusty`, and TypeScript `6.0.3`. The fork
downloaded its own required `@esbuild/linux-x64` version `0.25.5` package on
the first bundle invocation. No system `deno` executable participated.

For the fixture in this repository, the generated artifact was 379 bytes with
SHA-256 `debed3e12c1444ee00c5ec196e1cf3b489e208005a5cf1f066da40d8a95e2b43`.
celld loaded that artifact through its normal V8 host and returned the expected
200 JSON response from `GET https://base.test/bridge`.

## Responsibilities

Base Worker owns source processing for this proof:

- TypeScript processing;
- local module-graph resolution; and
- browser-target ES module artifact preparation.

celld owns execution:

- loading the generated ES module through `WorkerConfigOptions::src`;
- its existing V8 isolate and Web API bootstrap;
- host-operation registration;
- stateless Worker pool and request dispatch; and
- the resulting HTTP response.

Neither repository embeds the other's runtime. In particular, celld does not
instantiate Deno `JsRuntime`, does not add a second V8 engine, and does not
gain Deno host operations through a bundle.

## Reproduction

Build the fork binary from the Base Worker checkout:

```sh
cd /absolute/path/to/base-worker
cargo build -p deno --bin deno
```

Then run the celld proof from its checkout:

```sh
BASE_WORKER_REPO=/absolute/path/to/base-worker \
  scripts/prove-base-worker.sh
```

`BASE_WORKER_BIN` may point at an already built, absolute fork binary. The
script does not search `PATH` and never substitutes a system `deno`. It records
the Base Worker revision, executable path and version, fixture entrypoint,
bundle command, SHA-256 and size. The derived bundle remains in a temporary
directory and is not committed.

The command is equivalent to:

```sh
/absolute/path/to/base-worker/target/debug/deno bundle \
  --no-config --no-lock --no-remote --no-npm \
  --platform=browser --format=esm \
  -o /temporary/worker.js \
  crates/celld/tests/fixtures/base-worker/index.ts
```

The fixture imports a local `message.ts`, uses a TypeScript interface, and
exports a default `fetch` handler. The proof supplies the generated bundle as
`BASE_WORKER_ARTIFACT` to the ignored `base_worker_bridge` integration test.
That test constructs the standard `WorkerConfig`, starts `StatelessRuntime`,
and makes `GET https://base.test/bridge`. It requires an HTTP 200 JSON response:

```json
{"message":"Hello from Base Worker","path":"/bridge"}
```

No KV, D1, R2, Queue or Workflow binding is configured. This preserves the
B3.0.4-C Base-native product boundary.

## Negative coverage

The proof script verifies that a missing fork executable fails without a
system-Deno fallback. It also asks Base Worker to bundle an invalid TypeScript
fixture and a fixture with a missing local import; both must fail before celld
is invoked. The ordinary celld test suite verifies that invalid JavaScript
submitted through `WorkerConfig` is rejected by the existing host compiler.

## Limitations and next step

The proof does not demonstrate JSR or npm resolution, Deno-specific host APIs,
`Deno.serve`, persistent Worker configuration, Base Project source storage,
Branch ownership of executions, multiple Workers per Base Cell, or production
serving and deployment. A JavaScript bundle is not full Deno-runtime
compatibility.

B3.1.2 should build on this evidence by defining a reusable, still
artifact-only Base Worker build/execution integration. It should not change
the Branch--Cell model or assume one Worker per Cell.
