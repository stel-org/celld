# Base Worker artifact execution integration

## Scope

B3.1.1 proved one narrow cross-repository boundary: a JavaScript ES module
artifact produced by the `stel-org/base-worker` fork can run in celld's
existing JavaScript host. B3.1.2 turned that proof into a reusable local
artifact-preparation boundary. B3.1.3 extends that same boundary with one
explicit, lockfile-bound JSR resolution mode. The integration uses Base Worker
revision `a18ce33715e30cd2b0d99c7e322ef11a65490e4d` and starts B3.1.3 from
celld revision `957a3a50221d0380406091936b6123a6d74b45c8`.

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

## Reusable preparation boundary

`celld::base_worker_artifact::BaseWorkerArtifactPreparer` is the B3.1.2
technical boundary. It accepts two explicit absolute paths:

1. the chosen Base Worker fork executable; and
2. a selected local JavaScript or TypeScript entrypoint.

It invokes Base Worker's existing `deno bundle` implementation with the fixed
local browser-ESM profile. It returns `BaseWorkerArtifact`, which contains the
canonical entrypoint, selected executable, generated UTF-8 JavaScript,
SHA-256, and byte length. The caller transfers `artifact.into_source()` into
the existing `WorkerConfigOptions::src` field; no second Worker definition or
execution path exists.

The adapter does not search `PATH`, cache a previous artifact, or invoke a
system `deno`. A failed preparation returns a `PreparationFailed` error with
the selected entrypoint, executable, exit status, and Base Worker diagnostics.
An unreadable, empty, or non-UTF-8 generated artifact has a distinct error.
celld host loading errors remain separate from preparation errors.

The implementation is source-agnostic: the entrypoint is an input parameter,
not a fixture-specific condition. Tests prove independent TypeScript and
JavaScript entrypoints with different local dependencies and different output.

## Locked JSR preparation

`BaseWorkerArtifactPreparer::prepare()` remains the B3.1.2 local-only profile.
It passes `--no-remote` and `--no-npm` to Base Worker, so accepting JSR for one
preparation cannot silently weaken every preparation.

`BaseWorkerArtifactPreparer::prepare_with_mode()` adds the deliberately narrow
`BaseWorkerPreparationMode::JsrLocked { lockfile }` alternative. It requires
an absolute existing Deno-compatible lockfile, canonicalizes that path, and
records the mode with the returned artifact. It then invokes the selected Base
Worker executable with:

```sh
deno bundle --no-config --no-npm --allow-import=jsr.io \
  --lock /absolute/path/to/deno.lock --frozen-lockfile \
  --platform=browser --format=esm -o /temporary/worker.js entrypoint.ts
```

This is not a celld package resolver or cache. Base Worker owns JSR registry
access, exact-version selection, dependency-graph resolution, lockfile
validation, and its Deno cache. celld receives only the resulting JavaScript
as `WorkerConfigOptions::src`, exactly as it does for local artifacts.

The accepted fixture is
`crates/celld/tests/fixtures/base-worker-jsr/index.ts`. It imports the exact
`jsr:@std/bytes@1.0.6` package and calls its `concat()` function during a real
request. Its committed `deno.lock` selects `@std/bytes@1.0.6` and records the
JSR integrity `f6ac6adbd8ccd99314045f5703e23af0a68d7f7e58364b47d2c7f408aeb5820a`.
The lockfile is dependency-selection and integrity evidence; the generated
artifact SHA-256 is only an identity for one generated JavaScript result and
does not by itself prove dependency provenance.

With an empty `DENO_DIR`, the successful proof downloaded Base Worker's
required esbuild helper and the JSR metadata/modules, then produced a 776-byte
artifact with SHA-256
`fea3005b687f66cf851b1af5b0463efedc0b0b735b9a1690cb74a3f06640e703`.
Preparing again with the same `DENO_DIR` produced byte-identical JavaScript.
The generated artifact contains the bundled `concat` implementation and no
`jsr:@std/bytes@1.0.6` import. celld then loaded it through the normal V8 host
and `GET https://base.test/jsr` returned:

```json
{"message":"Hello from JSR","path":"/jsr"}
```

The output depends on `concat()` joining the two encoded message segments, so
the response is not merely evidence that an unresolved JSR specifier was
carried into celld.

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

For the reusable B3.1.2 proof, use:

```sh
BASE_WORKER_REPO=/absolute/path/to/base-worker \
  scripts/prove-base-worker-artifacts.sh
```

The script supplies the chosen fork executable to the ignored integration
tests. They prepare and execute both `typescript-worker.ts` and
`javascript-worker.js` through the same `BaseWorkerArtifactPreparer`, then
verify their separate JSON responses. Set `BASE_WORKER_BIN` to an already-built
absolute fork binary to avoid a build; the script still refuses PATH lookup.

For the JSR proof, run:

```sh
BASE_WORKER_REPO=/absolute/path/to/base-worker \
  BASE_WORKER_BIN=/absolute/path/to/base-worker/target/debug/deno \
  scripts/prove-base-worker-jsr.sh
```

The script uses a fresh temporary `DENO_DIR` for the initial locked preparation,
performs the same preparation again from that cache, runs the ignored JSR test
that sends a real request through `StatelessRuntime`, and runs the B3.1.1 and
B3.1.2 proof scripts as regressions. It never selects a system `deno`.

## Negative coverage

The proof script verifies that a missing fork executable fails without a
system-Deno fallback. It also asks Base Worker to bundle an invalid TypeScript
fixture and a fixture with a missing local import; both must fail before celld
is invoked. The ordinary celld test suite verifies that invalid JavaScript
submitted through `WorkerConfig` is rejected by the existing host compiler.

The JSR proof additionally verifies that Base Worker rejects a nonexistent
package and unavailable exact package version. It seeds a cache with only the
Base Worker esbuild helper, blocks network access through an unreachable local
proxy, then verifies that both an unlocked JSR registry attempt and a locked
preparation with missing cached JSR modules fail. These are preparation
failures, not celld execution failures. The underlying Deno CLI currently
reports its generic unresolved-path diagnostic after the failed registry
attempt; celld preserves this in `PreparationFailed` rather than relabeling it
as a host or V8 error.

## Limitations and next step

The proof demonstrates only explicitly locked JSR imports from `jsr.io`.
It does not demonstrate npm resolution, arbitrary remote module fetching,
Deno-specific host APIs, `Deno.serve`, persistent Worker configuration, Base
Project source storage, Branch ownership of executions, multiple Workers per
Base Cell, or production serving and deployment. It also does not provide an
offline-only Deno flag: the selected Base Worker CLI does not expose
`--cached-only` for `bundle`, so cache availability is demonstrated through a
fresh cache and deliberately unreachable registry instead. A JavaScript bundle
is not full Deno-runtime compatibility.

A subsequent integration can reuse `JsrLocked` when it has product-owned
source selection and lockfile lifecycle semantics. It must continue to keep
Base Worker responsible for preparation and celld responsible for execution.

## R8.2-C retained HTTP delivery

The paired Base control adapter now receives `base-celld` v7 buffered HTTP
delivery for an existing retained Worker realization. This is an adapter around
the established `WorkerRealizationRegistry::invoke()` path: request method,
path/query, body and ordered headers pass to its existing `StatelessRuntime`,
and the resulting status, field-line headers and bytes return through the
authenticated response. It is not a second JavaScript runtime or a public HTTP
listener.

v7 uses ordered `{name, valueBase64}` fields and authenticates them with the
same length-prefixed header material Base uses. Values that the existing string
host cannot represent as UTF-8 are rejected before invocation, never lossy
converted. Buffered request/response bodies are limited to 64 KiB; streaming,
SSE and WebSockets remain R8.3 work. The focused test in
`base_control::tests::v7_receiver_executes_a_real_retained_worker_with_http_headers`
executes a real retained Worker and proves its `201`, JSON `Content-Type`,
`Location`, repeated `Set-Cookie` fields and response proof.

## B3.2.1 Base-defined Worker invocation

B3.2.1 consumes the existing artifact boundary from a separately built Base
process. Base first selects a persisted Project-owned Worker definition,
revalidates its registered Source, resolves the relative entrypoint, and runs
the selected Base Worker fork with the existing local-only browser-ESM bundle
profile. celld does not receive the Source path and does not resolve TypeScript
or application imports. It receives only newly generated UTF-8 ESM bytes plus
their verified SHA-256 identity through the paired local control socket.

The control adapter's version-3 `executeWorker` operation has a signed metadata
envelope and an exact-length binary artifact phase. Artifact bytes are limited
to 512 KiB and their SHA-256 is recomputed before `WorkerConfigOptions::src`
is populated. The operation then constructs the existing `WorkerConfig` and
uses the existing `StatelessRuntime` request path. It returns a signed bounded
HTTP result. It does not add an artifact store, module registry, deployment
schema, scheduler, another V8 host, Deno `JsRuntime`, Cloudflare product
binding, or public ingress.

The reproducible real-process acceptance command lives in Base because the
test begins with durable Base State definitions:

```sh
cd /absolute/path/to/base
CELLD_REPO=/absolute/path/to/celld \
BASE_WORKER_REPO=/absolute/path/to/base-worker \
CELLD_ESBUILD=/absolute/path/to/esbuild \
scripts/prove-worker-execution.sh
```

It starts a temporary independent `celld dev` process with its normal private
pairing socket, creates one Base Project, Source, Branch, and two persisted
Workers, and checks different TypeScript/JavaScript response bodies. This is a
bounded stateless execution proof only. It does not establish persistent Worker
lifecycle, Worker Management interfaces, public serving, recovery, package
configuration, JSR selection in a Base Worker definition, or cardinality rules
between Branches, Cells, and future Worker realizations.

## B3.2.3 retained artifact execution

B3.2.3 consumes the same Base Worker artifact boundary without changing it.
Base prepares an ESM artifact externally, then the paired local Base-control
protocol can establish a bounded retained `StatelessRuntime` from that exact
artifact. Later invocations select the realization identity and do not resend,
rebuild, or replace the artifact. The normal celld host, V8 initialization,
isolate pool, and HTTP dispatch remain unchanged.

This only provides a local retained execution capability. It does not add
Base-source ownership to celld, Deno host APIs, package policy, public serving,
deployment records, automatic recovery, replicas, or a scheduler.
