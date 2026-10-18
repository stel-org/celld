# Base-native celld cleanup dependency map

## Purpose, baseline, and vocabulary

This is the B3.0.3 evidence record for a future Base-native cleanup. It was
prepared against `stel-org/celld` branch `run-to-launch-01` at
`e2738fe692c86deb05f4a3f33097625d459d6dfa`. It describes the implementation
that exists at that revision; it does not remove, rename, or replace it.

Base owns Project, Branch, Cell identity, and future execution intent. celld
owns execution decisions, actor effects, runtime placement, V8 hosting,
durability, ownership, and recovery. A Base Branch's Cell is neither a native
Git branch, celld scope, Worker, V8 isolate, nor process. In particular, this
document does not infer one Worker per Base Cell.

Classification used below:

| Classification | Meaning in this map |
| --- | --- |
| **PRESERVE** | Active infrastructure that is required independently of the inherited product taxonomy. |
| **REMOVE** | A Cloudflare-compatible user/operator product surface that Base does not adopt. |
| **SPLIT** | A mixed implementation: retain its general machinery while removing identified product paths. |
| **DEFER** | Existing capability whose future Base product role is intentionally undecided. |

## Existing execution architecture: evidence for preservation

### Decision core — PRESERVE

`crates/logic/lib.rs:6725-6900`, `types.rs:523-1025`, and
`types.rs:1025-...` form the decision/effect boundary. `on_event(&mut State,
Event) -> Vec<Effect>` advances the deterministic state machine, pumps
activation/release/swap, validates invariants in debug builds, and emits
versioned effects. Its events cover route admission, owner and node leases,
restore, runtime start/stop, durability, alarms, cancellation, output gating,
eviction, and rebalancing. The actor shell executes those effects in
`crates/celld/actor.rs:3184-...`; it must not be bypassed by Base.

The core's domain-neutral supporting mechanisms are PRESERVE:

| Location | Actual responsibility | Key dependencies |
| --- | --- | --- |
| `logic/cell.rs` | Cell-scope validation and stable cell naming boundary | `types::CellId` |
| `logic/isolate.rs` | isolate load/admission facts | decision state and `types` |
| `logic/durability.rs`, `restore.rs` | durable acknowledgement and restore outcomes | effects, ownership records |
| `logic/routing.rs` | local/remote route decision data | owner/epoch state |
| `logic/schedule.rs`, `gate.rs`, `output_gate.rs` | timers, admission and ordered output decisions | `Event`/`Effect` |
| `logic/sqlite.rs` | SQLite decision facts, not a D1 product API | core types |

### Actor, runtime, and JavaScript host — PRESERVE

`CellHost::fetch_cell` in `crates/celld/cell_host.rs:25-...` is the actor's
execution boundary. `AppHandle::fetch_worker` in `actor.rs:1481-...` dispatches
stateless Worker ingress to `RuntimeManager::fetch_worker_pool`
(`runtime.rs:924-...`); resident cell work uses `fetch_worker_on_cell`
(`runtime.rs:945-...`). `runtime::load_cell_isolate` at `2978-...` calls
`Worker::load_config`.

`Worker::load_config` in `crates/celld/js.rs:5807-...` is the critical V8 seam:
it creates an isolate, installs dynamic-import handling and state slots, then
calls `install_ops`, `install_prelude`, `install_harness`, compatibility and
storage injection, compiles/instantiates/evaluates the main module, and
registers builtins and additional modules. `install_ops` at `6523-...` binds
host operations on an internal non-user-visible object. This host, module
compilation/evaluation, request dispatch, cancellation, resource budgets, and
pooling are PRESERVE. Some installed operations and harness globals are product
specific; those are split below rather than grounds for removing the host.

### B3.0.4-C host-registration boundary

The active host now has a small private `LegacyProductCapabilities` description
derived from stored Worker configuration. It distinguishes KV, D1, R2, Queue
and Workflow requirements from the ordinary V8 host. Product registrations,
binding construction and product-only host operations activate only when that
trusted description requires them. Queue capability includes generation-wide
consumer registration; a consumer-only legacy script is therefore not
misclassified as generic. Dynamic Workers derive no inherited products.

The generic host retains Request/Response, web APIs, ordinary Durable Objects,
cell storage, generic SQL/alarm/storage operations, cancellation and module
mechanics. Product operation names remain internal bootstrap parameters for
stable source ordering, but invoke an explicit unavailable-operation callback
when their product is absent. They are never globals. `cloudflare:workflows`
is rejected for native workers for both static and dynamic imports; its legacy
implementation remains available only when Workflow support is derived.

This is a registration boundary, not code removal. KV, D1, R2, Queue and
Workflow source remains in the binary for existing deployment compatibility;
the subsequent B304-04 through B304-06 slices own its retirement.

`Generation::build` (`generation.rs:463-...`) is the single deployment-to-live
runtime conversion point. It builds `WorkerConfig`, starts stateless pools,
registers cell classes, and tags isolates with a generation. General artifact
loading, generation retention, module loading, pools, host channels, request
cancellation, and CellHost must survive any configuration-format cleanup.

## Cloudflare product inventory and real dependency graphs

### Workers KV — REMOVE with shared bucket split

**Implementation:** `logic/kv.rs`, `celld/js/services/kv.js`, `kv_cli.rs`,
`kv_blob.rs`, KV portions of `js.rs`, `js/bootstrap.rs`, `deploy.rs`,
`engine_api.rs`, `host_channels.rs`, and `storage.rs`.

**Current responsibility and entrypoints:** `celld kv` is the operator surface
(`kv_cli.rs:1-...`); `kv_namespaces` is parsed by `deploy.rs` (notably
`1581-1585`), represented by `WorkerConfig.kv_bindings` (`js.rs:1896-1900`),
and emitted into the JavaScript environment by `bootstrap.rs:603-624`.
`bootstrap.rs:125` concatenates `harness.js` and `services/kv.js`. The latter
implements the namespace Cell and binding client; `logic/kv.rs:1-...` provides
the Workers-KV-specific limits, addressing, and blob reference policy.

```text
wrangler kv_namespaces / celld kv
  -> deploy::build and WorkerConfig::kv_bindings
  -> Generation::build / Worker::load_config
  -> bootstrap::inject_* environment binding
  -> HARNESS + js/services/kv.js
  -> namespace Cell storage and __kv_blob op (js.rs:6780, 6936-6958)
  -> kv_blob::run -> host_channels::kv_blob_store -> Bucket
```

**State/runtime/config dependencies:** namespace scope comes from
`engine_api::kv_cell_scope` (`570-573`) and the reserved KV class configured
by `deploy.rs:78-89,1581-1585`; ordinary values use the cell's SQLite
connection, while large values use the fleet `Bucket`. `storage.rs:188-191`
contains `_cf_KV` statements.

**What must be removed:** Wrangler KV declarations/features, `celld kv`, the
reserved namespace class, binding injection, `services/kv.js`, KV-specific
host op and blob-record policy. **What must be preserved:** Cell ownership,
fencing, SQLite connection lifecycle, actor effects, Bucket credentials/API,
and the generic object-store durability path. `_cf_KV` is a *cell-internal
storage-table name* and not proof that a Workers KV customer namespace can be
discarded; existing cells may contain durable state in it.

**Split first:** remove `include_str!("services/kv.js")` from
`bootstrap.rs:125` only after all its harness dependencies and the KV op names
are separately removed. Do not remove generic storage primitives merely because
their API names are `get`/`put`.

**Existing tests / required regression:** archived files retain their tests;
B3.0.4 must preserve generic Cell SQLite, actor ownership, bucket replication,
and BaseDiagnostic tests while proving a new Base-native installation cannot
declare `kv_namespaces`. **Deletion order:** public config/CLI -> bindings and
reserved class -> JS service/op -> KV-only logic/blob code, after compatibility
migration policy is approved.

### D1 — REMOVE; internal SQLite is PRESERVE

**Implementation:** `d1_cli.rs`; D1 parsing/registration in `deploy.rs`;
feature validation in `protocol.rs:84-97`; `D1Request`, `D1Response`, and
`d1_run` in `storage.rs:2215-3217`; scopes in `engine_api.rs:504-558`; and
the reserved `__D1Database` server/client in `harness.js:7585-7921`.

```text
wrangler d1_databases / celld d1
  -> deploy::build + reserved __D1Database class
  -> protocol FEATURE_D1_V1 / manifest
  -> Generation::build creates WorkerConfig::d1_bindings
  -> bootstrap binding -> __makeD1Database
  -> __D1DatabaseCell -> __d1_run
  -> storage::d1_run on the Cell SQLite database
```

The D1 scope is derived by `engine_api::d1_cell_scope`; the D1 CLI follows
operator routing to that scope. D1's result-shaping, public error families,
limits, batch/exec/migrate formats, class identity, and binding are REMOVE.
SQLite opening, schema compatibility (`storage.rs:372-...`), write positions,
transactions, alarms, SQL host primitives, durability and replication are
PRESERVE. SQL itself is not a D1 product feature.

**SPLIT requirements:** isolate `D1Request`/`D1Response` and `d1_run` from the
generic connection/SQL operations in `storage.rs`; remove D1 sections from
`harness.js` only after the reserved-class and binding path is gone. Existing
D1 cell data and D1 manifests cannot be silently reinterpreted. Required
regression proves existing-cell SQLite durability and BaseDiagnostic, while a
new deployment rejects D1 config. Delete public CLI/config before reserved
class and D1 adapter.

### R2 — REMOVE binding; Bucket/ownership/replication are PRESERVE

**Implementation:** `r2_cli.rs`, `r2_store.rs`, `js/r2_ops.rs`, R2 harness
construction, `WorkerConfig.r2_bindings` (`js.rs:1889-1893`),
`deploy.rs:1791-1831`, and `host_channels::R2_STORE` (`277-281`).

```text
wrangler r2_buckets / celld r2
  -> deploy::build -> WorkerConfig.r2_bindings
  -> Worker::load_config / harness __makeR2Bucket
  -> js::r2_ops (__r2_* registrations at js.rs:6830-6842)
  -> r2_store record/condition/multipart adapter
  -> Bucket under r2/<bucket-name>/
```

`r2_store` and `r2_ops` are R2 API compatibility and are REMOVE, including
R2 object envelope, metadata, conditional and multipart semantics. The
`Bucket` abstraction, `ownership_store`, `ltx_repl`, and `ltx_replication`
are PRESERVE: they provide internal object storage, ownership and durable log
replication even when the configured backend is R2-compatible. `R2_STORE` is
a mixed global channel and must be replaced/removed only after the R2 binding
is gone; it is not authority to remove the fleet bucket.

Required regression: internal bucket-backed ownership/log replication remains
functional; R2 binding and CLI are absent from a fresh native deployment.
Delete R2 CLI/config first, then binding host operations/store facade; retain
Bucket APIs used by non-R2 infrastructure.

### Cloudflare Queues — REMOVE; actor/admission/scheduling remain PRESERVE

**Implementation:** `queue_cli.rs`, `logic/queue.rs`, `queue_policy.rs`,
`queue_batching.rs`, Queue manifest/config in `protocol.rs:119-296`,
generation dependency/catalog work (`generation.rs:132-265,472-500`),
`Queue*` protocol types in `engine_api.rs:138-224`, dispatch channel in
`host_channels.rs:134-177`, Queue runtime work, and the Queue server/client in
`harness.js:...-7583`.

```text
wrangler queues producer/consumer declarations / celld queue
  -> protocol::validate_queue_manifest + queue attachment records
  -> DeploymentGraph::load + Generation::build queue catalog
  -> WorkerConfig queue bindings/consumer registrations
  -> harness Queue and reserved __Queue Cell
  -> host_channels QueueDispatchReq / runtime consumer dispatch
  -> queue_policy -> logic::queue transition policy + Cell SQLite rows
```

Producer binding, consumer attachment, message format, retry/dead-letter
contract, lease settlement, reserved `__Queue` class, CLI, and Queue-specific
batch timing are REMOVE. `logic::queue` itself is product policy despite being
sans-I/O; its general-looking lease code cannot be retained merely by name.
Actor mailboxing, runtime jobs, isolate admission, core scheduling and alarms
are distinct PRESERVE mechanisms, not Cloudflare Queues.

**SPLIT first:** detach Queue-specific `WorkerConfig`, manifest validation,
generation graph dependencies, host channel, and harness registrations without
changing core `Event`/`Effect` scheduling. Required regression exercises core
admission/timers, non-Queue Cells and BaseDiagnostic. Existing Queue Cells and
deployment attachment records require a compatibility decision before any
schema/data cleanup.

### Cloudflare Workflows — REMOVE; Cell alarms/lifecycle remain PRESERVE

**Implementation:** workflow parsing in `deploy.rs:1704-1789`,
`WorkflowBinding` in `engine_api.rs:284-310`, bootstrap injection/validation
in `js/bootstrap.rs:444-... ,827-...`, builtin resolution in
`js/modules.rs:570-582,921`, and server/client implementation in
`harness.js:7922-9544`. The workflow class is script-scoped through
`deploy::workflow_class` (`112-...`).

```text
wrangler workflows declaration
  -> deploy::workflow_class and WorkflowBinding
  -> protocol feature + Generation::build reserved class config
  -> bootstrap::inject_workflows / modules cloudflare:workflows
  -> harness __WorkflowCell + Workflow/WorkflowInstance
  -> Cell storage kv, alarms, retry timers, user module WorkflowEntrypoint
```

Workflow instance identity, state schema (`__wf.*`), `cloudflare:workflows`,
WorkflowEntrypoint, retry/step orchestration and API are REMOVE. Generic Cell
alarms in `storage.rs:4205-4442`, core wake/scheduling, durable state, and
actor lifecycle remain PRESERVE. Workflow's use of `ctx.storage.kv` does not
turn cell-internal storage into Workers KV.

Split module resolution, bootstrap injection, reserved-class registration and
harness block before deletion. Regression must preserve non-Workflow alarms
and ordinary Worker module loading. Existing workflow cells/manifests need an
explicit migration/retirement plan; none is authorised here.

### Wrangler-shaped deployment — SPLIT

`deploy.rs:254-...` builds from current configuration; `build` is at
`577-...`, persists artifacts through `write` at `881-...`; `protocol::Manifest`
is at `protocol.rs:15-...`; `deployment_version` is at `485-...`; and
`DeploymentGraph::load`/`Generation::build` are at `generation.rs:137-...` and
`463-...`.

The config grammar, Wrangler binding names (`kv_namespaces`, `d1_databases`,
`r2_buckets`, queues, workflows), compatibility flags and product feature
markers are REMOVE. Artifact loading, manifest versioning, generation loading,
cohost/service dependency construction, module source records, pools and
deployment retirement are PRESERVE. `deploy.rs`, `protocol.rs`, and
`generation.rs` must therefore be split rather than deleted.

## Mixed-module separation map

| Mixed file | Classification | Product responsibility to remove | Infrastructure to preserve / first separation |
| --- | --- | --- | --- |
| `js.rs` | SPLIT | KV blob op (`6780,6936-6958`), R2 op registrations (`6830-6842`), D1/Queue/Workflow binding fields | `WorkerConfig`, V8 construction, ops framework, module evaluation, request dispatch, dynamic loader. Give product registration a separable installation list. |
| `js/bootstrap.rs` | SPLIT | KV concatenation (`125`), KV/D1/R2/Queue/Workflow env injection | prelude/harness installation, generic bindings, class registration; remove product injectors only after configs disappear. |
| `js/harness.js` | SPLIT | KV service/binding, Queue, D1, Workflow, R2 facades and `cloudflare:workflows` backing | Web APIs, Durable Object/Cell mechanics, storage bridge, request/event dispatch. Extract by explicit marker/symbol blocks, not broad text deletion. |
| `deploy.rs` | SPLIT | Wrangler parser and product binding/reserved classes | artifact build/write, module/assets loading, deployment versioning. |
| `protocol.rs` | SPLIT | product feature flags, Queue attachment schema and product manifest validation | generic manifest/artifact identifiers/version checks. |
| `generation.rs` | SPLIT | Queue catalog/dependency traversal and reserved product class registration | deployment graph loading, pool startup, generation tags, class registry. |
| `engine_api.rs` | SPLIT | D1/KV/Queue/Workflow scopes, configuration and product request types | request IDs, response bodies, cancellation/admission and generic Worker config/module types. |
| `host_channels.rs` | SPLIT | KV blob/R2 globals and Queue dispatch channel | generic gate/service/RPC/assets/websocket channels. |
| `storage.rs` | SPLIT | D1 request/result facade and product-specific compatibility adapters | SQLite lifecycle/schema, SQL primitives, alarms, durable positions, compatibility migration. `_cf_*` tables are persistent internals and require compatibility review. |

## Base B2 Diagnostic Compatibility — PRESERVE

The Base control listener is a private Unix-domain socket, not the public
Worker ingress (`base_control.rs:1-55,179-227`). `handle_control`
(`388-...`) authenticates a nonce/HMAC probe and reports readiness. For fixed
diagnostics, `handle_execution` (`460-540`) validates only execute, evict, or
inspect and derives, rather than accepts, the scope
`BaseDiagnostic:<CellId>` (`504-...`).

```text
authenticated Unix socket
  -> base_control::handle_control / handle_execution
  -> derived BaseDiagnostic:<CellId>
  -> AppHandle::request + local_request (execute_diagnostic:668-...)
  -> actor decision core / CellHost::fetch_cell
  -> RuntimeManager / Worker::load_config / JavaScript host
  -> buffered diagnostic response; or non-activating AppHandle::presence
```

`execute_diagnostic` verifies a local route, uses `CellHost::fetch_cell`, and
rejects streaming/websocket responses. `inspect_diagnostic` (`602-...`) reads
the actor presence census and reports only timestamped `locally_present` or
`locally_absent`; it does not claim a permanent running/stopped status. B3.0.4
must preserve this scope derivation, authentication, bounds, actor route,
ownership/fencing, output gate, CellHost and JS-host capabilities. These are
reusable Cell mechanisms, not removable Cloudflare product semantics.

## Base Worker JavaScript-host integration surface — PRESERVE, integration DEFER

The current ordinary stateless path is:

```text
stored deployment/artifact -> DeploymentGraph::load (generation.rs:137)
 -> Generation::build (463) -> WorkerConfig::new (js.rs:2017)
 -> StatelessRuntime pool -> Worker::load_config (5807)
 -> install_ops/prelude/harness + compile/instantiate/evaluate
 -> RuntimeManager::fetch_worker_pool (924) -> Worker fetch handler -> HttpResponse
```

`WorkerConfig` holds source, main-module name, module sources and scanned
external imports (`js.rs:1878-2015`). `js/modules.rs` supplies builtin/module
resolution; `runtime.rs:907-943` holds the current generation through the
request. Cancellation is represented by `RequestCancellationLifetime` before
stateless dispatch. This is the smallest *existing* seam that B3.1 can exercise
with a Base Worker artifact: build a valid existing `WorkerConfig`/generation
input and call the existing host path. B3.0.3 does not design a new interface.

Dynamic Workers are a separate existing route, not automatically the Base
choice: `WorkerConfig::into_dynamic_worker` is at `js.rs:2101`; loader registry
and slot acquisition are at `7325-7412`; the loader builds its own restricted
config around `7664-7682`. It can load additional code into V8 with loader
bindings, per-invocation limits and a distinct origin. Static deployment
Workers are generation-owned and receive normal deployment bindings. B3.1 must
compare those trust/configuration differences before selecting either path.

## State, persistence, and unresolved findings

`storage::schema` (`storage.rs:372-...`) explicitly preserves legacy names
`_cf_KV`, `_cf_ALARM`, `_cf_METADATA`, and `_cf_FACETS` (`386-444`), and facet
recovery reads legacy images (`1089-1172`). Existing Cell storage, deployment
manifests, reserved classes, feature flags and object-store keys may therefore
refer to product-era terms. No source removal may delete/recreate/reinterpret
them without a separately approved migration/retirement policy.

Confirmed unresolved work for B3.0.4 is deliberately narrow:

1. Determine supported behaviour for installations with active product
   deployments/cells before removing their manifest readers or reserved classes.
2. Inventory all `cloudflare:*` builtins beyond the required workflow module
   before changing generic module registration; this map identifies the
   product families requested here, not a blanket builtin deletion.
3. Decide the Base-native configuration/artifact input only after the existing
   static and dynamic Worker seams are exercised; no replacement is implied by
   this archive.
