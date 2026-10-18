# Base-native celld cleanup plan

## Scope and safety rule

This is the B3.0.4 implementation plan derived from
`base-native-cleanup.md`. B304-00 through B304-02 are complete; later slices
remain pending. It removes inherited
Cloudflare-compatible product surfaces while retaining celld's decision core,
CellHost, actor/effect execution, JavaScript host, SQLite durability, Bucket,
ownership, replication, recovery, alarms, generation mechanics, and Base B2
diagnostic compatibility.

Do not couple these slices to a replacement Base KV, database, queue, or
workflow product. No such replacement is a prerequisite unless an individual
slice demonstrates a real retained-infrastructure dependency.

## Dependency-aware order

The preliminary A-to-D ordering remains correct only with one refinement:
compatibility assessment precedes destructive removal. Product CLIs and fresh
configuration rejection can be separated early; product runtime code can be
removed only after its mixed registrations are separated and the fate of
existing deployments/data is decided.

Current status: **B304-00 through B304-02 complete** and **B304-03 through
B304-07 pending**. The active compatibility policy is
[`base-native-compatibility.md`](base-native-compatibility.md).

The next boundary, B304-03, separates product-specific registration from the
reusable JavaScript-host bootstrap in `js.rs`, `js/bootstrap.rs`,
`js/harness.js`, and `js/modules.rs`.

| Slice | Objective | Prerequisites | Main files/symbols | Acceptance / rollback condition | Next |
| --- | --- | --- | --- | --- | --- |
| B304-00 | Freeze compatibility inventory | archive and map at B3.0.3; inspect actual fleet/installations | manifests, feature flags, reserved scopes, `_cf_*` tables, bucket prefixes | documented list of supported legacy installations; no code change | B304-01 |
| B304-01 | Hide/remove public product CLI entrypoints | B304-00 | `main/cli.rs`, `kv_cli.rs`, `d1_cli.rs`, `r2_cli.rs`, `queue_cli.rs` registration/help/docs | `celld --help` has no product commands; Base control and normal host build pass. Roll back if an approved legacy operator flow has no migration path. | B304-02 |
| B304-02 | Reject product declarations for new deployments | B304-00, migration policy for legacy manifests | `deploy.rs`, `protocol.rs` config/feature validation | new native install rejects KV/D1/R2/Queue/Workflow declarations precisely; existing versioned manifests retain declared compatibility behaviour or are explicitly refused before activation. | B304-03 |
| B304-03 | Separate product registration from host bootstrap | B304-01/02 | `js.rs` op lists, `js/bootstrap.rs`, `js/harness.js`, `js/modules.rs` | generic V8 host loads a simple JS Worker and BaseDiagnostic works without product service registration. Roll back if module loading or Cell storage is coupled accidentally. | B304-04 |
| B304-04 | Retire KV and D1 product paths | B304-03 and product-data decision | `logic/kv.rs`, `kv_blob.rs`, `services/kv.js`, D1 blocks in `storage.rs`/harness, reserved classes/scopes | no KV/D1 classes/bindings/ops survive; Cell SQLite and alarms persist across restart. Keep archive byte checks. | B304-05 |
| B304-05 | Retire R2 and Queue product paths | B304-03 and product-data decision | `r2_store.rs`, `js/r2_ops.rs`, `logic/queue.rs`, `queue_policy.rs`, `queue_batching.rs`, queue graph/channels | no R2/Queue binding, CLI, or reserved class survives; Bucket ownership/log replication and core scheduling still work. | B304-06 |
| B304-06 | Retire Workflow product path | B304-03 and workflow-state decision | `deploy.rs`, `engine_api.rs`, bootstrap/modules/harness workflow symbols | no `cloudflare:workflows` or `__Workflow`; generic alarms and ordinary module loading pass. | B304-07 |
| B304-07 | Remove now-dead product protocol/generation pieces and prove native execution | B304-04..06 | `protocol.rs`, `generation.rs`, `host_channels.rs`, test fixtures/docs | no dead product features/routes; BaseDiagnostic and a normal JS fetch still run. Roll back on actor/generation/durability regression. | B3.1 |

## Detailed executable slices

### B304-00 — compatibility inventory

- **Objective:** classify existing deployment manifests, required feature flags
  (`kv-v1`, `d1-v1`, `r2-v1`, `queues-v1`, `workflows-v1`), reserved scopes,
  Queue attachments, workflow data and Cell SQLite state before code deletion.
- **Files to inspect, not change:** `protocol.rs`, `deploy.rs`, `generation.rs`,
  `storage.rs`, Bucket key conventions, `ownership_store.rs`, `ltx_repl.rs`.
- **Migration concern:** `_cf_KV`, `_cf_ALARM`, `_cf_METADATA`, `_cf_FACETS`
  are existing durable table names. They are not a licence to drop data.
- **Acceptance:** written policy identifies new installations vs each legacy
  state/deployment case. No automated code deletion begins without it.

### B304-01 — public surfaces

- **Symbols to remove:** command registration and help for `celld kv`,
  `celld d1`, `celld r2`, `celld queue`; product-facing documentation created
  by their active CLI registrations.
- **Retain:** generic operator routing, fleet authentication, bucket opening,
  `base_control` Unix socket and BaseDiagnostic paths.
- **Expected impact:** command parser and CLI tests only; no state migration.
- **Tests:** command absence; `cargo check`; BaseDiagnostic probe regression.
- **Rollback:** restore registration only, not product runtime redesign.

### B304-02 — config/manifest boundary

- **Symbols:** Wrangler-shaped `kv_namespaces`, `d1_databases`, `r2_buckets`,
  Queue consumer/producer, workflow parser branches in `deploy.rs`; matching
  `protocol::Manifest` validation and required-feature records.
- **Retain:** source/module artifact loading, manifest identity/version,
  deployment write/read, cohost/service graph, assets and generation loading.
- **Compatibility:** the parser cannot simply forget an active old manifest if
  the fleet claims to support it. Define explicit refusal/migration handling
  before changing read paths.
- **Acceptance:** a simple product-free deployment still reaches
  `Generation::build`; each forbidden fresh declaration produces a useful
  failure; no active stored manifest is silently mutated.

### B304-03 — host registration separation

- **Symbols:** product fields in `WorkerConfigOptions`/`WorkerConfig`, product
  injection in `bootstrap.rs`, product op registration in `js.rs`, product
  blocks in `harness.js`, and `cloudflare:workflows` module registration.
- **Retain:** `Worker::load_config`, `install_ops`, prelude, generic harness
  pieces, compiler/instantiator/evaluator, module registry, fetch dispatch,
  cancellation, V8 budget controls and dynamic-loader core.
- **Risk:** `harness.js` is one concatenated program; use symbol-boundary tests
  and explicit registrations rather than textual global deletion.
- **Acceptance:** a minimal existing WorkerConfig compiles, loads and returns a
  response; BaseDiagnostic execute/evict/inspect still has its current
  guarantees; no product globals are exposed.

### B304-04 — KV and D1 retirement

- **KV files:** `kv_cli.rs`, `logic/kv.rs`, `kv_blob.rs`,
  `js/services/kv.js`, KV sections of bootstrap/js/deploy/engine_api/storage.
- **D1 files/symbols:** `d1_cli.rs`, `D1Request`, `D1Response`, `d1_run`,
  `D1_CLASS`, `d1_cell_scope`, D1 harness binding/server and config.
- **Retain:** generic Cell SQLite connection/schema/open/close/transaction,
  write position, alarms, ownership and Bucket API. Do not delete `_cf_*`
  tables or legacy facet handling in this slice without B304-00's policy.
- **Acceptance:** restart-safe generic Cell storage/alarm tests, normal JS
  host fetch, BaseDiagnostic; fresh config has no KV/D1 product path.

### B304-05 — R2 and Queue retirement

- **R2:** remove `r2_cli`, `r2_store`, `r2_ops`, R2 binding config and
  `R2_STORE` binding channel. Retain `Bucket`, blob credentials, object-store
  use by ownership/replication and LTX machinery.
- **Queues:** remove `queue_cli`, `logic::queue`, `queue_policy`,
  `queue_batching`, Queue manifest attachment, queue catalog, Queue host
  channel/dispatch, reserved class and harness implementation.
- **Retain:** actor mailboxing, runtime task queues, admission, core scheduling
  and alarms. They are not user Queue features.
- **Acceptance:** ownership/replication tests and non-Queue Cell admission
  survive; no R2/Queue config, class, binding, or CLI remains.

### B304-06 — Workflow retirement

- **Files/symbols:** workflow config/parser, `WorkflowBinding`,
  `workflow_class`, bootstrap workflow injection/validation,
  `cloudflare:workflows`, `__Workflow`, `WorkflowEntrypoint`, `__wf.*` state
  protocol.
- **Retain:** general Cell timers/alarms and storage APIs. Workflow retry/step
  scheduling is product orchestration and must not be recast as a generic Base
  workflow engine in this cleanup.
- **Acceptance:** normal alarms and generic Worker module resolution pass; old
  workflow state handling follows B304-00's explicit policy.

### B304-07 — convergence and Base-native proof

- **Objective:** remove dead feature flags/manifest members, registrations,
  test fixtures and product-only dependencies left by prior slices.
- **Required checks:** no active references to archived product source; no
  Cargo workspace change unrelated to deletions; full supported workspace
  tests; a real BaseDiagnostic control-path test; a real ordinary JavaScript
  Worker artifact through the retained existing host.
- **Completion criterion:** a fresh Base-native celld installation can load and
  execute JavaScript without Cloudflare product configuration, while Base
  B2 diagnostics retain authenticated, bounded, non-activating observation
  semantics. This is the prerequisite for B3.1, not Base Worker integration
  itself.

## Compatibility and rollback policy

Each removal commit must be independently buildable/testable and have one
small semantic result. Never bundle a storage migration with a product API
replacement. Before a slice deletes a reader for persisted data, verify the
inventory and have a tested rollback that restores the prior binary or leaves
the data untouched. A failure to preserve legacy product deployments is a
compatibility decision to be explicitly announced, not an accidental SQLite
schema consequence.

## B3.1 preconditions

B3.1 may begin only after B304-07 proves the retained host seam:

1. `Worker::load_config` still loads an ordinary JavaScript artifact.
2. A fetch reaches V8 and returns a real response through the existing runtime.
3. BaseDiagnostic execute, evict and presence inspection still pass.
4. No Cloudflare product configuration, binding or reserved class is needed by
   that proof.
5. The static versus Dynamic Worker loader choice is made from demonstrated
   requirements, not assumed in the cleanup.
