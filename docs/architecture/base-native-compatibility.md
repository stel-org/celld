# Base-native compatibility boundary

## Purpose

B3.0.4 separates a fresh Base-native celld installation from inherited
Cloudflare-compatible product services. A fresh installation must not adopt
D1, KV, R2, Queues, or Workflows as Base products. This boundary is a staged
compatibility policy, not a claim that the retained execution host has already
removed every implementation path.

## Public command line

B3.0.4-A retires the public `celld d1`, `celld kv`, `celld r2`, and
`celld queue` commands. They are rejected by command parsing before a bucket
or network configuration is opened. The active CLI retains `deploy`, `dev`,
`cell`, `diagnose`, connection and credential operations, and normal node
startup.

A legacy operator who has an approved operational need may manually build the
pinned historical revision `bf39ed6d6e6bbb8e995f7a96b48112b7ba804bde` from
the repository history. The current executable never locates, starts, or
delegates to a legacy binary automatically.

## Temporary runtime compatibility

B3.0.4-C separates the generic JavaScript host from inherited product
registration. `WorkerConfig` derives a private capability set from trusted,
already-stored deployment facts: bindings, reserved product classes, Workflow
classes, and the generation-wide Queue consumer catalogue. It is not a public
manifest option, environment variable, CLI switch, or JavaScript setting.
Dynamic Workers always derive an empty product set and therefore do not inherit
their parent's legacy product authority.

The generic host still installs V8, Web APIs, Request/Response, Cell and
Durable Object machinery, generic storage/SQL/alarm operations, cancellation,
module loading, service bindings, variables, assets and loaders. Product-only
operations are fail-closed for a Base-native Worker: KV (`__kv_blob`), D1
(`__d1_run`), Queue (`__queue_*`), and R2 (`__r2_*`). They remain internal
bootstrap parameters rather than globals, so this does not expose host
operations to application JavaScript.

Only a legacy capability installs its matching binding factory and reserved
runtime class: KV namespace, D1 database, R2 bucket, Queue, or Workflow. The
KV service source is also installed for a Queue-only legacy deployment because
it supplies Queue's shared sweep helper; it still does not register a KV
namespace class or binding unless KV itself is required. `cloudflare:workflows`
is available only for a legacy Workflow configuration. Static and dynamic
imports otherwise fail explicitly; `cloudflare:workers` remains because its
general Durable Object and WorkerEntrypoint machinery is still part of the
host. The core and legacy harnesses have separate source and V8 code-cache
identities; the core source excludes `services/kv.js`, while the legacy source
places that shared function declaration before the harness so its lexical
dependency remains valid.

B3.0.4-B continues to reject new local and managed publication that declares
D1, KV, R2, Queue, or Workflow products. Those implementations remain compiled
only so existing deployments and data are not silently broken while the
removal sequence is verified. No product implementation, durable-data reader,
or manifest format was removed by this checkpoint.

Publication and loading are deliberately separate: `celld deploy`, `celld dev`,
and managed publication validate new manifests with the Base-native policy;
the existing immutable-manifest reader continues to use legacy feature and
Queue validation. No new copy of a legacy product deployment can be published,
but a previously stored compatible manifest remains readable.

No data migration is performed in this slice. Existing durable state remains
intact. In particular, `_cf_*` internal SQLite tables are protected
implementation state, not disposable product data. Bucket ownership records,
replication data, and the generic Bucket infrastructure are likewise retained
because they support core durability and recovery rather than an R2 product
replacement.

## Inventory and limitations

The B3.0.3 archive is an exact historical source reference, not executable
runtime code. The active `d1_cli`, `kv_cli`, `r2_cli`, and `queue_cli` modules
remain temporarily compiled, together with D1, KV, R2, Queue, and Workflow
configuration and registration paths. Their legacy durable state readers,
including protected `_cf_*` internals, remain in place. Bucket-backed
ownership, replication, recovery, and Cell SQLite mechanics remain because
they support core durability rather than an R2 product replacement.

The archive does not prove that an arbitrary historical deployment is safe to
operate on current code. It records the source boundary needed to make a
deliberate compatibility decision. No fresh Base-native product-service
declaration is authorized; no migration, deletion, or automatic conversion of
legacy durable data is performed; and the retired CLI cannot manage legacy
product resources. Runtime compatibility remains transitional until B304-04
through B304-07 complete their explicit acceptance criteria. B304-03 executes
real V8 host tests for an ordinary stateless fetch, native static and dynamic
Workflow-import rejection, and legacy Workflow host registration. It does not
claim a multi-node fleet or BaseDiagnostic process acceptance run.

## Trust and rollback

Each cleanup slice is independently buildable and reversible at its public
boundary. A later removal cannot drop a persisted-data reader or a Bucket
dependency without first documenting the affected legacy state, an explicit
refusal or migration policy, and rollback behavior. This protects the Base
diagnostic control path and generic Cell execution infrastructure while
preventing a Cloudflare-compatible product API from becoming part of the
Base-native public contract.
