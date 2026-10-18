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

B3.0.4-A/B does not remove product runtime code, manifest parsing, JavaScript
bindings, service registrations, or durable readers. B3.0.4-B now rejects new
local and managed publication that declares D1, KV, R2, Queue, or Workflow
products. Those paths remain compiled solely so existing deployments and their
data are not silently broken while the removal sequence is verified. Later
slices remove product runtime registrations only after their dependencies are
demonstrated.

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
product resources. Runtime compatibility remains transitional until B304-02
through B304-07 complete their explicit acceptance criteria.

## Trust and rollback

Each cleanup slice is independently buildable and reversible at its public
boundary. A later removal cannot drop a persisted-data reader or a Bucket
dependency without first documenting the affected legacy state, an explicit
refusal or migration policy, and rollback behavior. This protects the Base
diagnostic control path and generic Cell execution infrastructure while
preventing a Cloudflare-compatible product API from becoming part of the
Base-native public contract.
