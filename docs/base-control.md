# Local Base control probe

celld can optionally expose one local readiness probe for a separately built
Base installation. This capability is an infrastructure adapter, not Base
product logic: it knows no Projects, Sources, users, Base persistence, or HTTP
API.

Set `CELLD_BASE_CONTROL_DIR` to an absolute, private directory before starting
celld. The process creates or reuses:

| Object | Mode | Purpose |
| --- | --- | --- |
| control directory | `0700` | Private local pairing boundary |
| `base-control.sock` | `0600` | Unix-domain socket for the probe |
| `base-installation-id` | `0600` | Stable identity for this local installation |
| `base-control.secret` | `0600` | 32-byte hex HMAC pairing secret |

The installation identity and secret persist through a normal celld restart.
The socket is removed when the process begins shutdown. celld rejects symlink
or non-regular pairing files, and will not replace a non-socket path with the
socket.

## Protocol

The line-delimited JSON protocol is `base-celld` version `1`. A paired client
sends a fresh 32-hex-character nonce and an HMAC-SHA-256 proof over:

```text
request:base-celld:1:<nonce>
```

celld validates the proof and returns only its protocol/version, installation
ID, `celld` implementation/version, readiness, the nonce, and an HMAC proof
bound to every returned value. Invalid proof, nonce, or protocol is answered as
`authentication_rejected`. A probe reports `ready` only when celld's existing
`!draining && fleet_ready && actor.healthy()` readiness condition holds;
otherwise it reports `not_ready`.

## Fixed diagnostic realization

Version 1 also has two deliberately fixed, HMAC-authenticated operations for
the Base B2.3 integration test: `executeDiagnostic` and `evictDiagnostic`.
They are not a general celld control plane. An execution request contains a
canonical Cell UUID, operation name, fresh nonce, and a proof over:

```text
execution:base-celld:1:<operation>:<cell-id>:<nonce>
```

celld validates that UUID and derives the only scope it will act on:
`BaseDiagnostic:<cell-id>`. It never accepts an arbitrary Durable Object class,
scope, source path, route, HTTP request, method, or JavaScript operation. The
execution is routed through the normal actor, local request gate, `CellHost`,
and `RuntimeManager` path. The response is bounded to 1 KiB and signed over
the operation, installation, Cell, derived scope, outcome, optional HTTP
status, SHA-256 body digest, and nonce. Eviction delegates to celld's normal
actor eviction and never exposes lease, fencing, storage, or peer operations.

`examples/base-diagnostic` supplies the fixed Durable Object used for isolated
integration validation. It is not a deployment model or a user-programmable
Worker interface.

## B2.4 inspection and lifecycle hardening

Version 1 remains unchanged for readiness and the fixed B2.3 lifecycle pair.
Version 2 adds only `inspectDiagnostic`. Its signed request contains a valid
Base Cell UUID, fresh nonce, and proof over:

```text
execution:base-celld:2:inspectDiagnostic:<cell-id>:<nonce>
```

celld derives `BaseDiagnostic:<cell-id>` and reads `AppHandle::presence()`;
it does not route a request through `CellHost`, V8, or the diagnostic handler.
The signed response reports a new per-control-server process-instance ID, a
wall-clock observation timestamp, and either `locally_present` with the
published local epoch or `locally_absent`. Presence is the actor core's local,
published projection. Absence does not prove global absence, permanent stop,
deletion, or a safe storage-cleanup precondition.

The endpoint accepts no Worker deployment, source-path, storage, actor, peer,
or generic operator operation. It is a Unix socket only: it is not served
through the public Worker ingress or celld's internal HTTP listener. Requests
are limited to 4 KiB. The server admits at most eight connections; short body
and inspection work is bounded to one second, while the fixed execution and
eviction operations have a five-second bound. Admission overflow receives a
bounded `capacity_exhausted` rejection.

For the lifetime of one celld process, the control adapter retains a bounded
60-second cache of accepted mutation `(operation, cell-id, nonce)` values and
rejects a duplicate as `replay_rejected`. This prevents an ordinary replay of
the same captured message but is not a durable exactly-once guarantee across
process restart. A client must treat a lost or timed-out lifecycle response as
potentially applied and inspect current infrastructure facts rather than retry
automatically.

## B3.2.3 managed Worker realization

Protocol version 4 is a separately authenticated, local-only extension for
Base's internal lifecycle foundation. It adds exactly four operations:
`establishWorkerRealization`, `invokeWorkerRealization`,
`inspectWorkerRealization`, and `retireWorkerRealization`. Version 3 keeps its
original one-off `executeWorker` meaning.

The control adapter authenticates and delegates v4 operations to the
process-local `WorkerRealizationRegistry`. That registry is the single owner
of retained `StatelessRuntime` values, their selected artifact configuration,
request admission, and retirement. It is bounded to 16 retained realizations
per celld process. It reuses the established worker isolate-pool lifecycle; a
realization is not a promise that one V8 isolate remains permanently resident.

An establish envelope precedes the bounded verified artifact bytes as in v3.
The artifact digest is recomputed before `WorkerConfig` loading. The registry
assigns no Base identity: Base supplies a fresh realization UUID and all
Project, Worker, Branch, Cell, artifact, operation, request arguments and
nonce facts are HMAC-bound. The response additionally binds the current
process-instance identity.

Invocation sends no Source or artifact bytes and dispatches through the exact
retained runtime. Inspection is non-activating: it does not compile, warm, or
recreate an absent runtime. Retirement first removes new-request admission,
waits at most three seconds for already admitted requests to drain, and then
releases the retained runtime. A drain timeout reports `retiring`; it does not
claim that all application effects were cancelled.

The registry is process-local. celld shutdown drops it, while the paired
installation ID and secret remain stable. After restart a new process-instance
ID and a fresh inspection reveal local absence. This is not a public
deployment, hosting, Worker upload, generic operator, or lifecycle API.

The trust model is a same-user local operator. Unix permissions and the HMAC
secret stop ordinary other-user access and unauthenticated responders. A process
with equivalent Unix account and filesystem authority that can read the secret
is already inside the pairing boundary.

## R8.2-C authenticated HTTP realization delivery

`dispatchWorkerRealization` version 7 is the bounded HTTP successor to v6. It
does not establish, select or replace a realization. After authentication, the
control adapter derives `RealizationMetadata` from the signed identity,
artifact, profile and execution-contract fields, and invokes only a matching,
already accepting `WorkerRealizationRegistry` entry. That registry remains the
single owner of the retained `StatelessRuntime`, admission and retirement
logic; v7 adds no dispatcher, runtime or lifecycle.

Base first sends a non-executing `inspectWorkerHttpDelivery` v7 capability
request. Its proof covers the protocol, version, operation and nonce under
`worker-realization-http-capability-request-v7`. celld responds with its
installation, implementation/version, `[7]` supported delivery version and
the original nonce under
`worker-realization-http-capability-response-v7`. The probe does not inspect,
allocate, activate or execute a Worker, and v7 is advertised only by this
implemented receiver.

The v7 dispatch metadata contains the existing realization identity and
admission facts plus method, origin-free path/query, an ordered `headers`
array, body length/digest and nonce. A header is represented as:

```json
{ "name": "authorization", "valueBase64": "QmVhcmVyIGV4YW1wbGU=" }
```

The request proof is the fixed-order length-prefixed
`dispatch-worker-realization-request-v7` material. The header sub-material is
`worker-realization-http-headers-v1`, containing each name and base64 value in
wire order. No repeated field is merged. The receiver allows `GET`, `HEAD`,
`OPTIONS`, `POST`, `PUT`, `PATCH` and `DELETE`; limits request targets to 2,048
bytes, header fields to 64, names to 128 bytes, values to 4 KiB, aggregate
header bytes to 16 KiB, and buffered request/response bodies to 64 KiB. Its v7
metadata line is bounded to 128 KiB.

Before execution celld rejects invalid base64, invalid names/values, CR/LF or
other header injection, hop-by-hop headers, `Host`, `Content-Length`,
`Forwarded` and `X-Forwarded-*`; it reads exactly the signed body length and
checks SHA-256. The response includes explicit `headers` (including `[]`) and
is signed with `dispatch-worker-realization-response-v7`, binding request
headers, response status, response headers and response body facts. Duplicate
response fields, including `Set-Cookie`, retain their order.

The existing Worker host accepts `Vec<(String, String)>` headers. celld decodes
the binary-safe wire value losslessly only when it is valid UTF-8, then passes
it unchanged through `WorkerRealizationRegistry::invoke()` to the existing
`StatelessRuntime`. A non-UTF-8 request value is rejected rather than lossy
converted. Worker response values are host strings and are encoded directly to
base64 after the same HTTP validation. Streaming bodies, SSE and WebSockets are
not part of this receiver. A future R8.3 binary-header host interface must be a
compatible execution contract change, not a silent reinterpretation here.

The focused receiver proof establishes a real retained `StatelessRuntime` and
uses the v7 handler over a Unix stream. Its Worker verifies a JSON POST's
method, path/query, `Authorization`, `Cookie` and body, then returns `201`,
JSON `Content-Type`, `Location`, two independent `Set-Cookie` fields and a
JSON body. The test verifies the complete response HMAC material. Base's
isolated `prove-worker-realization.sh` process proof additionally starts this
celld binary with a temporary pairing directory, establishes a Worker through
the Base Worker compiler, and completes the separately versioned public v8
request through Base Runtime.

## R8.3-A public HTTP URL delivery

`dispatchWorkerRealization` v8 is the authenticated public-request successor to
v7. Capability discovery advertises both `[7, 8]`; v7 remains the existing
internal delivery contract. v8 adds a required `requestUrl` to the signed
request and response material (`dispatch-worker-realization-request-v8` and
`dispatch-worker-realization-response-v8`). celld accepts only an `http` URL
whose authority is syntactically safe and whose exact path/query equals the
already signed request target. It passes that URL unchanged to the retained
registry and existing `StatelessRuntime`.

The URL describes a request Base already authorized through its Gateway; it
does not select a realization, Worker, artifact or execution contract. celld
does not inspect forwarding headers or implement TLS/HTTPS. A v8 response keeps
the v7 status/header/body semantics, including repeated headers. If a public
request needs v8 but the paired receiver does not prove it, Base rejects rather
than silently substituting the v7 internal `https://base.local` URL.

## R8.5-B configuration-capable establishment

`establishWorkerRealization` v9 is the authenticated variable-bearing
successor to v4/v5 establishment. It does not change HTTP dispatch (v6/v7/v8
are untouched): v9 carries an explicitly selected, ordinary-variable payload
bound to one retained realization. Base first sends a non-executing
`inspectWorkerConfigurationEstablishment` v9 capability request, proved over
protocol, version, operation and nonce under
`worker-realization-configuration-capability-request-v9`. celld answers only
if it implements the contract, with installation, implementation/version,
`[9]` and the nonce under
`worker-realization-configuration-capability-response-v9`. Any other
well-formed unknown operation — including this probe on an older receiver —
is answered with an explicit unsigned `unsupported_operation` rejection, so
Base capability checks have a dependable older-peer signal; malformed frames
still close silently.

The v9 establish frame carries the existing realization identities plus the
selected Worker Version id, the Version configuration digest, and the
variable-payload length and SHA-256, all in signed
`establish-worker-realization-request-v9` material. The immutable artifact
bytes follow, then exactly the declared payload bytes. celld reads both in
full, verifies both SHA-256 digests, then validates the payload as canonical
`[[name, value]]` JSON: strictly ascending names (which rejects duplicates),
the same name/value bounds Base enforces on immutable Version configuration
(128 pairs, 128 B names, 8 KiB values, 64 KiB total), and byte-equality with
its own canonical re-serialization. Truncation, digest mismatch, or any
structural defect fails the establishment before any Worker starts — never
as a partial environment — and nothing about the payload contents is logged.

Validated pairs become `WorkerConfigOptions.vars` for exactly the retained
realization being established; the JS bootstrap installs them onto that
Worker's `env` object. v9 admits the established profile/contract pairs
only (default target exactly as its v4 predecessor; BaseWorker contract as
v5+), so configuration transport never selects a bootstrap. The signed v9
response echoes the accepted configuration digest under
`establish-worker-realization-response-v9`; a missing or mismatched echo
must fail the Base caller closed. v9 frames carrying v4/v5-incompatible
fields (and vice versa) are rejected rather than reinterpreted.

## B3.8.6 verified Base Workload Binding bridge

Base-managed retained realizations receive a celld host capability distinct
from celld deployment-graph service bindings. JavaScript can request only a
Binding name plus a bounded request through `env.NAME.fetch`; celld derives
the Project, Worker, Branch, realization, installation, and current
process-instance identity from the retained realization configuration. Those
facts cannot be supplied or replaced by JavaScript.

celld sends the request only to `base-workload.sock` in the paired private
control directory. `base-workload-callback` v1 is an independently
domain-separated HMAC protocol, not a reinterpretation of v4/v5 lifecycle or
v6 Base-to-celld delivery. It length-prefixes protocol/version/operation,
installation/process, source realization context, Binding name, method,
path/query, optional content type, body length/digest, and nonce. The request
is limited to five methods, a `https://service.internal` path/query of 2,048
bytes, sole optional `Content-Type` of 256 bytes, and 64 KiB body/response.
There are no arbitrary headers, streaming, WebSockets, target IDs, pairing
secret exposure, or outbound internet authority.

Base owns current Binding authorization and target selection. A missing,
revoked, retargeted, wrong-scope, or Secret-target Binding is refused by Base;
celld never routes this through `RuntimeManager::fetch_service` or its native
service graph. A Base callback failure is a rejected/uncertain operation, not
a fallback to an unprotected path. The capability is absent from ordinary
celld Workers and dynamically loaded isolates.

## B3.8.7 verified Base Secret Binding bridge

The same Base-retained realization capability also provides
`env.NAME.get()`. This is not a URL fetch and cannot select a Secret ID,
Project, Worker, Branch or realization. celld sends a separate
`readSecretBinding` request to the same private `base-workload.sock`; its
version-2 request has a distinct HMAC domain label and binds the retained
source context, Binding name and fresh nonce. It does not reuse or alter the
version-1 Worker-dispatch material.

Base alone resolves the current Binding and decrypts an authorized Secret. A
successful response is not a generic JSON body: Base seals it with
XChaCha20-Poly1305 using a key derived with a separate label from the local
Base–celld pairing secret. The response nonce and associated data bind the
original request context. celld verifies it before passing UTF-8 text to the
calling V8 Worker. The Base Secrets master key never enters celld, JavaScript,
Build artifacts or deployment configuration.

Secret reads are on-demand, bounded to 64 KiB, and not cached. A revoked
Binding blocks a later read but cannot erase an already delivered JavaScript
string. A Worker receiving a permitted Base Worker call may read its own bound
Secret; it still cannot initiate another Worker dispatch under the B3.8.6
one-hop restriction. Ordinary celld Workers retain no Base Secret capability.

## B4.6 verified Base data-Service bridge

Base-retained realizations also expose deliberately typed `env.NAME.kv`,
`env.NAME.sql`, and `env.NAME.objects` surfaces. They are not celld's inherited
deployment-local KV, D1, or R2 products. JavaScript provides a Binding name and
a closed operation only; celld derives the retained Project/Worker/Branch/
Realization/installation/process context and sends it to the paired private
`base-workload.sock`.

`dataServiceOperation` v3 has a distinct, domain-separated HMAC binding that
covers origin metadata, Binding name, serialized operation, payload length and
SHA-256, plus a fresh nonce. Base remains the authority: it verifies the
retained origin, replay nonce and current Binding revision, resolves the
Resource category and operates its own KV, SQLite or Object Storage engine.
celld does not open Service files, receive a Service ID, choose a filesystem
path or gain a fallback path when Base refuses an operation.

The callback has an independent bounded admission pool. Metadata and ordinary
results are bounded; Object uploads are raw authenticated payload bytes, and
Object downloads are request-bound HMAC frames of at most 256 KiB after a
sealed descriptor. celld materializes a successful bounded download as a
`Uint8Array` for the V8 caller. The current local limit is 64 MiB. Base uses a
distinct pairing-derived AEAD response key, so v3 responses cannot be confused
with Worker dispatch or Secret-read responses.

Revoking or retargeting a Binding prevents the next operation; operations
already admitted by Base retain their truthful outcome. This is a private
retained-Worker capability, not a public control API, Worker-created binding,
generic database protocol, Gateway feature, or new runtime lifecycle path.

## B3.2.1 bounded Base Worker invocation

Protocol version 3 adds one separately authenticated operation,
`executeWorker`. It is not a general celld control plane and it does not alter
the meaning, limits, or derived `BaseDiagnostic` scope of the version 1/2
diagnostic operations.

The client sends a JSON metadata envelope below the existing 4 KiB limit. The
envelope names a Project, Worker, Branch, and Base Cell UUID, a bounded `GET`
URL, artifact SHA-256 and byte count, a fresh nonce, and an HMAC proof bound to
every one of those values. It then writes exactly the declared UTF-8 artifact
bytes on that authenticated Unix socket. celld accepts at most 512 KiB,
recomputes the SHA-256 before compilation, rejects malformed/replayed/invalid
proof requests before reading or executing code, and limits execution to three
seconds. A response is at most 64 KiB, Base64 encoded, and HMAC-bound to all
selected identities, artifact digest, outcome, status, body digest/length, and
nonce.

celld loads a verified artifact through its existing `WorkerConfig` and
`StatelessRuntime`; this is the established V8 host, not Deno `JsRuntime`, a
new engine, a Node subprocess, a public listener, or a Durable Object Cell
realization. The supplied Base Cell is correlation context only. `executeWorker`
does not create a persistent Worker deployment, public endpoint, runtime
placement, or lifecycle state. A timeout or lost response after acceptance is
therefore an unknown one-off effect and the Base caller must not retry it
automatically.
