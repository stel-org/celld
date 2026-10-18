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

The endpoint accepts no Worker deployment, source-path, storage, actor, peer,
or generic operator operation. It is a Unix socket only: it is not served
through the public Worker ingress or celld's internal HTTP listener. Requests
are limited to 4 KiB, handled one at a time, and bounded to one second.

The trust model is a same-user local operator. Unix permissions and the HMAC
secret stop ordinary other-user access and unauthenticated responders. A process
with equivalent Unix account and filesystem authority that can read the secret
is already inside the pairing boundary.
