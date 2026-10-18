# Local Base control probe

celld can optionally expose one local readiness probe for a separately built
Base installation. This capability is an infrastructure adapter, not Base
product logic: it knows no Projects, Sources, users, Cells, or HTTP API.

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

The endpoint accepts no Cell lifecycle, Worker deployment, source-path,
storage, actor, peer, or generic operator operation. It is a Unix socket only:
it is not served through the public Worker ingress or celld's internal HTTP
listener. Requests are limited to 4 KiB, handled one at a time, and bounded to
one second.

The trust model is a same-user local operator. Unix permissions and the HMAC
secret stop ordinary other-user access and unauthenticated responders. A process
with equivalent Unix account and filesystem authority that can read the secret
is already inside the pairing boundary.
