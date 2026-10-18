# Cloudflare reference archive

This directory preserves selected, standalone Cloudflare-compatible product
implementations from `stel-org/celld` at the B3.0.3 baseline
`e2738fe692c86deb05f4a3f33097625d459d6dfa`.

It is a source reference, not an active runtime component. Nothing under this
directory is a Cargo workspace member, Rust module, JavaScript include, build
hook, or service registration. The running celld implementation continues to
use the files under `crates/`; this archive exists so a future Base-native
cleanup can remove an inherited product surface without losing its exact
implementation or provenance.

The archive deliberately preserves product code rather than relabelling it as
Base functionality. It does **not** implement Base KV, Base databases, Base
queues, or a replacement object store. Internal cell durability, SQLite,
ownership, replication, actor scheduling, and the JavaScript host remain
active infrastructure outside this archive.

See [manifest.md](manifest.md) for byte hashes, original and archived paths,
copyright/license headers, and revision provenance. The source dependency map
and planned cleanup slices are in:

- `docs/architecture/base-native-cleanup.md`
- `docs/architecture/base-native-removal-plan.md`
