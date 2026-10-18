# Cloudflare reference archive manifest

## Archive baseline and provenance method

- **Repository:** `stel-org/celld`
- **Archived revision:** `e2738fe692c86deb05f4a3f33097625d459d6dfa`
- **Working branch at archival:** `run-to-launch-01`
- **Archive rule:** each entry was copied byte-for-byte from the stated path at
  the archived revision. SHA-256 is over those exact bytes.
- **Copyright/license:** every archived source begins with
  `Copyright 2026 Deno Land Inc. Apache-2.0 license.` The files are preserved
  unchanged, including those headers.

The current fork's Git history identifies the listed last-touch commit and
author. That is evidence about this fork's history, not a claim that every
implementation originated at Cloudflare. The compatible product semantics are
the reason for archival; the visible copyright headers attribute the code to
Deno Land Inc.

| Product | Original path | Archived path | SHA-256 | Last-touch commit / author | Character |
| --- | --- | --- | --- | --- | --- |
| D1 | `crates/celld/d1_cli.rs` | `sources/crates/celld/d1_cli.rs` | `bde32e1e5d3b9ab5d9078d76c35e5b42a5b6729a07843f9be89ea664d411fe39` | `10cb1303` / Ryan Dahl | product CLI |
| Workers KV | `crates/celld/kv_cli.rs` | `sources/crates/celld/kv_cli.rs` | `b3d0c6144114d60d676abd16140963e4460e692dafe90c9c88189ec1ae09aa22` | `10cb1303` / Ryan Dahl | product CLI |
| Workers KV | `crates/celld/kv_blob.rs` | `sources/crates/celld/kv_blob.rs` | `ad91ee8ad74aade0828e29576ad6773823de8aa9481f0e2a8b1cb6763317ad6e` | `bad4649d` / Bartek Iwańczuk | mixed: KV large-value adapter over shared bucket |
| R2 | `crates/celld/r2_cli.rs` | `sources/crates/celld/r2_cli.rs` | `8f0b8640687d1289da495d2d334110365c09e803b39ba9f985024fe97a608f1e` | `42269c12` / Bartek Iwańczuk | product CLI |
| R2 | `crates/celld/r2_store.rs` | `sources/crates/celld/r2_store.rs` | `ad9dbcc82f9d0d5bb0adbc368f96b439c4263a1afb34d92dcdbae3124aceefa3` | `bad4649d` / Bartek Iwańczuk | product record/adapter over shared bucket |
| Queues | `crates/celld/queue_cli.rs` | `sources/crates/celld/queue_cli.rs` | `1ba010588d6d1a87fe8b4bd50ee3ead6d507644e0ea3501a9a0d55d8a527f004` | `a52f9905` / Ryan Dahl | product CLI |
| Queues | `crates/celld/queue_policy.rs` | `sources/crates/celld/queue_policy.rs` | `e8c31770a6d58478fbda5b7bfd724d3e456bd6f1923a352e9822720979198839` | `bad4649d` / Bartek Iwańczuk | product policy bridge |
| Queues | `crates/celld/queue_batching.rs` | `sources/crates/celld/queue_batching.rs` | `584a413e3afe3f7f2707f1280ab0ffdd8a0ecd490b56a5f3019aacaee07a1cdf` | `12d5b633` / Ryan Dahl | Queue-specific batching policy |
| R2 | `crates/celld/js/r2_ops.rs` | `sources/crates/celld/js/r2_ops.rs` | `84edf15f358fb573d103cd6b69451bd277a730d7592dbb6d1ae38ad598d74795` | `bad4649d` / Bartek Iwańczuk | V8 product-binding operations |
| Workers KV | `crates/celld/js/services/kv.js` | `sources/crates/celld/js/services/kv.js` | `6588b315acb0e8ca19728ed027972ac5adde877f54b245ee8879346df60746bf` | `bad4649d` / Bartek Iwańczuk | JavaScript namespace cell and binding |
| Workers KV | `crates/logic/kv.rs` | `sources/crates/logic/kv.rs` | `a6330ace5fbda162245c816cace4e65778db80a354f1f36232c96042f9531520` | `a52f9905` / Ryan Dahl | product-specific sans-I/O policy |
| Queues | `crates/logic/queue.rs` | `sources/crates/logic/queue.rs` | `ac0135726b1b034d146af3b4cc976f28b5ee764a246310ef8752098763d36f06` | `a52f9905` / Ryan Dahl | product-specific sans-I/O policy |

## Mixed-module references retained in Git

The following active mixed files are intentionally not copied: their full,
unchanged baseline form remains available at
`e2738fe:crates/celld/<path>`. Their product symbols and separation constraints
are mapped in `docs/architecture/base-native-cleanup.md` with line references:

`js.rs`, `js/bootstrap.rs`, `js/harness.js`, `deploy.rs`, `protocol.rs`,
`generation.rs`, `engine_api.rs`, `host_channels.rs`, and `storage.rs`.
