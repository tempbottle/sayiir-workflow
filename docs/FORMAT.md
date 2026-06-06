# Sayiir Durable Snapshot Format

Normative specification of the on-disk format used by Sayiir's durable
persistence backends (`sayiir-postgres`, `sayiir-d1`) to store a
`WorkflowSnapshot`. This is the format frozen for the 1.0 release line.

The canonical implementation lives in
[`sayiir-core/src/snapshot_format.rs`](../sayiir-core/src/snapshot_format.rs).

## Envelope

Every durable snapshot blob is a **self-describing envelope**: a fixed 6-byte
header followed by the codec payload.

```
offset  bytes  field
0..4      4    magic           = b"SYRS"   (ASCII; "SaYiiR Snapshot")
4         1    format_version  = 1
5         1    codec_id        (1 = JSON, 2 = rkyv)
6..       N    payload         = codec output of the WorkflowSnapshot
```

- **magic** identifies the blob as a Sayiir snapshot. A blob that does not begin
  with `SYRS` is rejected (see [Compatibility](#compatibility-policy)).
- **format_version** is the version of *this envelope and the payload layout it
  wraps*. It advances independently of the magic. Readers reject any version
  they do not understand rather than guess.
- **codec_id** records which codec produced the payload, so a reader can detect
  and reject a blob written by a different codec with a clear error instead of
  silently mis-decoding the bytes.
- **payload** is the raw output of the codec for that `codec_id` — e.g.
  `serde_json::to_vec(&snapshot)` for JSON, `rkyv::to_bytes(&snapshot)` for rkyv.

The header bytes are pure ASCII/integers with no length prefix; the payload runs
to the end of the blob. All framing is plain byte manipulation and is
WASM-safe (used unchanged on the Cloudflare Workers / D1 path).

### Codec ids

| id | codec | payload |
|----|-------|---------|
| 1  | JSON  | `serde_json` of the snapshot |
| 2  | rkyv  | `rkyv` archive of the snapshot |

## Compatibility policy

The 1.0 line commits to: **any 1.x build reads any snapshot written by any 1.x
build that used the same codec.** Concretely:

- **Additive, backward-compatible payload changes keep `format_version = 1`.**
  With the JSON codec, new fields must be optional (`#[serde(default)]`,
  `Option<T>`) and removed fields must remain decodable (`#[serde(default)]`).
  See the [Serialization & Migration guide][guide] for the field-level rules.
- **Any change that an older reader cannot decode bumps `format_version`** and
  ships the corresponding read path. An old reader encountering a newer version
  fails fast with `UnsupportedVersion` rather than mis-reading.
- **The rkyv codec is fragile across struct-layout changes.** rkyv archives have
  no field-default tolerance, so *any* change to the snapshot struct that affects
  its archived layout requires a `format_version` bump when the rkyv codec is in
  use. JSON is the recommended durable codec for this reason.

### Codec mismatch

If a backend configured with codec *A* loads a blob tagged with codec *B*, the
load fails with a `CodecMismatch` error naming both codecs. Switching the
durable codec of a running system therefore requires draining in-flight
workflows first — the bytes already on disk were written by the old codec.

### Pre-1.0 (0.x) blobs — no in-place migration

Snapshots written by 0.x are **headerless raw codec output** (no envelope). The
1.0 format does **not** read them: a load returns a `MissingMagic` error whose
message directs operators to drain. There is intentionally **no in-place
migration** — upgrading to 1.0 requires:

1. Stop starting new workflow instances on the old version.
2. Let in-flight workflows **drain** to completion.
3. Deploy 1.0.

This is a one-time cost at the 0.x → 1.0 boundary. Within 1.x, the
`format_version` mechanism above means snapshots are never stranded by an
upgrade.

## Freeze enforcement

The wire format is locked by tests in `sayiir-core/src/snapshot_format.rs`:

- `header_layout_is_frozen` pins the exact header bytes.
- `golden_blob_is_frozen` pins the full JSON wire bytes of a representative
  snapshot via a committed golden blob; it fails if the serialized form drifts.
- Negative tests cover `MissingMagic`, `Truncated`, `UnsupportedVersion`,
  `UnknownCodec`, and `CodecMismatch`.

A failing golden test is the signal that the durable format changed: such a
change **must** be accompanied by a `format_version` bump and a documented read
path, never a silent edit.

[guide]: https://docs.sayiir.dev/guides/serialization-and-versioning/
