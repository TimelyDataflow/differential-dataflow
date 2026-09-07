# Pinned generated data

The default [SNB run](SNB.md#run) needs no download. These are the two external
BI snapshots used in the exploratory measurements, not an official Interactive
driver dataset. They use `graphs/csv/bi/composite-merged-fk/initial_snapshot`.
Download outside the checkout; do not commit generated data or benchmark outputs.

## SF0.003: small generated-data gate

Use the pre-generated archive at datagen artifact revision
[`3fbc285b`](https://github.com/ldbc/ldbc_snb_datagen_spark/tree/3fbc285b0ffbc1a00d6ae9d2cfd95056d88e06e8).
It is 2,024,762 bytes. From the repository root on macOS (Linux can substitute
`sha256sum` for `shasum -a 256`):

```sh
LDBC_DATA_DIR=$(mktemp -d /tmp/ddir-ldbc-data.XXXXXX)
curl --fail --location --output "$LDBC_DATA_DIR/sf0003.zip" \
  https://raw.githubusercontent.com/ldbc/ldbc_snb_datagen_spark/3fbc285b0ffbc1a00d6ae9d2cfd95056d88e06e8/social-network-sf0.003-bi-composite-merged-fk.zip
shasum -a 256 "$LDBC_DATA_DIR/sf0003.zip"
```

Verify this exact SHA-256 **before extracting**:

```text
c66014cd90ae71f98f78e4be925fffad8a8d88558ad6ced173477fa318a46a6a
```

Then, in the same shell:

```sh
unzip -q "$LDBC_DATA_DIR/sf0003.zip" -d "$LDBC_DATA_DIR"
LDBC_SNAPSHOT="$LDBC_DATA_DIR/social-network-sf0.003-bi-composite-merged-fk/graphs/csv/bi/composite-merged-fk/initial_snapshot"
python3 interactive/server/bench/ldbc/suite.py --server target/release/ddir_server \
  --snapshot "$LDBC_SNAPSHOT" --queries is1 is3 ic11 --workers 1 \
  --rounds 3 --warmup 1
```

The full adapter projects 35,588 rows from this archive (50 people). This
selected panel is a first generated-data gate, not all-query coverage. Expand
only after inspecting memory and results. See [build/measurement conventions](MEASUREMENTS.md#making-a-new-comparable-record).
The download digest, extraction path, and this command were rechecked against
the merged foundation on 2026-09-07: both backends passed, with zero swap and
about 168 MiB sampled combined footprint under an external 1-GiB guard.
This is a recipe check, not a new SF1 performance result.

## SF1: historical scale identity, not a default run recommendation

The exploratory SF1 snapshot was the
[BI pre-audit archive](https://datasets.ldbcouncil.org/bi-pre-audit/bi-sf1-composite-merged-fk.tar.zst):

```text
archive: bi-sf1-composite-merged-fk.tar.zst
bytes:   216780094
sha256:  a72938e244e6aa9d99632fcd5065e50c669ecf4d00f60bd162b266df4a7aba13
snapshot within archive:
bi-sf1-composite-merged-fk/graphs/csv/bi/composite-merged-fk/initial_snapshot
```

Download with `curl --fail --location --output` into a new external directory,
verify the digest, and extract with a tar implementation supporting zstd (or
`zstd -dc archive.tar.zst | tar -xf - -C destination`). The dated archive name
is not itself immutable; the digest is the dataset identity. Its URL responded
with the recorded content length on 2026-09-07. Neither deletes nor inserts from
the archive are replayed by the current suite; it makes its own bounded churn.

The historical full projection had 11,519,991 rows, including 10,295 people and
2,860,664 messages. Historical selective IC11 loaded 214,768 rows. Selecting
IC11 in the merged suite does **not** reproduce that selective footprint:
all common relations are loaded and Python validates the query. Loader changes
also require checking the new report's hashes rather than assuming identity.

Do not copy the old overnight 8-GiB RSS limits as safe settings for a 16-GiB
mini. Use whole-process-group memory/pressure controls, including Python and
compressed memory, before trying SF1. The merged sampled server-RSS monitor is
not sufficient; portable safe scale execution is [LDBC-008](GAPS.md#ldbc-008-portable-scale-runs-and-representative-workload-banks).
There is no claim that all 41 concurrent SF1 queries fit this machine.
