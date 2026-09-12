# Benchmark data

The tiny fixture needs no download. The current generated-data measurements
use the standard SNB BI **SF0.003 composite-merged-fk** snapshot: no sampling,
custom projection, padded rows or precomputed answers.

From the repository root on macOS (Linux can use `sha256sum`):

```sh
LDBC_DATA_DIR=$(mktemp -d /tmp/ddir-ldbc-data.XXXXXX)
curl --fail --location --output "$LDBC_DATA_DIR/sf0003.zip" \
  https://raw.githubusercontent.com/ldbc/ldbc_snb_datagen_spark/3fbc285b0ffbc1a00d6ae9d2cfd95056d88e06e8/social-network-sf0.003-bi-composite-merged-fk.zip
shasum -a 256 "$LDBC_DATA_DIR/sf0003.zip"
```

Verify the 2,024,762-byte archive's exact SHA-256 before extracting:

```text
c66014cd90ae71f98f78e4be925fffad8a8d88558ad6ced173477fa318a46a6a
```

Then, in the same shell:

```sh
unzip -q "$LDBC_DATA_DIR/sf0003.zip" -d "$LDBC_DATA_DIR"
LDBC_SNAPSHOT="$LDBC_DATA_DIR/social-network-sf0.003-bi-composite-merged-fk/graphs/csv/bi/composite-merged-fk/initial_snapshot"
```

The full suite adapter projects 35,588 rows, including 50 people. All common
relations are loaded even for isolated queries. See [REFRESH.md](REFRESH.md)
for the exact standard-runner commands and [CURRENT.md](CURRENT.md) for the
measured results and their limits.

Download/extract outside the checkout. Snapshot files remain read-only; the
runner performs retractions/restoration through the server. Neither this
snapshot nor the suite's smoke parameter/update distribution establishes
official LDBC conformance or larger-scale performance.
