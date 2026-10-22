#!/bin/sh
# Start (or restart) Umbra on a CPU set, with DATA_DIR mounted at /data:
#     DATA_DIR=/path/to/projected-csv ./umbra_server.sh 0-3
# The container's CPU set fixes Umbra's thread count. --rm also removes its database volume.
DOCKER=${DOCKER:-docker}
PYTHON=${PYTHON:-python3}
HERE=$(cd "$(dirname "$0")" && pwd)
$DOCKER rm -f umbra-ldbc >/dev/null 2>&1
$DOCKER run -d --rm --name umbra-ldbc --cpuset-cpus "$1" -p 5433:5432 -v "$DATA_DIR:/data" umbradb/umbra:26.10 >/dev/null
for i in $(seq 1 30); do
  (cd "$HERE" && "$PYTHON" -c "import sqlsystems; sqlsystems.umbra_connect()") 2>/dev/null && exit 0
  sleep 1
done
echo "umbra did not start" >&2; exit 1
