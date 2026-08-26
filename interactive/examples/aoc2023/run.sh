#!/bin/sh
# Run every AoC 2023 program on the vec backend and check the answers.
# Usage: ./run.sh [path/to/ddir]   (build with: cargo build --release --example ddir)
cd "$(dirname "$0")" || exit 1
DDIR=${1:-../../../target/release/examples/ddir}
python3 transcribe.py || exit 1   # dense dayNN/input.txt -> gen/dayNN/ fact files
fail=0
while read -r day part expected; do
  case "$day" in ''|'#'*) continue;; esac
  dir=day$day
  inp=gen/$dir/input.txt
  [ -f "gen/$dir/input$part.txt" ] && inp=gen/$dir/input$part.txt   # day05/day15: per-part inputs
  arity=$(head -1 "$inp" | awk '{print NF}')
  got=$(EDGES_FILE=$inp "$DDIR" --backend=vec "$dir/part$part.ddp" "$arity" 10 0 1 0 2>&1 \
        | sed -n "s/.*\\[part$part\\].*Int(\\(-\\{0,1\\}[0-9]*\\)).*/\\1/p")
  if [ "$got" = "$expected" ]; then
    echo "day$day part$part: ok ($got)"
  else
    echo "day$day part$part: FAIL (expected $expected, got '$got')"
    fail=1
  fi
done < expected.txt
exit $fail
