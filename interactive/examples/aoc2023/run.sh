#!/bin/sh
# Run every AoC 2023 program through the DDIR server and check the answers.
# Usage: ./run.sh [vec|corgi] [path/to/ddir_server]
#   (build with: cargo build --release -p ddir-server)
cd "$(dirname "$0")" || exit 1
BACKEND=${1:-vec}
SERVER=${2:-../../../target/release/ddir_server}
PAD=; [ "$BACKEND" = corgi ] && PAD=--pad   # corgi needs day05's uniform-arity copies
python3 transcribe.py $PAD || exit 1   # dense dayNN/input.txt -> gen/dayNN/ fact files
fail=0
while read -r day part expected; do
  case "$day" in ''|'#'*) continue;; esac
  dir=day$day
  inp=gen/$dir/input.txt
  [ -f "gen/$dir/input$part.txt" ] && inp=gen/$dir/input$part.txt   # day05/day15: per-part inputs
  [ -n "$PAD" ] && [ -f "gen/$dir/input${part}p.txt" ] && inp=gen/$dir/input${part}p.txt
  # One session per part: load the program, feed its input from the fact
  # file, close the epoch. The answer is the `Int` on the `[partN]` inspect line.
  got=$(printf 'load p from %s\nfeed p 0 from %s\ntick\nexit\n' "$dir/part$part.ddp" "$inp" \
        | DDIR_BACKEND="$BACKEND" "$SERVER" 2>&1 \
        | sed -n "s/.*\\[part$part\\].*Int(\\(-\\{0,1\\}[0-9]*\\)).*/\\1/p")
  if [ "$got" = "$expected" ]; then
    echo "day$day part$part: ok ($got)"
  else
    echo "day$day part$part: FAIL (expected $expected, got '$got')"
    fail=1
  fi
done < expected.txt
exit $fail
