#!/bin/sh
# Run every AoC 2023 program through the DDIR server and check the answers.
# Usage: ./run.sh [vec|corgi] [path/to/ddir_server]
#   (build with: cargo build --release -p ddir-server)
cd "$(dirname "$0")" || exit 1
BACKEND=${1:-vec}
SERVER=${2:-../../../target/release/ddir_server}
python3 transcribe.py || exit 1   # dense dayNN/input.txt -> gen/dayNN/ fact files
fail=0
while read -r day part expected; do
  case "$day" in ''|'#'*) continue;; esac
  dir=day$day
  prog=$dir/part$part.ddp
  # One session per part: load the program, feed each input it reads from
  # its fact file (part-specific if present), close the epoch. The answer is
  # the `Int` on the `[partN]` inspect line.
  cmds="load p from $prog"
  k=0
  while :; do
    inp=gen/$dir/part$part.in$k.txt
    [ -f "$inp" ] || inp=gen/$dir/in$k.txt
    [ -f "$inp" ] || break
    grep -Eq "input $k([^0-9]|$)" "$prog" && cmds="$cmds
feed p $k from $inp"
    k=$((k + 1))
  done
  got=$(printf '%s\ntick\nexit\n' "$cmds" \
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
