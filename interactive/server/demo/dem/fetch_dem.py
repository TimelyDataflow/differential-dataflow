#!/usr/bin/env python3
"""Fetch and window a real DEM for the water demo: the Upper Engadine around
St. Moritz, from the AWS Open Data terrain tiles (Terrarium encoding).

Two committed boards, selected by preset name (default engadin_128):

  engadin_128  zoom 11, ~53 m cells, 128x128. The calibrated game board:
               Lej da San Murezzan (~1763 m) in the southwest, the Inn
               meadows (~1694-1710 m) running northeast to Samedan, and
               the dam line run_dem.py raises at column x=96 — a
               north-south wall across the meadows just west of Samedan.
               Celerina and Pontresina lie just past its east edge.
  engadin_256  zoom 12, ~26 m cells, 256x256. The same reach at twice the
               resolution, shifted 22 zoom-11 cells east and 6 south so
               Celerina (~(185,172)) and Pontresina (~(238,245)) are in
               frame. A separate board for looking closer; nothing that
               plays on engadin_128 is calibrated for it. The dam column
               x=96 of the game board is column x=148 here. See
               GEOGRAPHY.md for verified landmarks and georeferencing.

Row 0 is north. Cell size is 156543.03 * cos(latitude) / 2^zoom meters.
The generated files are committed, so this script is only needed to
regenerate or re-window. Pure stdlib: a minimal PNG decoder rather than a
PIL/GDAL dependency.
"""

import os
import struct
import sys
import urllib.request
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))

# window = (x0, y0, w, h) in pixels of the tile mosaic, origin at the
# northwest corner of the lowest-numbered tile.
PRESETS = {
    "engadin_128": dict(
        zoom=11,
        tiles=[(1079, 724), (1079, 725), (1080, 724), (1080, 725)],
        window=(200, 16, 128, 128),
        out="engadin_128.txt",
    ),
    "engadin_256": dict(
        zoom=12,
        tiles=[(2159, 1448), (2159, 1449), (2160, 1448), (2160, 1449)],
        window=(188, 44, 256, 256),
        out="engadin_256.txt",
    ),
}


def decode_png(data):
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    pos, idat, w, h, ctype = 8, b"", 0, 0, 0
    while pos < len(data):
        ln = struct.unpack(">I", data[pos:pos + 4])[0]
        typ = data[pos + 4:pos + 8]
        body = data[pos + 8:pos + 8 + ln]
        if typ == b"IHDR":
            w, h, depth, ctype = struct.unpack(">IIBB", body[:10])
            assert depth == 8 and ctype in (2, 6), (depth, ctype)
        elif typ == b"IDAT":
            idat += body
        pos += 12 + ln
    raw = zlib.decompress(idat)
    ch = 3 if ctype == 2 else 4
    stride = w * ch
    out = bytearray(h * stride)
    prev = bytearray(stride)
    p = 0
    for y in range(h):
        f = raw[p]
        p += 1
        line = bytearray(raw[p:p + stride])
        p += stride
        if f == 1:
            for i in range(ch, stride):
                line[i] = (line[i] + line[i - ch]) & 255
        elif f == 2:
            for i in range(stride):
                line[i] = (line[i] + prev[i]) & 255
        elif f == 3:
            for i in range(stride):
                a = line[i - ch] if i >= ch else 0
                line[i] = (line[i] + ((a + prev[i]) >> 1)) & 255
        elif f == 4:
            for i in range(stride):
                a = line[i - ch] if i >= ch else 0
                b = prev[i]
                c = prev[i - ch] if i >= ch else 0
                pp = a + b - c
                pa, pb, pc = abs(pp - a), abs(pp - b), abs(pp - c)
                pr = a if (pa <= pb and pa <= pc) else (b if pb <= pc else c)
                line[i] = (line[i] + pr) & 255
        out[y * stride:(y + 1) * stride] = line
        prev = line
    # Terrarium: height = R*256 + G + B/256 - 32768, rounded down to meters.
    heights = [[0] * w for _ in range(h)]
    for y in range(h):
        row = out[y * stride:(y + 1) * stride]
        hr = heights[y]
        for x in range(w):
            o = x * ch
            hr[x] = row[o] * 256 + row[o + 1] + row[o + 2] // 256 - 32768
    return heights


def main(preset):
    zoom, tiles = preset["zoom"], preset["tiles"]
    x0, y0, w, h = preset["window"]
    tx0 = min(tx for tx, _ in tiles)
    ty0 = min(ty for _, ty in tiles)
    mw = (max(tx for tx, _ in tiles) - tx0 + 1) * 256
    mh = (max(ty for _, ty in tiles) - ty0 + 1) * 256
    mosaic = [[0] * mw for _ in range(mh)]
    for tx, ty in tiles:
        url = f"https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{zoom}/{tx}/{ty}.png"
        print("fetching", url)
        tile = decode_png(urllib.request.urlopen(url).read())
        ox, oy = (tx - tx0) * 256, (ty - ty0) * 256
        for y in range(256):
            mosaic[oy + y][ox:ox + 256] = tile[y]
    out = os.path.join(HERE, preset["out"])
    with open(out, "w") as f:
        for y in range(y0, y0 + h):
            f.write(" ".join(str(mosaic[y][x]) for x in range(x0, x0 + w)) + "\n")
    print("wrote", out)


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "engadin_128"
    if name not in PRESETS:
        sys.exit(f"unknown preset {name!r}; one of {sorted(PRESETS)}")
    main(PRESETS[name])
