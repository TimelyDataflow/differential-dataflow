#!/usr/bin/env python3
"""Fetch and window a real DEM for the water demo: the Upper Engadine around
St. Moritz, from the AWS Open Data terrain tiles (Terrarium encoding, ~76 m
per pixel at zoom 11 / lat 46.5).

Writes engadin_128.txt: 128 rows of 128 integer heights (meters), the window
x in [200, 328), y in [16, 144) of the 2x2 tile mosaic at zoom 11, tiles
x in {1079, 1080}, y in {724, 725}. Contains Lej da San Murezzan (~1763 m),
the Inn valley exiting northeast toward Celerina (~1694 m floor), and the
Charnaduers gorge the run_dem.py driver dams.

The generated file is committed, so this script is only needed to regenerate
or re-window. Pure stdlib: a minimal PNG decoder rather than a PIL/GDAL
dependency.
"""

import os
import struct
import urllib.request
import zlib

TILES = [(1079, 724), (1079, 725), (1080, 724), (1080, 725)]
ZOOM = 11
X0, Y0, W, H = 200, 16, 128, 128
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "engadin_128.txt")


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


def main():
    mosaic = [[0] * 512 for _ in range(512)]
    for tx, ty in TILES:
        url = f"https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{ZOOM}/{tx}/{ty}.png"
        print("fetching", url)
        tile = decode_png(urllib.request.urlopen(url).read())
        ox, oy = (tx - 1079) * 256, (ty - 724) * 256
        for y in range(256):
            mosaic[oy + y][ox:ox + 256] = tile[y]
    with open(OUT, "w") as f:
        for y in range(Y0, Y0 + H):
            f.write(" ".join(str(mosaic[y][x]) for x in range(X0, X0 + W)) + "\n")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
