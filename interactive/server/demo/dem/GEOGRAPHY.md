# Where the boards actually are

Verified by web-mercator tile math plus elevation cross-checks against known
points (lake and valley-floor heights match the DEM to within a few meters).
In both boards **row 0 is north, x increases east**; a renderer that draws row
0 at the bottom shows the world mirrored north-south — no rotation fixes that,
re-check the vertical axis first.

Cell size is `156543.03 * cos(latitude) / 2^zoom` meters — note the cos
factor: zoom 11 here is ~52.6 m per cell (not ~76), zoom 12 is ~26.3 m.

| board | zoom | cells | NW corner | SE corner |
|---|---|---|---|---|
| `engadin_128.txt` | 11 | 128×128, ~53 m | 46.5513N 9.8053E | 46.4908N 9.8932E |
| `engadin_256.txt` | 12 | 256×256, ~26 m | 46.5485N 9.8204E | 46.4880N 9.9083E |
| `engadin_wide.txt` | 11 | 256×192, ~53 m | 46.5589N 9.7559E | 46.4681N 9.9316E |

The 256 board is the same reach 2× finer, nudged ~1.2 km east and ~0.3 km
south so Celerina and Pontresina make it into frame. It is a separate board
for looking closer: nothing calibrated for the game (budgets, dam, village
cells, road grants) transfers to it.

The wide board is instead the persistent game's V4 continuation. It keeps the
53 m scale and extends the calibrated board 72 cells west, 56 east, and 16
north and south. Every old terrain cell is exactly preserved at
`wide(x,y) = old(x+72,y+16)`. V4 freezes the completed V3 relations under that
transform rather than rerunning old route choices on a newly enlarged graph.

## Landmarks, (x, y) = (column, row)

| place | engadin_128 | engadin_256 | engadin_wide | height |
|---|---|---|---|---|
| Lej da San Murezzan (St. Moritz lake) | (59, 121) | (75, 231) | (131, 137) | 1763 (real 1768) |
| St. Moritz Dorf (hillside above) | ~(44, 112) | ~(44, 212) | ~(116, 128) | slope, 1820–1900 |
| Champfèr/Suvretta flats | SW corner, 1822 flat | SW corner, 1823 flat | ~(116, 136) | ~1822 |
| Samedan | (99, 36) | (153, 60) | (171, 52) | 1705 |
| Celerina (valley floor by the Inn) | off-board (~5 cells E) | ~(185, 172) | ~(150, 98) | 1722–1727 |
| Pontresina | off-board (~13 cells E) | (238, 245) | ~(211, 134) | 1793–1807 |
| Piz Nair summit | off-board west | off-board west | ~(45, 111) | 3016 |
| Piz Nair ridge observatory (game) | off-board west | off-board west | (55, 110) | 2861 |
| Muottas ridge shelter (game) | off-board east | off-board east | (213, 75) | ~2400 |
| natural Inn exit (toward Bever) | NE corner, east edge y≈14 | thread x≈180–199 to N/NE edge | ~(200, 30) | ~1694 |

Corrections to labels used earlier in this project's notes: the reservoir the
game dams is **not** a gorge by St. Moritz — it is the **Inn meadows between
Celerina and Samedan**, and the "dam" is a ~3.5 km north–south dyke crossing
them just west of Samedan (up to 71 m tall at the river). The flooded
"village" cells are the western edge of Samedan. Pontresina cannot be found
on the 128 board because it is not on it.

## What 2× resolution changed (and didn't)

Measured with the same boundary-draining priority flood the demos assert
against:

- **The dam is resolution-stable.** The same physical wall (column x=96 on
  128 = column x=148 on 256) with crest 1775 impounds the same reservoir:
  area agrees to 0.3% (3.52 vs 3.53 km²), volume to 1.8% (175 vs 178 M m³).
- **St. Moritz lake becomes credible.** On the 128 board its flat touches
  the south frame edge, so the boundary drain truncates it to an 86-cell
  puddle. On the 256 board it fills to 1767 over 1234 cells ≈ 0.86 km²
  (real: 1768 m, 0.78 km²) — though the pour point is still the frame edge
  upstream (row 255 dips to 1767), which happens to sit at nearly the real
  outlet level.
- **The Charnadüras gorge is still dammed by resolution itself.** The real
  gorge floor descends ~1768 → ~1730 toward Celerina, but it is so narrow
  that at 26 m cells its resolved saddle is **1792**. That — not the 1775
  crest — is why the game reservoir never backs up into St. Moritz lake on
  either board. On a board fine enough to resolve the gorge (zoom 13 is
  ~13 m; swissALTI3D is 0.5–2 m), the same 1775 dam **would** flood
  St. Moritz lake by ~7 m. Refinement does not just sharpen the picture;
  it changes which places are coupled.

The last point is the general lesson about resolution and one-cell-wide
things, in both directions: a terrain feature narrower than a cell (a 30 m
gorge) does not exist on the board, and a built feature one cell wide (a
road, a dyke) silently means "as wide as a cell". Both stop being artifacts
only when cells shrink below the feature's physical width — ~10 m for a
road, which is the natural floor for a one-voxel-road rule.
