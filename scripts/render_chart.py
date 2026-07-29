#!/usr/bin/env python3
"""Render docs/backpressure.csv (from demo-capture.sh) as a static SVG
chart for the README. Stdlib only, deterministic output."""
import csv
import sys

CSV_IN = sys.argv[1] if len(sys.argv) > 1 else "docs/backpressure.csv"
SVG_OUT = sys.argv[2] if len(sys.argv) > 2 else "docs/backpressure.svg"

# palette (light surface; the card paints its own background so it reads
# on both GitHub themes)
SURFACE, BORDER = "#fcfcfb", "rgba(11,11,11,0.10)"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, BASE = "#e1e0d9", "#c3c2b7"
NORMAL, HIGH, RATE = "#2a78d6", "#eb6834", "#898781"
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'

rows = []
with open(CSV_IN) as f:
    for r in csv.DictReader(f):
        rows.append({k: float(v) for k, v in r.items()})

# The capture stamps wall-clock time, which NTP can step backwards
# mid-run (seen on WSL2); clamp to monotonic so paths never fold back.
for prev, cur in zip(rows, rows[1:]):
    cur["t_s"] = max(cur["t_s"], prev["t_s"])

T_MAX = max(r["t_s"] for r in rows)
D_MAX = 12000.0   # drops axis ceiling (data ~11.4k)
R_MAX = 800000.0  # rate axis ceiling (burst spikes run ~650-750k pps)

# geometry
W, H = 880, 520
PL, PR = 88, 760            # plot x-range; 760..856 is the end-label gutter
P1_TOP, P1_BOT = 100, 300   # panel 1: cumulative drops
P2_TOP, P2_BOT = 360, 470   # panel 2: receive rate

def x(t): return PL + t / T_MAX * (PR - PL)
def y1(v): return P1_BOT - v / D_MAX * (P1_BOT - P1_TOP)
def y2(v): return P2_BOT - v / R_MAX * (P2_BOT - P2_TOP)
def fmt_k(v): return "0" if v == 0 else (f"{v/1000:g}k" if v >= 1000 else f"{v:g}")

def step_path(pts):  # step-after: counters hold their value until the next sample
    d = f"M{pts[0][0]:.1f} {pts[0][1]:.1f}"
    for (px, _), (nx, ny) in zip(pts, pts[1:]):
        d += f"H{nx:.1f}V{ny:.1f}"
    return d

s = []
s.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}" font-family=\'{FONT}\'>')
s.append(f'<rect x="0.5" y="0.5" width="{W-1}" height="{H-1}" rx="10" '
         f'fill="{SURFACE}" stroke="{BORDER}"/>')

# title, subtitle, legend
s.append(f'<text x="24" y="36" font-size="15" font-weight="600" fill="{INK}">'
         f'Backpressure under burst overload</text>')
s.append(f'<text x="24" y="56" font-size="12" fill="{INK2}">30k pps steady + a 200k-packet '
         f'burst every second &#183; lane capacity: normal 64, high 16 &#183; 1 worker</text>')
for lx, color, label in ((586, NORMAL, "Normal lane"), (706, HIGH, "High-priority lane")):
    s.append(f'<rect x="{lx}" y="27" width="10" height="10" rx="2" fill="{color}"/>')
    s.append(f'<text x="{lx+16}" y="36" font-size="12" fill="{INK2}">{label}</text>')

# shared vertical second-marks + x labels
for sec in range(0, int(T_MAX) + 1):
    px = x(sec)
    for top, bot in ((P1_TOP, P1_BOT), (P2_TOP, P2_BOT)):
        s.append(f'<line x1="{px:.1f}" y1="{top}" x2="{px:.1f}" y2="{bot}" stroke="{GRID}"/>')
    s.append(f'<text x="{px:.1f}" y="492" font-size="11" fill="{MUTED}" text-anchor="middle" '
             f'style="font-variant-numeric:tabular-nums">{sec}s</text>')

def panel(label, top, bot, ticks, yfn):
    s.append(f'<text x="{PL}" y="{top-10}" font-size="11" fill="{MUTED}" '
             f'letter-spacing="0.06em">{label}</text>')
    for v in ticks:
        py = yfn(v)
        stroke = BASE if v == 0 else GRID
        s.append(f'<line x1="{PL}" y1="{py:.1f}" x2="{PR}" y2="{py:.1f}" stroke="{stroke}"/>')
        s.append(f'<text x="{PL-8}" y="{py+4:.1f}" font-size="11" fill="{MUTED}" text-anchor="end" '
                 f'style="font-variant-numeric:tabular-nums">{fmt_k(v)}</text>')

panel("CUMULATIVE DROPPED PACKETS", P1_TOP, P1_BOT, [0, 4000, 8000, 12000], y1)
panel("RECEIVE RATE &#183; PACKETS/S", P2_TOP, P2_BOT, [0, 400000, 800000], y2)

# panel 1: step lines per lane
for key, color in (("dropped_normal", NORMAL), ("dropped_high", HIGH)):
    pts = [(x(r["t_s"]), y1(r[key])) for r in rows]
    s.append(f'<path d="{step_path(pts)}" fill="none" stroke="{color}" '
             f'stroke-width="2" stroke-linejoin="round"/>')

# panel 1: direct end labels (relief for the 175:1 scale gap)
for key, color, name in (("dropped_normal", NORMAL, "Normal"), ("dropped_high", HIGH, "High")):
    v = rows[-1][key]
    py = min(y1(v), P1_BOT - 6)
    s.append(f'<circle cx="{PR+8}" cy="{py:.1f}" r="4" fill="{color}"/>')
    s.append(f'<text x="{PR+18}" y="{py+4:.1f}" font-size="12" fill="{INK2}" '
             f'style="font-variant-numeric:tabular-nums">{name} &#183; {int(v):,}</text>')

# panel 2: receive rate from received-counter deltas (context series, recessive)
rate_pts = []
for a, b in zip(rows, rows[1:]):
    dt = b["t_s"] - a["t_s"]
    if dt > 0:
        rate = max(0.0, (b["received"] - a["received"]) / dt)
        rate_pts.append((x((a["t_s"] + b["t_s"]) / 2), y2(min(rate, R_MAX)), rate))
poly = " ".join(f"{px:.1f},{py:.1f}" for px, py, _ in rate_pts)
s.append(f'<polyline points="{poly}" fill="none" stroke="{RATE}" '
         f'stroke-width="1.5" stroke-linejoin="round"/>')

# annotate the tallest burst spike, beside the point so it never collides
# with the panel header above it
px, py, rv = max(rate_pts, key=lambda p: p[2])
if px < (PL + PR) / 2:
    tx, anchor = px + 8, "start"
else:
    tx, anchor = px - 8, "end"
s.append(f'<text x="{tx:.1f}" y="{py+4:.1f}" font-size="11" fill="{MUTED}" '
         f'text-anchor="{anchor}">burst peak &#8776; {fmt_k(round(rv, -3))} pps</text>')

s.append("</svg>")
with open(SVG_OUT, "w") as f:
    f.write("\n".join(s) + "\n")
print(f"wrote {SVG_OUT}")
