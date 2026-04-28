#!/usr/bin/env python3
"""Pair `zmqrs/<pattern>/...` and `libzmq/<pattern>/...` criterion rows
(same transport + size) and write a side-by-side `comparison.html`.

HTML shell + CSS live in `bench-templates/`. This script only emits the
table rows and substitutes them into the template.
"""

import json
import os
import shutil
import sys
from collections import defaultdict
from pathlib import Path

TEMPLATE_DIR = Path(__file__).parent / "bench-templates"

ARRIVAL_NOTE = (
    '<p class="summary"><strong>Per-sub columns</strong> '
    "(pub_sub, xpub_sub): median / p99 latency from publish to "
    "each subscriber receiving the message. Criterion&rsquo;s "
    "headline number is the barrier time (time until the slowest "
    "subscriber finishes) — informative as a tail metric but "
    "dominated by stragglers. Median-per-sub is the typical-case "
    "latency a real application would observe.</p>"
)


def walk_estimates(root: Path):
    """Yield (full_id, value_str, median_ns, arrival, bm_path) for every
    criterion benchmark under `root`. `arrival` is the optional per-sub
    distribution from a sibling `arrival.json` (see ArrivalStats in
    compare_libzmq.rs)."""
    for benchmark_json in root.rglob("new/benchmark.json"):
        est = benchmark_json.parent / "estimates.json"
        if not est.exists():
            continue
        try:
            bm = json.loads(benchmark_json.read_text())
            es = json.loads(est.read_text())
        except json.JSONDecodeError:
            continue
        full_id = bm.get("full_id")
        value_str = bm.get("value_str") or ""
        median_ns = es.get("median", {}).get("point_estimate")
        if not full_id or median_ns is None:
            continue
        arrival_path = benchmark_json.parent.parent / "arrival.json"
        arrival = None
        if arrival_path.exists():
            try:
                arrival = json.loads(arrival_path.read_text())
            except json.JSONDecodeError:
                arrival = None
        yield full_id, value_str, float(median_ns), arrival, benchmark_json


def fmt_time(ns: float) -> str:
    if ns >= 1e9:
        return f"{ns / 1e9:.2f} s"
    if ns >= 1e6:
        return f"{ns / 1e6:.2f} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.2f} µs"
    return f"{ns:.0f} ns"


def fmt_size(value_str: str) -> str:
    if not value_str:
        return "—"
    try:
        n = int(value_str)
        if n >= 1024 * 1024:
            return f"{n // (1024 * 1024)} MB"
        if n >= 1024:
            return f"{n // 1024} KB"
        return f"{n} B"
    except ValueError:
        return value_str


def fmt_arrival(a):
    """Format 'p50 / p99' from per_sub distribution."""
    if not a:
        return "&mdash;"
    ps = a.get("per_sub_ns") or {}
    p50, p99 = ps.get("p50"), ps.get("p99")
    if p50 is None:
        return "&mdash;"
    return f"{fmt_time(float(p50))} / {fmt_time(float(p99))}"


def ratio_cell(zmqrs_ns: float, libzmq_ns: float) -> str:
    """HTML cell colouring the ratio green (zmq.rs faster) or red (slower)."""
    if zmqrs_ns <= 0:
        return '<td>—</td>'
    r = libzmq_ns / zmqrs_ns
    if r >= 2.0:
        cls = "huge-win"
    elif r >= 1.1:
        cls = "win"
    elif r <= 0.9:
        cls = "loss"
    else:
        cls = "tie"
    label = f"{r:.2f}×"
    if r < 1:
        label = f"{r:.2f}× ({1 / r:.2f}× slower)"
    return f'<td class="ratio {cls}">{label}</td>'


def parse_id(full_id: str):
    """Split full_id into (side, pattern, transport, param).
    e.g. `libzmq/pub_sub/tcp/subs=8/16` → `("libzmq", "pub_sub", "tcp", "subs=8/16")`."""
    parts = full_id.split("/")
    if len(parts) < 3:
        return None
    side = parts[0]
    if side not in ("zmqrs", "libzmq"):
        return None
    transports = ("tcp", "ipc", "inproc")
    try:
        tidx = next(i for i, p in enumerate(parts) if p in transports)
    except StopIteration:
        return None
    pattern = "/".join(parts[1:tidx])
    transport = parts[tidx]
    param = "/".join(parts[tidx + 1 :])
    return side, pattern, transport, param


def render_paired_table(paired, has_arrival):
    out = ["<h2>Head-to-head</h2>"]
    header = (
        '<table><thead><tr>'
        "<th>Pattern</th><th>Transport</th><th>Size</th>"
        '<th class="num">zmq.rs barrier</th>'
        '<th class="num">libzmq barrier</th>'
    )
    if has_arrival:
        header += (
            '<th class="num">zmq.rs p50/p99</th>'
            '<th class="num">libzmq p50/p99</th>'
        )
    header += "<th>Ratio</th></tr></thead><tbody>"
    out.append(header)

    for (pattern, transport, param), v in paired:
        zmqrs_ns = v["zmqrs"]
        libzmq_ns = v["libzmq"]
        size = param.rsplit("/", 1)[-1]
        extra = param.rsplit("/", 1)[0] if "/" in param else ""
        display = pattern + (f" ({extra})" if extra else "")

        cells = [
            f'<td><a href="{v["zmqrs_report"]}">{display}</a></td>',
            f"<td>{transport}</td>",
            f"<td>{fmt_size(size)}</td>",
            f'<td class="num">{fmt_time(zmqrs_ns)}</td>',
            f'<td class="num"><a href="{v["libzmq_report"]}">{fmt_time(libzmq_ns)}</a></td>',
        ]
        if has_arrival:
            cells.append(f'<td class="num">{fmt_arrival(v.get("zmqrs_arrival"))}</td>')
            cells.append(f'<td class="num">{fmt_arrival(v.get("libzmq_arrival"))}</td>')

        # Use per-sub p50 when both sides have arrival data;
        # criterion barrier median otherwise.
        za_p50 = (v.get("zmqrs_arrival") or {}).get("per_sub_ns", {}).get("p50")
        la_p50 = (v.get("libzmq_arrival") or {}).get("per_sub_ns", {}).get("p50")
        if za_p50 and la_p50:
            cells.append(ratio_cell(float(za_p50), float(la_p50)))
        else:
            cells.append(ratio_cell(zmqrs_ns, libzmq_ns))

        out.append("<tr>" + "".join(cells) + "</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def render_unpaired_table(rows, side: str, heading: str):
    side_col = "zmq.rs" if side == "zmqrs" else "libzmq"
    out = [
        f"<h2>{heading}</h2>",
        '<table><thead><tr>'
        "<th>Pattern</th><th>Transport</th><th>Size</th>"
        f'<th class="num">{side_col}</th>'
        "</tr></thead><tbody>",
    ]
    for (pattern, transport, param), v in rows:
        size = param.rsplit("/", 1)[-1]
        extra = param.rsplit("/", 1)[0] if "/" in param else ""
        display = pattern + (f" ({extra})" if extra else "")
        out.append(
            f'<tr><td><a href="{v[f"{side}_report"]}">{display}</a></td>'
            f"<td>{transport}</td>"
            f"<td>{fmt_size(size)}</td>"
            f'<td class="num">{fmt_time(v[side])}</td></tr>'
        )
    out.append("</tbody></table>")
    return "\n".join(out)


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("latest")
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(".")
    if not root.exists():
        print(f"error: {root} does not exist", file=sys.stderr)
        sys.exit(1)

    rows: dict[tuple[str, str, str], dict] = defaultdict(dict)
    for full_id, _value, median_ns, arrival, bm_path in walk_estimates(root):
        parsed = parse_id(full_id)
        if not parsed:
            continue
        side, pattern, transport, param = parsed
        # bm_path: latest/<shard>/<group_dir>/<value>/new/benchmark.json
        report_dir = bm_path.parent.parent
        report_rel = os.path.relpath(report_dir / "report" / "index.html", out_dir)
        key = (pattern, transport, param)
        rows[key][side] = median_ns
        rows[key][f"{side}_report"] = report_rel
        if arrival is not None:
            rows[key][f"{side}_arrival"] = arrival

    def sort_key(item):
        (pattern, transport, param), _ = item
        transport_order = {"tcp": 0, "ipc": 1, "inproc": 2}
        try:
            size = int(param.rsplit("/", 1)[-1])
        except ValueError:
            size = 0
        return (pattern, transport_order.get(transport, 3), param.count("/"), size)

    paired = sorted(
        ((k, v) for k, v in rows.items() if "zmqrs" in v and "libzmq" in v),
        key=sort_key,
    )
    zmqrs_only = sorted(
        ((k, v) for k, v in rows.items() if "zmqrs" in v and "libzmq" not in v),
        key=sort_key,
    )
    libzmq_only = sorted(
        ((k, v) for k, v in rows.items() if "libzmq" in v and "zmqrs" not in v),
        key=sort_key,
    )

    has_arrival = any(
        "zmqrs_arrival" in v or "libzmq_arrival" in v for _, v in paired
    )
    tables = []
    if paired:
        tables.append(render_paired_table(paired, has_arrival))
    if zmqrs_only:
        tables.append(
            render_unpaired_table(
                zmqrs_only, "zmqrs",
                "zmq.rs only <small>(no libzmq counterpart)</small>",
            )
        )
    if libzmq_only:
        tables.append(render_unpaired_table(libzmq_only, "libzmq", "libzmq only"))

    template = (TEMPLATE_DIR / "comparison.html").read_text()
    page = template.format(
        arrival_note=ARRIVAL_NOTE if has_arrival else "",
        tables="\n".join(tables),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "comparison.html").write_text(page)
    shutil.copy(TEMPLATE_DIR / "style.css", out_dir / "style.css")


if __name__ == "__main__":
    main()
