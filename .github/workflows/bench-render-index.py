#!/usr/bin/env python3
"""Render the gh-pages landing page from `bench-templates/index.html`.

Walks `latest/` for shard directories and `runs/` for prior commit
snapshots, builds the two `<ul>` lists, and substitutes them into the
template. Replaces what used to be an inline heredoc in bench.yml.

Usage: bench-render-index.py <site-root> <commit-sha-short> <iso-date>
"""

import sys
from pathlib import Path

TEMPLATE_DIR = Path(__file__).parent / "bench-templates"


def shard_list(latest_dir: Path) -> str:
    if not latest_dir.exists():
        return "<li>No shards in latest run.</li>"
    items = []
    for d in sorted(latest_dir.iterdir()):
        if not d.is_dir():
            continue
        report = d / "report" / "index.html"
        if not report.exists():
            continue
        items.append(
            f'<li><a href="latest/{d.name}/report/index.html">{d.name}</a> '
            f'(<a href="latest/{d.name}/runner-info.md">runner</a>, '
            f'<a href="latest/{d.name}/bench-output.txt">raw output</a>)</li>'
        )
    return "\n".join(items) if items else "<li>No shards with reports.</li>"


def run_list(runs_dir: Path) -> str:
    if not runs_dir.exists():
        return "<li>No prior runs.</li>"
    items = []
    for d in sorted(runs_dir.iterdir(), reverse=True):
        if d.is_dir():
            items.append(f'<li><a href="runs/{d.name}/">{d.name}</a></li>')
    return "\n".join(items) if items else "<li>No prior runs.</li>"


def main():
    if len(sys.argv) != 4:
        print("usage: bench-render-index.py <site-root> <commit-short> <iso-date>",
              file=sys.stderr)
        sys.exit(1)
    site = Path(sys.argv[1])
    commit = sys.argv[2]
    date = sys.argv[3]

    template = (TEMPLATE_DIR / "index.html").read_text()
    page = template.format(
        commit=commit,
        date=date,
        shard_list=shard_list(site / "latest"),
        run_list=run_list(site / "runs"),
    )

    (site / "index.html").write_text(page)


if __name__ == "__main__":
    main()
