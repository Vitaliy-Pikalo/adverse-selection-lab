"""Render measured optimization results with an optional isolated Matplotlib install."""

import argparse
import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/opt/asl-spark/plot-cache")

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--before-profile", type=Path, required=True)
    parser.add_argument("--after-profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("spark/figures"))
    args = parser.parse_args()
    results = json.loads(args.results.read_text())
    before = json.loads(args.before_profile.read_text())
    after = json.loads(args.after_profile.read_text())
    assert all(item["status"] == "PASS" for item in (results, before, after))
    names = ("pandas", "spark_baseline", "spark")
    labels = ("pandas", "Original Spark", "Directory candidate")
    colors = ("#64748b", "#7c3aed", "#0f766e")
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "svg.fonttype": "none"})
    figure, (latency, tasks) = plt.subplots(
        1, 2, figsize=(14, 5.8), gridspec_kw={"width_ratios": [1.2, 1]}
    )
    figure.subplots_adjust(left=0.14, right=0.96, top=0.78, bottom=0.25, wspace=0.65)
    longest = max(run["wall_seconds"] for run in results["runs"])
    for position, (name, color) in enumerate(zip(names, colors, strict=True)):
        values = [run["wall_seconds"] for run in results["runs"] if run["engine"] == name]
        assert len(values) == 3
        median = results["median_seconds"][name]
        latency.barh(position, median, color=color, height=0.48)
        latency.scatter(
            values, [position - 0.07, position, position + 0.07], color="#111827", s=23, zorder=3
        )
        latency.text(
            max(values) + longest * 0.025,
            position,
            f"{median:.2f} s",
            va="center",
            fontsize=10,
            weight="bold",
        )
    latency.set(
        yticks=range(3),
        yticklabels=labels,
        xlim=(0, longest * 1.32),
        xlabel="Fresh-process wall time (seconds)",
        title="Median latency; dots show all 3 runs",
    )
    latency.invert_yaxis()
    metrics = [before["metrics"], after["metrics"]]
    for position, item in enumerate(metrics):
        listing = item["listing_tasks"]
        other = item["total_tasks"] - listing
        tasks.barh(
            position,
            listing,
            height=0.48,
            color="#f59e0b",
            label="File listing" if position == 0 else None,
        )
        tasks.barh(
            position,
            other,
            left=listing,
            height=0.48,
            color="#cbd5e1",
            label="Other work" if position == 0 else None,
        )
        tasks.text(
            item["total_tasks"] + before["metrics"]["total_tasks"] * 0.03,
            position,
            f"{item['total_tasks']:,} total\n{listing:,} listing",
            va="center",
            fontsize=10,
        )
    tasks.set(
        yticks=[0, 1],
        yticklabels=labels[1:],
        xlabel="Tasks in one instrumented full-data run",
        xlim=(0, max(item["total_tasks"] for item in metrics) * 1.34),
        title="Scheduler work measured from event logs",
    )
    tasks.invert_yaxis()
    tasks.legend(frameon=False, fontsize=9, loc="lower right")
    for axes in (latency, tasks):
        axes.spines[["top", "right", "left"]].set_visible(False)
        axes.spines["bottom"].set_color("#cbd5e1")
        axes.tick_params(axis="y", length=0)
        axes.set_axisbelow(True)
        axes.grid(axis="x", color="#e2e8f0", linewidth=0.8)
    figure.suptitle(
        "Spark file discovery: task counts are not batch latency",
        x=0.06,
        y=0.96,
        ha="left",
        fontsize=17,
        weight="bold",
    )
    figure.text(
        0.06,
        0.87,
        "1,121,544 trades · 27 label columns · full-data parity retained · local[*]",
        fontsize=11,
        color="#475569",
    )
    figure.text(
        0.06,
        0.08,
        "Latency: interleaved fresh-process runs; input caches pre-read for hashing.\n"
        "Task counts: separate instrumented profiles, not benchmark timings. "
        "Error bars/confidence intervals are not claimed.",
        fontsize=9,
        color="#475569",
        linespacing=1.6,
    )
    args.output.mkdir(parents=True, exist_ok=True)
    for suffix in ("svg", "png"):
        path = args.output / f"optimization.{suffix}"
        figure.savefig(
            path, dpi=160, facecolor="white", metadata={"Creator": "Adverse Selection Lab"}
        )
        if suffix == "svg":
            lines = (line.rstrip() for line in path.read_text().splitlines())
            path.write_text("\n".join(lines) + "\n")
        print(path)
    plt.close(figure)
    print(f"Matplotlib {matplotlib.__version__}")


if __name__ == "__main__":
    main()
