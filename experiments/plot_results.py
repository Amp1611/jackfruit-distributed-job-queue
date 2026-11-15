#!/usr/bin/env python3
"""Draw the graphs for the D2 report from the files written by run_experiments.py.

Usage (any Python with matplotlib):
  python3 experiments/plot_results.py

Reads   results/summary.csv and results/<exp>/*.csv
Writes  results/plots/*.png and results/summary_table.md
"""
import argparse
import csv
import os
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
COLORS = {"static": "tab:red", "dynamic": "tab:blue", "rr": "tab:red", "least": "tab:blue"}

# which numbers to draw for each experiment: (column, axis label)
METRICS = {
    "E2": [("avg_s", "Average latency (s)"), ("p95_s", "95th percentile latency (s)"), ("slow_jobs", "Slow or failed jobs")],
    "E3": [("completion_pct", "Jobs completed (%)"), ("max_s", "Maximum latency (s)"), ("recovery_s", "Recovery time (s)")],
    "E4": [("completion_pct", "Jobs completed (%)"), ("max_s", "Maximum latency (s)"), ("slow_jobs", "Slow or failed jobs")],
    "E5": [("avg_s", "Average latency (s)"), ("p95_s", "95th percentile latency (s)"), ("max_s", "Maximum latency (s)")],
}
TITLES = {"E1": "E1 normal load", "E2": "E2 congestion on the direct link", "E3": "E3 link failure",
          "E4": "E4 worker crash", "E5": "E5 scheduling policy (one slow worker)"}


def number(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def load_summary(path):
    with open(path) as fh:
        return list(csv.DictReader(fh))


def mean_std(rows, column):
    values = [number(r[column]) for r in rows if number(r[column]) is not None]
    if not values:
        return None, 0.0
    return statistics.mean(values), (statistics.stdev(values) if len(values) > 1 else 0.0)


def groups(rows, exp):
    """{variant: {cond: [rows of all repeats]}} for one experiment."""
    out = {}
    for r in rows:
        if r["exp"] == exp:
            out.setdefault(r["variant"], {}).setdefault(r["cond"], []).append(r)
    return out


def save(fig, out, name):
    path = os.path.join(out, name)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print("wrote", path)


def plot_e1(rows, out):
    data = groups(rows, "E1")
    if not data:
        return
    panels = [("avg_s", "Average latency (s)"), ("p95_s", "95th percentile latency (s)"),
              ("completion_pct", "Jobs completed (%)")]
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))
    for ax, (col, label) in zip(axes, panels):
        for variant, conds in data.items():
            xs = sorted(conds, key=lambda c: int(c))
            stats = [mean_std(conds[c], col) for c in xs]
            keep = [(int(c), s) for c, s in zip(xs, stats) if s[0] is not None]
            if keep:
                ax.errorbar([k[0] for k in keep], [k[1][0] for k in keep], yerr=[k[1][1] for k in keep],
                            marker="o", capsize=3, label=variant, color=COLORS.get(variant))
        ax.set_xlabel("Concurrent clients")
        ax.set_ylabel(label)
        ax.grid(alpha=0.3)
        ax.legend()
    fig.suptitle(TITLES["E1"])
    save(fig, out, "E1_load.png")


def plot_bars(rows, exp, out):
    data = groups(rows, exp)
    if not data:
        return
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8))
    for ax, (col, label) in zip(axes, METRICS[exp]):
        names, means, errs, colors = [], [], [], []
        for variant, conds in data.items():
            m, s = mean_std([r for rs in conds.values() for r in rs], col)
            names.append(variant)
            means.append(m or 0.0)
            errs.append(s)
            colors.append(COLORS.get(variant, "gray"))
        ax.bar(names, means, yerr=errs, capsize=4, color=colors)
        ax.set_ylabel(label)
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle(TITLES[exp])
    save(fig, out, "%s_bars.png" % exp)


def plot_timeline(rows, exp, results, out):
    """Latency of every job over time for repeat 1 (a failure shows up as a spike)."""
    data = groups(rows, exp)
    fig, ax = plt.subplots(figsize=(9, 4))
    drawn = False
    event_rel = None
    for variant, conds in data.items():
        for cond, rs in conds.items():
            first = sorted(rs, key=lambda r: int(r["rep"]))[0]
            path = os.path.join(results, exp, "%s_%s_c%s_r%s.csv" % (exp, variant, cond, first["rep"]))
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                jobs = [r for r in csv.DictReader(fh) if r["submit_t"]]
            if not jobs:
                continue
            t0 = min(float(j["submit_t"]) for j in jobs)
            ok = [(float(j["submit_t"]) - t0, float(j["latency_s"])) for j in jobs if j["status"] == "OK"]
            bad = [(float(j["submit_t"]) - t0, float(j["latency_s"] or 0)) for j in jobs if j["status"] != "OK"]
            ax.scatter([p[0] for p in ok], [p[1] for p in ok], s=12, color=COLORS.get(variant), label=variant)
            if bad:
                ax.scatter([p[0] for p in bad], [p[1] for p in bad], s=30, marker="x", color="black",
                           label="%s: not finished" % variant)
            if first["event_t"]:
                event_rel = float(first["event_t"]) - t0
            drawn = True
    if not drawn:
        plt.close(fig)
        return
    if event_rel is not None:
        ax.axvline(event_rel, linestyle="--", color="gray")
        ax.text(event_rel, ax.get_ylim()[1], " failure", va="top", color="gray")
    ax.set_xlabel("Time since the first job was submitted (s)")
    ax.set_ylabel("Job latency (s)")
    ax.set_title(TITLES[exp] + ": latency of each job")
    ax.grid(alpha=0.3)
    ax.legend()
    save(fig, out, "%s_timeline.png" % exp)


def write_table(rows, path):
    cols = [("completion_pct", "done %"), ("avg_s", "avg s"), ("p95_s", "p95 s"), ("max_s", "max s"),
            ("slow_jobs", "slow jobs"), ("recovery_s", "recovery s")]
    lines = ["| exp | variant | cond | runs | " + " | ".join(c[1] for c in cols) + " |",
             "|---|---|---|---|" + "---|" * len(cols)]
    for exp in sorted({r["exp"] for r in rows}):
        for variant, conds in groups(rows, exp).items():
            for cond, rs in sorted(conds.items()):
                cells = []
                for col, _ in cols:
                    m, s = mean_std(rs, col)
                    cells.append("-" if m is None else ("%.2f" % m if s == 0 else "%.2f +/- %.2f" % (m, s)))
                lines.append("| %s | %s | %s | %d | %s |" % (exp, variant, cond, len(rs), " | ".join(cells)))
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print("wrote", path)


def main():
    ap = argparse.ArgumentParser(description="Plot the D2 results")
    ap.add_argument("--results", default=os.path.join(ROOT, "results"))
    args = ap.parse_args()
    summary = os.path.join(args.results, "summary.csv")
    if not os.path.exists(summary):
        raise SystemExit("no %s yet, run experiments/run_experiments.py first" % summary)
    rows = load_summary(summary)
    out = os.path.join(args.results, "plots")
    os.makedirs(out, exist_ok=True)

    plot_e1(rows, out)
    for exp in ("E2", "E3", "E4", "E5"):
        plot_bars(rows, exp, out)
    for exp in ("E2", "E3", "E4"):
        plot_timeline(rows, exp, args.results, out)
    write_table(rows, os.path.join(args.results, "summary_table.md"))


if __name__ == "__main__":
    main()
