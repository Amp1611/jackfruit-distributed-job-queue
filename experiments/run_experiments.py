#!/usr/bin/env python3
"""Run the D2 experiments and save the results as CSV files.

For every run this script does the same simple thing:
  1. start the Ryu controller (static or dynamic mode)
  2. build the Mininet network and start the scheduler + 2 workers
  3. run clients and, if the experiment needs it, break something
  4. stop everything and save the numbers

Experiments (each is run with the controller in static mode and in dynamic mode):
  E1  normal load: 1, 5, 10, 20 clients
  E2  congestion: iperf traffic fills the direct link s1-s3
  E3  link failure: the link s1-s3 goes down for 10 s during a run
  E4  worker crash: worker1 is killed during a run
  E5  scheduling: round-robin vs least-loaded with one slow worker
      (needs the D2 scheduler/worker; skipped automatically if they lack the flags)

Usage (system Python, needs sudo, run from the project root):
  sudo python3 experiments/run_experiments.py --quick          # fast test, ~10 min
  sudo python3 experiments/run_experiments.py                  # everything, 3 repeats
  sudo python3 experiments/run_experiments.py --exp E1 E3      # only some experiments

Results:
  results/summary.csv         one row per run (used by plot_results.py)
  results/<exp>/<run>.csv     one row per job (written by app/client.py)
  results/logs/<run>/         controller, scheduler, worker and client logs
"""
import argparse
import csv
import importlib.util
import json
import math
import os
import socket
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
APP = os.path.join(ROOT, "app")
CONTROLLER = os.path.join(ROOT, "controller", "controller.py")
STATUS_FILE = "/tmp/sdn_status.json"
SERVER_IP = "10.0.0.2"
OF_PORT = 6653

STATIC = {"label": "static", "mode": "static", "policy": None, "slow": None}
DYNAMIC = {"label": "dynamic", "mode": "dynamic", "policy": None, "slow": None}
ROUND_ROBIN = {"label": "rr", "mode": "dynamic", "policy": "rr", "slow": {"worker2": 3.0}}
LEAST = {"label": "least", "mode": "dynamic", "policy": "least", "slow": {"worker2": 3.0}}

SUMMARY_FIELDS = ["exp", "variant", "cond", "rep", "jobs", "ok", "completion_pct", "avg_s", "p95_s",
                  "max_s", "slow_jobs", "recovery_s", "event_t", "link_down", "route_change",
                  "congestion", "wall_s"]


def say(msg):
    print("[exp] " + msg, flush=True)


# ---------------------------------------------------------------- scenarios
# Each scenario receives "ctx" and runs the clients (and the failure) for one run.

def scenario_e1(ctx):
    n = int(ctx.cond)
    p = ctx.client(parallel=n, count=10 if ctx.quick else 20, interval=0.1, payload=0.2, timeout=20)
    ctx.wait(p, 120)


def scenario_e2(ctx):
    h1, h3 = ctx.net.get("h1", "h3")
    ctx.background(h3, ["iperf3", "-s"])
    time.sleep(1)
    ctx.background(h1, ["iperf3", "-c", "10.0.0.3", "-t", "20" if ctx.quick else "30"])
    ctx.event("iperf_start")
    time.sleep(2)
    count = 20 if ctx.quick else 40
    p = ctx.client(parallel=2, count=count, interval=0.5, payload=0.2, timeout=20)
    ctx.wait(p, count * 0.5 + 60)


def scenario_e3(ctx):
    count = 20 if ctx.quick else 40
    p = ctx.client(parallel=3, count=count, interval=0.5, payload=0.2, timeout=25)
    time.sleep(4 if ctx.quick else 6)
    ctx.event("link_down")
    ctx.net.configLinkStatus("s1", "s3", "down")
    time.sleep(6 if ctx.quick else 10)
    ctx.net.configLinkStatus("s1", "s3", "up")
    ctx.wait(p, count * 0.5 + 60)


def scenario_e4(ctx):
    count = 20 if ctx.quick else 40
    p = ctx.client(parallel=3, count=count, interval=0.5, payload=1.0, timeout=25)
    time.sleep(4 if ctx.quick else 8)
    ctx.event("worker_kill")
    ctx.net.get("h3").cmd("pkill -f 'worker.py --id worker1'")
    ctx.wait(p, count * 0.5 + 60)


def scenario_e5(ctx):
    count = 15 if ctx.quick else 30
    p = ctx.client(parallel=4, count=count, interval=0.3, payload=1.0, timeout=30)
    ctx.wait(p, count * 0.3 + 90)


# name: (title, variants, conditions, scenario, normal_latency_s, needs_d2_app)
EXPERIMENTS = {
    "E1": ("normal load", [STATIC, DYNAMIC], ["1", "5", "10", "20"], scenario_e1, 1.0, False),
    "E2": ("congestion", [STATIC, DYNAMIC], ["base"], scenario_e2, 1.0, False),
    "E3": ("link failure", [STATIC, DYNAMIC], ["base"], scenario_e3, 1.0, False),
    "E4": ("worker crash", [DYNAMIC], ["base"], scenario_e4, 2.0, False),
    "E5": ("scheduling policy", [ROUND_ROBIN, LEAST], ["base"], scenario_e5, None, True),
}


class Ctx:
    """What a scenario can use: the network, the clients and an event log."""

    def __init__(self, net, exp, cond, quick, csv_path, logdir):
        self.net, self.exp, self.cond, self.quick = net, exp, cond, quick
        self.csv_path, self.logdir = csv_path, logdir
        self.events = {}
        self.open_files = []
        self.procs = []

    def event(self, name):
        self.events[name] = time.time()
        say("event: " + name)

    def client(self, parallel, count, interval, payload, timeout):
        fh = open(os.path.join(self.logdir, "client.log"), "a")
        self.open_files.append(fh)
        cmd = ["python3", os.path.join(APP, "client.py"), "--server", SERVER_IP, "--type", "SLEEP",
               "--payload", str(payload), "--parallel", str(parallel), "--count", str(count),
               "--interval", str(interval), "--timeout", str(timeout), "--csv", self.csv_path, "--quiet"]
        p = self.net.get("h1").popen(cmd, stdout=fh, stderr=subprocess.STDOUT)
        self.procs.append(p)
        return p

    def background(self, host, cmd):
        p = host.popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        return p

    def wait(self, proc, limit):
        try:
            proc.wait(timeout=limit)
        except subprocess.TimeoutExpired:
            say("client did not finish in %ds, stopping it" % limit)
            proc.kill()

    def cleanup(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
        for fh in self.open_files:
            fh.close()


# ---------------------------------------------------------------- helpers
def load_topology():
    spec = importlib.util.spec_from_file_location("sdn_topology", os.path.join(ROOT, "topology", "topology.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cleanup_system():
    """Remove leftovers. 'mn -c' also kills ryu-manager, so call this BEFORE starting it."""
    subprocess.run(["mn", "-c"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["pkill", "-9", "iperf3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(STATUS_FILE):
        os.remove(STATUS_FILE)


def start_controller(mode, logfile, ryu_bin):
    fh = open(logfile, "w")
    proc = subprocess.Popen([ryu_bin, CONTROLLER], env=dict(os.environ, SDN_MODE=mode),
                            stdout=fh, stderr=subprocess.STDOUT)
    end = time.time() + 20
    while time.time() < end:
        if proc.poll() is not None:
            raise RuntimeError("ryu-manager stopped, see " + logfile)
        try:
            socket.create_connection(("127.0.0.1", OF_PORT), timeout=0.5).close()
            return proc, fh
        except OSError:
            time.sleep(0.5)
    raise RuntimeError("controller is not listening on port %d" % OF_PORT)


def stop_controller(proc, fh):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    fh.close()


def wait_for_workers(count=2, timeout=25):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with open(STATUS_FILE) as fh:
                if len(json.load(fh).get("workers", [])) >= count:
                    return True
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    return False


def supports(script, flag):
    try:
        out = subprocess.run(["python3", script, "--help"], stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, universal_newlines=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return False
    return flag in out


def count_lines(path, text):
    try:
        with open(path) as fh:
            return sum(1 for line in fh if text in line)
    except OSError:
        return 0


def read_jobs(path):
    jobs = []
    if os.path.exists(path):
        with open(path) as fh:
            for r in csv.DictReader(fh):
                jobs.append({"status": r["status"],
                             "latency": float(r["latency_s"]) if r["latency_s"] else None,
                             "submit": float(r["submit_t"]) if r["submit_t"] else None})
    return jobs


def percentile(values, p):
    s = sorted(values)
    return s[max(0, int(math.ceil(p / 100.0 * len(s))) - 1)]


def summarize(jobs, normal, event_t):
    """Turn the per-job rows of one run into the numbers we report."""
    ok = [j["latency"] for j in jobs if j["status"] == "OK" and j["latency"] is not None]
    out = {"jobs": len(jobs), "ok": len(ok),
           "completion_pct": round(100.0 * len(ok) / len(jobs), 1) if jobs else 0.0,
           "avg_s": round(sum(ok) / len(ok), 3) if ok else "",
           "p95_s": round(percentile(ok, 95), 3) if ok else "",
           "max_s": round(max(ok), 3) if ok else "",
           "slow_jobs": "", "recovery_s": ""}
    if normal is not None:
        out["slow_jobs"] = sum(1 for j in jobs if j["status"] != "OK" or (j["latency"] or 0) > normal)
    if event_t and normal is not None:
        # recovery = time from the failure until a newly submitted job is fast again
        fast = [j["submit"] for j in jobs if j["status"] == "OK" and j["submit"] and
                j["submit"] >= event_t and j["latency"] <= normal]
        if fast:
            out["recovery_s"] = round(min(fast) - event_t, 2)
    return out


def append_summary(path, row):
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS, lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(row)


# ---------------------------------------------------------------- one run
def run_one(topo, args, exp, variant, cond, rep):
    title, _variants, _conds, scenario, normal, _needs = EXPERIMENTS[exp]
    name = "%s_%s_c%s_r%d" % (exp, variant["label"], cond, rep)
    csv_path = os.path.join(args.out, exp, name + ".csv")
    logdir = os.path.join(args.out, "logs", name)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    os.makedirs(logdir, exist_ok=True)
    if os.path.exists(csv_path):
        os.remove(csv_path)                       # the client appends, so start clean
    say("=== %s (%s) variant=%s cond=%s repeat=%d" % (exp, title, variant["label"], cond, rep))

    started = time.time()
    cleanup_system()
    ctrl = net = ctx = None
    try:
        ctrl = start_controller(variant["mode"], os.path.join(logdir, "controller.log"), args.ryu)
        net = topo.build()
        net.start()
        if not topo.wait_ready(net):
            raise RuntimeError("network not ready (pingAll failed)")
        topo.start_app(net, logdir=logdir, policy=variant["policy"], slow=variant["slow"])
        if not wait_for_workers():
            raise RuntimeError("workers did not register")
        ctx = Ctx(net, exp, cond, args.quick, csv_path, logdir)
        scenario(ctx)
    finally:
        if ctx:
            ctx.cleanup()
        if net:
            topo.stop_app(net)
            net.stop()
        if ctrl:
            stop_controller(*ctrl)

    event_t = None
    for key in ("link_down", "worker_kill"):
        if ctx and key in ctx.events:
            event_t = ctx.events[key]
    row = {"exp": exp, "variant": variant["label"], "cond": cond, "rep": rep,
           "event_t": round(event_t, 3) if event_t else "", "wall_s": round(time.time() - started, 1)}
    row.update(summarize(read_jobs(csv_path), normal, event_t))
    log = os.path.join(logdir, "controller.log")
    row["link_down"] = count_lines(log, "LINK DOWN")
    row["route_change"] = count_lines(log, "ROUTE CHANGE")
    row["congestion"] = count_lines(log, "CONGESTION:")
    append_summary(os.path.join(args.out, "summary.csv"), row)
    say("done: %s/%s jobs ok, avg latency %s s" % (row["ok"], row["jobs"], row["avg_s"]))


def main():
    ap = argparse.ArgumentParser(description="Run the D2 experiments")
    ap.add_argument("--exp", nargs="+", choices=sorted(EXPERIMENTS), default=sorted(EXPERIMENTS))
    ap.add_argument("--repeats", type=int, default=None, help="runs per setting (default 3, or 1 with --quick)")
    ap.add_argument("--quick", action="store_true", help="shorter runs, fewer client counts")
    ap.add_argument("--out", default=os.path.join(ROOT, "results"))
    ap.add_argument("--ryu", default=os.path.join(ROOT, "venv", "bin", "ryu-manager"))
    args = ap.parse_args()
    repeats = args.repeats or (1 if args.quick else 3)

    if os.geteuid() != 0:
        sys.exit("Run with sudo and the system Python:  sudo python3 experiments/run_experiments.py")
    if not os.path.exists(args.ryu):
        sys.exit("ryu-manager not found at %s (use --ryu PATH)" % args.ryu)

    runs = []
    for exp in args.exp:
        title, variants, conds, _scenario, _normal, needs_d2 = EXPERIMENTS[exp]
        if needs_d2 and not (supports(os.path.join(APP, "scheduler.py"), "--policy")
                             and supports(os.path.join(APP, "worker.py"), "--slow")):
            say("skipping %s: scheduler/worker do not have --policy / --slow yet" % exp)
            continue
        if args.quick and len(conds) > 2:
            conds = conds[:2]
        for rep in range(1, repeats + 1):
            for cond in conds:
                for variant in variants:
                    runs.append((exp, variant, cond, rep))
    say("%d runs planned, roughly %d minutes" % (len(runs), len(runs)))

    topo = load_topology()
    from mininet.log import setLogLevel
    setLogLevel("warning")
    for exp, variant, cond, rep in runs:
        try:
            run_one(topo, args, exp, variant, cond, rep)
        except KeyboardInterrupt:
            say("stopped by user")
            break
        except Exception as e:
            say("RUN FAILED (%s %s %s r%d): %s" % (exp, variant["label"], cond, rep, e))
    cleanup_system()
    say("finished. Results in %s" % args.out)


if __name__ == "__main__":
    main()
