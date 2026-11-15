#!/usr/bin/env python3
"""Job client (runs on h1, or any host).

Examples:
  python3 app/client.py --type REVERSE --payload "hello world"
  python3 app/client.py --type SUM --payload "1 2 3 4" --count 5
  python3 app/client.py --type SLEEP --payload 1 --count 4 --parallel 3
  python3 app/client.py --type SLEEP --payload 0.2 --count 40 --interval 0.5 --csv results/run.csv --quiet

D2 additions:
  --interval S   wait S seconds between submissions on each connection (steady load)
  --csv FILE     append one row per job: client,job_id,status,latency_s,submit_t,done_t
                 status is OK, FAILED, ERROR, TIMEOUT or CONN_ERROR
                 submit_t / done_t are epoch seconds (same clock as the experiment script)
  --quiet        print only the summary, not every job
  --timeout S    now means: how long to keep waiting for unfinished jobs after the
                 LAST job of a connection has been submitted
"""
import argparse
import csv
import math
import os
import socket
import threading
import time

from protocol import PORT, send_line

PRINT = threading.Lock()
ROWS = []
ROWS_LOCK = threading.Lock()
QUIET = False
CSV_HEADER = ["client", "job_id", "status", "latency_s", "submit_t", "done_t"]


def say(msg):
    with PRINT:
        print(msg, flush=True)


def vsay(msg):
    if not QUIET:
        say(msg)


def add_row(client, jid, status, latency, submit_t, done_t):
    with ROWS_LOCK:
        ROWS.append((client, jid, status, latency, submit_t, done_t))


def percentile(values, p):
    s = sorted(values)
    k = max(0, int(math.ceil(p / 100.0 * len(s))) - 1)
    return s[k]


def fmt(x, nd):
    return "" if x is None else ("%." + str(nd) + "f") % x


def one_client(idx, a):
    try:
        sock = socket.create_connection((a.server, a.port), timeout=10)
    except OSError as e:
        say("[client %d] cannot connect to %s:%d: %s" % (idx, a.server, a.port, e))
        now = time.time()
        for _ in range(a.count):
            add_row(idx, "-", "CONN_ERROR", None, now, None)
        return

    lock = threading.Lock()
    subs = []                      # one dict per submitted job, in submission order
    sent = {"all": threading.Event(), "t": None}

    def sender():
        try:
            for i in range(a.count):
                subs.append({"submit_t": time.time(), "jid": "-", "status": None, "done_t": None})
                send_line(sock, lock, "SUBMIT %s %s" % (a.type, a.payload))
                if a.interval > 0 and i < a.count - 1:
                    time.sleep(a.interval)
        except OSError:
            pass
        finally:
            sent["t"] = time.time()
            sent["all"].set()

    threading.Thread(target=sender, daemon=True).start()

    by_jid = {}
    next_ack = 0
    finished = 0
    buf = b""
    end_reason = None
    sock.settimeout(0.5)

    while finished < a.count:
        if sent["all"].is_set() and time.time() - sent["t"] > a.timeout:
            end_reason = "TIMEOUT"
            break
        try:
            data = sock.recv(4096)
        except socket.timeout:
            continue
        except OSError:
            end_reason = "CONN_ERROR"
            break
        if not data:
            end_reason = "CONN_ERROR"
            break
        buf += data
        while b"\n" in buf:
            raw, buf = buf.split(b"\n", 1)
            p = raw.decode("utf-8", "replace").strip().split(" ", 2)
            now = time.time()
            if p[0] == "ACK" and len(p) >= 2:
                if next_ack < len(subs):
                    subs[next_ack]["jid"] = p[1]
                    by_jid[p[1]] = subs[next_ack]
                    next_ack += 1
                vsay("[client %d] job %s acknowledged" % (idx, p[1]))
            elif p[0] in ("RESULT", "FAILED") and len(p) >= 2:
                sub = by_jid.get(p[1])
                if sub is not None and sub["status"] is None:
                    sub["status"] = "OK" if p[0] == "RESULT" else "FAILED"
                    sub["done_t"] = now
                    finished += 1
                    vsay("[client %d] job %s %s: %s (%.3fs)"
                         % (idx, p[1], p[0], p[2] if len(p) > 2 else "", now - sub["submit_t"]))
            elif p[0] == "ERROR":
                if next_ack < len(subs):
                    sub = subs[next_ack]
                    next_ack += 1
                    sub["status"] = "ERROR"
                    sub["done_t"] = now
                    finished += 1
                say("[client %d] ERROR from scheduler: %s" % (idx, " ".join(p[1:])))

    try:
        send_line(sock, lock, "QUIT")
    except OSError:
        pass
    sock.close()

    now = time.time()
    if end_reason == "TIMEOUT":
        say("[client %d] timed out: %d of %d jobs unfinished" % (idx, a.count - finished, a.count))
    for sub in subs:
        if sub["status"] is None:
            sub["status"] = end_reason or "CONN_ERROR"
        lat = (sub["done_t"] if sub["done_t"] else now) - sub["submit_t"]
        add_row(idx, sub["jid"], sub["status"], lat, sub["submit_t"], sub["done_t"])
    for _ in range(a.count - len(subs)):          # jobs that could not even be sent
        add_row(idx, "-", "CONN_ERROR", None, now, None)


def write_csv(path):
    folder = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        if new:
            w.writerow(CSV_HEADER)
        for client, jid, status, lat, st, dt in ROWS:
            w.writerow([client, jid, status, fmt(lat, 4), fmt(st, 3), fmt(dt, 3)])


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Job client")
    ap.add_argument("--server", default="10.0.0.2")
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--type", default="REVERSE")
    ap.add_argument("--payload", default="hello")
    ap.add_argument("--count", type=int, default=1, help="jobs per connection")
    ap.add_argument("--parallel", type=int, default=1, help="concurrent connections")
    ap.add_argument("--interval", type=float, default=0.0,
                    help="seconds between submissions on one connection (0 = send all at once)")
    ap.add_argument("--timeout", type=float, default=60,
                    help="seconds to wait for unfinished jobs after the last submission")
    ap.add_argument("--csv", default=None, help="append one row per job to this CSV file")
    ap.add_argument("--quiet", action="store_true", help="print only the summary")
    a = ap.parse_args()
    QUIET = a.quiet

    t0 = time.time()
    threads = [threading.Thread(target=one_client, args=(i, a)) for i in range(a.parallel)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = a.count * a.parallel
    ok = [r[3] for r in ROWS if r[2] == "OK" and r[3] is not None]
    say("--- %d/%d jobs succeeded (%.1f%%) in %.2fs"
        % (len(ok), total, 100.0 * len(ok) / total if total else 0.0, time.time() - t0))
    if ok:
        say("--- latency avg %.3fs  min %.3fs  p95 %.3fs  max %.3fs"
            % (sum(ok) / len(ok), min(ok), percentile(ok, 95), max(ok)))
    other = {}
    for r in ROWS:
        if r[2] != "OK":
            other[r[2]] = other.get(r[2], 0) + 1
    if other:
        say("--- not OK: " + ", ".join("%s=%d" % kv for kv in sorted(other.items())))
    if a.csv:
        write_csv(a.csv)
        say("--- wrote %d rows to %s" % (len(ROWS), a.csv))
