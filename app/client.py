#!/usr/bin/env python3
"""Job client (runs on h1, or any host).

Examples:
  python3 app/client.py --type REVERSE --payload "hello world"
  python3 app/client.py --type SUM --payload "1 2 3 4" --count 5
  python3 app/client.py --type SLEEP --payload 1 --count 4 --parallel 3
"""
import argparse
import collections
import socket
import threading
import time

from protocol import PORT, send_line

PRINT = threading.Lock()
LATENCIES = []


def say(msg):
    with PRINT:
        print(msg, flush=True)


def one_client(idx, a):
    sock = socket.create_connection((a.server, a.port), timeout=10)
    sock.settimeout(a.timeout)
    f = sock.makefile("r", encoding="utf-8", newline="\n")
    lock = threading.Lock()
    submit_times = collections.deque()
    t_of = {}
    for _ in range(a.count):
        submit_times.append(time.time())
        send_line(sock, lock, "SUBMIT %s %s" % (a.type, a.payload))
    finished = 0
    try:
        while finished < a.count:
            line = f.readline()
            if not line:
                say("[client %d] connection closed early" % idx)
                break
            p = line.strip().split(" ", 2)
            if p[0] == "ACK":
                t_of[p[1]] = submit_times.popleft()
                say("[client %d] job %s acknowledged" % (idx, p[1]))
            elif p[0] in ("RESULT", "FAILED"):
                dt = time.time() - t_of.get(p[1], time.time())
                finished += 1
                if p[0] == "RESULT":
                    LATENCIES.append(dt)
                say("[client %d] job %s %s: %s (%.3fs)"
                    % (idx, p[1], p[0], p[2] if len(p) > 2 else "", dt))
            elif p[0] == "ERROR":
                submit_times.popleft()
                finished += 1
                say("[client %d] ERROR from scheduler: %s" % (idx, " ".join(p[1:])))
        send_line(sock, lock, "QUIT")
    except socket.timeout:
        say("[client %d] timed out waiting for results" % idx)
    finally:
        sock.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Job client")
    ap.add_argument("--server", default="10.0.0.2")
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--type", default="REVERSE")
    ap.add_argument("--payload", default="hello")
    ap.add_argument("--count", type=int, default=1, help="jobs per connection")
    ap.add_argument("--parallel", type=int, default=1, help="concurrent connections")
    ap.add_argument("--timeout", type=float, default=60)
    a = ap.parse_args()
    t0 = time.time()
    threads = [threading.Thread(target=one_client, args=(i, a)) for i in range(a.parallel)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = a.count * a.parallel
    ok = len(LATENCIES)
    say("--- %d/%d jobs succeeded in %.2fs" % (ok, total, time.time() - t0))
    if ok:
        say("--- latency avg %.3fs  min %.3fs  max %.3fs"
            % (sum(LATENCIES) / ok, min(LATENCIES), max(LATENCIES)))
