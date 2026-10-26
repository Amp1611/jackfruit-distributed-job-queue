#!/usr/bin/env python3
"""Worker node (runs on h3 / h4).

Connects to the scheduler, registers, sends heartbeats, executes JOB
messages (each in its own thread) and replies DONE / FAIL.
Reconnects automatically if the connection is lost.
"""
import argparse
import socket
import threading
import time

from protocol import PORT, send_line, log


def count_primes(n):
    if n < 2:
        return 0
    sieve = bytearray([1]) * (n + 1)
    sieve[0:2] = b"\x00\x00"
    for i in range(2, int(n ** 0.5) + 1):
        if sieve[i]:
            sieve[i * i::i] = bytearray(len(sieve[i * i::i]))
    return sum(sieve)


def execute(jtype, payload):
    if jtype == "REVERSE":
        return payload[::-1]
    if jtype == "SUM":
        nums = [float(x) for x in payload.replace(",", " ").split()]
        total = sum(nums)
        return str(int(total)) if total == int(total) else str(total)
    if jtype == "SLEEP":
        secs = float(payload)
        if not 0 <= secs <= 60:
            raise ValueError("sleep must be 0-60 s")
        time.sleep(secs)
        return "slept %gs" % secs
    if jtype == "PRIME":
        n = int(payload)
        if not 0 <= n <= 5000000:
            raise ValueError("n must be 0-5000000")
        return "%d primes <= %d" % (count_primes(n), n)
    raise ValueError("unknown job type %s" % jtype)


def run_once(args):
    sock = socket.create_connection((args.server, args.port), timeout=5)
    sock.settimeout(None)
    lock = threading.Lock()
    f = sock.makefile("r", encoding="utf-8", newline="\n")
    send_line(sock, lock, "REGISTER %s" % args.id)
    resp = f.readline().strip()
    if not resp.startswith("REGISTERED"):
        raise ConnectionError("registration failed: %r" % resp)
    log(args.id, "registered with scheduler %s:%d" % (args.server, args.port))

    stop = threading.Event()
    active = [0]
    alock = threading.Lock()

    def heartbeat():
        while not stop.is_set():
            try:
                send_line(sock, lock, "HEARTBEAT %s %d" % (args.id, active[0]))
            except OSError:
                return
            stop.wait(args.heartbeat)

    def do_job(jid, jtype, payload):
        with alock:
            active[0] += 1
        t0 = time.time()
        try:
            out = execute(jtype, payload)
            send_line(sock, lock, "DONE %s %s" % (jid, out))
            log(args.id, "job %s %s done in %.3fs" % (jid, jtype, time.time() - t0))
        except OSError:
            pass
        except Exception as e:          # bad payload etc.
            try:
                send_line(sock, lock, "FAIL %s %s" % (jid, e))
            except OSError:
                pass
            log(args.id, "job %s failed: %s" % (jid, e))
        finally:
            with alock:
                active[0] -= 1

    threading.Thread(target=heartbeat, daemon=True).start()
    try:
        while True:
            line = f.readline()
            if not line:
                break
            parts = line.strip().split(" ", 3)
            if parts[0] == "JOB" and len(parts) >= 3:
                payload = parts[3] if len(parts) > 3 else ""
                log(args.id, "received job %s %s" % (parts[1], parts[2]))
                threading.Thread(target=do_job, args=(parts[1], parts[2], payload),
                                 daemon=True).start()
    finally:
        stop.set()
        sock.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Job worker")
    ap.add_argument("--id", required=True)
    ap.add_argument("--server", default="10.0.0.2")
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--heartbeat", type=float, default=2.0)
    a = ap.parse_args()
    try:
        while True:
            try:
                run_once(a)
            except (OSError, ConnectionError) as e:
                log(a.id, "scheduler unavailable (%s); retrying in 2s" % e)
            time.sleep(2)
    except KeyboardInterrupt:
        pass
