#!/usr/bin/env python3
"""Scheduler / job queue server (runs on h2).

* Accepts many concurrent TCP connections (one thread per connection).
* A connection whose first line is REGISTER is a worker; anything else is a client.
* Clients SUBMIT jobs -> scheduler replies ACK, queues the job, and later
  forwards the worker's result as RESULT.
* A dispatcher thread assigns queued jobs to live workers (round-robin).
* Workers send heartbeats; silent workers are removed and their unfinished
  jobs are put back in the queue.
* Writes worker/queue status to /tmp/sdn_status.json so the SDN controller
  can read it (application -> network integration hook).

D2: --policy rr|least (round-robin or fewest active jobs); --job-timeout N (a job
not finished after N s is sent to another worker); "[timing]" lines per job.
"""
import argparse
import itertools
import json
import os
import queue
import socket
import threading
import time

from protocol import PORT, VALID_TYPES, send_line, log

STATUS_FILE = "/tmp/sdn_status.json"
HEARTBEAT_TIMEOUT = 8.0   # seconds of silence before a worker is declared dead


class Worker:
    def __init__(self, wid, conn, addr):
        self.id = wid
        self.conn = conn
        self.addr = addr
        self.lock = threading.Lock()
        self.last_seen = time.time()
        self.active = 0
        self.done = 0
        self.assigned = set()
        self.alive = True


class ClientConn:
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.lock = threading.Lock()

    def send(self, text):
        try:
            send_line(self.conn, self.lock, text)
            return True
        except OSError:
            return False


class Scheduler:
    def __init__(self, host, port, policy="rr", job_timeout=10.0):
        self.host, self.port = host, port
        self.policy, self.job_timeout = policy, job_timeout
        self.state = threading.Lock()
        self.workers = {}          # worker_id -> Worker
        self.order = []            # worker ids, used for round-robin
        self.rr = 0
        self.jobs = {}             # job_id -> dict
        self.queue = queue.Queue()
        self.next_id = itertools.count(1)
        self.stats = {"submitted": 0, "completed": 0, "failed": 0, "requeued": 0}

    # ------------------------------------------------------------ server
    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(128)
        log("sched", "listening on %s:%d (policy=%s, job-timeout=%gs)"
            % (self.host, self.port, self.policy, self.job_timeout))
        for fn in (self.dispatch_loop, self.monitor_loop, self.status_loop):
            threading.Thread(target=fn, daemon=True).start()
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle, args=(conn, addr), daemon=True).start()

    def handle(self, conn, addr):
        f = conn.makefile("r", encoding="utf-8", newline="\n")
        try:
            first = f.readline()
            if not first:
                return
            first = first.strip()
            if first.upper().startswith("REGISTER"):
                self.worker_loop(conn, addr, f, first)
            else:
                self.client_loop(conn, addr, f, first)
        except (OSError, ValueError) as e:
            log("sched", "connection %s:%d error: %s" % (addr[0], addr[1], e))
        finally:
            try:
                conn.close()
            except OSError:
                pass

    # ------------------------------------------------------------ clients
    def client_loop(self, conn, addr, f, first):
        cc = ClientConn(conn, addr)
        log("sched", "client connected %s:%d" % addr)
        line = first
        while line is not None:
            if line and not self.client_line(cc, line):
                break
            nxt = f.readline()
            line = nxt.strip() if nxt else None
        log("sched", "client disconnected %s:%d" % addr)

    def client_line(self, cc, line):
        parts = line.split(" ", 2)
        cmd = parts[0].upper()
        if cmd == "SUBMIT" and len(parts) >= 2:
            jtype = parts[1].upper()
            payload = parts[2] if len(parts) > 2 else ""
            if jtype not in VALID_TYPES:
                cc.send("ERROR unknown job type %s" % jtype)
                return True
            jid = next(self.next_id)
            t0 = time.time()
            with self.state:
                self.jobs[jid] = {"client": cc, "type": jtype, "payload": payload,
                                  "worker": None, "t0": t0, "sent": 0.0, "tried": set()}
                self.stats["submitted"] += 1
            cc.send("ACK %d" % jid)
            log("timing", "job=%d event=submitted t=%.3f" % (jid, t0))
            self.queue.put(jid)
            log("sched", "job %d %s submitted by %s:%d (queue=%d)"
                % (jid, jtype, cc.addr[0], cc.addr[1], self.queue.qsize()))
        elif cmd == "PING":
            cc.send("PONG")
        elif cmd == "QUIT":
            return False
        else:
            cc.send("ERROR bad command")
        return True

    # ------------------------------------------------------------ workers
    def worker_loop(self, conn, addr, f, first):
        parts = first.split()
        wid = parts[1] if len(parts) > 1 else "worker-%s" % addr[0]
        w = Worker(wid, conn, addr)
        with self.state:
            old = self.workers.get(wid)
        if old:
            self.remove_worker(old, "re-registered")
        send_line(conn, w.lock, "REGISTERED %s" % wid)   # before the worker can get a JOB
        with self.state:
            self.workers[wid] = w
            self.order.append(wid)
        log("sched", "worker %s registered from %s:%d (workers=%d)"
            % (wid, addr[0], addr[1], len(self.workers)))
        try:
            while True:
                line = f.readline()
                if not line:
                    break
                w.last_seen = time.time()
                p = line.strip().split(" ", 2)
                if p[0] == "DONE" and len(p) >= 2:
                    self.finish_job(w, int(p[1]), p[2] if len(p) > 2 else "", True)
                elif p[0] == "FAIL" and len(p) >= 2:
                    self.finish_job(w, int(p[1]), p[2] if len(p) > 2 else "", False)
                # HEARTBEAT: nothing to do besides updating last_seen
        finally:
            self.remove_worker(w, "connection closed")

    def remove_worker(self, w, reason):
        with self.state:
            if not w.alive:
                return
            w.alive = False
            if self.workers.get(w.id) is w:
                del self.workers[w.id]
            if w.id in self.order:
                self.order.remove(w.id)
            pending = list(w.assigned)
            w.assigned.clear()
            for jid in pending:
                if jid in self.jobs:
                    self.jobs[jid]["worker"] = None
                    self.stats["requeued"] += 1
        for jid in pending:
            if jid in self.jobs:
                self.queue.put(jid)
        try:
            w.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        log("sched", "worker %s removed (%s); %d job(s) requeued" % (w.id, reason, len(pending)))

    def finish_job(self, w, jid, output, ok):
        with self.state:
            job = self.jobs.pop(jid, None)
            if jid in w.assigned:              # not if the job was already taken back
                w.assigned.discard(jid)
                w.active = max(0, w.active - 1)
            if job is None:                    # late duplicate result: ignore
                return
            w.done += 1
            self.stats["completed" if ok else "failed"] += 1
        took = time.time() - job["t0"]
        msg = ("RESULT %d %s" if ok else "FAILED %d %s") % (jid, output)
        if not job["client"].send(msg):
            log("sched", "job %d finished but client is gone" % jid)
        log("sched", "job %d %s by %s in %.3fs" % (jid, "done" if ok else "FAILED", w.id, took))
        log("timing", "job=%d event=finished worker=%s status=%s t=%.3f"
            % (jid, w.id, "done" if ok else "failed", time.time()))

    # ------------------------------------------------------------ dispatch
    def pick_worker(self, avoid=()):
        """rr: next worker in turn. least: fewest active jobs (ties: next in turn).
        Workers in `avoid` (they timed out on this job) are skipped if possible."""
        with self.state:
            n = len(self.order)
            if n == 0:
                return None
            turn = [self.workers[self.order[(self.rr + i) % n]] for i in range(n)]
            self.rr += 1
            pool = [w for w in turn if w.id not in avoid] or turn
            return min(pool, key=lambda w: w.active) if self.policy == "least" else pool[0]

    def dispatch_loop(self):
        while True:
            jid = self.queue.get()
            while True:
                job = self.jobs.get(jid)
                if job is None:
                    break
                w = self.pick_worker(job["tried"])
                if w is None:
                    time.sleep(0.5)          # no workers yet: wait
                    continue
                with self.state:
                    w.active += 1
                    w.assigned.add(jid)
                    job["worker"] = w.id
                    job["sent"] = time.time()
                try:
                    send_line(w.conn, w.lock, "JOB %d %s %s" % (jid, job["type"], job["payload"]))
                    log("sched", "job %d -> %s" % (jid, w.id))
                    log("timing", "job=%d event=dispatched worker=%s t=%.3f" % (jid, w.id, job["sent"]))
                except OSError:
                    self.remove_worker(w, "send failed")   # also requeues jid
                break

    def monitor_loop(self):
        while True:
            time.sleep(1)
            now = time.time()
            slow = []                          # jobs that took longer than --job-timeout
            with self.state:
                dead = [w for w in self.workers.values() if now - w.last_seen > HEARTBEAT_TIMEOUT]
                for jid, job in self.jobs.items():
                    if job["worker"] is not None and now - job["sent"] > self.job_timeout:
                        old = self.workers.get(job["worker"])
                        if old is not None and jid in old.assigned:
                            old.assigned.discard(jid)
                            old.active = max(0, old.active - 1)
                        slow.append((jid, job["worker"]))
                        job["tried"].add(job["worker"])
                        job["worker"] = None
                        self.stats["requeued"] += 1
            for w in dead:
                self.remove_worker(w, "heartbeat timeout")
            for jid, wid in slow:              # send it to another worker
                log("sched", "job %d timed out on %s; requeued" % (jid, wid))
                log("timing", "job=%d event=timeout worker=%s t=%.3f" % (jid, wid, time.time()))
                self.queue.put(jid)

    def status_loop(self):
        """Publish state for the SDN controller (integration hook)."""
        while True:
            time.sleep(2)
            now = time.time()
            with self.state:
                data = {
                    "time": now,
                    "queue_len": self.queue.qsize(),
                    "stats": dict(self.stats),
                    "workers": [{"id": w.id, "ip": w.addr[0], "active": w.active,
                                 "done": w.done, "idle_s": round(now - w.last_seen, 1)}
                                for w in self.workers.values()],
                }
            try:
                tmp = STATUS_FILE + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump(data, fh)
                os.replace(tmp, STATUS_FILE)
            except OSError:
                pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Job queue scheduler")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--policy", choices=["rr", "least"], default="rr")
    ap.add_argument("--job-timeout", type=float, default=10.0, help="seconds")
    a = ap.parse_args()
    try:
        Scheduler(a.host, a.port, a.policy, a.job_timeout).serve()
    except KeyboardInterrupt:
        pass
    