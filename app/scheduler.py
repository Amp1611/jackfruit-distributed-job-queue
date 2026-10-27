#!/usr/bin/env python3
"""
scheduler.py - TCP job scheduler (Member 1). Runs on h2 (10.0.0.2), port 5000.

Clients and workers connect TO this server and speak the line protocol in
docs/protocol.md (helpers in protocol.py). The first message tells the
scheduler what the peer is: REGISTER = worker, SUBMIT/PING = client.

WHAT IT DOES
    - serves many clients and workers at once (one thread per connection)
    - gives each job a unique ID, replies "ACK <id>", queues it
    - dispatches jobs round-robin over live workers
    - tracks heartbeats; 8 s of silence marks a worker DEAD
    - re-queues the unfinished jobs of a dead or disconnected worker
    - publishes worker/queue state to /tmp/sdn_status.json for the controller

RUN (topology.py does this for you)
    cd app && python3 -u scheduler.py

STATUS FILE (read by controller/controller.py)   default /tmp/sdn_status.json
    Rewritten atomically every second (temp file + rename).
    {
      "timestamp": 1700000000.1,       # unix time; treat as stale after 5 s
      "scheduler_running": true,       # false after a clean shutdown
      "port": 5000,
      "heartbeat_timeout": 8.0,
      "queue_len": 0,
      "workers": [                     # LIVE workers only
        {"id": "worker1", "ip": "10.0.0.3", "status": "ALIVE",
         "active": 1, "done": 4, "failed": 0, "reported_active": 1,
         "last_heartbeat_age": 0.8}
      ],
      "dead_workers": [ ...same fields, "status": "DEAD"... ],
      "stats": {"submitted": 9, "queued": 0, "running": 1, "done": 8,
                "failed": 0, "requeued": 0}
    }
    "workers" keeps the keys the controller already reads (id, active, done,
    queue_len, stats). A dead worker moves from "workers" to "dead_workers".

RELIABILITY AND LIMITATIONS
    - Worker disconnect or heartbeat timeout: its RUNNING jobs go back to the
      FRONT of the queue. After --max-attempts (3) hand-outs a job is FAILED.
    - A job the worker itself reports as FAIL (bad payload) is NOT retried.
    - Guarantee is AT-LEAST-ONCE: a slow but alive worker may still finish a
      job that was already re-queued. The first result wins; later ones are
      ignored.
    - All state is IN MEMORY. If the scheduler dies, queued and running jobs
      are lost. On a clean stop (Ctrl+C / SIGTERM) their IDs are logged.
    - Single scheduler = single point of failure. Jobs are never purged.
    - If the client that submitted a job has gone, its result is dropped
      (the protocol has no "fetch result later" message).
    - A worker revived by a late heartbeat may still be running a job that was
      already re-queued, so it can briefly hold extra work.
"""

import argparse
import collections
import json
import logging
import os
import signal
import socket
import threading
import time

from protocol import (
    ACK, DONE, ERROR, FAIL, FAILED, HEARTBEAT, HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT, JOB, JOB_DONE, JOB_FAILED, JOB_QUEUED, JOB_RUNNING,
    JOB_TYPES, PING, PONG, PORT, QUIT, REGISTER, REGISTERED, RESULT, SUBMIT,
    LineReader, ProtocolError, parse_message, send_line,
)

log = logging.getLogger("scheduler")

SOCKET_TIMEOUT = 1.0          # lets threads notice shutdown
MAX_BAD_MESSAGES = 10         # close a connection after this many bad lines
DEFAULT_STATUS_FILE = "/tmp/sdn_status.json"
WORKER_ALIVE = "ALIVE"
WORKER_DEAD = "DEAD"


class Job:
    """One unit of work and its current state."""

    def __init__(self, job_id, job_type, payload, client_conn):
        self.job_id = job_id
        self.job_type = job_type
        self.payload = payload
        self.client_conn = client_conn    # where RESULT goes (may be gone)
        self.status = JOB_QUEUED
        self.worker_id = None
        self.attempts = 0                 # times it was handed to a worker
        self.result = None
        self.error = None


class WorkerInfo:
    """Registry entry for one worker."""

    def __init__(self, worker_id, conn):
        self.worker_id = worker_id
        self.conn = conn
        self.ip = conn.ip
        self.status = WORKER_ALIVE
        self.last_heartbeat = time.monotonic()
        self.running_jobs = set()         # job IDs currently assigned
        self.reported_active = 0          # what the worker says in HEARTBEAT
        self.done = 0
        self.failed = 0


class Connection:
    """A client or worker socket plus the helpers we need around it."""

    def __init__(self, sock, addr):
        self.sock = sock
        self.ip, self.port = addr[0], addr[1]
        self.reader = LineReader(sock)
        self.send_lock = threading.Lock()    # several threads may send to it
        self.role = "unknown"                # "client" or "worker"
        self.worker_id = None
        self.closed = False

    def send(self, text):
        """Send one line. Returns True on success; closes the socket on failure."""
        try:
            send_line(self.sock, self.send_lock, text)
            return True
        except (OSError, ProtocolError) as err:
            log.warning("send to %s:%s failed: %s", self.ip, self.port, err)
            self.close()
            return False

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class Scheduler:
    def __init__(self, host="0.0.0.0", port=PORT,
                 heartbeat_timeout=HEARTBEAT_TIMEOUT,
                 status_file=DEFAULT_STATUS_FILE, status_interval=1.0,
                 max_attempts=3, max_queue_size=1000, max_active=0):
        self.host = host
        self.port = port
        self.heartbeat_timeout = heartbeat_timeout
        self.status_file = status_file
        self.status_interval = status_interval
        self.max_attempts = max_attempts
        self.max_queue_size = max_queue_size
        self.max_active = max_active          # per-worker cap; 0 = unlimited

        # ---- shared state; ALWAYS touch it while holding self.cond ----
        self.cond = threading.Condition(threading.Lock())
        self.jobs = {}                        # job_id -> Job
        self.queue = collections.deque()      # pending job IDs (FIFO)
        self.workers = {}                     # worker_id -> WorkerInfo
        self.worker_order = []                # registration order (round-robin)
        self.rr_next = 0                      # round-robin pointer
        self.next_job_id = 1
        self.requeued_total = 0
        self.connections = set()              # open Connection objects

        self.stop_event = threading.Event()
        self.server_sock = None
        self.threads = []
        self._status_error_logged = False

    # ================================================================ start/stop
    def start(self):
        """Bind the port and start all background threads (non-blocking)."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(50)
        self.server_sock.settimeout(SOCKET_TIMEOUT)
        self.port = self.server_sock.getsockname()[1]   # real port if 0 was used
        log.info("Scheduler listening on %s:%d", self.host, self.port)

        for target in (self._accept_loop, self._dispatcher_loop,
                       self._heartbeat_monitor_loop, self._status_writer_loop):
            thread = threading.Thread(target=target, name=target.__name__,
                                      daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        """Clean shutdown: stop threads, close sockets, report unfinished jobs."""
        log.info("Shutting down...")
        self.stop_event.set()
        with self.cond:
            self.cond.notify_all()
        try:
            self.server_sock.close()
        except (OSError, AttributeError):
            pass
        with self.cond:
            open_connections = list(self.connections)
        for conn in open_connections:
            conn.close()
        for thread in self.threads:
            thread.join(timeout=3)

        with self.cond:
            unfinished = sorted(j.job_id for j in self.jobs.values()
                                if j.status in (JOB_QUEUED, JOB_RUNNING))
        if unfinished:
            log.warning("UNFINISHED JOBS LOST (in-memory only): %s", unfinished)
        self._write_status_file(running=False)
        log.info("Scheduler stopped.")

    # ============================================================== accept loop
    def _accept_loop(self):
        while not self.stop_event.is_set():
            try:
                sock, addr = self.server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break    # server socket closed
            sock.settimeout(SOCKET_TIMEOUT)
            conn = Connection(sock, addr)
            with self.cond:
                self.connections.add(conn)
            log.info("New connection from %s:%d", conn.ip, conn.port)
            threading.Thread(target=self._handle_connection, args=(conn,),
                             name="conn-%s:%d" % (conn.ip, conn.port),
                             daemon=True).start()

    # ========================================================== per-connection
    def _handle_connection(self, conn):
        """Read messages from one connection until it closes."""
        bad_messages = 0
        try:
            while not self.stop_event.is_set() and not conn.closed:
                try:
                    line = conn.reader.read_line()
                    if line is None:
                        break                      # peer closed
                    msg = parse_message(line)
                except socket.timeout:
                    continue
                except ProtocolError as err:
                    bad_messages += 1
                    log.warning("Bad message from %s:%d: %s",
                                conn.ip, conn.port, err)
                    conn.send("%s %s" % (ERROR, err))
                    if err.fatal or bad_messages >= MAX_BAD_MESSAGES:
                        break
                    continue
                except OSError:
                    break
                try:
                    self._process_message(conn, msg)
                except Exception:   # a bug must not silently kill the thread
                    log.exception("Internal error handling %s", msg.verb)
                    conn.send("%s internal scheduler error" % ERROR)
        finally:
            self._cleanup_connection(conn)

    def _cleanup_connection(self, conn):
        conn.close()
        with self.cond:
            self.connections.discard(conn)
        if conn.role == "worker":
            log.warning("Worker %s disconnected", conn.worker_id)
            self._mark_worker_dead(conn.worker_id, conn, "connection lost")
        else:
            log.info("Connection %s:%d closed", conn.ip, conn.port)

    def _process_message(self, conn, msg):
        verb, args = msg.verb, msg.args
        worker_verbs = (HEARTBEAT, DONE, FAIL)

        if verb in worker_verbs and conn.role != "worker":
            conn.send("%s register first with REGISTER <worker_id>" % ERROR)
        elif verb in (SUBMIT, QUIT) and conn.role == "worker":
            conn.send("%s workers cannot send %s" % (ERROR, verb))
        elif verb == SUBMIT:
            conn.role = "client"
            self._handle_submit(conn, args[0], args[1])
        elif verb == PING:
            conn.send(PONG)
        elif verb == QUIT:
            conn.close()                     # handler loop ends on conn.closed
        elif verb == REGISTER:
            self._handle_register(conn, args[0])
        elif verb == HEARTBEAT:
            self._handle_heartbeat(conn, args[0], args[1])
        elif verb == DONE:
            self._handle_worker_result(conn, args[0], True, args[1])
        elif verb == FAIL:
            self._handle_worker_result(conn, args[0], False, args[1])

    # ============================================================ client side
    def _handle_submit(self, conn, job_type, payload):
        if job_type not in JOB_TYPES:
            conn.send("%s unknown job type %s (allowed: %s)"
                      % (ERROR, job_type, ", ".join(JOB_TYPES)))
            return
        with self.cond:
            if len(self.queue) >= self.max_queue_size:
                job = None
            else:
                job = Job(self.next_job_id, job_type, payload, conn)
                self.next_job_id += 1
                self.jobs[job.job_id] = job
        if job is None:
            conn.send("%s queue full, try again later" % ERROR)
            return

        # ACK first, enqueue second: otherwise a fast worker could finish the
        # job and the client would see RESULT before ACK.
        conn.send("%s %d" % (ACK, job.job_id))
        with self.cond:
            self.queue.append(job.job_id)
            self.cond.notify_all()
        log.info("Job %d queued (%s) from %s:%d", job.job_id, job_type,
                 conn.ip, conn.port)

    def _push_result(self, job):
        """Tell the submitting client a job finished (call WITHOUT the lock)."""
        conn = job.client_conn
        if conn is None or conn.closed:
            log.info("Client of job %d is gone; result dropped", job.job_id)
            return
        if job.status == JOB_DONE:
            conn.send("%s %d %s" % (RESULT, job.job_id, job.result))
        else:
            conn.send("%s %d %s" % (FAILED, job.job_id, job.error))

    # ============================================================ worker side
    def _handle_register(self, conn, worker_id):
        if conn.role == "worker":
            conn.send("%s already registered" % ERROR)
            return

        # Reply BEFORE adding the worker to the registry. As soon as it is in
        # the registry the dispatcher may send it a queued JOB, and worker.py
        # expects REGISTERED to be the very first line it receives.
        if not conn.send("%s %s" % (REGISTERED, worker_id)):
            return

        old_conn = None
        failed_jobs = []
        with self.cond:
            worker = self.workers.get(worker_id)
            if worker is None:
                worker = WorkerInfo(worker_id, conn)
                self.workers[worker_id] = worker
                self.worker_order.append(worker_id)
            else:
                # Same ID again (worker restarted): whatever the old connection
                # was running is gone, so re-queue it.
                old_conn = worker.conn
                failed_jobs = self._fail_worker_locked(worker, "re-registered")
                worker.conn = conn
                worker.ip = conn.ip
                worker.status = WORKER_ALIVE
                worker.last_heartbeat = time.monotonic()
                worker.reported_active = 0
            conn.role = "worker"
            conn.worker_id = worker_id
            self.cond.notify_all()

        if old_conn is not None and old_conn is not conn:
            old_conn.close()
        for job in failed_jobs:
            self._push_result(job)
        log.info("Worker %s registered from %s", worker_id, conn.ip)

    def _handle_heartbeat(self, conn, worker_id, active):
        if worker_id != conn.worker_id:
            conn.send("%s worker id does not match registration" % ERROR)
            return
        with self.cond:
            worker = self.workers.get(conn.worker_id)
            if worker is None or worker.conn is not conn:
                return                       # replaced by a newer registration
            worker.last_heartbeat = time.monotonic()
            worker.reported_active = active
            if worker.status == WORKER_DEAD:
                worker.status = WORKER_ALIVE
                log.info("Worker %s is alive again (heartbeat resumed)", worker_id)
                self.cond.notify_all()

    def _handle_worker_result(self, conn, job_id, success, text):
        finished_job = None
        with self.cond:
            worker = self.workers.get(conn.worker_id)
            if worker is None or worker.conn is not conn:
                return
            worker.last_heartbeat = time.monotonic()    # a result proves liveness
            if worker.status == WORKER_DEAD:
                worker.status = WORKER_ALIVE
            worker.running_jobs.discard(job_id)
            job = self.jobs.get(job_id)

            if job is None:
                log.warning("Result for unknown job %d from %s", job_id,
                            worker.worker_id)
            elif job.status in (JOB_DONE, JOB_FAILED):
                log.info("Ignoring duplicate result for job %d from %s",
                         job_id, worker.worker_id)
            else:
                if job.status == JOB_QUEUED:
                    # Re-queued after this worker was declared dead, but the
                    # worker finished it anyway: take the result.
                    try:
                        self.queue.remove(job_id)
                    except ValueError:
                        pass
                elif job.worker_id != worker.worker_id:
                    # Running on another worker now; that copy is redundant.
                    other = self.workers.get(job.worker_id)
                    if other is not None:
                        other.running_jobs.discard(job_id)
                job.worker_id = worker.worker_id
                if success:
                    job.status = JOB_DONE
                    job.result = text
                    worker.done += 1
                else:
                    job.status = JOB_FAILED
                    job.error = text or "job failed on worker"
                    worker.failed += 1
                finished_job = job
            self.cond.notify_all()

        if finished_job is not None:
            log.info("Job %d %s by %s", job_id, finished_job.status,
                     finished_job.worker_id)
            self._push_result(finished_job)

    # ======================================================= failure handling
    def _requeue_job_locked(self, job, reason):
        """Put an unfinished job back at the queue front. Returns False if it
        had to be marked FAILED because it used up all its attempts."""
        if job.attempts >= self.max_attempts:
            job.status = JOB_FAILED
            job.error = "gave up after %d attempts (last: %s)" % (
                job.attempts, reason)
            log.error("Job %d FAILED: %s", job.job_id, job.error)
            return False
        job.status = JOB_QUEUED
        job.worker_id = None
        self.queue.appendleft(job.job_id)
        self.requeued_total += 1
        log.warning("Job %d re-queued (%s), %d of %d attempts used",
                    job.job_id, reason, job.attempts, self.max_attempts)
        return True

    def _fail_worker_locked(self, worker, reason):
        """Mark a worker DEAD and re-queue its jobs. Caller holds the lock.
        Returns the jobs that became FAILED (caller must push results)."""
        worker.status = WORKER_DEAD
        failed = []
        for job_id in sorted(worker.running_jobs, reverse=True):
            job = self.jobs[job_id]
            if job.status == JOB_RUNNING and job.worker_id == worker.worker_id:
                if not self._requeue_job_locked(job, reason):
                    failed.append(job)
        worker.running_jobs.clear()
        self.cond.notify_all()
        return failed

    def _mark_worker_dead(self, worker_id, expected_conn, reason):
        with self.cond:
            worker = self.workers.get(worker_id)
            if worker is None or worker.conn is not expected_conn:
                return                       # already replaced
            failed = self._fail_worker_locked(worker, reason)
        for job in failed:
            self._push_result(job)

    # ============================================================ scheduling
    def _is_available_locked(self, worker):
        if worker.status != WORKER_ALIVE:
            return False
        return self.max_active == 0 or len(worker.running_jobs) < self.max_active

    def _pick_worker_locked(self):
        """Round-robin: next available worker after the previous pick."""
        count = len(self.worker_order)
        for offset in range(count):
            index = (self.rr_next + offset) % count
            worker = self.workers[self.worker_order[index]]
            if self._is_available_locked(worker):
                self.rr_next = (index + 1) % count
                return worker
        return None

    def _dispatcher_loop(self):
        """Hand queued jobs to available workers until shutdown."""
        while not self.stop_event.is_set():
            with self.cond:
                while not self.stop_event.is_set() and not (
                        self.queue and any(self._is_available_locked(w)
                                           for w in self.workers.values())):
                    self.cond.wait(timeout=1.0)
                if self.stop_event.is_set():
                    break
                job = self.jobs[self.queue.popleft()]
                if job.status != JOB_QUEUED:
                    continue
                worker = self._pick_worker_locked()
                job.status = JOB_RUNNING
                job.worker_id = worker.worker_id
                job.attempts += 1
                worker.running_jobs.add(job.job_id)
                conn = worker.conn
                line = "%s %d %s %s" % (JOB, job.job_id, job.job_type, job.payload)

            # The network send happens OUTSIDE the lock so a slow worker
            # cannot block the whole scheduler.
            log.info("Job %d -> %s (attempt %d)", job.job_id, worker.worker_id,
                     job.attempts)
            if not conn.send(line):
                with self.cond:
                    if job.status == JOB_RUNNING and job.worker_id == worker.worker_id:
                        job.attempts -= 1    # the worker never got it
                self._mark_worker_dead(worker.worker_id, conn, "send failed")

    def _heartbeat_monitor_loop(self):
        """Mark workers DEAD when their heartbeat is older than the timeout."""
        check_every = max(0.1, min(1.0, self.heartbeat_timeout / 4.0))
        while not self.stop_event.wait(check_every):
            failed = []
            with self.cond:
                now = time.monotonic()
                for worker in self.workers.values():
                    silent_for = now - worker.last_heartbeat
                    if (worker.status == WORKER_ALIVE
                            and silent_for > self.heartbeat_timeout):
                        log.warning("Worker %s timed out (no heartbeat for %.1fs)",
                                    worker.worker_id, silent_for)
                        failed.extend(self._fail_worker_locked(
                            worker, "heartbeat timeout"))
            for job in failed:
                self._push_result(job)

    # ======================================================= controller status
    def get_status_snapshot(self):
        """Dictionary described in the module docstring."""
        with self.cond:
            now_mono = time.monotonic()
            alive, dead = [], []
            for worker_id in self.worker_order:
                w = self.workers[worker_id]
                entry = {
                    "id": w.worker_id,
                    "ip": w.ip,
                    "status": w.status,
                    "active": len(w.running_jobs),
                    "done": w.done,
                    "failed": w.failed,
                    "reported_active": w.reported_active,
                    "last_heartbeat_age": round(now_mono - w.last_heartbeat, 2),
                }
                (alive if w.status == WORKER_ALIVE else dead).append(entry)
            counts = collections.Counter(j.status for j in self.jobs.values())
            return {
                "timestamp": time.time(),
                "scheduler_running": not self.stop_event.is_set(),
                "port": self.port,
                "heartbeat_timeout": self.heartbeat_timeout,
                "queue_len": len(self.queue),
                "workers": alive,
                "dead_workers": dead,
                "stats": {"submitted": len(self.jobs),
                          "queued": counts[JOB_QUEUED],
                          "running": counts[JOB_RUNNING],
                          "done": counts[JOB_DONE],
                          "failed": counts[JOB_FAILED],
                          "requeued": self.requeued_total},
            }

    def _write_status_file(self, running=True):
        snapshot = self.get_status_snapshot()
        snapshot["scheduler_running"] = running
        try:
            folder = os.path.dirname(self.status_file)
            if folder:
                os.makedirs(folder, exist_ok=True)
            temp_path = self.status_file + ".tmp"
            with open(temp_path, "w") as handle:
                json.dump(snapshot, handle, indent=2)
            os.replace(temp_path, self.status_file)      # atomic swap
            self._status_error_logged = False
        except OSError as err:
            if not self._status_error_logged:   # avoid one log line per second
                log.error("Cannot write status file %s: %s",
                          self.status_file, err)
                self._status_error_logged = True

    def _status_writer_loop(self):
        while not self.stop_event.is_set():
            self._write_status_file()
            self.stop_event.wait(self.status_interval)


# ====================================================================== main
def main():
    parser = argparse.ArgumentParser(description="Distributed job scheduler")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--heartbeat-timeout", type=float,
                        default=HEARTBEAT_TIMEOUT,
                        help="seconds of silence before a worker is DEAD")
    parser.add_argument("--status-file", default=DEFAULT_STATUS_FILE)
    parser.add_argument("--status-interval", type=float, default=1.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--max-active", type=int, default=0,
                        help="max jobs per worker at once (0 = unlimited)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s")

    scheduler = Scheduler(
        host=args.host, port=args.port,
        heartbeat_timeout=args.heartbeat_timeout,
        status_file=args.status_file, status_interval=args.status_interval,
        max_attempts=args.max_attempts, max_active=args.max_active)
    scheduler.start()
    # SIGTERM (topology.py's pkill) gets the same clean shutdown as Ctrl+C
    signal.signal(signal.SIGTERM, lambda *_: scheduler.stop_event.set())
    try:
        while not scheduler.stop_event.wait(0.5):
            pass
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()
