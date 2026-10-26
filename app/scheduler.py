"""
scheduler.py - TCP job scheduler for the Distributed Job Queue project (Member 1).

Clients and workers both connect to this server (default port 5000) and speak
the newline-delimited JSON protocol defined in protocol.py.

WHAT IT DOES
    - accepts many client and worker connections at once (one thread each)
    - gives every submitted job a unique ID and acknowledges it
    - keeps pending jobs in a queue and hands them to workers round-robin
    - tracks worker heartbeats; silent workers are marked DEAD
    - re-queues the unfinished jobs of a dead/disconnected worker
    - writes a JSON status file for the Ryu controller (Member 3)

CONTROLLER STATUS INTERFACE (file based)
    The scheduler rewrites STATUS_FILE (default status/scheduler_status.json)
    every --status-interval seconds (default 1 s). Writes are atomic (temp
    file + rename), so a reader never sees half a file. Mininet hosts and the
    Ryu process share one filesystem, so the controller simply reads the file.
    The same content is available over TCP via a GET_STATUS message.

    {
      "timestamp": 1700000000.12,        # unix time of this snapshot
      "scheduler_running": true,         # false after a clean shutdown
      "port": 5000,
      "heartbeat_timeout": 6.0,
      "queue_length": 3,
      "alive_workers": 2,
      "jobs": {"total": 9, "queued": 3, "running": 2, "done": 4, "failed": 0},
      "workers": [
        {"worker_id": "w1", "ip": "10.0.0.3", "status": "ALIVE",
         "available": true, "capacity": 1, "active_jobs": 0, "load": 0.0,
         "completed_jobs": 4, "failed_jobs": 0,
         "last_heartbeat_age": 0.8, "registered_at": 1700000000.0}
      ]
    }
    - "ip" is the address the worker connected from (key for the controller to
      find the worker's switch port). "status" is ALIVE or DEAD.
    - "load" = active_jobs / capacity (0.0 idle .. 1.0 full).
    - Treat the file as STALE if now - timestamp > 5 s (scheduler stopped).
    - Dead workers stay in the list (status DEAD) so the controller can see
      failures.

RELIABILITY AND LIMITATIONS (read this before the viva)
    - Worker disconnect or heartbeat timeout -> its RUNNING jobs go back to the
      FRONT of the queue (up to --max-attempts executions per job, then FAILED).
    - Guarantee is AT-LEAST-ONCE: if a worker was only slow (not dead), its job
      may run twice. The first result that arrives wins; later ones are ignored.
      Jobs should therefore be safe to repeat (ours are).
    - All state (queue, jobs, results) is IN MEMORY. If the scheduler process
      dies, queued and running jobs are lost. On a clean Ctrl+C the unfinished
      job IDs are logged, but they are not saved to disk.
    - The scheduler is a single point of failure.
    - A worker revived by a late heartbeat may still be busy with a job that was
      already re-queued, so it can briefly be given more work than its capacity.
    - If the client that submitted a job has disconnected, the result is kept
      and can be fetched with GET_JOB_STATUS, but it is not pushed.
    - Jobs are never removed from memory (fine for a mini project).
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
    ASSIGN_JOB, ERROR, GET_JOB_STATUS, GET_STATUS, HEARTBEAT, HEARTBEAT_ACK,
    JOB_ACK, JOB_DONE, JOB_FAILED, JOB_QUEUED, JOB_RESULT, JOB_RUNNING,
    JOB_STATUS, REGISTER_ACK, REGISTER_WORKER, SCHEDULER_PORT, STATUS,
    SUBMIT_JOB, WORKER_ALIVE, WORKER_DEAD, WORKER_RESULT,
    MessageReader, ProtocolError, make_message, send_message,
)

log = logging.getLogger("scheduler")

SOCKET_TIMEOUT = 1.0       # lets threads notice shutdown
MAX_BAD_MESSAGES = 10      # close a connection after this many bad lines


class Job:
    """One unit of work and its current state."""

    def __init__(self, job_id, job_type, payload, client_id, client_conn):
        self.job_id = job_id
        self.job_type = job_type
        self.payload = payload
        self.client_id = client_id
        self.client_conn = client_conn   # where to push JOB_RESULT (may be gone)
        self.status = JOB_QUEUED
        self.worker_id = None
        self.attempts = 0                # number of times it was handed to a worker
        self.result = None
        self.error = None
        self.submitted_at = time.time()
        self.finished_at = None

    def to_fields(self):
        """Fields sent to clients in JOB_STATUS / JOB_RESULT."""
        return {"job_id": self.job_id, "status": self.status,
                "result": self.result, "error": self.error,
                "worker_id": self.worker_id, "attempts": self.attempts}


class WorkerInfo:
    """Registry entry for one worker."""

    def __init__(self, worker_id, conn, capacity):
        self.worker_id = worker_id
        self.conn = conn
        self.ip = conn.ip
        self.capacity = capacity
        self.status = WORKER_ALIVE
        self.last_heartbeat = time.monotonic()
        self.registered_at = time.time()
        self.running_jobs = set()        # job IDs currently assigned
        self.completed_jobs = 0
        self.failed_jobs = 0

    def is_available(self):
        return (self.status == WORKER_ALIVE
                and len(self.running_jobs) < self.capacity)


class Connection:
    """A client or worker socket plus the helpers we need around it."""

    def __init__(self, sock, addr):
        self.sock = sock
        self.ip = addr[0]
        self.port = addr[1]
        self.reader = MessageReader(sock)
        self.send_lock = threading.Lock()   # several threads may send to it
        self.role = "unknown"               # "client" or "worker"
        self.worker_id = None
        self.closed = False

    def send(self, msg_type, **fields):
        """Send a message. Returns True on success; closes socket on failure."""
        try:
            send_message(self.sock, make_message(msg_type, **fields),
                         self.send_lock)
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
    def __init__(self, host="0.0.0.0", port=SCHEDULER_PORT,
                 heartbeat_interval=2.0, heartbeat_timeout=6.0,
                 status_file="status/scheduler_status.json",
                 status_interval=1.0, max_attempts=3, max_queue_size=1000):
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.status_file = status_file
        self.status_interval = status_interval
        self.max_attempts = max_attempts
        self.max_queue_size = max_queue_size

        # ---- shared state; ALWAYS touch it while holding self.cond ----
        self.cond = threading.Condition(threading.Lock())
        self.jobs = {}                          # job_id -> Job
        self.queue = collections.deque()        # pending job IDs (FIFO)
        self.workers = {}                       # worker_id -> WorkerInfo
        self.worker_order = []                  # registration order (round-robin)
        self.rr_next = 0                        # round-robin pointer
        self.next_job_id = 1
        self.connections = set()                # open Connection objects

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
                    msg = conn.reader.read_message()
                except socket.timeout:
                    continue
                except ProtocolError as err:
                    bad_messages += 1
                    log.warning("Bad message from %s:%d: %s",
                                conn.ip, conn.port, err)
                    conn.send(ERROR, code="bad_message", message=str(err))
                    if err.fatal or bad_messages >= MAX_BAD_MESSAGES:
                        break
                    continue
                except OSError:
                    break
                if msg is None:
                    break   # peer closed
                try:
                    self._process_message(conn, msg)
                except Exception:   # a bug must not silently kill the thread
                    log.exception("Internal error handling %s", msg.get("type"))
                    conn.send(ERROR, code="internal_error",
                              message="scheduler failed to process message")
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
        msg_type = msg["type"]
        is_worker_msg = msg_type in (HEARTBEAT, WORKER_RESULT)
        is_client_msg = msg_type in (SUBMIT_JOB, GET_JOB_STATUS)

        if is_worker_msg and conn.role != "worker":
            conn.send(ERROR, code="not_registered",
                      message="send REGISTER_WORKER first")
        elif is_client_msg and conn.role == "worker":
            conn.send(ERROR, code="wrong_role",
                      message="workers cannot send %s" % msg_type)
        elif msg_type == SUBMIT_JOB:
            conn.role = "client"
            self._handle_submit_job(conn, msg)
        elif msg_type == GET_JOB_STATUS:
            conn.role = "client"
            self._handle_get_job_status(conn, msg)
        elif msg_type == GET_STATUS:
            conn.send(STATUS, **self.get_status_snapshot())
        elif msg_type == REGISTER_WORKER:
            self._handle_register_worker(conn, msg)
        elif msg_type == HEARTBEAT:
            self._handle_heartbeat(conn, msg)
        elif msg_type == WORKER_RESULT:
            self._handle_worker_result(conn, msg)
        else:
            conn.send(ERROR, code="unexpected_type",
                      message="scheduler does not accept %s" % msg_type)

    # ============================================================ client side
    def _handle_submit_job(self, conn, msg):
        with self.cond:
            if len(self.queue) >= self.max_queue_size:
                full = True
            else:
                full = False
                job_id = self.next_job_id
                self.next_job_id += 1
                job = Job(job_id, msg["job_type"], msg["payload"],
                          msg.get("client_id"), conn)
                self.jobs[job_id] = job
        if full:
            conn.send(ERROR, code="queue_full",
                      message="too many pending jobs, try again later")
            return

        # ACK first, enqueue second: otherwise a fast worker could finish the
        # job and the client would see JOB_RESULT before JOB_ACK.
        conn.send(JOB_ACK, job_id=job_id, status=JOB_QUEUED)
        with self.cond:
            self.queue.append(job_id)
            self.cond.notify_all()
        log.info("Job %d queued (%s) from %s", job_id, job.job_type,
                 job.client_id or conn.ip)

    def _handle_get_job_status(self, conn, msg):
        with self.cond:
            job = self.jobs.get(msg["job_id"])
            fields = job.to_fields() if job else None
        if fields is None:
            conn.send(ERROR, code="unknown_job",
                      message="no job with id %d" % msg["job_id"])
        else:
            conn.send(JOB_STATUS, **fields)

    def _push_result(self, job):
        """Tell the submitting client a job finished (call WITHOUT the lock)."""
        if job.client_conn is not None and not job.client_conn.closed:
            job.client_conn.send(JOB_RESULT, **job.to_fields())

    # ============================================================ worker side
    def _handle_register_worker(self, conn, msg):
        worker_id = msg["worker_id"]
        capacity = msg.get("capacity", 1)
        if conn.role == "worker":
            conn.send(ERROR, code="already_registered",
                      message="this connection is already registered")
            return

        old_conn = None
        failed_jobs = []
        with self.cond:
            worker = self.workers.get(worker_id)
            if worker is None:
                worker = WorkerInfo(worker_id, conn, capacity)
                self.workers[worker_id] = worker
                self.worker_order.append(worker_id)
            else:
                # Same ID registering again (worker restarted): whatever the old
                # connection was running is gone, so re-queue it.
                old_conn = worker.conn
                failed_jobs = self._fail_worker_locked(worker, "re-registered")
                worker.conn = conn
                worker.ip = conn.ip
                worker.capacity = capacity
                worker.status = WORKER_ALIVE
                worker.last_heartbeat = time.monotonic()
                worker.registered_at = time.time()
            conn.role = "worker"
            conn.worker_id = worker_id
            self.cond.notify_all()

        if old_conn is not None and old_conn is not conn:
            old_conn.close()
        for job in failed_jobs:
            self._push_result(job)
        log.info("Worker %s registered from %s (capacity %d)",
                 worker_id, conn.ip, capacity)
        conn.send(REGISTER_ACK, worker_id=worker_id,
                  heartbeat_interval=self.heartbeat_interval,
                  heartbeat_timeout=self.heartbeat_timeout)

    def _handle_heartbeat(self, conn, msg):
        if msg["worker_id"] != conn.worker_id:
            conn.send(ERROR, code="wrong_worker_id",
                      message="worker_id does not match registration")
            return
        with self.cond:
            worker = self.workers.get(conn.worker_id)
            if worker is None or worker.conn is not conn:
                return   # replaced by a newer registration
            worker.last_heartbeat = time.monotonic()
            if worker.status == WORKER_DEAD:
                worker.status = WORKER_ALIVE
                log.info("Worker %s is alive again (heartbeat resumed)",
                         worker.worker_id)
                self.cond.notify_all()
        conn.send(HEARTBEAT_ACK, worker_id=conn.worker_id)

    def _handle_worker_result(self, conn, msg):
        job_id = msg["job_id"]
        finished_job = None
        with self.cond:
            worker = self.workers.get(conn.worker_id)
            if worker is None or worker.conn is not conn:
                return
            worker.last_heartbeat = time.monotonic()   # a result proves liveness
            if worker.status == WORKER_DEAD:
                worker.status = WORKER_ALIVE
            worker.running_jobs.discard(job_id)
            job = self.jobs.get(job_id)

            if job is None:
                log.warning("Result for unknown job %d from %s",
                            job_id, worker.worker_id)
            elif job.status in (JOB_DONE, JOB_FAILED):
                log.info("Ignoring duplicate result for job %d from %s",
                         job_id, worker.worker_id)
            else:
                if job.status == JOB_QUEUED:
                    # It was re-queued after this worker was declared dead,
                    # but the worker finished it anyway: take the result.
                    try:
                        self.queue.remove(job_id)
                    except ValueError:
                        pass
                elif job.worker_id != worker.worker_id:
                    # Running on another worker now; that copy is redundant.
                    other = self.workers.get(job.worker_id)
                    if other is not None:
                        other.running_jobs.discard(job_id)
                job.status = msg["status"]
                job.result = msg.get("result")
                job.error = msg.get("error")
                job.worker_id = worker.worker_id
                job.finished_at = time.time()
                if job.status == JOB_DONE:
                    worker.completed_jobs += 1
                else:
                    worker.failed_jobs += 1
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
            job.finished_at = time.time()
            log.error("Job %d FAILED: %s", job.job_id, job.error)
            return False
        job.status = JOB_QUEUED
        job.worker_id = None
        self.queue.appendleft(job.job_id)
        log.warning("Job %d re-queued (%s), attempt %d of %d used",
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
                return   # already replaced by a newer registration
            failed = self._fail_worker_locked(worker, reason)
        for job in failed:
            self._push_result(job)

    # ============================================================ scheduling
    def _pick_worker_locked(self):
        """Round-robin: next available worker after the previous pick."""
        count = len(self.worker_order)
        for offset in range(count):
            index = (self.rr_next + offset) % count
            worker = self.workers[self.worker_order[index]]
            if worker.is_available():
                self.rr_next = (index + 1) % count
                return worker
        return None

    def _has_available_worker_locked(self):
        return any(w.is_available() for w in self.workers.values())

    def _dispatcher_loop(self):
        """Hand queued jobs to available workers until shutdown."""
        while not self.stop_event.is_set():
            with self.cond:
                while not self.stop_event.is_set() and not (
                        self.queue and self._has_available_worker_locked()):
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
                assignment = dict(job_id=job.job_id, job_type=job.job_type,
                                  payload=job.payload, attempt=job.attempts)

            # Network send happens OUTSIDE the lock so a slow worker cannot
            # block the whole scheduler.
            log.info("Job %d -> %s (attempt %d)", job.job_id, worker.worker_id,
                     job.attempts)
            if not conn.send(ASSIGN_JOB, **assignment):
                with self.cond:
                    if job.status == JOB_RUNNING and job.worker_id == worker.worker_id:
                        job.attempts -= 1   # the worker never got it
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
            workers = []
            for worker_id in self.worker_order:
                w = self.workers[worker_id]
                active = len(w.running_jobs)
                workers.append({
                    "worker_id": w.worker_id,
                    "ip": w.ip,
                    "status": w.status,
                    "available": w.is_available(),
                    "capacity": w.capacity,
                    "active_jobs": active,
                    "load": round(active / float(w.capacity), 3),
                    "completed_jobs": w.completed_jobs,
                    "failed_jobs": w.failed_jobs,
                    "last_heartbeat_age": round(now_mono - w.last_heartbeat, 2),
                    "registered_at": w.registered_at,
                })
            counts = collections.Counter(j.status for j in self.jobs.values())
            return {
                "timestamp": time.time(),
                "scheduler_running": not self.stop_event.is_set(),
                "port": self.port,
                "heartbeat_timeout": self.heartbeat_timeout,
                "queue_length": len(self.queue),
                "alive_workers": sum(1 for w in workers
                                     if w["status"] == WORKER_ALIVE),
                "jobs": {"total": len(self.jobs),
                         "queued": counts[JOB_QUEUED],
                         "running": counts[JOB_RUNNING],
                         "done": counts[JOB_DONE],
                         "failed": counts[JOB_FAILED]},
                "workers": workers,
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
            os.replace(temp_path, self.status_file)   # atomic swap
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
    parser.add_argument("--port", type=int, default=SCHEDULER_PORT)
    parser.add_argument("--heartbeat-interval", type=float, default=2.0,
                        help="seconds between worker heartbeats (advertised)")
    parser.add_argument("--heartbeat-timeout", type=float, default=6.0,
                        help="seconds of silence before a worker is DEAD")
    parser.add_argument("--status-file", default="status/scheduler_status.json")
    parser.add_argument("--status-interval", type=float, default=1.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s")

    scheduler = Scheduler(
        host=args.host, port=args.port,
        heartbeat_interval=args.heartbeat_interval,
        heartbeat_timeout=args.heartbeat_timeout,
        status_file=args.status_file, status_interval=args.status_interval,
        max_attempts=args.max_attempts)
    scheduler.start()
    # SIGTERM (e.g. `kill`) gets the same clean shutdown as Ctrl+C
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
