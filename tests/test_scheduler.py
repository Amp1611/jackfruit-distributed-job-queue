"""
Tests for app/protocol.py and app/scheduler.py (Member 1).

Run from the project root:
    python3 -m unittest tests.test_scheduler -v

Most tests start their own Scheduler on a free port (port 0) and play the
role of clients and workers with plain sockets. The last test class runs the
REAL app/worker.py and app/client.py (Member 2) against the scheduler; it is
skipped automatically if those files are not in app/.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
sys.path.insert(0, APP_DIR)

import protocol  # noqa: E402
from protocol import LineReader, ProtocolError, parse_message, send_line  # noqa: E402
from scheduler import Scheduler  # noqa: E402


class Peer:
    """A tiny test client/worker: send lines, wait for replies."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.reader = LineReader(self.sock)

    def send(self, text):
        send_line(self.sock, None, text)

    def send_raw(self, data):
        self.sock.sendall(data)

    def recv(self, timeout=5):
        self.sock.settimeout(timeout)
        return self.reader.read_line()

    def recv_prefix(self, prefix, timeout=5):
        """Read until a line starting with prefix arrives; skip the others."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self.recv(max(0.1, deadline - time.time()))
            if line is None:
                raise AssertionError("connection closed waiting for " + prefix)
            if line.startswith(prefix):
                return line
        raise AssertionError("timed out waiting for " + prefix)

    def close(self):
        self.sock.close()


class SchedulerTestCase(unittest.TestCase):
    heartbeat_timeout = 8.0

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.status_path = os.path.join(self.tmpdir, "status.json")
        self.sched = Scheduler(host="127.0.0.1", port=0,
                               heartbeat_timeout=self.heartbeat_timeout,
                               status_file=self.status_path,
                               status_interval=0.2)
        self.sched.start()
        self.peers = []

    def tearDown(self):
        for peer in self.peers:
            peer.close()
        self.sched.stop()

    def connect(self):
        peer = Peer(self.sched.port)
        self.peers.append(peer)
        return peer

    def register_worker(self, worker_id):
        worker = self.connect()
        worker.send("REGISTER %s" % worker_id)
        # worker.py requires REGISTERED to be the FIRST line it receives
        self.assertEqual(worker.recv(), "REGISTERED %s" % worker_id)
        return worker

    def wait_until(self, condition, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return
            time.sleep(0.05)
        self.fail("condition not met within %s s" % timeout)

    def submit(self, client, text="hello", job_type="REVERSE"):
        client.send("SUBMIT %s %s" % (job_type, text))
        return int(client.recv_prefix("ACK").split()[1])

    def snapshot(self):
        return self.sched.get_status_snapshot()


# ------------------------------------------------------------------ protocol
class ProtocolTests(unittest.TestCase):
    def test_parse_valid_messages(self):
        self.assertEqual(parse_message("SUBMIT REVERSE hello world").args,
                         ("REVERSE", "hello world"))
        self.assertEqual(parse_message("SUBMIT SUM").args, ("SUM", ""))
        self.assertEqual(parse_message("REGISTER worker1").args, ("worker1",))
        self.assertEqual(parse_message("HEARTBEAT worker1 3").args, ("worker1", 3))
        self.assertEqual(parse_message("DONE 7 6.5").args, (7, "6.5"))
        self.assertEqual(parse_message("FAIL 7 bad payload here").args,
                         (7, "bad payload here"))
        self.assertEqual(parse_message("PING").verb, "PING")
        self.assertEqual(parse_message("QUIT").verb, "QUIT")

    def test_parse_invalid_messages(self):
        for text in ["", "   ", "HELLO", "SUBMIT", "REGISTER", "REGISTER a b",
                     "HEARTBEAT", "HEARTBEAT w x", "HEARTBEAT w -1",
                     "DONE", "DONE abc out", "FAIL x", "submit REVERSE hi"]:
            with self.assertRaises(ProtocolError, msg=repr(text)):
                parse_message(text)

    def test_send_line_removes_embedded_newlines(self):
        a, b = socket.socketpair()
        send_line(a, threading.Lock(), "DONE 1 line1\nline2\r\nline3")
        a.close()
        reader = LineReader(b)
        self.assertEqual(reader.read_line(), "DONE 1 line1 line2 line3")
        self.assertIsNone(reader.read_line())
        b.close()

    def test_reader_handles_split_and_merged_lines(self):
        a, b = socket.socketpair()
        reader = LineReader(b)
        a.sendall(b"SUBMIT REV")                       # half a line
        a.sendall(b"ERSE hi\nPING\n\nQUIT\n")          # rest + 2 lines + blank
        a.close()
        self.assertEqual(reader.read_line(), "SUBMIT REVERSE hi")
        self.assertEqual(reader.read_line(), "PING")
        self.assertEqual(reader.read_line(), "QUIT")
        self.assertIsNone(reader.read_line())
        b.close()

    def test_reader_survives_bad_utf8_but_not_huge_line(self):
        a, b = socket.socketpair()
        reader = LineReader(b)
        a.sendall(b"\xff\xfe\nPING\n")
        with self.assertRaises(ProtocolError) as ctx:
            reader.read_line()
        self.assertFalse(ctx.exception.fatal)
        self.assertEqual(reader.read_line(), "PING")
        a.sendall(b"x" * (protocol.MAX_LINE_BYTES + 10))
        with self.assertRaises(ProtocolError) as ctx:
            reader.read_line()
        self.assertTrue(ctx.exception.fatal)
        a.close()
        b.close()


# ----------------------------------------------------------------- scheduler
class JobSubmissionTests(SchedulerTestCase):
    def test_submit_ack_and_sequential_ids(self):
        client = self.connect()
        client.send("SUBMIT SUM 1 2 3")
        self.assertEqual(client.recv(), "ACK 1")
        client.send("SUBMIT SLEEP 1")
        self.assertEqual(client.recv(), "ACK 2")
        # (a job joins the queue just AFTER its ACK is sent, so poll briefly)
        self.wait_until(lambda: self.snapshot()["queue_len"] == 2)  # no worker yet

    def test_bad_requests_get_error_and_connection_survives(self):
        client = self.connect()
        client.send("SUBMIT FOO bar")
        self.assertTrue(client.recv().startswith("ERROR unknown job type FOO"))
        client.send("SUBMIT")
        self.assertTrue(client.recv().startswith("ERROR"))
        client.send("NONSENSE 1 2")
        self.assertTrue(client.recv().startswith("ERROR"))
        self.assertIsInstance(self.submit(client), int)     # still usable

    def test_concurrent_clients_get_unique_ids(self):
        ids, errors = [], []
        lock = threading.Lock()

        def run_client():
            try:
                peer = Peer(self.sched.port)
                for i in range(5):
                    job_id = self.submit(peer, "x%d" % i)
                    with lock:
                        ids.append(job_id)
                peer.close()
            except Exception as err:   # noqa: BLE001
                errors.append(err)

        threads = [threading.Thread(target=run_client) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(15)
        self.assertEqual(errors, [])
        self.assertEqual(len(ids), 50)
        self.assertEqual(len(set(ids)), 50)

    def test_ping_and_quit(self):
        client = self.connect()
        client.send("PING")
        self.assertEqual(client.recv(), "PONG")
        client.send("QUIT")
        self.assertIsNone(client.recv())          # scheduler closed it


class WorkerAndDispatchTests(SchedulerTestCase):
    def test_registration_visible_in_status(self):
        self.register_worker("worker1")
        # REGISTERED is sent just before the registry entry appears: poll
        self.wait_until(lambda: len(self.snapshot()["workers"]) == 1)
        snap = self.snapshot()
        self.assertEqual(len(snap["workers"]), 1)
        w = snap["workers"][0]
        self.assertEqual((w["id"], w["ip"], w["status"], w["active"], w["done"]),
                         ("worker1", "127.0.0.1", "ALIVE", 0, 0))

    def test_worker_message_errors(self):
        peer = self.connect()
        peer.send("HEARTBEAT ghost 0")
        self.assertTrue(peer.recv().startswith("ERROR"))
        worker = self.register_worker("w1")
        worker.send("REGISTER w1")
        self.assertTrue(worker.recv_prefix("ERROR").startswith("ERROR already"))
        worker.send("HEARTBEAT someone_else 0")
        self.assertTrue(worker.recv_prefix("ERROR").startswith("ERROR"))
        worker.send("SUBMIT REVERSE x")
        self.assertTrue(worker.recv_prefix("ERROR").startswith("ERROR workers"))

    def test_dispatch_result_and_ack_comes_first(self):
        worker = self.register_worker("w1")
        client = self.connect()
        client.send("SUBMIT REVERSE hello world")
        self.assertEqual(client.recv(), "ACK 1")
        self.assertEqual(worker.recv_prefix("JOB"), "JOB 1 REVERSE hello world")
        worker.send("DONE 1 dlrow olleh")
        self.assertEqual(client.recv(), "RESULT 1 dlrow olleh")
        snap = self.snapshot()
        self.assertEqual(snap["workers"][0]["done"], 1)
        self.assertEqual(snap["stats"]["done"], 1)

    def test_worker_fail_is_reported_and_not_retried(self):
        worker = self.register_worker("w1")
        client = self.connect()
        job_id = self.submit(client, "abc", "SUM")
        worker.recv_prefix("JOB")
        worker.send("FAIL %d could not convert string" % job_id)
        self.assertEqual(client.recv(), "FAILED %d could not convert string" % job_id)
        with self.assertRaises(socket.timeout):
            worker.recv(timeout=0.6)               # no second attempt
        self.assertEqual(self.snapshot()["stats"]["failed"], 1)

    def test_jobs_wait_until_a_worker_registers(self):
        client = self.connect()
        job_id = self.submit(client)
        time.sleep(0.3)
        worker = self.register_worker("late")
        self.assertTrue(worker.recv_prefix("JOB").startswith("JOB %d " % job_id))

    def test_registered_is_first_line_even_with_jobs_waiting(self):
        client = self.connect()
        first = self.submit(client)
        second = self.submit(client)
        self.wait_until(lambda: self.snapshot()["queue_len"] == 2)
        worker = self.connect()
        worker.send("REGISTER late")
        self.assertEqual(worker.recv(), "REGISTERED late")   # NOT a JOB line
        self.assertTrue(worker.recv().startswith("JOB %d " % first))
        self.assertTrue(worker.recv().startswith("JOB %d " % second))

    def test_unlimited_by_default_worker_gets_all_jobs(self):
        worker = self.register_worker("w1")
        client = self.connect()
        for _ in range(3):
            self.submit(client)
        got = [worker.recv_prefix("JOB") for _ in range(3)]
        self.assertEqual(len(got), 3)

    def test_max_active_cap(self):
        self.sched.max_active = 1
        worker = self.register_worker("w1")
        client = self.connect()
        first = self.submit(client)
        second = self.submit(client)
        self.assertTrue(worker.recv_prefix("JOB").startswith("JOB %d " % first))
        with self.assertRaises(socket.timeout):    # second must wait
            worker.recv(timeout=0.7)
        worker.send("DONE %d ok" % first)
        self.assertTrue(worker.recv_prefix("JOB").startswith("JOB %d " % second))

    def test_round_robin_distribution(self):
        workers = {n: self.register_worker(n) for n in ("w1", "w2", "w3")}
        client = self.connect()
        owner = {}
        for _ in range(6):
            job_id = self.submit(client)
            for name, peer in workers.items():
                try:
                    line = peer.recv(timeout=0.5)
                except socket.timeout:
                    continue
                self.assertTrue(line.startswith("JOB %d " % job_id), line)
                owner[job_id] = name
                break
        self.assertEqual([owner[j] for j in sorted(owner)],
                         ["w1", "w2", "w3", "w1", "w2", "w3"])


class HeartbeatTests(SchedulerTestCase):
    heartbeat_timeout = 1.0

    def test_silent_worker_expires_and_job_moves(self):
        silent = self.register_worker("silent")
        client = self.connect()
        job_id = self.submit(client)
        silent.recv_prefix("JOB")
        backup = self.register_worker("backup")
        got = None
        deadline = time.time() + 6
        while time.time() < deadline and got is None:
            backup.send("HEARTBEAT backup 0")
            try:
                line = backup.recv(timeout=0.3)
            except socket.timeout:
                continue
            if line.startswith("JOB"):
                got = line
        self.assertIsNotNone(got, "job was not re-assigned after expiry")
        self.assertTrue(got.startswith("JOB %d " % job_id))
        snap = self.snapshot()
        self.assertEqual([w["id"] for w in snap["workers"]], ["backup"])
        self.assertEqual([w["id"] for w in snap["dead_workers"]], ["silent"])
        self.assertEqual(snap["stats"]["requeued"], 1)

    def test_heartbeats_keep_alive_and_dead_worker_revives(self):
        worker = self.register_worker("w1")
        for _ in range(6):                       # about 1.8 s > 1 s timeout
            worker.send("HEARTBEAT w1 0")
            time.sleep(0.3)
        self.assertEqual(self.snapshot()["workers"][0]["status"], "ALIVE")
        time.sleep(1.8)                          # go silent
        snap = self.snapshot()
        self.assertEqual((len(snap["workers"]), len(snap["dead_workers"])), (0, 1))
        worker.send("HEARTBEAT w1 0")
        time.sleep(0.3)
        self.assertEqual(self.snapshot()["workers"][0]["status"], "ALIVE")


class FailureTests(SchedulerTestCase):
    def test_worker_disconnect_requeues_job(self):
        doomed = self.register_worker("doomed")
        client = self.connect()
        job_id = self.submit(client)
        doomed.recv_prefix("JOB")
        survivor = self.register_worker("survivor")
        doomed.close()                           # abrupt disconnect
        self.assertTrue(survivor.recv_prefix("JOB").startswith("JOB %d " % job_id))
        survivor.send("DONE %d ok" % job_id)
        self.assertEqual(client.recv_prefix("RESULT"), "RESULT %d ok" % job_id)

    def test_same_worker_id_reregisters_and_jobs_requeue(self):
        old = self.register_worker("w1")
        client = self.connect()
        job_id = self.submit(client)
        old.recv_prefix("JOB")
        new = self.register_worker("w1")         # restarted worker
        self.assertTrue(new.recv_prefix("JOB").startswith("JOB %d " % job_id))

    def test_job_fails_after_max_attempts(self):
        self.sched.max_attempts = 2
        client = self.connect()
        job_id = self.submit(client)
        for i in range(2):
            worker = self.register_worker("flaky%d" % i)
            worker.recv_prefix("JOB")
            worker.close()
            time.sleep(0.3)
        line = client.recv_prefix("FAILED")
        self.assertTrue(line.startswith("FAILED %d gave up" % job_id), line)

    def test_late_result_from_dead_worker_is_accepted_once(self):
        self.sched.heartbeat_timeout = 0.6
        slow = self.register_worker("slow")
        client = self.connect()
        job_id = self.submit(client)
        slow.recv_prefix("JOB")
        time.sleep(1.6)                          # slow is declared DEAD, job queued
        self.assertEqual(self.snapshot()["queue_len"], 1)
        slow.send("DONE %d late" % job_id)       # ...but it finishes anyway
        self.assertEqual(client.recv_prefix("RESULT"), "RESULT %d late" % job_id)
        self.assertEqual(self.snapshot()["queue_len"], 0)
        slow.send("DONE %d again" % job_id)      # duplicate is ignored
        with self.assertRaises(socket.timeout):
            client.recv(timeout=0.6)

    def test_too_many_bad_lines_closes_connection(self):
        client = self.connect()
        client.send_raw(b"junk\n" * 12)
        closed = False
        deadline = time.time() + 5
        while time.time() < deadline and not closed:
            try:
                closed = client.recv(timeout=1) is None
            except (socket.timeout, ConnectionError):
                pass
        self.assertTrue(closed)

    def test_oversized_line_closes_connection(self):
        client = self.connect()
        try:
            client.send_raw(b"x" * (protocol.MAX_LINE_BYTES + 5000))
        except OSError:
            pass
        closed = False
        deadline = time.time() + 5
        while time.time() < deadline and not closed:
            try:
                closed = client.recv(timeout=1) is None
            except (socket.timeout, ConnectionError):
                pass
            except OSError:
                closed = True
        self.assertTrue(closed)


class StatusInterfaceTests(SchedulerTestCase):
    def read_status_file(self):
        with open(self.status_path) as handle:
            return json.load(handle)

    def test_status_file_matches_what_the_controller_reads(self):
        worker = self.register_worker("worker1")
        client = self.connect()
        job_id = self.submit(client)
        worker.recv_prefix("JOB")
        time.sleep(0.6)                          # let the 0.2 s writer run
        st = self.read_status_file()
        # This is exactly the expression controller._log_worker_status uses:
        workers = ", ".join("%s(active=%d,done=%d)" % (w["id"], w["active"], w["done"])
                            for w in st.get("workers", [])) or "none"
        line = "workers: %s | queue=%d | jobs %s" % (
            workers, st.get("queue_len", 0), st.get("stats"))
        self.assertIn("worker1(active=1,done=0)", line)
        self.assertTrue(st["scheduler_running"])
        self.assertLess(time.time() - st["timestamp"], 2)
        worker.send("DONE %d r" % job_id)
        client.recv_prefix("RESULT")
        time.sleep(0.6)
        st = self.read_status_file()
        self.assertEqual((st["workers"][0]["active"], st["workers"][0]["done"]), (0, 1))
        self.assertEqual(st["stats"]["done"], 1)

    def test_final_status_marks_scheduler_stopped(self):
        self.sched.stop()
        self.assertFalse(self.read_status_file()["scheduler_running"])


class CommandLineTests(unittest.TestCase):
    def run_and_stop(self, sig):
        tmp = tempfile.mkdtemp()
        status = os.path.join(tmp, "s.json")
        proc = subprocess.Popen(
            [sys.executable, "-u", "scheduler.py", "--port", "0",
             "--status-file", status, "--status-interval", "0.2"],
            cwd=APP_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True)
        try:
            time.sleep(1.0)
            proc.send_signal(sig)
            out, _ = proc.communicate(timeout=10)
        finally:
            if proc.poll() is None:
                proc.kill()
        self.assertEqual(proc.returncode, 0, out)
        self.assertIn("Scheduler stopped", out)
        with open(status) as handle:
            self.assertFalse(json.load(handle)["scheduler_running"])

    def test_clean_shutdown_on_sigint(self):
        self.run_and_stop(signal.SIGINT)

    def test_clean_shutdown_on_sigterm(self):
        self.run_and_stop(signal.SIGTERM)


# ------------------------------------------------- with Member 2's real files
@unittest.skipUnless(
    os.path.exists(os.path.join(APP_DIR, "worker.py"))
    and os.path.exists(os.path.join(APP_DIR, "client.py")),
    "app/worker.py and app/client.py not present")
class TeamFilesIntegrationTests(SchedulerTestCase):
    def start_worker(self, worker_id):
        proc = subprocess.Popen(
            [sys.executable, "-u", "worker.py", "--id", worker_id,
             "--server", "127.0.0.1", "--port", str(self.sched.port),
             "--heartbeat", "0.5"],
            cwd=APP_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        return proc

    def wait_for_workers(self, count):
        deadline = time.time() + 5
        while time.time() < deadline:
            if len(self.snapshot()["workers"]) >= count:
                return
            time.sleep(0.1)
        self.fail("workers did not register")

    def run_client(self, *extra):
        result = subprocess.run(
            [sys.executable, "client.py", "--server", "127.0.0.1",
             "--port", str(self.sched.port)] + list(extra),
            cwd=APP_DIR, capture_output=True, text=True, timeout=60)
        return result.stdout

    def test_end_to_end_reverse(self):
        self.start_worker("worker1")
        self.wait_for_workers(1)
        out = self.run_client("--type", "REVERSE", "--payload", "hello world")
        self.assertIn("RESULT: dlrow olleh", out)
        self.assertIn("--- 1/1 jobs succeeded", out)

    def test_round_robin_over_two_real_workers(self):
        self.start_worker("worker1")
        self.start_worker("worker2")
        self.wait_for_workers(2)
        out = self.run_client("--type", "SUM", "--payload", "1 2 3.5", "--count", "6")
        self.assertIn("--- 6/6 jobs succeeded", out)
        done = {w["id"]: w["done"] for w in self.snapshot()["workers"]}
        self.assertEqual(done, {"worker1": 3, "worker2": 3})

    def test_error_and_bad_payload_like_readme_tests_7_and_8(self):
        self.start_worker("worker1")
        self.wait_for_workers(1)
        self.assertIn("ERROR from scheduler: unknown job type FOO",
                      self.run_client("--type", "FOO"))
        self.assertIn("FAILED",
                      self.run_client("--type", "SUM", "--payload", "abc"))
        self.assertIn("RESULT: 6.5",       # scheduler still works afterwards
                      self.run_client("--type", "SUM", "--payload", "1 2 3.5"))

    def test_concurrent_clients_like_readme_test_5(self):
        self.start_worker("worker1")
        self.start_worker("worker2")
        self.wait_for_workers(2)
        out = self.run_client("--type", "SLEEP", "--payload", "1",
                              "--count", "3", "--parallel", "5")
        self.assertIn("--- 15/15 jobs succeeded", out)

    def test_real_worker_joining_after_jobs_are_queued(self):
        result = {}

        def run():
            result["out"] = self.run_client("--type", "REVERSE",
                                            "--payload", "abc", "--count", "3")
        thread = threading.Thread(target=run)
        thread.start()
        self.wait_until(lambda: self.snapshot()["queue_len"] == 3)
        self.start_worker("worker1")              # registers with jobs waiting
        thread.join(30)
        self.assertIn("--- 3/3 jobs succeeded", result.get("out", ""))

    def test_worker_killed_mid_run_like_readme_test_12(self):
        victim = self.start_worker("worker1")
        self.start_worker("worker2")
        self.wait_for_workers(2)
        result = {}

        def run():
            result["out"] = self.run_client("--type", "SLEEP", "--payload", "2",
                                            "--count", "6")
        thread = threading.Thread(target=run)
        thread.start()
        time.sleep(1.0)                           # jobs are running on both
        victim.kill()
        thread.join(30)
        self.assertIn("--- 6/6 jobs succeeded", result.get("out", ""))
        self.assertGreaterEqual(self.snapshot()["stats"]["requeued"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
