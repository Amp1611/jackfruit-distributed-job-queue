"""
Tests for app/protocol.py and app/scheduler.py (Member 1).

Run from the project root:
    python3 -m unittest tests.test_scheduler -v

Each test starts its own Scheduler on a free port (port 0) and talks to it with
plain sockets, playing the roles of clients, workers and the controller.
"""

import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import protocol  # noqa: E402
from protocol import MessageReader, ProtocolError, make_message, send_message  # noqa: E402
from scheduler import Scheduler  # noqa: E402


class Peer:
    """A tiny test client/worker: send messages, wait for a given reply."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.reader = MessageReader(self.sock)

    def send(self, msg_type, **fields):
        send_message(self.sock, make_message(msg_type, **fields))

    def send_raw(self, data):
        self.sock.sendall(data)

    def recv(self, timeout=5):
        self.sock.settimeout(timeout)
        return self.reader.read_message()

    def recv_type(self, msg_type, timeout=5):
        """Read until a message of msg_type arrives (skips HEARTBEAT_ACK etc.)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = self.recv(max(0.1, deadline - time.time()))
            if msg is None:
                raise AssertionError("connection closed waiting for " + msg_type)
            if msg["type"] == msg_type:
                return msg
        raise AssertionError("timed out waiting for " + msg_type)

    def close(self):
        self.sock.close()


class SchedulerTestCase(unittest.TestCase):
    heartbeat_timeout = 6.0

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

    def register_worker(self, worker_id, capacity=1):
        worker = self.connect()
        worker.send(protocol.REGISTER_WORKER, worker_id=worker_id,
                    capacity=capacity)
        ack = worker.recv_type(protocol.REGISTER_ACK)
        self.assertEqual(ack["worker_id"], worker_id)
        return worker

    def submit(self, client, text="hello"):
        client.send(protocol.SUBMIT_JOB, job_type="reverse",
                    payload={"text": text})
        return client.recv_type(protocol.JOB_ACK)["job_id"]

    def status_of(self, peer):
        peer.send(protocol.GET_STATUS)
        return peer.recv_type(protocol.STATUS)


# ------------------------------------------------------------------ protocol
class ProtocolTests(unittest.TestCase):
    def test_encode_decode_roundtrip(self):
        msg = make_message(protocol.SUBMIT_JOB, job_type="echo",
                           payload={"text": "héllo\nworld"})
        data = protocol.encode_message(msg)
        self.assertTrue(data.endswith(b"\n"))
        self.assertEqual(data.count(b"\n"), 1)   # embedded newline is escaped
        self.assertEqual(protocol.decode_message(data), msg)

    def test_invalid_messages_rejected(self):
        bad_inputs = [b"not json\n", b"[1,2]\n", b'{"no":"type"}\n',
                      b'{"type":"NOPE"}\n', b'{"type":"SUBMIT_JOB"}\n',
                      b'{"type":"SUBMIT_JOB","job_type":"rm","payload":{}}\n',
                      b'{"type":"GET_JOB_STATUS","job_id":true}\n',
                      b'{"type":"SUBMIT_JOB","job_type":"factorial","payload":{"n":-1}}\n',
                      b"\xff\xfe\n"]
        for raw in bad_inputs:
            with self.assertRaises(ProtocolError, msg=repr(raw)):
                protocol.decode_message(raw)

    def test_reader_handles_split_and_merged_messages(self):
        a, b = socket.socketpair()
        reader = MessageReader(b)
        one = protocol.encode_message(make_message(protocol.GET_STATUS))
        two = protocol.encode_message(
            make_message(protocol.GET_JOB_STATUS, job_id=7))
        a.sendall(one[:5])                 # half a message
        a.sendall(one[5:] + two)           # rest of #1 plus all of #2
        a.close()
        self.assertEqual(reader.read_message()["type"], protocol.GET_STATUS)
        self.assertEqual(reader.read_message()["job_id"], 7)
        self.assertIsNone(reader.read_message())   # EOF
        b.close()

    def test_reader_survives_bad_line(self):
        a, b = socket.socketpair()
        reader = MessageReader(b)
        a.sendall(b"garbage\n" + protocol.encode_message(
            make_message(protocol.GET_STATUS)))
        with self.assertRaises(ProtocolError):
            reader.read_message()
        self.assertEqual(reader.read_message()["type"], protocol.GET_STATUS)
        a.close()
        b.close()


# ----------------------------------------------------------------- scheduler
class JobSubmissionTests(SchedulerTestCase):
    def test_submit_ack_and_status(self):
        client = self.connect()
        client.send(protocol.SUBMIT_JOB, job_type="factorial",
                    payload={"n": 10}, client_id="c1")
        ack = client.recv_type(protocol.JOB_ACK)
        self.assertEqual(ack["status"], "QUEUED")
        self.assertIsInstance(ack["job_id"], int)
        client.send(protocol.GET_JOB_STATUS, job_id=ack["job_id"])
        status = client.recv_type(protocol.JOB_STATUS)
        self.assertEqual(status["status"], "QUEUED")   # no worker yet

    def test_unknown_job_id(self):
        client = self.connect()
        client.send(protocol.GET_JOB_STATUS, job_id=999)
        self.assertEqual(client.recv_type(protocol.ERROR)["code"], "unknown_job")

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


class WorkerAndDispatchTests(SchedulerTestCase):
    def test_worker_registration_visible_in_status(self):
        self.register_worker("w1", capacity=2)
        status = self.status_of(self.connect())
        self.assertEqual(status["alive_workers"], 1)
        worker = status["workers"][0]
        self.assertEqual(worker["worker_id"], "w1")
        self.assertEqual(worker["status"], "ALIVE")
        self.assertEqual(worker["ip"], "127.0.0.1")
        self.assertEqual(worker["capacity"], 2)
        self.assertTrue(worker["available"])

    def test_dispatch_result_and_push_to_client(self):
        worker = self.register_worker("w1")
        client = self.connect()
        job_id = self.submit(client, "abc")
        assign = worker.recv_type(protocol.ASSIGN_JOB)
        self.assertEqual(assign["job_id"], job_id)
        self.assertEqual(assign["payload"], {"text": "abc"})
        self.assertEqual(assign["attempt"], 1)
        worker.send(protocol.WORKER_RESULT, worker_id="w1", job_id=job_id,
                    status="DONE", result="cba")
        result = client.recv_type(protocol.JOB_RESULT)
        self.assertEqual((result["job_id"], result["status"], result["result"],
                          result["worker_id"]), (job_id, "DONE", "cba", "w1"))

    def test_jobs_wait_until_worker_available(self):
        client = self.connect()
        job_id = self.submit(client)
        time.sleep(0.3)
        worker = self.register_worker("late")
        self.assertEqual(worker.recv_type(protocol.ASSIGN_JOB)["job_id"], job_id)

    def test_capacity_respected(self):
        worker = self.register_worker("w1", capacity=1)
        client = self.connect()
        first = self.submit(client)
        second = self.submit(client)
        self.assertEqual(worker.recv_type(protocol.ASSIGN_JOB)["job_id"], first)
        with self.assertRaises(socket.timeout):    # second job must wait
            worker.recv(timeout=0.7)
        worker.send(protocol.WORKER_RESULT, worker_id="w1", job_id=first,
                    status="DONE", result="ok")
        self.assertEqual(worker.recv_type(protocol.ASSIGN_JOB)["job_id"], second)

    def test_round_robin_distribution(self):
        workers = {name: self.register_worker(name, capacity=10)
                   for name in ("w1", "w2", "w3")}
        client = self.connect()
        owner = {}
        for _ in range(6):
            job_id = self.submit(client)
            # find who got it (poll each worker briefly)
            for name, peer in workers.items():
                try:
                    msg = peer.recv(timeout=0.5)
                except socket.timeout:
                    continue
                self.assertEqual(msg["type"], protocol.ASSIGN_JOB)
                self.assertEqual(msg["job_id"], job_id)
                owner[job_id] = name
                break
        order = [owner[j] for j in sorted(owner)]
        self.assertEqual(order, ["w1", "w2", "w3", "w1", "w2", "w3"])

    def test_registration_errors(self):
        peer = self.connect()
        peer.send(protocol.HEARTBEAT, worker_id="ghost")
        self.assertEqual(peer.recv_type(protocol.ERROR)["code"], "not_registered")
        worker = self.register_worker("w1")
        worker.send(protocol.REGISTER_WORKER, worker_id="w1")
        self.assertEqual(worker.recv_type(protocol.ERROR)["code"],
                         "already_registered")


class HeartbeatTests(SchedulerTestCase):
    heartbeat_timeout = 1.0

    def test_silent_worker_expires_and_job_moves(self):
        silent = self.register_worker("silent")
        client = self.connect()
        job_id = self.submit(client)
        self.assertEqual(silent.recv_type(protocol.ASSIGN_JOB)["job_id"], job_id)
        backup = self.register_worker("backup")
        # keep backup alive while silent goes quiet
        deadline = time.time() + 5
        got = None
        while time.time() < deadline and got is None:
            backup.send(protocol.HEARTBEAT, worker_id="backup")
            try:
                msg = backup.recv(timeout=0.3)
            except socket.timeout:
                continue
            if msg["type"] == protocol.ASSIGN_JOB:
                got = msg
        self.assertIsNotNone(got, "job was not re-assigned after expiry")
        self.assertEqual(got["job_id"], job_id)
        self.assertEqual(got["attempt"], 2)
        status = self.status_of(self.connect())
        states = {w["worker_id"]: w["status"] for w in status["workers"]}
        self.assertEqual(states["silent"], "DEAD")
        self.assertEqual(states["backup"], "ALIVE")

    def test_heartbeats_keep_worker_alive_and_dead_worker_revives(self):
        worker = self.register_worker("w1")
        for _ in range(6):                        # ~1.8 s > timeout of 1 s
            worker.send(protocol.HEARTBEAT, worker_id="w1")
            self.assertEqual(worker.recv_type(protocol.HEARTBEAT_ACK)["worker_id"], "w1")
            time.sleep(0.3)
        observer = self.connect()
        self.assertEqual(self.status_of(observer)["workers"][0]["status"], "ALIVE")
        time.sleep(1.8)                           # go silent -> DEAD
        self.assertEqual(self.status_of(observer)["workers"][0]["status"], "DEAD")
        worker.send(protocol.HEARTBEAT, worker_id="w1")
        worker.recv_type(protocol.HEARTBEAT_ACK)
        self.assertEqual(self.status_of(observer)["workers"][0]["status"], "ALIVE")


class FailureTests(SchedulerTestCase):
    def test_worker_disconnect_requeues_job(self):
        doomed = self.register_worker("doomed")
        client = self.connect()
        job_id = self.submit(client)
        doomed.recv_type(protocol.ASSIGN_JOB)
        survivor = self.register_worker("survivor")
        doomed.close()                            # abrupt disconnect
        msg = survivor.recv_type(protocol.ASSIGN_JOB, timeout=5)
        self.assertEqual(msg["job_id"], job_id)
        self.assertEqual(msg["attempt"], 2)
        survivor.send(protocol.WORKER_RESULT, worker_id="survivor",
                      job_id=job_id, status="DONE", result="ok")
        self.assertEqual(client.recv_type(protocol.JOB_RESULT)["status"], "DONE")

    def test_job_fails_after_max_attempts(self):
        self.sched.max_attempts = 2
        client = self.connect()
        job_id = self.submit(client)
        for i in range(2):
            worker = self.register_worker("flaky%d" % i)
            worker.recv_type(protocol.ASSIGN_JOB)
            worker.close()
            time.sleep(0.3)
        result = client.recv_type(protocol.JOB_RESULT)
        self.assertEqual((result["job_id"], result["status"]), (job_id, "FAILED"))
        self.assertIn("gave up", result["error"])

    def test_malformed_messages_do_not_break_connection(self):
        client = self.connect()
        client.send_raw(b"this is not json\n")
        self.assertEqual(client.recv_type(protocol.ERROR)["code"], "bad_message")
        client.send_raw(b'{"type":"SUBMIT_JOB","job_type":"rm -rf","payload":{}}\n')
        self.assertEqual(client.recv_type(protocol.ERROR)["code"], "bad_message")
        client.send_raw(b'{"type":"JOB_ACK","job_id":1,"status":"QUEUED"}\n')
        self.assertEqual(client.recv_type(protocol.ERROR)["code"], "unexpected_type")
        self.assertIsInstance(self.submit(client), int)   # still usable

    def test_too_many_bad_messages_closes_connection(self):
        client = self.connect()
        client.send_raw(b"junk\n" * 12)
        deadline = time.time() + 5
        closed = False
        while time.time() < deadline:
            try:
                if client.recv(timeout=1) is None:
                    closed = True
                    break
            except (socket.timeout, ConnectionError):
                continue
        self.assertTrue(closed)

    def test_worker_cannot_submit_jobs(self):
        worker = self.register_worker("w1")
        worker.send(protocol.SUBMIT_JOB, job_type="echo", payload={"text": "x"})
        self.assertEqual(worker.recv_type(protocol.ERROR)["code"], "wrong_role")


class StatusInterfaceTests(SchedulerTestCase):
    def test_status_file_content_and_updates(self):
        worker = self.register_worker("w1", capacity=2)
        client = self.connect()
        job_id = self.submit(client)
        worker.recv_type(protocol.ASSIGN_JOB)
        time.sleep(0.6)   # allow the 0.2 s writer to run
        with open(self.status_path) as handle:
            data = json.load(handle)
        for key in ("timestamp", "scheduler_running", "port", "queue_length",
                    "alive_workers", "jobs", "workers", "heartbeat_timeout"):
            self.assertIn(key, data)
        self.assertTrue(data["scheduler_running"])
        w = data["workers"][0]
        self.assertEqual((w["worker_id"], w["ip"], w["status"], w["active_jobs"],
                          w["load"]), ("w1", "127.0.0.1", "ALIVE", 1, 0.5))
        self.assertEqual(data["jobs"]["running"], 1)
        self.assertLess(time.time() - data["timestamp"], 2)
        worker.send(protocol.WORKER_RESULT, worker_id="w1", job_id=job_id,
                    status="DONE", result="r")
        client.recv_type(protocol.JOB_RESULT)
        time.sleep(0.6)
        with open(self.status_path) as handle:
            data = json.load(handle)
        self.assertEqual(data["jobs"]["done"], 1)
        self.assertEqual(data["workers"][0]["completed_jobs"], 1)
        self.assertEqual(data["workers"][0]["load"], 0.0)

    def test_final_status_marks_scheduler_stopped(self):
        self.sched.stop()
        with open(self.status_path) as handle:
            self.assertFalse(json.load(handle)["scheduler_running"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
