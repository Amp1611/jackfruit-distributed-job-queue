"""
protocol.py - Shared message protocol for the Distributed Job Queue project.

Used by: scheduler.py (Member 1), worker.py / client.py (Member 2).
Member 3's Ryu controller does NOT use TCP; it reads the status file
(see scheduler.py) whose layout is documented there.

FRAMING
    Every message is ONE JSON object on ONE line, terminated by "\\n",
    encoded as UTF-8. Max line size: MAX_MESSAGE_BYTES (1 MiB).
    json.dumps(ensure_ascii=True) never emits a raw newline, so "\\n"
    is a safe delimiter.

ENVELOPE
    {"type": "<MESSAGE_TYPE>", ...fields...}

MESSAGES (direction: fields) - "?" marks an optional field
    CLIENT -> SCHEDULER
      SUBMIT_JOB       job_type:str, payload:dict, client_id?:str
      GET_JOB_STATUS   job_id:int
    ANYONE -> SCHEDULER
      GET_STATUS       (no fields)   - returns a STATUS snapshot
    SCHEDULER -> CLIENT
      JOB_ACK          job_id:int, status:str ("QUEUED")
      JOB_STATUS       job_id:int, status:str, result, error, worker_id, attempts
      JOB_RESULT       job_id:int, status:str ("DONE"|"FAILED"), result, error,
                       worker_id, attempts       (pushed when the job finishes)
      STATUS           full snapshot (same content as the status file)
    WORKER -> SCHEDULER
      REGISTER_WORKER  worker_id:str, capacity?:int (default 1)
      HEARTBEAT        worker_id:str
      WORKER_RESULT    worker_id:str, job_id:int, status:str ("DONE"|"FAILED"),
                       result?, error?
    SCHEDULER -> WORKER
      REGISTER_ACK     worker_id:str, heartbeat_interval:number,
                       heartbeat_timeout:number
      HEARTBEAT_ACK    worker_id:str
      ASSIGN_JOB       job_id:int, job_type:str, payload:dict, attempt:int
    SCHEDULER -> ANYONE
      ERROR            code:str, message:str

JOB TYPES (payload)
    factorial  {"n": int 0..100000}     (return a SMALL result, e.g. digit
                                        count - not the full number)
    reverse    {"text": str}
    echo       {"text": str}
    sleep      {"seconds": number 0..60}
"""

import json
import threading

# ---------------------------------------------------------------- constants
SCHEDULER_PORT = 5000
ENCODING = "utf-8"
MAX_MESSAGE_BYTES = 1024 * 1024
RECV_CHUNK_SIZE = 4096

# Message types
SUBMIT_JOB = "SUBMIT_JOB"
GET_JOB_STATUS = "GET_JOB_STATUS"
GET_STATUS = "GET_STATUS"
JOB_ACK = "JOB_ACK"
JOB_STATUS = "JOB_STATUS"
JOB_RESULT = "JOB_RESULT"
STATUS = "STATUS"
REGISTER_WORKER = "REGISTER_WORKER"
REGISTER_ACK = "REGISTER_ACK"
HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"
ASSIGN_JOB = "ASSIGN_JOB"
WORKER_RESULT = "WORKER_RESULT"
ERROR = "ERROR"

# Job statuses
JOB_QUEUED = "QUEUED"
JOB_RUNNING = "RUNNING"
JOB_DONE = "DONE"
JOB_FAILED = "FAILED"

# Worker statuses
WORKER_ALIVE = "ALIVE"
WORKER_DEAD = "DEAD"

JOB_TYPES = ("factorial", "reverse", "echo", "sleep")
MAX_FACTORIAL_N = 100000
MAX_SLEEP_SECONDS = 60
MAX_WORKER_CAPACITY = 64

# Required and optional fields for each message type: {field: python_type}
REQUIRED_FIELDS = {
    SUBMIT_JOB: {"job_type": str, "payload": dict},
    GET_JOB_STATUS: {"job_id": int},
    GET_STATUS: {},
    JOB_ACK: {"job_id": int, "status": str},
    JOB_STATUS: {"job_id": int, "status": str},
    JOB_RESULT: {"job_id": int, "status": str},
    STATUS: {},
    REGISTER_WORKER: {"worker_id": str},
    REGISTER_ACK: {"worker_id": str},
    HEARTBEAT: {"worker_id": str},
    HEARTBEAT_ACK: {"worker_id": str},
    ASSIGN_JOB: {"job_id": int, "job_type": str, "payload": dict, "attempt": int},
    WORKER_RESULT: {"worker_id": str, "job_id": int, "status": str},
    ERROR: {"code": str, "message": str},
}
OPTIONAL_FIELDS = {
    SUBMIT_JOB: {"client_id": str},
    REGISTER_WORKER: {"capacity": int},
}


class ProtocolError(Exception):
    """Raised for malformed / invalid messages.

    fatal=True means the byte stream can no longer be trusted (for example a
    line that is far too long) and the connection should be closed.
    """

    def __init__(self, message, fatal=False):
        super().__init__(message)
        self.fatal = fatal


# ---------------------------------------------------------------- validation
def _is_type(value, expected):
    """isinstance() that does not accept True/False as an int."""
    if expected is int:
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, expected)


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_job(job_type, payload):
    """Check that job_type is known and the payload has the right shape."""
    if job_type not in JOB_TYPES:
        raise ProtocolError("unknown job_type %r (allowed: %s)"
                            % (job_type, ", ".join(JOB_TYPES)))
    if job_type == "factorial":
        n = payload.get("n")
        if not _is_type(n, int) or not 0 <= n <= MAX_FACTORIAL_N:
            raise ProtocolError("factorial needs integer payload.n in 0..%d"
                                % MAX_FACTORIAL_N)
    elif job_type in ("reverse", "echo"):
        if not isinstance(payload.get("text"), str):
            raise ProtocolError("%s needs string payload.text" % job_type)
    elif job_type == "sleep":
        seconds = payload.get("seconds")
        if not _is_number(seconds) or not 0 <= seconds <= MAX_SLEEP_SECONDS:
            raise ProtocolError("sleep needs numeric payload.seconds in 0..%d"
                                % MAX_SLEEP_SECONDS)


def validate_message(msg):
    """Raise ProtocolError unless msg is a well-formed protocol message."""
    if not isinstance(msg, dict):
        raise ProtocolError("message must be a JSON object")
    msg_type = msg.get("type")
    if not isinstance(msg_type, str):
        raise ProtocolError("message has no string 'type' field")
    if msg_type not in REQUIRED_FIELDS:
        raise ProtocolError("unknown message type %r" % msg_type)

    for field, expected in REQUIRED_FIELDS[msg_type].items():
        if field not in msg:
            raise ProtocolError("%s is missing required field '%s'"
                                % (msg_type, field))
        if not _is_type(msg[field], expected):
            raise ProtocolError("%s field '%s' must be %s"
                                % (msg_type, field, expected.__name__))
    for field, expected in OPTIONAL_FIELDS.get(msg_type, {}).items():
        if field in msg and not _is_type(msg[field], expected):
            raise ProtocolError("%s field '%s' must be %s"
                                % (msg_type, field, expected.__name__))

    # Extra value checks
    if msg_type in (SUBMIT_JOB, ASSIGN_JOB):
        validate_job(msg["job_type"], msg["payload"])
    if msg_type == REGISTER_WORKER:
        if not msg["worker_id"].strip():
            raise ProtocolError("worker_id must not be empty")
        capacity = msg.get("capacity", 1)
        if not 1 <= capacity <= MAX_WORKER_CAPACITY:
            raise ProtocolError("capacity must be 1..%d" % MAX_WORKER_CAPACITY)
    if msg_type == WORKER_RESULT and msg["status"] not in (JOB_DONE, JOB_FAILED):
        raise ProtocolError("WORKER_RESULT status must be DONE or FAILED")


# ------------------------------------------------------- encode / decode
def make_message(msg_type, **fields):
    """Build a message dict: make_message(HEARTBEAT, worker_id="w1")."""
    msg = {"type": msg_type}
    msg.update(fields)
    return msg


def encode_message(msg):
    """dict -> bytes ending in newline. Validates first so bugs show early."""
    validate_message(msg)
    line = json.dumps(msg, separators=(",", ":"), ensure_ascii=True)
    data = line.encode(ENCODING) + b"\n"
    if len(data) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message too large (%d bytes)" % len(data))
    return data


def decode_message(raw):
    """One line (bytes or str, with or without newline) -> validated dict."""
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode(ENCODING)
        except UnicodeDecodeError:
            raise ProtocolError("message is not valid UTF-8")
    try:
        msg = json.loads(raw)
    except ValueError:
        raise ProtocolError("message is not valid JSON")
    validate_message(msg)
    return msg


# --------------------------------------------------------- socket helpers
def send_message(sock, msg, lock=None):
    """Encode and send one message. Pass a lock if several threads share sock.

    Raises OSError on socket problems, ProtocolError on a bad message.
    """
    data = encode_message(msg)
    if lock is None:
        sock.sendall(data)
    else:
        with lock:
            sock.sendall(data)


class MessageReader:
    """Reads newline-delimited messages from a socket (handles TCP framing).

    TCP is a byte stream: one recv() may return half a message or several
    messages. This class buffers bytes and splits them on "\\n".

    read_message() returns:
        dict  - one valid message
        None  - the peer closed the connection (EOF)
    and raises:
        ProtocolError - bad line (already consumed, you may keep reading;
                        check .fatal to decide whether to give up)
        socket.timeout / OSError - passed through from the socket
    """

    def __init__(self, sock):
        self.sock = sock
        self.buffer = bytearray()

    def read_message(self):
        while True:
            newline_pos = self.buffer.find(b"\n")
            if newline_pos >= 0:
                line = bytes(self.buffer[:newline_pos])
                del self.buffer[:newline_pos + 1]
                if not line.strip():
                    continue  # ignore blank lines
                return decode_message(line)

            if len(self.buffer) > MAX_MESSAGE_BYTES:
                self.buffer.clear()
                raise ProtocolError("line exceeds %d bytes without newline"
                                    % MAX_MESSAGE_BYTES, fatal=True)

            chunk = self.sock.recv(RECV_CHUNK_SIZE)  # may raise socket.timeout
            if not chunk:
                return None  # EOF; any partial line is discarded
            self.buffer.extend(chunk)
