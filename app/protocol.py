"""
protocol.py - shared helpers for the line-based job-queue protocol (Member 1).

Implements docs/protocol.md. Used by scheduler.py, worker.py and client.py:
    from protocol import PORT, send_line, log

FRAMING
    One message per line, UTF-8, ended by "\\n", TCP port 5000.
    A line is "VERB arg arg ...". Only the LAST argument may contain spaces.

MESSAGES THE SCHEDULER ACCEPTS (parsed by parse_message)
    SUBMIT <TYPE> <payload>            client  -> scheduler
    PING / QUIT                        client  -> scheduler
    REGISTER <worker_id>               worker  -> scheduler
    HEARTBEAT <worker_id> <active>     worker  -> scheduler (every 2 s)
    DONE <job_id> <output>             worker  -> scheduler
    FAIL <job_id> <reason>             worker  -> scheduler

MESSAGES THE SCHEDULER SENDS
    ACK <job_id>                       job accepted and queued
    RESULT <job_id> <output>           job finished
    FAILED <job_id> <reason>           job failed
    ERROR <reason>                     bad request
    PONG                               reply to PING (not in docs/protocol.md)
    REGISTERED <worker_id>             registration accepted
    JOB <job_id> <TYPE> <payload>      run this job

JOB TYPES: REVERSE <text>, SUM <numbers>, SLEEP <seconds>, PRIME <n>
"""

import collections
import time

# ---------------------------------------------------------------- constants
PORT = 5000
ENCODING = "utf-8"
MAX_LINE_BYTES = 64 * 1024
RECV_CHUNK_SIZE = 4096
HEARTBEAT_INTERVAL = 2.0     # workers send a heartbeat this often
HEARTBEAT_TIMEOUT = 8.0      # silence for this long = worker dead

JOB_TYPES = ("REVERSE", "SUM", "SLEEP", "PRIME")

# Verbs the scheduler receives
SUBMIT = "SUBMIT"
PING = "PING"
QUIT = "QUIT"
REGISTER = "REGISTER"
HEARTBEAT = "HEARTBEAT"
DONE = "DONE"
FAIL = "FAIL"

# Verbs the scheduler sends
ACK = "ACK"
RESULT = "RESULT"
FAILED = "FAILED"
ERROR = "ERROR"
PONG = "PONG"
REGISTERED = "REGISTERED"
JOB = "JOB"

# Job states (used inside the scheduler and in the status file)
JOB_QUEUED = "QUEUED"
JOB_RUNNING = "RUNNING"
JOB_DONE = "DONE"
JOB_FAILED = "FAILED"

Message = collections.namedtuple("Message", "verb args")


class ProtocolError(Exception):
    """A bad or unreadable message.

    fatal=True means the stream can't be trusted any more (e.g. a huge line)
    and the connection should be closed.
    """

    def __init__(self, message, fatal=False):
        super().__init__(message)
        self.fatal = fatal


# ------------------------------------------------------------------ logging
def log(who, message):
    """Print a timestamped line, e.g.  12:00:01 [worker1] registered."""
    print("%s [%s] %s" % (time.strftime("%H:%M:%S"), who, message), flush=True)


# ------------------------------------------------------------------ sending
def send_line(sock, lock, text):
    """Send one message line. `lock` may be None.

    Newlines inside text are replaced by spaces so one message can never
    become two. Raises OSError on socket problems.
    """
    text = " ".join(str(text).splitlines())
    data = (text + "\n").encode(ENCODING)
    if len(data) > MAX_LINE_BYTES:
        raise ProtocolError("message too long (%d bytes)" % len(data))
    if lock is None:
        sock.sendall(data)
    else:
        with lock:
            sock.sendall(data)


# ------------------------------------------------------------------ reading
class LineReader:
    """Reads newline-terminated lines from a socket (handles TCP framing).

    One recv() can return half a line or several lines; this class buffers
    bytes and splits them on "\\n".

    read_line() returns:
        str  - one non-empty line (without the newline)
        None - the peer closed the connection (EOF)
    and raises:
        ProtocolError - invalid UTF-8 (line already consumed, keep reading) or
                        line too long (fatal=True, close the connection)
        socket.timeout / OSError - passed through from the socket
    """

    def __init__(self, sock):
        self.sock = sock
        self.buffer = bytearray()

    def read_line(self):
        while True:
            pos = self.buffer.find(b"\n")
            if pos >= 0:
                raw = bytes(self.buffer[:pos])
                del self.buffer[:pos + 1]
                if len(raw) > MAX_LINE_BYTES:
                    raise ProtocolError("line too long", fatal=True)
                try:
                    text = raw.decode(ENCODING).strip()
                except UnicodeDecodeError:
                    raise ProtocolError("line is not valid UTF-8")
                if not text:
                    continue          # ignore blank lines
                return text

            if len(self.buffer) > MAX_LINE_BYTES:
                self.buffer.clear()
                raise ProtocolError("line too long", fatal=True)

            chunk = self.sock.recv(RECV_CHUNK_SIZE)   # may raise socket.timeout
            if not chunk:
                return None           # EOF; a partial last line is discarded
            self.buffer.extend(chunk)


# ------------------------------------------------------------------ parsing
def _to_int(token, what):
    try:
        return int(token)
    except ValueError:
        raise ProtocolError("%s must be an integer, got %r" % (what, token))


def parse_message(line):
    """Turn one received line into Message(verb, args), or raise ProtocolError.

    args by verb:
        SUBMIT    (job_type, payload)      payload may be ""
        REGISTER  (worker_id,)
        HEARTBEAT (worker_id, active_jobs)
        DONE      (job_id, output)
        FAIL      (job_id, reason)
        PING/QUIT ()
    """
    text = line.strip()
    if not text:
        raise ProtocolError("empty message")
    verb = text.split(" ", 1)[0]

    if verb == SUBMIT:
        parts = text.split(" ", 2)
        if len(parts) < 2 or not parts[1]:
            raise ProtocolError("SUBMIT needs a job type: SUBMIT <TYPE> <payload>")
        return Message(verb, (parts[1], parts[2].strip() if len(parts) > 2 else ""))

    if verb == REGISTER:
        parts = text.split()
        if len(parts) != 2:
            raise ProtocolError("usage: REGISTER <worker_id>")
        return Message(verb, (parts[1],))

    if verb == HEARTBEAT:
        parts = text.split()
        if len(parts) not in (2, 3):
            raise ProtocolError("usage: HEARTBEAT <worker_id> <active_jobs>")
        active = _to_int(parts[2], "active_jobs") if len(parts) == 3 else 0
        if active < 0:
            raise ProtocolError("active_jobs must not be negative")
        return Message(verb, (parts[1], active))

    if verb in (DONE, FAIL):
        parts = text.split(" ", 2)
        if len(parts) < 2:
            raise ProtocolError("usage: %s <job_id> <text>" % verb)
        return Message(verb, (_to_int(parts[1], "job_id"),
                              parts[2] if len(parts) > 2 else ""))

    if verb in (PING, QUIT):
        return Message(verb, ())

    raise ProtocolError("unknown command %r" % verb)
