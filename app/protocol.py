"""Shared helpers and message format for the job-queue system.

All messages are single text lines terminated by '\n' over TCP.

Client  -> Scheduler : SUBMIT <TYPE> <payload>        | PING | QUIT
Scheduler -> Client  : ACK <job_id> | RESULT <job_id> <output>
                       | FAILED <job_id> <reason> | ERROR <reason> | PONG
Worker  -> Scheduler : REGISTER <worker_id> | HEARTBEAT <worker_id> <active_jobs>
                       | DONE <job_id> <output> | FAIL <job_id> <reason>
Scheduler -> Worker  : REGISTERED <worker_id> | JOB <job_id> <TYPE> <payload>
"""
import time

PORT = 5000                      # one well-known port for ALL job traffic
ENC = "utf-8"
VALID_TYPES = {"REVERSE", "SUM", "SLEEP", "PRIME"}


def send_line(sock, lock, text):
    """Send one newline-terminated message; lock makes it thread-safe."""
    data = (text.replace("\n", " ").replace("\r", " ") + "\n").encode(ENC)
    with lock:
        sock.sendall(data)


def log(tag, msg):
    print("%s [%s] %s" % (time.strftime("%H:%M:%S"), tag, msg), flush=True)
