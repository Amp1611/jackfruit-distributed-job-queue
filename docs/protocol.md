# Application protocol (TCP, port 5000)

One message per line, UTF-8, terminated by `\n`. The scheduler (h2) listens on 5000.
Clients and workers both connect TO the scheduler; the first line tells it which is which.

| Direction | Message | Meaning |
|---|---|---|
| Client -> Scheduler | `SUBMIT <TYPE> <payload>` | Submit a job |
| Scheduler -> Client | `ACK <job_id>` | Job accepted and queued |
| Scheduler -> Client | `RESULT <job_id> <output>` | Job finished |
| Scheduler -> Client | `FAILED <job_id> <reason>` | Job failed on the worker |
| Scheduler -> Client | `ERROR <reason>` | Bad request (e.g. unknown type) |
| Client -> Scheduler | `PING` / `QUIT` | Liveness check / close |
| Worker -> Scheduler | `REGISTER <worker_id>` | Join the pool |
| Scheduler -> Worker | `REGISTERED <worker_id>` | Registration accepted |
| Worker -> Scheduler | `HEARTBEAT <worker_id> <active_jobs>` | Every 2 s; silence for 8 s = dead |
| Scheduler -> Worker | `JOB <job_id> <TYPE> <payload>` | Run this job |
| Worker -> Scheduler | `DONE <job_id> <output>` / `FAIL <job_id> <reason>` | Job outcome |

Job types: `REVERSE <text>`, `SUM <numbers>`, `SLEEP <seconds>`, `PRIME <n>` (counts primes up to n).

Scheduling: round-robin over live workers. If a worker dies, its unfinished jobs go back in the queue.
