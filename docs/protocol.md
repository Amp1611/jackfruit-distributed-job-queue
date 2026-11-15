# Application protocol (TCP, port 5000)

Unchanged from D1. One message per line, UTF-8, terminated by `\n`. The scheduler (h2) listens on 5000.
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

Job types: `REVERSE <text>`, `SUM <numbers>`, `SLEEP <seconds>`, `PRIME <n>`.

## D2 changes (behaviour only, no new messages)
* Scheduling policy (`--policy`): `rr` = round-robin over live workers; `least` = worker with the fewest active jobs (ties: next in turn).
* Job timeout (`--job-timeout`, default 10 s): a job not finished in time is requeued to a different worker if one is available. A late duplicate result is ignored.
* A dead worker (heartbeat silent 8 s, or connection closed) has its unfinished jobs requeued.
* Scheduler writes `[timing]` log lines (`submitted`, `dispatched`, `finished`, `timeout`) per job.
* Client CSV status values: `OK`, `FAILED`, `ERROR`, `TIMEOUT`, `CONN_ERROR`.
