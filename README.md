# Distributed Job Queue and Worker Management (SDN + TCP) - Deliverable 2

D2 builds on D1: the controller now reacts to link failures and congestion by itself,
the scheduler has two policies and job timeouts, and an experiment suite measures it all.

## Layout
```
app/          protocol.py scheduler.py worker.py client.py   (plain TCP sockets, stdlib only)
controller/   controller.py                                  (Ryu, OpenFlow 1.3, static/dynamic mode)
topology/     topology.py                                    (Mininet)
experiments/  run_experiments.py plot_results.py             (D2 experiments + graphs)
docs/         protocol.md
results/      summary.csv, <exp>/*.csv, logs/, plots/        (created by the experiments)
logs/         scheduler.log worker1.log worker2.log          (created at run time)
```

## What is new in D2
| Part | Change |
|---|---|
| Controller | `SDN_MODE=static` (baseline, never reacts) or `dynamic` (default). Dynamic: **link failover** (Port-Status: direct link s1-s3 down -> all traffic via s2, back when it returns) and **congestion reroute** (port stats every 2 s; if the direct link stays above 7 Mbps for 2 readings, worker2 traffic moves via s2, and returns when the load drops) |
| Scheduler | `--policy rr\|least` (round-robin / fewest active jobs), `--job-timeout N` (a job unfinished after N s goes to another worker), `[timing]` log lines per job |
| Worker | `--slow S` adds S seconds to every job (simulates a slow worker) |
| Client | `--interval` (steady load), `--csv` (one row per job), `--quiet`, `--timeout` (wait after last submission) |
| Topology | `--policy`, `--no-cli`; `build()`, `wait_ready()`, `start_app()`, `stop_app()` reused by the experiment script |
| Experiments | `run_experiments.py` runs E1-E5, `plot_results.py` draws the graphs |

## Topology
```
 h1 client   h2 scheduler                 h3 worker1   h4 worker2
      \        /                               \        /
          [s1] ------- direct link ------------ [s3]
             \                                  /
              +-----------[s2]-----------------+
```
IPs 10.0.0.1-4. Switch-to-switch links are 10 Mbps / 5 ms. Two paths between s1 and s3 (direct, or via s2).

## Requirements
* Mininet, Open vSwitch, Ryu (in `venv/`), `iperf3` (E2), `matplotlib` (plots)
* Run Mininet scripts with the **system** Python and `sudo`; Ryu runs from the venv.

## Manual run (3 terminals, from ~/sdn-project)
**Terminal 1 - controller**
```bash
source venv/bin/activate
SDN_MODE=dynamic ryu-manager controller/controller.py     # or SDN_MODE=static
```
**Terminal 2 - network + app**
```bash
sudo python3 topology/topology.py                 # round-robin
sudo python3 topology/topology.py --policy least  # least-loaded
```
Clients are then run at the `mininet>` prompt, e.g.
`h1 python3 app/client.py --type SLEEP --payload 0.2 --count 20 --interval 0.5`

If a run is interrupted: `sudo mn -c` (this also kills ryu-manager, so do it BEFORE starting the controller).

## Experiments
Every experiment is run with the controller in static and dynamic mode (E4: dynamic only, E5: two policies).

| ID | Scenario | What is measured |
|---|---|---|
| E1 | Normal load: 1, 5, 10, 20 clients | avg / p95 latency, completion % |
| E2 | iperf3 fills the direct link s1-s3 | latency, slow jobs, congestion reroute |
| E3 | Link s1-s3 goes down for 10 s | completion %, max latency, recovery time |
| E4 | worker1 is killed during the run | completion %, requeued jobs |
| E5 | rr vs least with worker2 slowed by 3 s | avg / p95 / max latency (skipped if scheduler/worker lack `--policy` / `--slow`) |

```bash
sudo python3 experiments/run_experiments.py --quick           # fast test, ~10 min
sudo python3 experiments/run_experiments.py                   # everything, 3 repeats
sudo python3 experiments/run_experiments.py --exp E1 E3       # only some
python3 experiments/plot_results.py                           # graphs + table
```
Outputs:
* `results/summary.csv` - one row per run
* `results/<exp>/<run>.csv` - one row per job (`client,job_id,status,latency_s,submit_t,done_t`)
* `results/logs/<run>/` - controller, scheduler, worker and client logs
* `results/plots/*.png` and `results/summary_table.md` - from `plot_results.py`

Useful controller log markers: `LINK DOWN`, `LINK UP`, `CONGESTION`, `ROUTE CHANGE`, `[link load]`, `[job traffic]`.

## Quick manual checks
| Test | Command | Expected |
|---|---|---|
| Connectivity | `pingall` | 0% dropped |
| Workers registered | `h2 cat /tmp/sdn_status.json` | worker1 and worker2 |
| Single job | `h1 python3 app/client.py --type REVERSE --payload "hello world"` | `dlrow olleh` |
| Link failover | `link s1 s3 down` during a client run, then `link s1 s3 up` | controller: `LINK DOWN` + `ROUTE CHANGE`, jobs keep finishing (dynamic) |
| Worker crash | `h3 pkill -f worker.py` during a run | scheduler: worker removed, jobs requeued |
| Flow rules | `sudo ovs-ofctl -O OpenFlow13 dump-flows s1` | priority=100 rules for tcp port 5000 |

## Troubleshooting
| Symptom | Fix |
|---|---|
| pingall loses packets | Start Ryu first, then the topology |
| `Address already in use` / odd state | `sudo mn -c`, then restart |
| `ModuleNotFoundError: mininet` | Use system Python with sudo, not the venv |
| `ryu-manager not found` in experiments | `--ryu PATH` |
| E5 skipped | Make sure `app/scheduler.py` and `app/worker.py` are the D2 versions |
| No plots | `pip install matplotlib`, run `plot_results.py` after the experiments |
