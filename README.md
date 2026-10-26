# Distributed Job Queue and Worker Management (SDN + TCP) - Deliverable 1

## Layout
```
app/         protocol.py scheduler.py worker.py client.py   (plain TCP sockets, stdlib only)
controller/  controller.py                                  (Ryu, OpenFlow 1.3)
topology/    topology.py                                    (Mininet)
docs/        protocol.md
logs/        scheduler.log worker1.log worker2.log          (created at run time)
```

## Topology
```
 h1 client   h2 scheduler                 h3 worker1   h4 worker2
      \        /                               \        /
          [s1] ------- direct link ------------ [s3]
             \                                  /
              +-----------[s2]-----------------+
```
IPs 10.0.0.1-4. Two paths between s1 and s3 (direct, or via s2).
Job traffic that crosses the backbone: scheduler (h2) <-> workers (h3, h4).

## Run order (3 terminals), always from ~/sdn-project
**Terminal 1 - controller**
```bash
cd ~/sdn-project
source venv/bin/activate
ryu-manager controller/controller.py
```
**Terminal 2 - network + app** (starts scheduler on h2 and workers on h3/h4 automatically)
```bash
cd ~/sdn-project
sudo python3 topology/topology.py
```
Wait for "Ready". You get a `mininet>` prompt. Clients are run from there.

**Terminal 3 - checks** (flow tables, logs, captures)

If a run is interrupted: `sudo mn -c` before starting again.

## Test cases (type these at the `mininet>` prompt)
| # | Test | Command | Expected |
|---|---|---|---|
| 1 | Connectivity through SDN | `pingall` | 0% dropped |
| 2 | Workers registered | `h2 cat /tmp/sdn_status.json` | worker1 and worker2 listed |
| 3 | Single job | `h1 python3 app/client.py --type REVERSE --payload "hello world"` | `dlrow olleh` |
| 4 | Several jobs, round-robin | `h1 python3 app/client.py --type SUM --payload "1 2 3.5" --count 6` | 6 results of 6.5; `logs/worker1.log` and `logs/worker2.log` each ran 3 |
| 5 | Concurrent clients | `h1 python3 app/client.py --type SLEEP --payload 1 --count 3 --parallel 5` | 15/15 succeed |
| 6 | Clients on different hosts | `h1 python3 app/client.py --count 3 &` then `h2 python3 app/client.py --count 3` | all succeed |
| 7 | Error handling | `h1 python3 app/client.py --type FOO` | ERROR unknown job type |
| 8 | Bad payload | `h1 python3 app/client.py --type SUM --payload abc` | FAILED, scheduler keeps running |
| 9 | **SDN sees app traffic** | after test 4, look at Terminal 1 | `[job traffic] ...` lines with packet/byte counts on s1 and s3 |
| 10 | Flow rules | Terminal 3: `sudo ovs-ofctl -O OpenFlow13 dump-flows s1` | priority=100 rules with tcp_dst=5000 / tcp_src=5000 |
| 11 | **Dynamic path change** | Terminal 3: `echo via_s2 > /tmp/sdn_path`, then run test 4 again | Terminal 1 prints `PATH CHANGE`; job traffic now appears on s2; back with `echo direct > /tmp/sdn_path` |
| 12 | Worker failure (bonus) | `h3 pkill -f worker.py` while running test 5 | scheduler log: worker removed, jobs requeued, all jobs still finish |

## How the app and SDN are integrated (for the 3 integration marks)
1. The controller gives the job-queue port (TCP 5000) its own high-priority flow rules, so application traffic is identified and counted in the network, per switch.
2. The scheduler publishes worker/queue state to `/tmp/sdn_status.json`; the controller reads it and prints it next to the traffic counters.
3. The controller changes the switch-to-switch path at run time (`/tmp/sdn_path`). D2 will drive this automatically from worker failure and load.

Note: the files in /tmp work because Mininet hosts and the controller share the VM's filesystem. For D2 this can become a proper control channel.

## Wireshark / tcpdump
* Job traffic on the direct link: interface `s1-eth4`, filter `tcp.port == 5000`
* Job traffic on the s2 path: interface `s2-eth1`
* OpenFlow messages: interface `lo`, filter `openflow_v4`
* Save evidence: `sudo tcpdump -i s1-eth4 -w captures/direct.pcap`

## Troubleshooting
| Symptom | Fix |
|---|---|
| pingall loses packets | Controller not running or started after the topology. Start Ryu first, rerun the topology |
| `Address already in use` / strange state | `sudo mn -c`, then restart |
| `ModuleNotFoundError: mininet` | Run with system Python: `sudo python3 topology/topology.py` (not the venv) |
| Workers say "scheduler unavailable" | Normal for 1-2 s at start; they retry automatically |
| No `[job traffic]` lines | Stats print every 5 s; run a job and wait |
