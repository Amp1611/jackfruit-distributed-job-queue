#!/usr/bin/env python3
"""Mininet topology for the Distributed Job Queue project.

        h1 (client)   h2 (scheduler)            h3 (worker1)   h4 (worker2)
              \\         /                              \\         /
                 [ s1 ]-------- direct link ---------- [ s3 ]
                    \\                                   /
                     +-------------[ s2 ]--------------+

Hosts: h1 10.0.0.1, h2 10.0.0.2, h3 10.0.0.3, h4 10.0.0.4 (fixed MACs 00:..:01-04)
Two paths exist between s1 and s3:  direct  (s1-s3)   and   via s2.

Start the Ryu controller FIRST, then:   sudo python3 topology/topology.py
Run it from the project root (~/sdn-project).

Options:
  --policy rr|least   scheduler policy (needs the D2 scheduler)
  --no-cli            do not open the mininet> prompt; run until Ctrl+C

The functions build(), wait_ready(), start_app() and stop_app() are also used by
experiments/run_experiments.py.
"""
import argparse
import os
import sys
import time

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel, info
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOGS = os.path.join(ROOT, "logs")


def build(bw=10, delay="5ms", controller_ip="127.0.0.1", controller_port=6653):
    """Create the network. bw/delay apply to the three switch-to-switch links
    (bw=None means no shaping). The controller assumes the default 10 Mbps."""
    net = Mininet(controller=None, switch=OVSKernelSwitch, link=TCLink, autoSetMacs=False)
    net.addController("c0", controller=RemoteController, ip=controller_ip, port=controller_port)

    sw = {}
    for n in (1, 2, 3):
        sw[n] = net.addSwitch("s%d" % n, dpid="%016x" % n,
                              protocols="OpenFlow13", failMode="secure")

    h1 = net.addHost("h1", ip="10.0.0.1/24", mac="00:00:00:00:00:01")
    h2 = net.addHost("h2", ip="10.0.0.2/24", mac="00:00:00:00:00:02")
    h3 = net.addHost("h3", ip="10.0.0.3/24", mac="00:00:00:00:00:03")
    h4 = net.addHost("h4", ip="10.0.0.4/24", mac="00:00:00:00:00:04")

    link_opts = {} if bw is None else dict(bw=bw, delay=delay)

    # Port numbers are fixed on purpose: controller.py relies on them.
    net.addLink(h1, sw[1], port2=1)
    net.addLink(h2, sw[1], port2=2)
    net.addLink(h3, sw[3], port2=1)
    net.addLink(h4, sw[3], port2=2)
    net.addLink(sw[1], sw[2], port1=3, port2=1, **link_opts)   # s1 <-> s2
    net.addLink(sw[2], sw[3], port1=2, port2=3, **link_opts)   # s2 <-> s3
    net.addLink(sw[1], sw[3], port1=4, port2=4, **link_opts)   # s1 <-> s3 (direct)
    return net


def wait_ready(net, timeout=20, tries=5):
    """Wait until the switches reach the controller and every host can ping every
    other host. Returns True when the network is usable."""
    try:
        net.waitConnected(timeout=timeout)
    except AttributeError:
        time.sleep(4)
    time.sleep(1)                                  # let the controller install its flows
    for _ in range(tries):
        if net.pingAll(timeout=2) == 0:
            return True
        time.sleep(1)
    return False


def start_app(net, logdir=LOGS, policy=None, slow=None):
    """Start the scheduler on h2 and the workers on h3/h4.
    policy: None or "rr"/"least" (passed as --policy, D2 scheduler only)
    slow:   None or {"worker1": 3.0, "worker2": 3.0} (passed as --slow, D2 worker only)"""
    os.makedirs(logdir, exist_ok=True)
    h2, h3, h4 = net.get("h2", "h3", "h4")
    app = os.path.join(ROOT, "app")
    sched = "python3 -u scheduler.py"
    if policy:
        sched += " --policy %s" % policy
    h2.cmd("cd %s && %s > %s/scheduler.log 2>&1 &" % (app, sched, logdir))
    time.sleep(1)
    for host, wid in ((h3, "worker1"), (h4, "worker2")):
        cmd = "python3 -u worker.py --id %s --server 10.0.0.2" % wid
        if slow and wid in slow:
            cmd += " --slow %s" % slow[wid]
        host.cmd("cd %s && %s > %s/%s.log 2>&1 &" % (app, cmd, logdir, wid))
    time.sleep(2)


def stop_app(net):
    for h in net.hosts:
        h.cmd("pkill -f 'python3 -u (scheduler|worker).py' || true")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Job queue Mininet topology")
    ap.add_argument("--policy", choices=["rr", "least"], default=None)
    ap.add_argument("--no-cli", action="store_true")
    args = ap.parse_args()

    if os.geteuid() != 0:
        sys.exit("Run with sudo:  sudo python3 topology/topology.py")
    setLogLevel("info")
    # Do NOT run "mn -c" here: it also kills ryu-manager.
    net = build()
    net.start()
    info("*** Waiting for switches to connect to the controller\n")
    ready = wait_ready(net)
    if not ready:
        info("*** WARNING: connectivity check failed. Is ryu-manager running?\n")
    info("*** Starting scheduler (h2) and workers (h3, h4); logs in %s\n" % LOGS)
    start_app(net, policy=args.policy)
    info("*** Ready. Try:  h1 python3 app/client.py --type REVERSE --payload hello\n")
    try:
        if args.no_cli:
            while True:
                time.sleep(1)
        else:
            CLI(net)
    except KeyboardInterrupt:
        pass
    info("*** Stopping\n")
    stop_app(net)
    net.stop()
