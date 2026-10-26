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
"""
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


def build():
    net = Mininet(controller=None, switch=OVSKernelSwitch, link=TCLink, autoSetMacs=False)
    net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)

    sw = {}
    for n in (1, 2, 3):
        sw[n] = net.addSwitch("s%d" % n, dpid="%016x" % n,
                              protocols="OpenFlow13", failMode="secure")

    h1 = net.addHost("h1", ip="10.0.0.1/24", mac="00:00:00:00:00:01")
    h2 = net.addHost("h2", ip="10.0.0.2/24", mac="00:00:00:00:00:02")
    h3 = net.addHost("h3", ip="10.0.0.3/24", mac="00:00:00:00:00:03")
    h4 = net.addHost("h4", ip="10.0.0.4/24", mac="00:00:00:00:00:04")

    # Port numbers are fixed on purpose: controller.py relies on them.
    net.addLink(h1, sw[1], port2=1)
    net.addLink(h2, sw[1], port2=2)
    net.addLink(h3, sw[3], port2=1)
    net.addLink(h4, sw[3], port2=2)
    net.addLink(sw[1], sw[2], port1=3, port2=1, bw=10, delay="5ms")   # s1 <-> s2
    net.addLink(sw[2], sw[3], port1=2, port2=3, bw=10, delay="5ms")   # s2 <-> s3
    net.addLink(sw[1], sw[3], port1=4, port2=4, bw=10, delay="5ms")   # s1 <-> s3 (direct)
    return net


def start_app(net):
    os.makedirs(LOGS, exist_ok=True)
    h2, h3, h4 = net.get("h2", "h3", "h4")
    app = os.path.join(ROOT, "app")
    h2.cmd("cd %s && python3 -u scheduler.py > %s/scheduler.log 2>&1 &" % (app, LOGS))
    time.sleep(1)
    h3.cmd("cd %s && python3 -u worker.py --id worker1 --server 10.0.0.2 > %s/worker1.log 2>&1 &" % (app, LOGS))
    h4.cmd("cd %s && python3 -u worker.py --id worker2 --server 10.0.0.2 > %s/worker2.log 2>&1 &" % (app, LOGS))
    time.sleep(2)


if __name__ == "__main__":
    if os.geteuid() != 0:
        sys.exit("Run with sudo:  sudo python3 topology/topology.py")
    setLogLevel("info")
##    os.system("mn -c > /dev/null 2>&1")          # clean leftovers from earlier runs
    net = build()
    net.start()
    info("*** Waiting for switches to connect to the controller\n")
    time.sleep(4)
    info("*** Connectivity check\n")
    loss = net.pingAll()
    if loss > 0:
        info("*** WARNING: %.0f%% loss. Is ryu-manager running?\n" % loss)
    info("*** Starting scheduler (h2) and workers (h3, h4); logs in %s\n" % LOGS)
    start_app(net)
    info("*** Ready. Try:  h1 python3 app/client.py --type REVERSE --payload hello\n")
    CLI(net)
    info("*** Stopping\n")
    for h in net.hosts:
        h.cmd("pkill -f 'python3 -u (scheduler|worker).py' || true")
    net.stop()
