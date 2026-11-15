"""Ryu OpenFlow 1.3 controller for the Distributed Job Queue project (D2).

Modes (env var SDN_MODE):
  static   baseline. Fixed direct path s1-s3. The controller never reacts to
           link failures or congestion (it still logs them and the job traffic).
  dynamic  (default) reacts by itself:
             * LINK FAILOVER: Port-Status events. If the direct link s1:4 <-> s3:4
               goes down, all traffic is moved to the s2 path and flows are
               reinstalled. When the link comes back, traffic moves back.
             * CONGESTION REROUTE: every PORT_STATS_INTERVAL seconds port counters
               of the direct link are read. If the load is above CONGEST_BPS for
               2 readings in a row, worker2 traffic (both directions) is sent via
               s2 while worker1 stays on the direct link. It moves back once the
               load has stayed low.

Always on (both modes)
  * Proactive, loop-free forwarding (flows pushed when a switch connects).
  * ARP proxy (no flooding).
  * HIGH-priority flows for job-queue traffic (TCP port 5000) so it is counted
    separately; flow counters + worker status (/tmp/sdn_status.json) are logged
    every STATS_INTERVAL seconds.

Grep-able log markers for the experiment scripts:
  "LINK DOWN", "LINK UP", "CONGESTION", "ROUTE CHANGE", "[link load]", "[job traffic]"

Run:  SDN_MODE=dynamic ryu-manager controller/controller.py
      SDN_MODE=static  ryu-manager controller/controller.py
"""
import json
import os
import time

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, arp, ether_types
from ryu.ofproto import ofproto_v1_3

JOB_PORT = 5000
PRIO_GENERAL = 10
PRIO_JOB = 100
STATS_INTERVAL = 5            # seconds, flow stats + worker status
PORT_STATS_INTERVAL = 2       # seconds, port stats (congestion detection)
STATUS_FILE = "/tmp/sdn_status.json"

# ---- congestion tuning (direct link is assumed to be 10 Mbps in topology.py)
CONGEST_BPS = 7e6             # above this ...
CONGEST_READINGS = 2          # ... for this many readings in a row -> reroute
CLEAR_BPS = 3e6               # below this ...
CLEAR_READINGS = 3            # ... for this many readings -> move back
CONGEST_HOLD = 10             # seconds to stay rerouted before moving back (anti-flapping)
CONGEST_MOVE_IP = "10.0.0.4"  # worker2 is the one moved off the direct link

# ip -> (mac, switch dpid, switch port, friendly name)   -- must match topology.py
HOSTS = {
    "10.0.0.1": ("00:00:00:00:00:01", 1, 1, "h1-client"),
    "10.0.0.2": ("00:00:00:00:00:02", 1, 2, "h2-scheduler"),
    "10.0.0.3": ("00:00:00:00:00:03", 3, 1, "h3-worker1"),
    "10.0.0.4": ("00:00:00:00:00:04", 3, 2, "h4-worker2"),
}
WORKER_IPS = ("10.0.0.3", "10.0.0.4")

# (current switch, destination switch) -> output port, for each path choice.
# Switch links in topology.py:  s1:3<->s2:1   s2:2<->s3:3   s1:4<->s3:4
NEXT_HOP = {
    "direct": {(1, 3): 4, (3, 1): 4},
    "via_s2": {(1, 3): 3, (2, 3): 2, (3, 1): 3, (2, 1): 1},
}
SWITCH_NAME = {1: "s1", 2: "s2", 3: "s3"}

# (dpid, port) of each link end, used to decide which paths are usable
DIRECT_PORTS = {(1, 4), (3, 4)}
VIA_PORTS = {(1, 3), (2, 1), (2, 2), (3, 3)}
WATCHED_PORTS = DIRECT_PORTS | VIA_PORTS


class JobQueueController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(JobQueueController, self).__init__(*args, **kwargs)
        mode = os.environ.get("SDN_MODE", "dynamic").strip().lower()
        if mode not in ("static", "dynamic"):
            self.logger.warning("unknown SDN_MODE=%r, using dynamic", mode)
            mode = "dynamic"
        self.mode = mode
        self.datapaths = {}
        self.link_down = set()                       # (dpid, port) ends that are down
        self.worker_path = {ip: "direct" for ip in WORKER_IPS}   # per-worker path
        # congestion state
        self.congested = False
        self.cong_since = 0.0
        self.hi_count = 0
        self.lo_count = 0
        self.port_prev = {}                          # (dpid, port) -> (tx_bytes, time)
        self.port_load = {}                          # (dpid, port) -> bits/s
        self.logger.info("controller started in %s mode", self.mode.upper())
        self.monitor_thread = hub.spawn(self._monitor)

    # ------------------------------------------------------------ helpers
    def add_flow(self, dp, priority, match, actions):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        dp.send_msg(parser.OFPFlowMod(datapath=dp, priority=priority,
                                      match=match, instructions=inst))

    def clear_flows(self, dp):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        dp.send_msg(parser.OFPFlowMod(datapath=dp, table_id=ofp.OFPTT_ALL,
                                      command=ofp.OFPFC_DELETE,
                                      out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                                      match=parser.OFPMatch()))

    def direct_ok(self):
        return not (self.link_down & DIRECT_PORTS)

    def via_ok(self):
        return not (self.link_down & VIA_PORTS)

    def route_path(self, src_ip, dst_ip):
        """Which path (direct / via_s2) a src->dst pair uses."""
        if HOSTS[src_ip][1] == HOSTS[dst_ip][1]:
            return "direct"                          # same switch, path is irrelevant
        for ip in (src_ip, dst_ip):                  # the s3-side host is always a worker
            if ip in self.worker_path:
                return self.worker_path[ip]
        return "direct"

    def out_port(self, dpid, src_ip, dst_ip):
        """Output port on switch dpid for src_ip -> dst_ip, or None if the
        switch is not on the path."""
        sw_s = HOSTS[src_ip][1]
        sw_d, port_d = HOSTS[dst_ip][1], HOSTS[dst_ip][2]
        if dpid == sw_d:
            return port_d
        if sw_s == sw_d:
            return None
        path = self.route_path(src_ip, dst_ip)
        if dpid == sw_s or (dpid == 2 and path == "via_s2"):
            return NEXT_HOP[path].get((dpid, sw_d))
        return None

    def install_switch(self, dp):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        # table-miss: send unknown packets (ARP) to the controller
        self.add_flow(dp, 0, parser.OFPMatch(),
                      [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)])
        # one set of flows per (src, dst) pair so each worker can use its own path
        for src_ip in HOSTS:
            for dst_ip in HOSTS:
                if src_ip == dst_ip:
                    continue
                out = self.out_port(dp.id, src_ip, dst_ip)
                if out is None:
                    continue                          # this switch is not on the path
                actions = [parser.OFPActionOutput(out)]
                self.add_flow(dp, PRIO_GENERAL,
                              parser.OFPMatch(eth_type=0x0800, ipv4_src=src_ip,
                                              ipv4_dst=dst_ip), actions)
                # job-queue traffic: packets TO port 5000 and replies FROM port 5000
                self.add_flow(dp, PRIO_JOB,
                              parser.OFPMatch(eth_type=0x0800, ip_proto=6, ipv4_src=src_ip,
                                              ipv4_dst=dst_ip, tcp_dst=JOB_PORT), actions)
                self.add_flow(dp, PRIO_JOB,
                              parser.OFPMatch(eth_type=0x0800, ip_proto=6, ipv4_src=src_ip,
                                              ipv4_dst=dst_ip, tcp_src=JOB_PORT), actions)
        self.logger.info("flows installed on %s (paths: %s)", SWITCH_NAME.get(dp.id, dp.id),
                         self._paths_str())

    def _paths_str(self):
        return ", ".join("%s=%s" % (HOSTS[ip][3], self.worker_path[ip]) for ip in WORKER_IPS)

    # ------------------------------------------------------------ route decision
    def _desired_paths(self):
        paths = {ip: "direct" for ip in WORKER_IPS}
        if self.mode == "static":
            return paths                              # baseline: never reacts
        if not self.direct_ok() and self.via_ok():
            return {ip: "via_s2" for ip in WORKER_IPS}
        if self.congested and self.direct_ok() and self.via_ok():
            paths[CONGEST_MOVE_IP] = "via_s2"
        return paths

    def _reconcile(self):
        """Compare wanted routes with installed ones, reinstall flows if different."""
        new = self._desired_paths()
        if new == self.worker_path:
            return
        old = self._paths_str()
        self.worker_path = new
        self.logger.info("ROUTE CHANGE: [%s] -> [%s]", old, self._paths_str())
        for dp in list(self.datapaths.values()):
            self.clear_flows(dp)
            self.install_switch(dp)

    # ------------------------------------------------------------ switch events
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features(self, ev):
        dp = ev.msg.datapath
        self.datapaths[dp.id] = dp
        self.clear_flows(dp)
        self.install_switch(dp)

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change(self, ev):
        dp = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[dp.id] = dp
            # learn the current port state (in case a link is already down)
            dp.send_msg(dp.ofproto_parser.OFPPortDescStatsRequest(dp, 0))
        elif dp.id in self.datapaths:
            del self.datapaths[dp.id]

    # ------------------------------------------------------------ Port-Status (link failover)
    def _update_port(self, dpid, port_no, down):
        key = (dpid, port_no)
        if key not in WATCHED_PORTS:
            return
        if down:
            self.link_down.add(key)
        else:
            self.link_down.discard(key)

    def _port_is_down(self, ofp, port, reason=None):
        down = bool(port.state & ofp.OFPPS_LINK_DOWN) or bool(port.config & ofp.OFPPC_PORT_DOWN)
        if reason == ofp.OFPPR_DELETE:
            down = True
        return down

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        port = msg.desc
        if (dp.id, port.port_no) not in WATCHED_PORTS:
            return
        was_ok = self.direct_ok()
        down = self._port_is_down(ofp, port, msg.reason)
        self.logger.info("PORT STATUS: %s port %d is %s", SWITCH_NAME.get(dp.id, dp.id),
                         port.port_no, "DOWN" if down else "UP")
        self._update_port(dp.id, port.port_no, down)
        now_ok = self.direct_ok()

        if was_ok and not now_ok:
            self.congested = False
            self.hi_count = self.lo_count = 0
            if self.mode == "dynamic":
                if self.via_ok():
                    self.logger.info("LINK DOWN: moving to via_s2")
                else:
                    self.logger.warning("LINK DOWN: direct AND s2 path are down, no route")
            else:
                self.logger.info("LINK DOWN: static mode, keeping the direct path "
                                 "(traffic across s1-s3 will be lost)")
        elif not was_ok and now_ok:
            if self.mode == "dynamic":
                self.logger.info("LINK UP: moving back to direct")
            else:
                self.logger.info("LINK UP: static mode, direct path usable again")
        self._reconcile()

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_reply(self, ev):
        dp = ev.msg.datapath
        for port in ev.msg.body:
            self._update_port(dp.id, port.port_no, self._port_is_down(dp.ofproto, port))
        self._reconcile()

    # ------------------------------------------------------------ ARP proxy
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp, parser = dp.ofproto, dp.ofproto_parser
        in_port = msg.match["in_port"]
        pkt = packet.Packet(msg.data)
        req = pkt.get_protocol(arp.arp)
        if req is None or req.opcode != arp.ARP_REQUEST or req.dst_ip not in HOSTS:
            return                              # everything else is dropped
        mac = HOSTS[req.dst_ip][0]
        reply = packet.Packet()
        reply.add_protocol(ethernet.ethernet(ethertype=ether_types.ETH_TYPE_ARP,
                                             dst=req.src_mac, src=mac))
        reply.add_protocol(arp.arp(opcode=arp.ARP_REPLY, src_mac=mac, src_ip=req.dst_ip,
                                   dst_mac=req.src_mac, dst_ip=req.src_ip))
        reply.serialize()
        dp.send_msg(parser.OFPPacketOut(datapath=dp, buffer_id=ofp.OFP_NO_BUFFER,
                                        in_port=ofp.OFPP_CONTROLLER,
                                        actions=[parser.OFPActionOutput(in_port)],
                                        data=reply.data))

    # ------------------------------------------------------------ monitoring
    def _monitor(self):
        ticks = 0
        while True:
            hub.sleep(1)
            ticks += 1
            if ticks % PORT_STATS_INTERVAL == 0:
                for dp in list(self.datapaths.values()):
                    parser, ofp = dp.ofproto_parser, dp.ofproto
                    dp.send_msg(parser.OFPPortStatsRequest(dp, 0, ofp.OFPP_ANY))
            if ticks % STATS_INTERVAL == 0:
                for dp in list(self.datapaths.values()):
                    dp.send_msg(dp.ofproto_parser.OFPFlowStatsRequest(dp))
                self._log_worker_status()

    def _log_worker_status(self):
        try:
            with open(STATUS_FILE) as fh:
                st = json.load(fh)
        except (OSError, ValueError):
            return
        workers = ", ".join("%s(active=%d,done=%d)" % (w["id"], w["active"], w["done"])
                            for w in st.get("workers", [])) or "none"
        self.logger.info("[app status] workers: %s | queue=%d | jobs %s",
                         workers, st.get("queue_len", 0), st.get("stats"))

    # ------------------------------------------------------------ port stats (congestion)
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply(self, ev):
        dpid = ev.msg.datapath.id
        now = time.time()
        for st in ev.msg.body:
            key = (dpid, st.port_no)
            if key not in DIRECT_PORTS:
                continue
            prev = self.port_prev.get(key)
            self.port_prev[key] = (st.tx_bytes, now)
            if prev and now > prev[1]:
                self.port_load[key] = max(0, st.tx_bytes - prev[0]) * 8.0 / (now - prev[1])
        if dpid == 1:                               # once per polling round
            self._evaluate_congestion()

    def _evaluate_congestion(self):
        load = max([self.port_load.get(k, 0.0) for k in DIRECT_PORTS] or [0.0])
        if load > 1e6:
            self.logger.info("[link load] direct link s1-s3: %.1f Mbps (%s)",
                             load / 1e6, "rerouted" if self.congested else "normal")
        if self.mode != "dynamic" or not self.direct_ok():
            return
        now = time.time()
        if not self.congested:
            self.hi_count = self.hi_count + 1 if load > CONGEST_BPS else 0
            if self.hi_count >= CONGEST_READINGS:
                self.congested = True
                self.cong_since = now
                self.hi_count = self.lo_count = 0
                self.logger.info("CONGESTION: direct link at %.1f Mbps, moving %s via s2",
                                 load / 1e6, HOSTS[CONGEST_MOVE_IP][3])
                self._reconcile()
        else:
            self.lo_count = self.lo_count + 1 if load < CLEAR_BPS else 0
            if self.lo_count >= CLEAR_READINGS and now - self.cong_since >= CONGEST_HOLD:
                self.congested = False
                self.hi_count = self.lo_count = 0
                self.logger.info("CONGESTION CLEARED: direct link at %.1f Mbps, "
                                 "moving %s back to direct", load / 1e6, HOSTS[CONGEST_MOVE_IP][3])
                self._reconcile()

    # ------------------------------------------------------------ flow stats (job traffic)
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply(self, ev):
        dpid = ev.msg.datapath.id
        for st in ev.msg.body:
            if st.priority != PRIO_JOB or st.packet_count == 0:
                continue
            m = st.match
            src = HOSTS.get(m.get("ipv4_src"), (None, None, None, m.get("ipv4_src")))[3]
            dst = HOSTS.get(m.get("ipv4_dst"), (None, None, None, m.get("ipv4_dst")))[3]
            kind = "to-port-5000" if m.get("tcp_dst") == JOB_PORT else "from-port-5000"
            self.logger.info("[job traffic] %s %s -> %s (%s): %d pkts, %d bytes",
                             SWITCH_NAME.get(dpid, dpid), src, dst, kind,
                             st.packet_count, st.byte_count)
