"""Ryu OpenFlow 1.3 controller for the Distributed Job Queue project (D1).

What it does
  1. Proactive, loop-free forwarding: the topology is known, so IPv4 flows are
     pushed to every switch as soon as it connects (no flooding, so the
     redundant s1-s2-s3 / s1-s3 links cannot cause broadcast storms).
  2. Answers ARP requests itself (ARP proxy) instead of flooding them.
  3. Installs HIGH-priority flows for the job-queue traffic (TCP port 5000) so
     application traffic is identified and counted separately from other traffic.
  4. Every STATS_INTERVAL seconds it polls flow counters and prints how much
     job traffic crosses each switch, together with the worker status the
     scheduler publishes in /tmp/sdn_status.json  (app -> network hook).
  5. Dynamic path control: write "direct" or "via_s2" into /tmp/sdn_path and
     the controller re-routes switch-to-switch traffic within ~1 second.

Run:  ryu-manager controller/controller.py
"""
import json
import os

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, arp, ether_types
from ryu.ofproto import ofproto_v1_3

JOB_PORT = 5000
PRIO_GENERAL = 10
PRIO_JOB = 100
STATS_INTERVAL = 5          # seconds
PATH_FILE = "/tmp/sdn_path"
STATUS_FILE = "/tmp/sdn_status.json"

# ip -> (mac, switch dpid, switch port, friendly name)   -- must match topology.py
HOSTS = {
    "10.0.0.1": ("00:00:00:00:00:01", 1, 1, "h1-client"),
    "10.0.0.2": ("00:00:00:00:00:02", 1, 2, "h2-scheduler"),
    "10.0.0.3": ("00:00:00:00:00:03", 3, 1, "h3-worker1"),
    "10.0.0.4": ("00:00:00:00:00:04", 3, 2, "h4-worker2"),
}

# (current switch, destination switch) -> output port, for each path choice.
# Switch links in topology.py:  s1:3<->s2:1   s2:2<->s3:3   s1:4<->s3:4
NEXT_HOP = {
    "direct": {(1, 3): 4, (3, 1): 4},
    "via_s2": {(1, 3): 3, (2, 3): 2, (3, 1): 3, (2, 1): 1},
}
SWITCH_NAME = {1: "s1", 2: "s2", 3: "s3"}


class JobQueueController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(JobQueueController, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.path = "direct"
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

    def out_port(self, dpid, dst_sw, dst_port):
        if dpid == dst_sw:
            return dst_port
        return NEXT_HOP[self.path].get((dpid, dst_sw))

    def install_switch(self, dp):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        # table-miss: send unknown packets (ARP) to the controller
        self.add_flow(dp, 0, parser.OFPMatch(),
                      [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)])
        for ip, (mac, sw, port, name) in HOSTS.items():
            out = self.out_port(dp.id, sw, port)
            if out is None:
                continue                      # this switch is not on the path
            actions = [parser.OFPActionOutput(out)]
            self.add_flow(dp, PRIO_GENERAL,
                          parser.OFPMatch(eth_type=0x0800, ipv4_dst=ip), actions)
            # job-queue traffic: packets TO port 5000 and replies FROM port 5000
            self.add_flow(dp, PRIO_JOB,
                          parser.OFPMatch(eth_type=0x0800, ip_proto=6, ipv4_dst=ip,
                                          tcp_dst=JOB_PORT), actions)
            self.add_flow(dp, PRIO_JOB,
                          parser.OFPMatch(eth_type=0x0800, ip_proto=6, ipv4_dst=ip,
                                          tcp_src=JOB_PORT), actions)
        self.logger.info("flows installed on %s (path=%s)", SWITCH_NAME.get(dp.id, dp.id), self.path)

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
        elif dp.id in self.datapaths:
            del self.datapaths[dp.id]

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

    # ------------------------------------------------------------ monitoring + path control
    def _monitor(self):
        ticks = 0
        while True:
            hub.sleep(1)
            self._check_path_file()
            ticks += 1
            if ticks % STATS_INTERVAL == 0:
                for dp in list(self.datapaths.values()):
                    parser = dp.ofproto_parser
                    dp.send_msg(parser.OFPFlowStatsRequest(dp))
                self._log_worker_status()

    def _check_path_file(self):
        try:
            with open(PATH_FILE) as fh:
                wanted = fh.read().strip()
        except OSError:
            return
        if wanted in NEXT_HOP and wanted != self.path:
            self.logger.info("PATH CHANGE: %s -> %s", self.path, wanted)
            self.path = wanted
            for dp in list(self.datapaths.values()):
                self.clear_flows(dp)
                self.install_switch(dp)

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

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply(self, ev):
        dpid = ev.msg.datapath.id
        for st in ev.msg.body:
            if st.priority != PRIO_JOB or st.packet_count == 0:
                continue
            m = st.match
            dst = HOSTS.get(m.get("ipv4_dst"), (None, None, None, m.get("ipv4_dst")))[3]
            kind = "to-port-5000" if m.get("tcp_dst") == JOB_PORT else "from-port-5000"
            self.logger.info("[job traffic] %s path=%s -> %s (%s): %d pkts, %d bytes",
                             SWITCH_NAME.get(dpid, dpid), self.path, dst, kind,
                             st.packet_count, st.byte_count)
