"""Load balancing: consistent hashing, bounded loads, rack awareness.

Plain consistent hashing maps each key to the first node clockwise
from the key's hash on a ring of virtual nodes. It spreads distinct
keys well, but a single hot key lands all of its requests on one node.

The bounded-loads variant (in the style of Google's consistent
hashing with bounded loads) processes requests in order and walks
clockwise from the key's primary node to the first node whose current
load is below a capacity bound:

    capacity = ceil((1 + epsilon) * total_requests / num_nodes)

That caps the busiest node's load near (1 + epsilon) times the mean,
at the cost of sending some requests for a key to a non-primary node.

Replica placement is rack aware: replicas for a key are chosen by
walking the ring and taking nodes from racks not yet used, so a rack
outage cannot take out more than one replica of a key (while enough
racks exist).
"""

import bisect
import hashlib
import math
from collections import defaultdict


def _hash(text):
    return int(hashlib.md5(text.encode()).hexdigest()[:8], 16)


class ConsistentHashRing:
    def __init__(self, node_ids, virtual_nodes=100):
        self.node_ids = list(node_ids)
        self.ring = []  # sorted list of (hash, node_id)
        for nid in self.node_ids:
            for v in range(virtual_nodes):
                self.ring.append((_hash(f"node{nid}#{v}"), nid))
        self.ring.sort()
        self._hashes = [h for h, _ in self.ring]

    def primary(self, key):
        idx = bisect.bisect(self._hashes, _hash(str(key))) % len(self.ring)
        return self.ring[idx][1]

    def walk(self, key):
        """Yield node ids clockwise from the key's primary, no repeats."""
        start = bisect.bisect(self._hashes, _hash(str(key))) % len(self.ring)
        seen = set()
        for off in range(len(self.ring)):
            nid = self.ring[(start + off) % len(self.ring)][1]
            if nid not in seen:
                seen.add(nid)
                yield nid
            if len(seen) == len(self.node_ids):
                return


class LoadBalancer:
    def __init__(self, topology, epsilon=0.25, virtual_nodes=100):
        self.topo = topology
        self.epsilon = epsilon
        self.ring = ConsistentHashRing(
            [n.id for n in topology.nodes], virtual_nodes)
        self.num_nodes = len(topology.nodes)

    # -- baselines and bounded assignment ---------------------------------
    def assign_plain(self, requests):
        """Map every request to its key's primary node."""
        loads = defaultdict(int)
        for key in requests:
            loads[self.ring.primary(key)] += 1
        return dict(loads)

    def capacity(self, total_requests):
        return math.ceil((1 + self.epsilon) * total_requests / self.num_nodes)

    def assign_bounded(self, requests):
        """Map requests with the bounded-loads rule, in request order."""
        cap = self.capacity(len(requests))
        loads = defaultdict(int)
        assignment = {}
        for key in requests:
            for nid in self.ring.walk(key):
                if loads[nid] < cap:
                    loads[nid] += 1
                    assignment.setdefault(key, nid)
                    break
        return dict(loads)

    def route_bounded(self, requests):
        """Like assign_bounded but returns the chosen node per request."""
        cap = self.capacity(len(requests))
        loads = defaultdict(int)
        chosen = []
        for key in requests:
            for nid in self.ring.walk(key):
                if loads[nid] < cap:
                    loads[nid] += 1
                    chosen.append(nid)
                    break
        return chosen

    # -- rack-aware replica placement ---------------------------------------
    def replicas(self, key, count=3):
        """Choose `count` replica nodes for key, in distinct racks
        while distinct racks remain available on the ring walk."""
        chosen = []
        used_racks = set()
        fallback = []
        for nid in self.ring.walk(key):
            rack = self.topo.by_id[nid].rack
            if rack not in used_racks:
                chosen.append(nid)
                used_racks.add(rack)
            else:
                fallback.append(nid)
            if len(chosen) == count:
                return chosen
        for nid in fallback:
            if len(chosen) == count:
                break
            chosen.append(nid)
        return chosen


def imbalance(loads, num_nodes):
    """max / mean load ratio. Mean is over all nodes, zeros included."""
    if not loads:
        return 0.0
    total = sum(loads.values())
    mean = total / num_nodes
    if mean == 0:
        return 0.0
    return max(loads.values()) / mean
