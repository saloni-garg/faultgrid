"""Network topology model.

Nodes are grouped into racks, and racks into availability zones (AZs).
Link latency follows the physical layout:

    intra-rack  <  inter-rack (same AZ)  <  inter-AZ

Links can be cut between two groups of nodes to create a network
partition, and later restored with heal(). Reachability is symmetric.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Node:
    id: int
    rack: int
    az: int

    def __repr__(self):
        return f"n{self.id}(r{self.rack},az{self.az})"


INTRA_RACK_MS = 1.0
INTER_RACK_MS = 5.0
INTER_AZ_MS = 20.0


class Topology:
    def __init__(self, num_nodes=12, num_racks=3, num_azs=2):
        if num_nodes % num_racks != 0:
            raise ValueError("num_nodes must divide evenly across racks")
        self.nodes = []
        per_rack = num_nodes // num_racks
        for rack in range(num_racks):
            az = rack % num_azs
            for i in range(per_rack):
                nid = rack * per_rack + i
                self.nodes.append(Node(id=nid, rack=rack, az=az))
        self.by_id = {n.id: n for n in self.nodes}
        self._cut = set()  # set of frozenset({a, b}) pairs whose link is cut

    def latency_ms(self, a, b):
        na, nb = self.by_id[a], self.by_id[b]
        if a == b:
            return 0.0
        if na.rack == nb.rack:
            return INTRA_RACK_MS
        if na.az == nb.az:
            return INTER_RACK_MS
        return INTER_AZ_MS

    def cost_matrix(self):
        return {
            (a.id, b.id): self.latency_ms(a.id, b.id)
            for a in self.nodes
            for b in self.nodes
        }

    def partition(self, group_a, group_b):
        """Cut every link between group_a and group_b."""
        for a in group_a:
            for b in group_b:
                if a != b:
                    self._cut.add(frozenset((a, b)))

    def heal(self):
        self._cut.clear()

    def reachable(self, a, b):
        if a == b:
            return True
        return frozenset((a, b)) not in self._cut

    def reachable_peers(self, node_id, candidates):
        return [c for c in candidates if self.reachable(node_id, c)]

    def nodes_in_rack(self, rack):
        return [n.id for n in self.nodes if n.rack == rack]

    def racks(self):
        return sorted({n.rack for n in self.nodes})
