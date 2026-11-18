"""faultgrid: a small fault-tolerant distributed key-value system.

In-process simulation of a replicated store with a SWIM-style failure
detector, majority quorums, rack-aware replica placement, and
consistent hashing with bounded loads, over a rack/AZ network
topology that can be partitioned on demand.
"""

from .balance import LoadBalancer
from .detector import FailureDetector
from .store import Cluster, QuorumError
from .topology import Topology

__all__ = ["Cluster", "FailureDetector", "LoadBalancer", "QuorumError",
           "Topology"]
