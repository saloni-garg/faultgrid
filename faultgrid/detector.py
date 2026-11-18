"""SWIM-style failure detector.

Each node probes its peers. A probe is a direct ping; if that fails the
prober asks a few other peers to ping on its behalf (indirect probing,
as in SWIM). A peer that fails probing repeatedly is first marked
"suspect"; after further failed rounds it is marked "dead".

Membership uses incarnation numbers. When a node learns, after a link
heals, that some observer had marked it suspect or dead, it refutes the
suspicion by bumping its incarnation number and gossiping itself alive
again; observers accept the higher incarnation and clear the entry.

Time is a simulated clock: one step() is one probe period of
``period_s`` seconds. Detection times reported by the benchmark are in
this simulated time, which keeps runs deterministic.

Partition semantics (read this before trusting a "dead" mark):
membership is per observer. While a partition lasts, nodes on one side
cannot probe the other side, so each side will eventually mark the
other side dead *in its own view*. That mark means "unreachable from
here", not "the process crashed". A partitioned-but-alive node refutes
the mark on heal via the incarnation mechanism above, so a partition
alone never leaves a live node permanently marked dead. A node that
actually crashed never refutes, and stays dead.
"""

import random

ALIVE = "alive"
SUSPECT = "suspect"
DEAD = "dead"


class MemberState:
    __slots__ = ("status", "incarnation", "missed")

    def __init__(self, incarnation=0):
        self.status = ALIVE
        self.incarnation = incarnation
        self.missed = 0


class FailureDetector:
    def __init__(self, topology, period_s=0.1, suspect_after=3,
                 dead_after=6, indirect_k=2, seed=7):
        self.topo = topology
        self.period_s = period_s
        self.suspect_after = suspect_after
        self.dead_after = dead_after
        self.indirect_k = indirect_k
        self.rng = random.Random(seed)
        self.tick = 0
        self._alive_process = {n.id: True for n in topology.nodes}
        self._incarnation = {n.id: 0 for n in topology.nodes}
        # views[observer][target] -> MemberState
        self.views = {
            n.id: {m.id: MemberState() for m in topology.nodes}
            for n in topology.nodes
        }

    # -- process control (the simulated world) -------------------------
    def kill(self, node_id):
        self._alive_process[node_id] = False

    def is_process_alive(self, node_id):
        return self._alive_process[node_id]

    # -- probing --------------------------------------------------------
    def _probe(self, observer, target):
        """Direct probe, then indirect probes through peers."""
        if self._alive_process[target] and self.topo.reachable(observer, target):
            return True
        peers = [p for p in self._alive_process
                 if p not in (observer, target) and self._alive_process[p]]
        self.rng.shuffle(peers)
        for helper in peers[: self.indirect_k]:
            if (self.topo.reachable(observer, helper)
                    and self._alive_process[target]
                    and self.topo.reachable(helper, target)):
                return True
        return False

    def step(self):
        self.tick += 1
        ids = list(self._alive_process)
        for observer in ids:
            if not self._alive_process[observer]:
                continue
            for target in ids:
                if target == observer:
                    continue
                state = self.views[observer][target]
                if self._probe(observer, target):
                    if state.status in (SUSPECT, DEAD):
                        # Refutation: the target is reachable and alive
                        # but had been marked. It raises its incarnation
                        # and the new incarnation clears the mark.
                        self._incarnation[target] += 1
                    state.status = ALIVE
                    state.incarnation = self._incarnation[target]
                    state.missed = 0
                else:
                    state.missed += 1
                    if state.missed >= self.dead_after:
                        state.status = DEAD
                    elif state.missed >= self.suspect_after:
                        state.status = SUSPECT

    def run(self, steps):
        for _ in range(steps):
            self.step()

    # -- queries ---------------------------------------------------------
    def status(self, observer, target):
        return self.views[observer][target].status

    def incarnation(self, node_id):
        return self._incarnation[node_id]

    def observers_marking_dead(self, target):
        return [o for o in self.views
                if o != target and self._alive_process[o]
                and self.views[o][target].status == DEAD]

    def time_s(self):
        return self.tick * self.period_s

    def detect_dead(self, target, max_steps=200):
        """Step until every live observer marks target dead.

        Returns simulated seconds elapsed, or None if not detected
        within max_steps.
        """
        start = self.tick
        for _ in range(max_steps):
            self.step()
            live = [o for o in self._alive_process
                    if o != target and self._alive_process[o]]
            if live and all(self.views[o][target].status == DEAD for o in live):
                return (self.tick - start) * self.period_s
        return None
