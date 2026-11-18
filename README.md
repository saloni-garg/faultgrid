# faultgrid

A small fault-tolerant distributed key-value system, built as an
in-process simulation. It replicates data across a cluster laid out
in racks and availability zones, detects node failures with a
SWIM-style gossip protocol, serves reads and writes through majority
quorums, and routes requests with consistent hashing that has bounded
loads and rack-aware replica placement.

The interesting part is what happens when things break: nodes are
killed mid-load, a whole rack is partitioned away, and the system has
to keep serving on the majority side, refuse writes on the minority
side, and lose nothing that it acknowledged once the partition heals.
A chaos harness checks all of that programmatically.

## Layout

    faultgrid/
      topology.py   racks, zones, latency matrix, partitionable links
      detector.py   SWIM-style failure detector with incarnation numbers
      store.py      replicated KV store with majority quorums
      balance.py    consistent hashing, bounded loads, rack-aware replicas
      bench.py      chaos scenarios and measurements
    tests/          pytest suite
    scripts/
      wallclock_detector.py   the failure detector driven with real
                              time.sleep pacing and a stopwatch
    results.json    measured output of the last bench run

## Design

### Topology

Nodes are grouped into racks, racks into availability zones. Link
latency follows the layout: 1 ms inside a rack, 5 ms between racks in
the same zone, 20 ms across zones. Links between any two groups of
nodes can be cut to form a partition and later healed.

### Failure detector

Each node probes its peers every period (0.1 s of simulated time). A
failed direct probe is retried indirectly through two other peers, as
in SWIM. A peer that keeps failing is marked suspect after 3 failed
rounds and dead after 6. Membership carries incarnation numbers: a
node that learns it was marked suspect or dead raises its incarnation
and gossips itself alive again, and observers accept the higher
incarnation.

Partition semantics, stated plainly: membership is per observer.
During a partition, each side marks the other side dead in its own
view, because it cannot probe it. That mark means "unreachable from
here", not "crashed". When the partition heals, live nodes refute the
mark through the incarnation mechanism, so a partition alone never
leaves a live node permanently marked dead. A node that actually
crashed never refutes anything and stays dead.

### Storage and quorums

Data is fully replicated. A write or read needs responses from a
majority of the cluster (7 of 12), so any two quorums intersect.
Versions are (counter, writer) pairs; a coordinator reads the current
counter from its reachable replicas and proposes counter + 1.

A coordinator confirms it can reach a majority *before* applying a
write anywhere. That ordering is what makes the minority side safe:
it rejects the write outright instead of applying it locally and
hoping.

After a partition heals, anti-entropy copies the highest version of
every key to every live node.

### Load balancer

Plain consistent hashing maps each key to the first node clockwise
from the key on a ring with 100 virtual nodes per node. Hot keys break
it: every request for one key lands on one node.

The bounded-loads variant walks clockwise from the primary and picks
the first node under a capacity bound,
capacity = ceil((1 + epsilon) * mean load), with epsilon = 0.25 here.
Replica placement is rack aware: walking the ring, it takes nodes
from racks not yet used, so one rack outage loses at most one replica
of a key.

## Correctness invariants

1. A write is acknowledged only if it was applied to a majority of
   the cluster.
2. A coordinator that cannot reach a majority rejects writes and
   reads, and applies nothing.
3. An acknowledged write is never lost after a partition heals: it
   sits on a majority, any post-heal quorum intersects that majority,
   and anti-entropy keeps the highest version.
4. A rejected minority write is never visible after the heal.
5. A crashed node is eventually marked dead by every live observer.
   A live node is never permanently marked dead; partition marks are
   refuted by incarnation on heal.
6. With the bounded balancer, no node's load exceeds
   ceil((1 + epsilon) * mean), so max/mean stays near 1 + epsilon.
7. Replicas of a key sit in distinct racks while distinct racks
   remain on the ring walk.

## Running it

    python -m pytest tests/ -q     # or: pytest
    python -m faultgrid.bench      # writes results.json
    python3 scripts/wallclock_detector.py --trials 5   # real-time detector check

Pure Python 3 standard library. The test suite needs pytest
(`pip install pytest`). There is also a Dockerfile if you prefer:

    docker build -t faultgrid .
    docker run --rm faultgrid

## Measured results

From `python -m faultgrid.bench` on a 12 node, 3 rack, 2 zone cluster
(quorum 7), recorded in results.json:

* Kill and failover: nodes 1 and 5 killed mid-load. Both were marked
  dead by every live observer after 0.6 s of simulated detector time
  (6 probe periods at 0.1 s). The following 200 requests (mixed reads
  and writes, coordinators chosen among survivors) completed with
  0 errors, an error rate of 0.0.
* Partition: rack 0 (nodes 0 to 3) cut away from the other 8 nodes.
  The majority side acknowledged 30 of 30 writes and served reads
  throughout. The minority side rejected 10 of 10 write attempts.
  After healing and anti-entropy, 0 acknowledged writes were lost and
  0 rejected minority writes were visible. The harness checks this
  and reports correct: true.
* Load balance: 10,000 requests over 200 keys with Zipf-like
  popularity. Plain consistent hashing put a max load of 2,325 on the
  busiest node, a max/mean ratio of 2.79. The bounded balancer capped
  the busiest node at its capacity of 1,042, a max/mean ratio of
  1.2504 against a mean of 833.3.

Tests: 14 passed with pytest.

## Results on a dedicated machine

From `results-verda.json` in this repo: `python -m faultgrid.bench`
run 10 times on a dedicated Verda CPU-TURIN.4V.16G instance,
4 vCPU, 16 GB, Ubuntu 24.04 (FIN-03), Python 3.12.3, plus a
wall-clock detector check on the same machine.

* Kill and failover: detection time 0.6 s of simulated detector
  time in every one of the 10 runs (mean 0.6, range 0.6 to 0.6).
  Failover errors: 0 in every run, error rate 0.0 throughout.
* Partition: correct in all 10 runs. The minority side rejected
  10 of 10 writes in every run, and 0 acknowledged writes were
  lost after heal in every run.
* Load balance: plain consistent hashing max/mean 2.79 and
  bounded loads max/mean 1.2504 in every run.
* Wall-clock detector: `scripts/wallclock_detector.py` drives the
  same detector with a real `time.sleep(0.1)` between probe rounds
  and a stopwatch. Over 5 trials on the dedicated machine, elapsed
  time to detection was 0.601 s every trial (mean 0.601, range
  0.601 to 0.601), matching the 0.6 s the simulated clock reports:
  on an idle machine the detector's real pacing costs about a
  millisecond over its simulated time.

One honest note: the bench is seeded, so its scenarios are
deterministic and the 10-run aggregates have zero spread. The
repetitions confirm the harness runs identically on the dedicated
machine; the wall-clock check is the part that adds information a
simulated clock cannot, and it agrees with the simulation.

## Limitations

* In-process simulation. Nodes are objects in one Python process;
  there are no real sockets, no serialization, and no actual network.
  Partitions are modeled by cutting reachability in the topology, not
  by dropping packets.
* One clock. The detector runs on a simulated clock advanced in
  discrete steps, and detection times are in simulated seconds. The
  wall-clock check above shows the same detector, paced by real
  sleeps on an idle dedicated machine, detecting in 0.601 s against
  the simulated 0.6 s; but real deployments face clock skew and
  variable delays that this model still smooths away.
* Full replication only. Every node holds every key, which is fine at
  this scale and wrong at large scale; the balancer routes requests
  but does not shard data.
* Anti-entropy is a full scan of every key on every live node. Real
  systems would use Merkle trees or version summaries.
* The failure detector probes all peers each round rather than the
  random-subset probing SWIM uses to scale. The suspicion, indirect
  probe, and incarnation mechanics are faithful; the fan-out is not.
* No persistence, no TLS, no authentication, no Byzantine behavior.
  Nodes are fail-stop only.

## License

MIT, see LICENSE.
