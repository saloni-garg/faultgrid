"""Wall-clock failure-detector check.

The benchmark drives the detector on its simulated clock. This
script drives the exact same FailureDetector API with real time:
one time.sleep(period_s) between step() calls, a node killed at the
start, and a stopwatch on how long until every live observer marks
the killed node dead. Nothing about the detector is changed; only
the pacing is real.

Run:  python3 scripts/wallclock_detector.py [--trials 5]
Prints JSON with the per-trial elapsed times.
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from faultgrid.detector import FailureDetector
from faultgrid.topology import Topology


def one_trial(target=3, period_s=0.1):
    topo = Topology(12, 3, 2)
    detector = FailureDetector(topo, period_s=period_s)
    detector.kill(target)
    live = [n.id for n in topo.nodes if n.id != target]
    started = time.perf_counter()
    # dead_after is 6 missed rounds, so detection needs at least 6
    # steps; the cap is generous headroom, not an expectation.
    for _ in range(1000):
        time.sleep(period_s)
        detector.step()
        if all(detector.status(o, target) == "dead" for o in live):
            break
    return time.perf_counter() - started


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()
    times = [round(one_trial(), 3) for _ in range(args.trials)]
    out = {
        "trials": len(times),
        "period_s": 0.1,
        "elapsed_s_per_trial": times,
        "mean_elapsed_s": round(sum(times) / len(times), 3),
        "min_elapsed_s": min(times),
        "max_elapsed_s": max(times),
    }
    print(json.dumps(out, indent=2))
    return out


if __name__ == "__main__":
    main()
