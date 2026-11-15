#!/usr/bin/env python3
"""End-to-end demo of the private noisy-count budget service.

Starts the service as a subprocess (exactly like a deployment), talks to it
over HTTP, and walks through the worked example from the specification:

    group g = [a, b], each subject limited to 1/1
    j reserves 1/1 for both  -> k is rejected
    cancel j                -> k is accepted
    read k                  -> cancel k is refused

Run::

    python3 examples/demo.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from privbudget.client import Client  # noqa: E402


def show(label, value):
    print("%-46s %s" % (label + ":", json.dumps(value)))


def main():
    data_dir = tempfile.mkdtemp(prefix="privbudget-demo-")
    cmd = [sys.executable, "-m", "privbudget", "--data", data_dir, "--test-noise"]
    proc = subprocess.Popen(
        cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        port = json.loads(proc.stdout.readline())["port"]
        client = Client(port=port)
        print("service listening on 127.0.0.1:%d, data in %s\n" % (port, data_dir))

        show("initialize", client.initialize({
            "bits": {"a": 0, "b": 1, "d": 1, "e": 1, "f": 0},
            "groups": {"g": ["a", "b"], "h": ["d", "e"], "m": ["d"],
                       "n": ["f"], "empty": []},
            "limits": {"a": "1/1", "b": "1/1", "d": "3/2", "e": "3/2",
                       "f": "1/1"},
        }))
        show("initialize again (only first counts)", client.initialize({}))

        print("\n-- worked example: g = [a, b], limits 1/1 --")
        show("start j (epsilon 1/1)", client.start("j", "g", "1/1"))
        show("start k, same group (must be refused)",
             client.start("k", "g", "1/1"))
        show("budgets while j is pending", {
            s: v for s, v in client.budgets().items() if s in ("a", "b")
        })
        show("cancel j", client.cancel("j"))
        show("cancel j again (idempotent)", client.cancel("j"))
        show("start k now", client.start("k", "g", "1/1"))

        state = client.status("k", wait=10)
        show("status k (counter answered)", state)
        show("read k", client.read("k"))
        show("read k again (same value)", client.read("k"))
        show("cancel k (published: refused)", client.cancel("k"))
        show("budgets after publication", {
            s: v for s, v in client.budgets().items() if s in ("a", "b")
        })

        print("\n-- overlapping budget: d is in both h and m (limit 3/2) --")
        for name in ("shared1", "shared2", "shared3"):
            show("start %s on group h, epsilon 1/2" % name,
                 client.start(name, "h", "1/2"))
        show("start shared4 on group h, epsilon 1/2 (3/2 used up)",
             client.start("shared4", "h", "1/2"))
        show("d is shared with group m, so m is blocked too",
             client.start("m1", "m", "1/2"))
        show("budgets for h's subjects",
             {s: v for s, v in client.budgets().items()
              if s in ("d", "e")})
        show("start on empty group", client.start("nothing", "empty", "1/1"))
        state = client.status("nothing", wait=10)
        show("status nothing", state)
        show("read nothing (pure noise)", client.read("nothing"))

        print("\n-- deterministic noise injection; value = bits + noise --")
        show("start noisy on n with epsilon 1/1 and noise 3/4",
             client.start("noisy", "n", "1/1", noise="3/4"))
        show("status noisy", client.status("noisy", wait=10))
        show("read noisy (bits f = 0, so 0 + 3/4)", client.read("noisy"))

        print("\n-- invalid input --")
        show("start with bad epsilon", client.start("bad", "g", "0/1"))
        show("start with bad job id", client.start("bad id", "g", "1/1"))
        show("status of unknown job", client.status("missing"))

        print("\n-- persistence across a full restart --")
        value_before = client.read("noisy")["value"]
        proc.terminate()
        proc.wait(timeout=10)
        proc = subprocess.Popen(
            cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        port = json.loads(proc.stdout.readline())["port"]
        client = Client(port=port)
        show("initialize after restart (refused)", client.initialize({}))
        show("status noisy", client.status("noisy"))
        show("read noisy (same value)", client.read("noisy"))
        assert client.read("noisy")["value"] == value_before
        print("\ndone: budgets survive restarts and values never change.")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
    return 0


if __name__ == "__main__":
    sys.exit(main())
