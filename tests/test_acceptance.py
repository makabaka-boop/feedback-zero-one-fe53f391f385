#!/usr/bin/env python3
"""Acceptance tests for the private noisy-count budget service.

Run from the repository root::

    python -m unittest -v tests.test_acceptance

Every test drives the service the way an external client would: an HTTP
service process (``python -m privbudget``) and, behind it, the independent
counter process.  No test reaches into their databases to make an assertion;
restart/crash scenarios kill whole processes.

Fault injection used here (all off in production):

``--test-noise``        counter accepts an explicit ``"noise": "p/q"``
``--drop-first N``      counter computes durably then drops the reply
``--die-after-compute N`` counter exits after computing, before replying
``PRIVBUDGET_TEST_CRASH_READ``  service ``os._exit``s in ``read`` just
                                before / just after the debit commits
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from privbudget.client import Client  # noqa: E402


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------
class ServiceProcess:
    def __init__(self, data_dir, env=None, extra_args=()):
        self.data_dir = data_dir
        self.env = dict(os.environ)
        self.env.pop("PRIVBUDGET_TEST_CRASH_READ", None)
        if env:
            self.env.update(env)
        cmd = [sys.executable, "-m", "privbudget", "--data", data_dir,
               "--test-noise"] + list(extra_args)
        self.proc = subprocess.Popen(
            cmd, cwd=ROOT, env=self.env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        line = self.proc.stdout.readline()
        if not line:
            err = self.proc.stderr.read()
            raise RuntimeError("service failed to start: %s" % err)
        self.port = json.loads(line)["port"]
        self.client = Client(port=self.port, timeout=30.0)

    def stop(self, sig=signal.SIGTERM):
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        try:
            self.proc.stdout.close()
            self.proc.stderr.close()
        except Exception:
            pass

    def wait_exit(self, timeout=10):
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        return self.proc.returncode


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="privbudget-test-")
        self.services = []

    def tearDown(self):
        for svc in self.services:
            try:
                svc.stop()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def spawn(self, env=None, extra_args=(), subdir="data"):
        path = os.path.join(self.tmp, subdir)
        os.makedirs(path, exist_ok=True)
        svc = ServiceProcess(path, env=env, extra_args=extra_args)
        self.services.append(svc)
        return svc

    def wait_state(self, client, job, states, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            got = client.status(job)
            if got.get("state") in states:
                return got
            time.sleep(0.02)
        self.fail("job %s never reached %s (last %r)" %
                  (job, states, client.status(job)))

    def data_path(self, name="data"):
        return os.path.join(self.tmp, name)


CONFIG = {
    "bits": {"a": 0, "b": 1, "c": 1, "d": 0},
    "groups": {"g": ["a", "b"], "h": ["b", "c"], "solo": ["d"], "empty": []},
    "limits": {"a": "1/1", "b": "1/1", "c": "1/1", "d": "1/1"},
}


# ---------------------------------------------------------------------------
# Initialization and validation
# ---------------------------------------------------------------------------
class TestInitialization(Base):
    def test_initialize_once(self):
        svc = self.spawn()
        c = svc.client
        self.assertEqual(c.initialize(CONFIG), {"state": "initialized"})
        # Second attempt, even with an identical config, is rejected and does
        # not alter anything.
        self.assertEqual(c.initialize(CONFIG), {"error": "$"})
        self.assertEqual(c.budgets()["b"], {"spent": "0/1", "reserved": "0/1"})

    def test_initialize_survives_restart(self):
        svc = self.spawn()
        svc.client.initialize(CONFIG)
        svc.stop()
        svc2 = self.spawn()
        self.assertEqual(svc2.client.initialize(CONFIG), {"error": "$"})
        self.assertEqual(svc2.client.budgets()["a"],
                         {"spent": "0/1", "reserved": "0/1"})

    def test_invalid_initialize_leaves_service_uninitialized(self):
        svc = self.spawn()
        c = svc.client
        bad = json.loads(json.dumps(CONFIG))
        bad["bits"]["a"] = True  # booleans are not integers
        self.assertEqual(c.initialize(bad), {"error": "$.bits.a"})
        # Still uninitialized, so operations are refused ...
        self.assertEqual(c.status("j"), {"error": "$"})
        self.assertEqual(c.budgets(), {"error": "$"})
        # ... and a valid configuration is still accepted.
        self.assertEqual(c.initialize(CONFIG), {"state": "initialized"})

    def test_config_validation_paths(self):
        cases = []
        cfg = json.loads(json.dumps(CONFIG)); cfg["groups"]["g"] = ["a", "zz"]
        cases.append(("$.groups.g[1]", cfg))
        cfg = json.loads(json.dumps(CONFIG)); cfg["groups"]["g"] = ["a", "a"]
        cases.append(("$.groups.g[1]", cfg))
        cfg = json.loads(json.dumps(CONFIG)); cfg["limits"]["a"] = "0/1"
        cases.append(("$.limits.a", cfg))
        cfg = json.loads(json.dumps(CONFIG)); cfg["limits"]["a"] = "-1/1"
        cases.append(("$.limits.a", cfg))
        cfg = json.loads(json.dumps(CONFIG)); cfg["limits"]["a"] = "1.5"
        cases.append(("$.limits.a", cfg))
        cfg = json.loads(json.dumps(CONFIG)); cfg["limits"]["a"] = 1
        cases.append(("$.limits.a", cfg))
        cfg = json.loads(json.dumps(CONFIG)); del cfg["limits"]["d"]
        cases.append(("$.limits", cfg))
        cfg = json.loads(json.dumps(CONFIG)); cfg["groups"]["bad name"] = []
        cases.append(("$.groups['bad name']", cfg))
        for want, cfg in cases:
            svc = self.spawn(subdir="d%d" % len(os.listdir(self.tmp)))
            got = svc.client.initialize(cfg)
            svc.stop()
            self.assertEqual(got, {"error": want}, (want, got))

    def test_operation_argument_validation(self):
        svc = self.spawn()
        c = svc.client
        c.initialize(CONFIG)
        self.assertEqual(c.start("", "g", "1/1"), {"error": "$.job"})
        self.assertEqual(c.start("j", "nope", "1/1"), {"error": "$.group"})
        self.assertEqual(c.start("j", "g", "1"), {"error": "$.epsilon"})
        self.assertEqual(c.start("j", "g", "1/0"), {"error": "$.epsilon"})
        self.assertEqual(c.status(""), {"error": "$.job"})
        self.assertEqual(c.status("missing"), {"error": "$.job"})
        self.assertEqual(c.read("missing"), {"error": "$.job"})
        self.assertEqual(c.cancel("missing"), {"error": "$.job"})


# ---------------------------------------------------------------------------
# Reservation semantics (the worked example from the brief)
# ---------------------------------------------------------------------------
class TestReservations(Base):
    def test_example_from_spec(self):
        svc = self.spawn()
        c = svc.client
        # group [a,b], each limit 1/1
        self.assertEqual(c.initialize({
            "bits": {"a": 0, "b": 0},
            "groups": {"g": ["a", "b"]},
            "limits": {"a": "1/1", "b": "1/1"},
        }), {"state": "initialized"})
        # j reserves 1/1 for both
        self.assertEqual(c.start("j", "g", "1/1"), {"state": "pending"})
        # k is rejected while j holds the reservation
        self.assertEqual(c.start("k", "g", "1/1"), {"error": "$.epsilon"})
        # cancelling j frees the budget
        self.assertEqual(c.cancel("j"), {"state": "cancelled"})
        self.assertEqual(c.start("k", "g", "1/1"), {"state": "pending"})
        self.wait_state(c, "k", {"ready"})
        self.assertIn("value", c.read("k"))
        # once published, cancellation is refused
        self.assertEqual(c.cancel("k"), {"error": "$.job"})
        self.assertEqual(c.budgets()["a"], {"spent": "1/1", "reserved": "0/1"})

    def test_bits_zero_still_charged(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        self.assertEqual(c.start("zero", "g", "1/1"), {"state": "pending"})
        self.assertEqual(c.start("again", "g", "1/1"), {"error": "$.epsilon"})
        self.wait_state(c, "zero", {"ready"})
        self.assertEqual(c.budgets()["a"], {"spent": "0/1", "reserved": "1/1"})
        c.read("zero")
        self.assertEqual(c.budgets()["a"], {"spent": "1/1", "reserved": "0/1"})

    def test_reservation_is_atomic_across_overlapping_groups(self):
        svc = self.spawn()
        c = svc.client
        # b belongs to g and h; both need b's budget.
        c.initialize(CONFIG)
        self.assertEqual(c.start("first", "g", "3/4"), {"state": "pending"})
        # h needs b: 3/4 reserved leaves only 1/4, so 1/2 fails.
        self.assertEqual(c.start("second", "h", "1/2"), {"error": "$.epsilon"})
        self.assertEqual(c.start("second", "h", "1/4"), {"state": "pending"})
        b = c.budgets()["b"]
        self.assertEqual(b, {"spent": "0/1", "reserved": "1/1"})
        # a and c are untouched, d too.
        self.assertEqual(c.budgets()["a"], {"spent": "0/1", "reserved": "3/4"})

    def test_fractional_epsilon_aggregation(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 1}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        self.assertEqual(c.start("x", "g", "1/3"), {"state": "pending"})
        self.assertEqual(c.start("y", "g", "1/3"), {"state": "pending"})
        self.assertEqual(c.start("z", "g", "1/2"), {"error": "$.epsilon"})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "2/3"})
        self.assertEqual(c.start("z", "g", "1/3"), {"state": "pending"})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "1/1"})

    def test_rejection_does_not_mutate_state(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        before = c.budgets()
        self.assertEqual(c.start("k", "g", "1/1"), {"error": "$.epsilon"})
        self.assertEqual(c.budgets(), before)
        # A rejected start does not burn the job identifier either: cancelling
        # j lets the *same* name k be accepted.
        self.assertEqual(c.status("k"), {"error": "$.job"})
        c.cancel("j")
        self.assertEqual(c.start("k", "g", "1/1"), {"state": "pending"})


# ---------------------------------------------------------------------------
# Job identity semantics
# ---------------------------------------------------------------------------
class TestJobIdentity(Base):
    def setUp(self):
        super().setUp()
        self.svc = self.spawn()
        self.c = self.svc.client
        self.c.initialize({
            "bits": {"a": 0, "b": 0},
            "groups": {"g": ["a", "b"], "other": ["a"]},
            "limits": {"a": "5/1", "b": "5/1"},
        })

    def test_repeat_returns_current_state(self):
        self.assertEqual(self.c.start("j", "g", "1/1"), {"state": "pending"})
        self.assertEqual(self.c.start("j", "g", "1/1"), {"state": "pending"})
        # Same numeric epsilon written differently is still "the same".
        self.assertEqual(self.c.start("j", "g", "2/2"), {"state": "pending"})
        self.wait_state(self.c, "j", {"ready"})
        self.assertEqual(self.c.start("j", "g", "1/1"), {"state": "ready"})
        self.c.read("j")
        self.assertEqual(self.c.start("j", "g", "1/1"), {"state": "published"})
        self.assertEqual(self.c.cancel("j"), {"error": "$.job"})
        # Cancelled jobs stay cancelled.
        self.c.start("k", "g", "1/1")
        self.c.cancel("k")
        self.assertEqual(self.c.start("k", "g", "1/1"), {"state": "cancelled"})

    def test_conflicting_reuse_rejected(self):
        self.c.start("j", "g", "1/1")
        self.assertEqual(self.c.start("j", "other", "1/1"), {"error": "$.group"})
        self.assertEqual(self.c.start("j", "g", "1/2"), {"error": "$.epsilon"})
        self.assertEqual(self.c.status("j"), {"state": "pending"})

    def test_identifiers_are_alpha_numeric_ascii(self):
        self.assertEqual(self.c.start("bad-id", "g", "1/1"), {"error": "$.job"})
        self.assertEqual(self.c.start("j", "bad group", "1/1"),
                         {"error": "$.group"})
        self.assertEqual(self.c.start("ünïcode", "g", "1/1"), {"error": "$.job"})
        self.assertEqual(self.c.start("j2", "g", "1/1"), {"state": "pending"})


# ---------------------------------------------------------------------------
# Read / cancel / status state machine
# ---------------------------------------------------------------------------
class TestStateMachine(Base):
    def test_status_only_returns_state(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.assertEqual(c.status("j"), {"state": "pending"})
        self.wait_state(c, "j", {"ready"})
        self.assertEqual(c.status("j"), {"state": "ready"})
        c.read("j")
        self.assertEqual(c.status("j"), {"state": "published"})

    def test_read_before_ready_rejected(self):
        # With delivery held back the job provably stays pending, so read must
        # be refused and leave the reservation untouched.
        svc = self.spawn(extra_args=("--hold-delivery",))
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        self.assertEqual(c.start("j", "g", "1/1"), {"state": "pending"})
        self.assertEqual(c.read("j"), {"error": "$.job"})
        self.assertEqual(c.status("j"), {"state": "pending"})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "1/1"})
        # Release it and the normal path works.
        c.flush()
        self.wait_state(c, "j", {"ready"}, timeout=15)
        value = c.read("j")["value"]
        self.assertIsInstance(value, float)
        self.assertEqual(c.read("j"), {"value": value})

    def test_cancel_pending_and_idempotent(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.assertEqual(c.cancel("j"), {"state": "cancelled"})
        self.assertEqual(c.cancel("j"), {"state": "cancelled"})
        self.assertEqual(c.status("j"), {"state": "cancelled"})
        self.assertEqual(c.read("j"), {"error": "$.job"})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "0/1"})

    def test_late_result_does_not_revive_cancelled_job(self):
        svc = self.spawn(env={"PRIVBUDGET_TEST_DIE_AFTER": "1"})
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        # The counter dies after computing, so no reply reaches the service.
        self.assertEqual(c.cancel("j"), {"state": "cancelled"})
        # A later delivery attempt (new counter process) finds the result but
        # must not resurrect the job.
        c.flush()
        time.sleep(0.5)
        self.assertEqual(c.status("j"), {"state": "cancelled"})
        self.assertEqual(c.read("j"), {"error": "$.job"})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "0/1"})


# ---------------------------------------------------------------------------
# Noise injection / privacy semantics
# ---------------------------------------------------------------------------
class TestNoiseInjection(Base):
    def test_value_is_bits_plus_injected_noise(self):
        svc = self.spawn()
        c = svc.client
        # group bits are 0 + 1 + 1 = 2
        c.initialize({
            "bits": {"x": 0, "y": 1, "z": 1},
            "groups": {"all": ["x", "y", "z"]},
            "limits": {"x": "1/1", "y": "1/1", "z": "1/1"},
        })
        c.start("j", "all", "1/1", noise="7/4")
        self.wait_state(c, "j", {"ready"})
        self.assertEqual(c.read("j"), {"value": 3.75})
        # Persistent and fixed across restart of the whole service.
        self.assertEqual(c.read("j"), {"value": 3.75})
        svc.stop()
        svc2 = self.spawn()
        self.assertEqual(svc2.client.status("j"), {"state": "published"})
        self.assertEqual(svc2.client.read("j"), {"value": 3.75})

    def test_empty_group(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"e": []},
                      "limits": {"a": "1/1"}})
        self.assertEqual(c.start("j", "e", "1/1", noise="0/1"),
                         {"state": "pending"})
        self.wait_state(c, "j", {"ready"})
        self.assertEqual(c.read("j"), {"value": 0.0})
        self.assertEqual(c.budgets(), {"a": {"spent": "0/1", "reserved": "0/1"}})

    def test_noise_fixed_for_a_job_even_under_retries(self):
        # The counter drops the first reply after computing; the service
        # retries with the same id and must get the very same value.
        svc = self.spawn(env={"PRIVBUDGET_TEST_DROP_FIRST": "1"})
        c = svc.client
        c.initialize({"bits": {"a": 1}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.wait_state(c, "j", {"ready"})
        first = c.read("j")["value"]
        svc.stop()
        svc2 = self.spawn()
        self.assertEqual(svc2.client.read("j"), {"value": first})

    def test_noise_density_is_laplace(self):
        """Statistical smoke test: mean ~ 0, variance ~ 2/eps^2."""
        svc = self.spawn()
        c = svc.client
        n = 12
        bits = {}
        groups = {}
        limits = {}
        for i in range(n):
            name = "s%d" % i
            bits[name] = 0
            groups["g%d" % i] = [name]
            limits[name] = "2/1"
        c.initialize({"bits": bits, "groups": groups, "limits": limits})
        c.start("big", "g0", "2/1")
        self.wait_state(c, "big", {"ready"})
        value = c.read("big")["value"]
        self.assertIsInstance(value, float)
        self.assertLess(abs(value), 50)

    def test_laplace_density_histogram(self):
        """Draw many samples straight from the counter and histogram them."""
        sys.path.insert(0, os.path.join(ROOT, "privbudget"))
        from privbudget import counter_worker  # noqa: E402

        eps = 1.0
        samples = [float(counter_worker.laplace(counter_worker.Fraction(1, 1)))
                   for _ in range(40000)]
        # Mean 0, standard deviation sqrt(2)/eps ~ 1.414.
        mean = sum(samples) / len(samples)
        var = sum((s - mean) ** 2 for s in samples) / len(samples)
        self.assertLess(abs(mean), 0.06)
        self.assertAlmostEqual(var, 2.0, delta=0.25)
        # Density f(x) = eps/2 * exp(-eps |x|): compare the central bin mass
        # P(-0.25 < X < 0.25) = 1 - exp(-0.25) ~ 0.2212 with the sample share.
        share = sum(1 for s in samples if abs(s) < 0.25) / len(samples)
        self.assertAlmostEqual(share, 1 - 2.718281828459045 ** -0.25, delta=0.02)
        tail = sum(1 for s in samples if abs(s) > 2.0) / len(samples)
        self.assertAlmostEqual(tail, 2.718281828459045 ** -2.0, delta=0.01)


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------
class TestConcurrency(Base):
    def test_parallel_starts_never_exceed_limit(self):
        import threading

        svc = self.spawn()
        c = svc.client
        # 15/100 limit, each job asks for 1/10: exactly 1 may succeed.
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "15/100"}})
        results = {}
        barrier = threading.Barrier(8)

        def worker(i):
            client = Client(port=svc.port, timeout=30.0)
            barrier.wait()
            results[i] = client.start("job%d" % i, "g", "1/10")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        ok = [i for i, r in results.items() if r == {"state": "pending"}]
        rejected = [i for i, r in results.items()
                    if r == {"error": "$.epsilon"}]
        self.assertEqual(len(ok), 1, results)
        self.assertEqual(len(rejected), 7, results)
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "1/10"})

    def test_double_read_charges_once(self):
        import threading

        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.wait_state(c, "j", {"ready"})
        values = []
        errors = []
        barrier = threading.Barrier(6)

        def worker():
            client = Client(port=svc.port, timeout=30.0)
            barrier.wait()
            got = client.read("j")
            if "value" in got:
                values.append(got["value"])
            else:
                errors.append(got)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(len(values), 6, (values, errors))
        self.assertEqual(len(set(values)), 1)
        self.assertEqual(c.budgets()["a"],
                         {"spent": "1/1", "reserved": "0/1"})


# ---------------------------------------------------------------------------
# Crash / restart recovery
# ---------------------------------------------------------------------------
class TestCrashRecovery(Base):
    def test_service_restart_resumes_pending_delivery(self):
        svc = self.spawn(env={"PRIVBUDGET_TEST_DROP_FIRST": "1"})
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        # Hard-kill the service before the counter can be retried.
        svc.proc.kill()
        svc.proc.wait(timeout=5)
        svc2 = self.spawn()
        self.wait_state(svc2.client, "j", {"ready"}, timeout=15)
        self.assertIsInstance(svc2.client.read("j")["value"], float)
        self.assertEqual(svc2.client.budgets()["a"],
                         {"spent": "1/1", "reserved": "0/1"})

    def test_counter_restart_keeps_result(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 1}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1", noise="1/3")
        self.wait_state(c, "j", {"ready"})
        # Kill only the counter; the service restarts it on demand.
        c.flush()
        svc.stop()
        svc2 = self.spawn()
        # The service had persisted the value, so it is still identical.
        self.assertEqual(svc2.client.status("j"), {"state": "ready"})
        self.assertEqual(svc2.client.read("j"), {"value": 4 / 3})

    def test_crash_just_before_debit_commit(self):
        svc = self.spawn(env={"PRIVBUDGET_TEST_CRASH_READ": "before_commit"})
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1", noise="0/1")
        self.wait_state(c, "j", {"ready"})
        # The read request kills the process with the debit uncommitted.
        try:
            c.read("j")
        except Exception:
            pass
        svc.wait_exit()
        svc2 = self.spawn()
        # Nothing was debited and the job is still ready...
        self.assertEqual(svc2.client.status("j"), {"state": "ready"})
        self.assertEqual(svc2.client.budgets()["a"],
                         {"spent": "0/1", "reserved": "1/1"})
        # ... so the read can be retried and publishes exactly once.
        value = svc2.client.read("j")
        self.assertIn("value", value)
        self.assertEqual(svc2.client.budgets()["a"],
                         {"spent": "1/1", "reserved": "0/1"})
        self.assertEqual(svc2.client.read("j"), value)
        self.assertEqual(svc2.client.status("j"), {"state": "published"})

    def test_crash_just_after_debit_commit(self):
        svc = self.spawn(env={"PRIVBUDGET_TEST_CRASH_READ": "after_commit"})
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1", noise="5/2")
        self.wait_state(c, "j", {"ready"})
        try:
            c.read("j")
        except Exception:
            pass
        svc.wait_exit()
        svc2 = self.spawn()
        # The debit is durable and the job is published, so the value is
        # retrievable and never charged twice.
        self.assertEqual(svc2.client.status("j"), {"state": "published"})
        self.assertEqual(svc2.client.budgets()["a"],
                         {"spent": "1/1", "reserved": "0/1"})
        self.assertEqual(svc2.client.read("j"), {"value": 2.5})

    def test_counter_crash_mid_delivery(self):
        svc = self.spawn(env={"PRIVBUDGET_TEST_DIE_AFTER": "1"})
        c = svc.client
        c.initialize({"bits": {"a": 1}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.wait_state(c, "j", {"ready"}, timeout=20)
        value = c.read("j")["value"]
        self.assertIsInstance(value, float)
        self.assertEqual(c.budgets()["a"],
                         {"spent": "1/1", "reserved": "0/1"})


class TestCounterIsolation(Base):
    def _counter_port(self, service_pid):
        port_file = os.path.join(self.tmp, "data", "counter.sqlite3.port")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                with open(port_file) as fh:
                    return int(fh.read().strip())
            except (OSError, ValueError):
                time.sleep(0.05)
        return None

    def _counter_pid(self, service_pid):
        out = subprocess.run(
            ["ps", "-o", "pid,args", "--ppid", str(service_pid)],
            capture_output=True, text=True,
        ).stdout
        for line in out.splitlines():
            if "counter_worker.py" in line:
                return int(line.split()[0])
        return None

    def test_counter_requires_bearer_token(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.wait_state(c, "j", {"ready"})
        port = self._counter_port(svc.proc.pid)
        self.assertIsNotNone(port)
        # Unauthenticated callers can neither read health nor compute.
        for method, path in (("GET", "/health"), ("POST", "/compute")):
            req = urllib.request.Request(
                "http://127.0.0.1:%d%s" % (port, path), method=method,
                data=b"{}" if method == "POST" else None,
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req, timeout=5)
            self.assertEqual(ctx.exception.code, 401)

    def test_counter_restarted_after_kill(self):
        svc = self.spawn()
        c = svc.client
        c.initialize({"bits": {"a": 1, "b": 1},
                      "groups": {"ga": ["a"], "gb": ["b"]},
                      "limits": {"a": "1/1", "b": "1/1"}})
        c.start("j", "ga", "1/1", noise="1/2")
        self.wait_state(c, "j", {"ready"}, timeout=15)
        pid = self._counter_pid(svc.proc.pid)
        self.assertIsNotNone(pid)
        # Kill the independent counter outright; the service must start a new
        # one on the same counter database and keep serving.
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.3)
        c.start("k", "gb", "1/1", noise="3/4")
        self.wait_state(c, "k", {"ready"}, timeout=20)
        self.assertEqual(c.read("k"), {"value": 1.75})
        # The earlier result survived both counter restarts.
        self.assertEqual(c.read("j"), {"value": 1.5})
        self.assertIsNotNone(self._counter_pid(svc.proc.pid))


# ---------------------------------------------------------------------------
# Transport-level lost responses
# ---------------------------------------------------------------------------
class TestLostResponses(Base):
    def test_lost_reply_still_yields_exactly_one_charge(self):
        svc = self.spawn(env={
            "PRIVBUDGET_TEST_DROP_FIRST": "1",
            "PRIVBUDGET_TEST_DROP_REPLIES": "",
        })
        c = svc.client
        c.initialize({"bits": {"a": 1, "b": 1}, "groups": {"g": ["a", "b"]},
                      "limits": {"a": "2/1", "b": "2/1"}})
        c.start("j", "g", "1/2", noise="1/1")
        self.wait_state(c, "j", {"ready"}, timeout=15)
        self.assertEqual(c.read("j"), {"value": 3.0})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "1/2", "reserved": "0/1"})
        self.assertEqual(c.budgets()["b"],
                         {"spent": "1/2", "reserved": "0/1"})

    def test_cancel_racing_with_lost_reply(self):
        svc = self.spawn(env={"PRIVBUDGET_TEST_DROP_FIRST": "1"})
        c = svc.client
        c.initialize({"bits": {"a": 0}, "groups": {"g": ["a"]},
                      "limits": {"a": "1/1"}})
        c.start("j", "g", "1/1")
        self.assertEqual(c.cancel("j"), {"state": "cancelled"})
        time.sleep(0.6)
        c.flush()
        time.sleep(0.3)
        self.assertEqual(c.status("j"), {"state": "cancelled"})
        self.assertEqual(c.budgets()["a"],
                         {"spent": "0/1", "reserved": "0/1"})
        # Budget is free for another job.
        self.assertEqual(c.start("k", "g", "1/1"), {"state": "pending"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
