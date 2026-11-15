#!/usr/bin/env python3
"""Independent, persistent counting process (HTTP over loopback).

The budget service starts this process as a *separate OS process*.  They never
share memory or a database file: the only channel is loopback HTTP, guarded by
a bearer token the service generates at startup, so tentative (unpublished)
noisy values can only be obtained by the budget service.

Durability contract
-------------------
``counter.sqlite3`` holds, per job, the one noisy value z that was drawn for
it, plus a per-request-id reply cache.  Consequences:

* the value for a job is fixed for good, even across restarts of this
  process (results stay reachable only through the budget service);
* if the service retransmits a request because the response was lost, the
  counter returns the *cached* answer -- no second noise draw, no double
  accounting.

The value is returned as an exact rational ``z_num / z_den`` so the service's
bookkeeping stays exact.  Noise is Laplace(0, 1/eps), whose density is
``(eps/2) * exp(-eps*|x|)``: half-normal magnitude ``-ln(U)/eps`` with a
random sign.

Test hook: when started with ``--test-noise`` (or
``PRIVBUDGET_TEST_NOISE=1``), a request may carry ``"noise": "p/q"`` which is
used instead of drawing.  This exists purely so acceptance tests can inject
deterministic noise; it is off by default and the budget service never sets
it on its own.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
from fractions import Fraction
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY = 1 << 20
ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class Store:
    def __init__(self, path):
        self.path = path
        conn = self._conn()
        conn.execute(
            """CREATE TABLE IF NOT EXISTS results (
                   req_id     TEXT PRIMARY KEY,
                   job        TEXT NOT NULL,
                   group_json TEXT NOT NULL,
                   bits       INTEGER NOT NULL,
                   eps_num    INTEGER NOT NULL,
                   eps_den    INTEGER NOT NULL,
                   z_num      TEXT NOT NULL,
                   z_den      TEXT NOT NULL
               )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS job_values (
                   job       TEXT PRIMARY KEY,
                   eps_num   INTEGER NOT NULL,
                   eps_den   INTEGER NOT NULL,
                   z_num     TEXT NOT NULL,
                   z_den     TEXT NOT NULL
               )"""
        )
        conn.close()

    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def compute(self, req_id, job, group, bits, eps, injected_noise):
        """Return ``(z_num, z_den)``; atomic and idempotent per req_id/job."""
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT z_num, z_den FROM results WHERE req_id=?", (req_id,)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                return row

            row = conn.execute(
                "SELECT z_num, z_den FROM job_values WHERE job=?", (job,)
            ).fetchone()
            if row is not None:
                z = (row[0], row[1])
            else:
                if injected_noise is None:
                    noise = laplace(eps)
                else:
                    noise = Fraction(injected_noise)
                z = Fraction(bits) + noise
                z = (str(z.numerator), str(z.denominator))
                conn.execute(
                    "INSERT INTO job_values VALUES (?,?,?,?,?)",
                    (job, eps.numerator, eps.denominator, z[0], z[1]),
                )
            conn.execute(
                "INSERT INTO results VALUES (?,?,?,?,?,?,?,?)",
                (req_id, job, json.dumps(group), bits, eps.numerator,
                 eps.denominator, z[0], z[1]),
            )
            conn.execute("COMMIT")
            return z
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()


def laplace(eps):
    """Draw from Laplace(0, 1/eps) exactly enough, as a Fraction."""
    # Magnitude ~ Exp(1/eps): -ln(U)/eps with U uniform on (0,1].
    u = 1.0 - random.random()
    if u <= 0.0:
        u = sys.float_info.min
    mag = -math.log(u) / float(eps)
    if random.getrandbits(1):
        mag = -mag
    return Fraction(mag).limit_denominator(10 ** 12)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "privbudget-counter/1"

    def log_message(self, fmt, *args):  # silence
        pass

    def _authorized(self):
        token = self.server.token
        return self.headers.get("Authorization", "") == "Bearer " + token

    def _send(self, code, obj):
        data = json.dumps(obj, separators=(",", ":")).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except OSError:
            pass

    def do_GET(self):
        if self.path == "/health":
            if not self._authorized():
                self._send(401, {"error": "unauthorized"})
                return
            self._send(200, {"ok": True})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/compute":
            self._send(404, {"error": "not found"})
            return
        if not self._authorized():
            self._send(401, {"error": "unauthorized"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > MAX_BODY:
            self._send(413, {"error": "bad length"})
            return
        raw = self.rfile.read(length)
        try:
            msg = json.loads(raw)
            req_id = msg["id"]
            job = msg["job"]
            group = msg["group"]
            eps = Fraction(int(msg["epsilon"]["p"]), int(msg["epsilon"]["q"]))
            if not isinstance(req_id, str) or not ID_RE.match(req_id):
                raise ValueError("bad id")
            if not isinstance(job, str):
                raise ValueError("bad job")
            if not isinstance(group, list) or not all(
                isinstance(s, str) for s in group
            ):
                raise ValueError("bad group")
            if eps <= 0:
                raise ValueError("bad epsilon")
            bits_in = msg.get("bits")
            if bits_in is None:
                values = msg.get("values")
                if not isinstance(values, list) or not all(
                    isinstance(v, int) and not isinstance(v, bool) and v in (0, 1)
                    for v in values
                ):
                    raise ValueError("bad values")
                if len(values) != len(group):
                    raise ValueError("values/group length mismatch")
                bits = sum(values)
            elif isinstance(bits_in, int) and not isinstance(bits_in, bool) and bits_in >= 0:
                bits = bits_in
            else:
                raise ValueError("bad bits")
            noise = None
            if "noise" in msg and msg["noise"] is not None:
                if not self.server.test_noise:
                    raise ValueError("noise injection disabled")
                noise = parse_injected_noise(msg["noise"])
        except Exception as exc:  # noqa: BLE001
            self._send(400, {"ok": False, "error": "bad request: %s" % exc})
            return
        try:
            z_num, z_den = self.server.store.compute(
                req_id, job, group, bits, eps, noise
            )
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"ok": False, "error": "%s: %s" % (type(exc).__name__, exc)})
            return
        if self._must_drop(req_id):
            # Test hook: compute (durably) then hang up without answering.
            try:
                self.close_connection = True
                self.connection.shutdown(2)
            except OSError:
                pass
            return
        self._send(200, {"ok": True, "result": {"z_num": z_num, "z_den": z_den}})

    def _must_drop(self, req_id):
        drops = getattr(self.server, "drop_replies", None)
        with self.server.drop_lock:
            hit = False
            if drops and req_id in drops and req_id not in self.server.dropped_once:
                self.server.dropped_once.add(req_id)
                hit = True
            if self.server.drop_first > 0:
                self.server.drop_first -= 1
                hit = True
            if self.server.die_after_compute > 0:
                self.server.die_after_compute -= 1
                _consume_faults(self.server)
                # Simulate losing the whole process right after the counter
                # durably fixed the answer but before the reply was sent.
                os._exit(70)
            if hit:
                _consume_faults(self.server)
        return hit


def _consume_faults(server):
    try:
        with open(server.fault_marker, "w") as fh:
            fh.write("spent\n")
    except OSError:
        pass


def parse_injected_noise(value):
    if isinstance(value, bool):
        raise ValueError("bad noise")
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, str):
        return Fraction(value)
    if isinstance(value, dict):
        return Fraction(int(value["p"]), int(value["q"]))
    raise ValueError("bad noise")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("PRIVBUDGET_COUNTER_DB"))
    ap.add_argument("--token", default=os.environ.get("PRIVBUDGET_COUNTER_TOKEN"))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--test-noise", action="store_true",
                    default=os.environ.get("PRIVBUDGET_TEST_NOISE") == "1")
    ap.add_argument("--drop-replies", default="",
                    help="TEST: comma list of request ids whose reply is "
                         "withheld once (simulates a lost response)")
    ap.add_argument("--drop-first", type=int, default=0,
                    help="TEST: withhold the reply to the first N requests "
                         "(after durably computing), simulating lost responses")
    ap.add_argument("--die-after-compute", type=int, default=0,
                    help="TEST: exit(70) after durably computing an answer, "
                         "before replying, for the first N requests")
    ap.add_argument("--seed", default=None,
                    help="TEST: seed the RNG so noise draws repeat")
    ap.add_argument("--parent-pid", type=int, default=0,
                    help="exit if the parent process disappears")
    args = ap.parse_args(argv)
    if not args.db or not args.token:
        sys.stderr.write("counter: --db and --token are required\n")
        return 2
    if args.seed is not None:
        random.seed(int(args.seed))

    store = Store(args.db)
    try:
        os.chmod(args.db, 0o600)
    except OSError:
        pass
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    httpd.daemon_threads = True
    httpd.token = args.token
    httpd.store = store
    httpd.test_noise = args.test_noise
    httpd.drop_replies = {s for s in args.drop_replies.split(",") if s}
    httpd.dropped_once = set()
    httpd.drop_first = args.drop_first
    httpd.die_after_compute = args.die_after_compute
    httpd.drop_lock = threading.Lock()
    # One-shot test faults must not repeat after the counter restarts, so a
    # marker file records that they were already spent.
    httpd.fault_marker = args.db + ".fault-injected"
    if os.path.exists(httpd.fault_marker):
        httpd.drop_replies = set()
        httpd.drop_first = 0
        httpd.die_after_compute = 0

    # Handshake on stdout: the parent reads exactly this one line.  The chosen
    # port is also recorded next to the database (not a secret) so operators
    # and tests can find the endpoint; every request still needs the token.
    port = httpd.server_address[1]
    try:
        port_file = args.db + ".port"
        with open(port_file, "w") as fh:
            fh.write(str(port))
        os.chmod(port_file, 0o600)
    except OSError:
        pass
    sys.stdout.write(json.dumps({"port": port}) + "\n")
    sys.stdout.flush()
    if args.parent_pid:
        watchdog = threading.Thread(
            target=_watch_parent, args=(args.parent_pid,), daemon=True
        )
        watchdog.start()
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


def _watch_parent(pid):
    """Exit when the budget service process goes away (no orphans)."""
    while True:
        time.sleep(0.5)
        try:
            os.kill(pid, 0)
        except OSError:
            os._exit(0)
        except Exception:
            return


if __name__ == "__main__":
    sys.exit(main())
