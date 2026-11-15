"""Budget service core: per-subject differential-privacy budget accounting.

Design notes
------------
* All authoritative state lives in ``budget.sqlite3`` (WAL, synchronous=FULL).
  Every state transition is a single ``BEGIN IMMEDIATE`` transaction, so the
  service behaves as if all operations were serialized, even though HTTP
  requests are handled on several threads.
* Jobs are started optimistically: ``start`` only validates, reserves and
  durably enqueues a compute request (``outbox``), then returns
  ``{"state": "pending"}``.  A background delivery thread talks to the
  independent counter over HTTP.  If a response is lost, the same request id
  is retransmitted; the counter's durable dedupe makes that safe.
* A job's noisy value is persisted by the counter and *also* by the service
  once received.  The service is the only party allowed to reveal it, and
  only through ``read`` on a ``ready`` job.
* ``read`` atomically flips reservations into spend and marks the job
  ``published``.  A crash before COMMIT leaves the job ``ready`` with
  reservations intact (retry works); a crash after COMMIT leaves it published
  with spend charged.  Both are exercised by the acceptance tests.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
from fractions import Fraction

from .errors import (
    Reject,
    frac_str,
    ident,
    is_int,
    parse_fraction,
    parse_positive_fraction,
    quote,
)
from .rpc import CounterLink, RpcLost, RpcProtocolError

PENDING = "pending"
READY = "ready"
PUBLISHED = "published"
CANCELLED = "cancelled"

STATES = {PENDING, READY, PUBLISHED, CANCELLED}


def _err(path):
    return {"error": path}


class BudgetService:
    def __init__(self, data_dir, counter_link=None, deliver=True):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "budget.sqlite3")
        self.counter_db_path = os.path.join(self.data_dir, "counter.sqlite3")
        self.test_noise = os.environ.get("PRIVBUDGET_TEST_NOISE") == "1"
        self._crash_read = os.environ.get("PRIVBUDGET_TEST_CRASH_READ", "")
        self.hold_delivery = (
            os.environ.get("PRIVBUDGET_TEST_HOLD_DELIVERY") == "1"
        )
        self._lock = threading.RLock()
        self._secure_dir()
        self._conn = sqlite3.connect(
            self.db_path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._init_schema()
        self._secure_files()
        if counter_link is None:
            counter_args = []
            if self.test_noise:
                counter_args.append("--test-noise")
            drop = os.environ.get("PRIVBUDGET_TEST_DROP_REPLIES", "")
            if drop:
                counter_args += ["--drop-replies", drop]
            drop_first = os.environ.get("PRIVBUDGET_TEST_DROP_FIRST", "")
            if drop_first:
                counter_args += ["--drop-first", drop_first]
            die_after = os.environ.get("PRIVBUDGET_TEST_DIE_AFTER", "")
            if die_after:
                counter_args += ["--die-after-compute", die_after]
            seed = os.environ.get("PRIVBUDGET_TEST_SEED", "")
            if seed:
                counter_args += ["--seed", seed]
            counter_link = CounterLink(self.counter_db_path,
                                       argv_extra=counter_args)
        counter_link.test_noise = self.test_noise
        self.counter_link = counter_link
        self._stop = threading.Event()
        self._wake = threading.Condition()
        self._deliverer = None
        if deliver and not self.hold_delivery:
            self._deliverer = threading.Thread(
                target=self._delivery_loop, name="privbudget-delivery", daemon=True
            )
            self._deliverer.start()

    # ------------------------------------------------------------------ setup
    def _secure_dir(self):
        """Best effort: only this user may read the private state."""
        try:
            os.chmod(self.data_dir, 0o700)
        except OSError:
            pass

    def _secure_files(self):
        for name in os.listdir(self.data_dir):
            if name.startswith(("budget.sqlite3", "counter.sqlite3")):
                try:
                    os.chmod(os.path.join(self.data_dir, name), 0o600)
                except OSError:
                    pass

    def _init_schema(self):
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            c.execute(
                """CREATE TABLE IF NOT EXISTS jobs (
                       seq       INTEGER PRIMARY KEY AUTOINCREMENT,
                       job       TEXT UNIQUE NOT NULL,
                       grp       TEXT NOT NULL,
                       eps_p     INTEGER NOT NULL,
                       eps_q     INTEGER NOT NULL,
                       state     TEXT NOT NULL,
                       z_num     TEXT,
                       z_den     TEXT
                   )"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS reservations (
                       job    TEXT NOT NULL,
                       subject TEXT NOT NULL,
                       p INTEGER NOT NULL,
                       q INTEGER NOT NULL,
                       PRIMARY KEY (job, subject)
                   )"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS spent (
                       subject TEXT PRIMARY KEY,
                       p INTEGER NOT NULL,
                       q INTEGER NOT NULL
                   )"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS outbox (
                       req_id    TEXT PRIMARY KEY,
                       seq       INTEGER NOT NULL UNIQUE,
                       job       TEXT NOT NULL,
                       payload   TEXT NOT NULL,
                       status    TEXT NOT NULL
                   )"""
            )
        finally:
            c.execute("COMMIT")

    # --------------------------------------------------------------- helpers
    def _meta(self, key, default=None):
        row = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def _set_meta(self, key, value):
        self._conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def _config(self):
        raw = self._meta("config")
        return json.loads(raw) if raw else None

    def _initialized(self):
        return self._meta("initialized") == "1"

    def _require_initialized(self):
        if not self._initialized():
            raise Reject("$")

    def _limits(self):
        cfg = self._config()
        return cfg["limits"]

    def _groups(self):
        cfg = self._config()
        return cfg["groups"]

    def _bits_values(self, members):
        bits = self._config()["bits"]
        return [bits[m] for m in members]

    def _spent_fraction(self, subject):
        row = self._conn.execute(
            "SELECT p,q FROM spent WHERE subject=?", (subject,)
        ).fetchone()
        return Fraction(row["p"], row["q"]) if row else Fraction(0)

    def _reserved_fraction(self, subject):
        rows = self._conn.execute(
            "SELECT p,q FROM reservations WHERE subject=?", (subject,)
        ).fetchall()
        total = Fraction(0)
        for r in rows:
            total += Fraction(r["p"], r["q"])
        return total

    def _job_row(self, job):
        return self._conn.execute("SELECT * FROM jobs WHERE job=?", (job,)).fetchone()

    def _add_spent(self, subject, amount):
        current = self._spent_fraction(subject)
        new = current + amount
        self._conn.execute(
            "INSERT INTO spent(subject,p,q) VALUES(?,?,?) "
            "ON CONFLICT(subject) DO UPDATE SET p=excluded.p, q=excluded.q",
            (subject, new.numerator, new.denominator),
        )

    def _release_reservations(self, job):
        self._conn.execute("DELETE FROM reservations WHERE job=?", (job,))

    # ------------------------------------------------------------ initialize
    def initialize(self, config):
        """One-shot configuration.  Returns ``{"state": "initialized"}``.

        Validates before taking the lock, then re-checks under a write
        transaction: only the first successful call may ever win, including
        across concurrent requests and process restarts.
        """
        if self.is_initialized():
            return _err("$")
        try:
            clean = validate_config(config)
        except Reject as exc:
            return _err(exc.path)
        with self._lock:
            if self._initialized():
                return _err("$")
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._meta("initialized") == "1":
                    self._conn.execute("ROLLBACK")
                    return _err("$")
                self._set_meta("initialized", "1")
                self._set_meta("config", json.dumps(clean, separators=(",", ":")))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self._notify()
        return {"state": "initialized"}

    def is_initialized(self):
        with self._lock:
            return self._initialized()

    # ------------------------------------------------------------------ start
    def start(self, job, group, epsilon, noise=None):
        try:
            ident(job, "$.job")
            ident(group, "$.group")
            eps = parse_positive_fraction(epsilon, "$.epsilon")
        except Reject as exc:
            return _err(exc.path)
        if noise is not None:
            # Test-only: an explicit noise value must be a well-formed rational.
            if not self.test_noise:
                return _err("$.noise")
            try:
                noise = _parse_noise(noise)
            except Reject as exc:
                return _err(exc.path)
            except (ValueError, ZeroDivisionError, TypeError):
                return _err("$.noise")
        with self._lock:
            try:
                self._require_initialized()
            except Reject as exc:
                return _err(exc.path)
            existing = self._job_row(job)
            if existing is not None:
                if existing["grp"] != group:
                    return _err("$.group")
                if Fraction(existing["eps_p"], existing["eps_q"]) != eps:
                    return _err("$.epsilon")
                return {"state": existing["state"]}

            groups = self._groups()
            if group not in groups:
                return _err("$.group")
            members = groups[group]
            limits = self._limits()

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Re-check under the write lock (another thread may have won).
                existing = self._job_row(job)
                if existing is not None:
                    if existing["grp"] != group:
                        self._conn.execute("ROLLBACK")
                        return _err("$.group")
                    if Fraction(existing["eps_p"], existing["eps_q"]) != eps:
                        self._conn.execute("ROLLBACK")
                        return _err("$.epsilon")
                    state = existing["state"]
                    self._conn.execute("ROLLBACK")
                    return {"state": state}

                # Atomic affordability check for every member.
                for subject in members:
                    cap = Fraction(limits[subject])
                    used = self._spent_fraction(subject) + self._reserved_fraction(subject)
                    if used + eps > cap:
                        self._conn.execute("ROLLBACK")
                        return _err("$.epsilon")

                self._conn.execute(
                    "INSERT INTO jobs(job,grp,eps_p,eps_q,state) VALUES(?,?,?,?,?)",
                    (job, group, eps.numerator, eps.denominator, PENDING),
                )
                for subject in members:
                    self._conn.execute(
                        "INSERT INTO reservations(job,subject,p,q) VALUES(?,?,?,?)",
                        (job, subject, eps.numerator, eps.denominator),
                    )
                values = self._bits_values(members)
                req_id = secrets.token_hex(16)
                payload = {
                    "id": req_id,
                    "job": job,
                    "group_name": group,
                    "group": list(members),
                    "subjects": list(members),
                    "values": values,
                    "epsilon": {"p": eps.numerator, "q": eps.denominator},
                }
                if noise is not None:
                    payload["noise"] = noise
                seq = self._conn.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 FROM outbox"
                ).fetchone()[0]
                self._conn.execute(
                    "INSERT INTO outbox(req_id,seq,job,payload,status) VALUES(?,?,?,?,?)",
                    (req_id, seq, job,
                     json.dumps(payload, separators=(",", ":")), "pending"),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self._notify()
        return {"state": PENDING}

    # ----------------------------------------------------------------- status
    def status(self, job):
        try:
            ident(job, "$.job")
        except Reject as exc:
            return _err(exc.path)
        with self._lock:
            try:
                self._require_initialized()
            except Reject as exc:
                return _err(exc.path)
            row = self._job_row(job)
            if row is None:
                return _err("$.job")
            return {"state": row["state"]}

    # ------------------------------------------------------------------- read
    def read(self, job):
        try:
            ident(job, "$.job")
        except Reject as exc:
            return _err(exc.path)
        with self._lock:
            try:
                self._require_initialized()
            except Reject as exc:
                return _err(exc.path)
            row = self._job_row(job)
            if row is None:
                return _err("$.job")
            if row["state"] == PUBLISHED:
                return {"value": _real(row["z_num"], row["z_den"])}
            if row["state"] != READY:
                return _err("$.job")

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._job_row(job)
                if row is None or row["state"] != READY:
                    # State changed between checks; be conservative.
                    self._conn.execute("ROLLBACK")
                    if row is not None and row["state"] == PUBLISHED:
                        return {"value": _real(row["z_num"], row["z_den"])}
                    return _err("$.job")
                amount = Fraction(row["eps_p"], row["eps_q"])
                reservations = self._conn.execute(
                    "SELECT subject,p,q FROM reservations WHERE job=?", (job,)
                ).fetchall()
                for r in reservations:
                    if Fraction(r["p"], r["q"]) != amount:
                        # Defensive: the reserve table must mirror the job.
                        self._conn.execute("ROLLBACK")
                        raise RuntimeError("reservation/bookkeeping mismatch")
                for r in reservations:
                    self._add_spent(r["subject"], Fraction(r["p"], r["q"]))
                self._conn.execute("DELETE FROM reservations WHERE job=?", (job,))
                self._conn.execute(
                    "UPDATE jobs SET state=? WHERE job=?", (PUBLISHED, job)
                )
                if self._crash_read == "before_commit":
                    # Fault injection: die with the transaction uncommitted.
                    os._exit(137)
                self._conn.execute("COMMIT")
                if self._crash_read == "after_commit":
                    # Fault injection: die after the debit is durable.
                    os._exit(137)
            except Exception:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            value = _real(row["z_num"], row["z_den"])
            return {"value": value}

    # ----------------------------------------------------------------- cancel
    def cancel(self, job):
        try:
            ident(job, "$.job")
        except Reject as exc:
            return _err(exc.path)
        with self._lock:
            try:
                self._require_initialized()
            except Reject as exc:
                return _err(exc.path)
            row = self._job_row(job)
            if row is None:
                return _err("$.job")
            if row["state"] == PUBLISHED:
                return _err("$.job")
            if row["state"] == CANCELLED:
                return {"state": CANCELLED}
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._job_row(job)
                if row is None:
                    self._conn.execute("ROLLBACK")
                    return _err("$.job")
                if row["state"] == PUBLISHED:
                    self._conn.execute("ROLLBACK")
                    return _err("$.job")
                if row["state"] == CANCELLED:
                    self._conn.execute("ROLLBACK")
                    return {"state": CANCELLED}
                self._release_reservations(job)
                self._conn.execute(
                    "UPDATE jobs SET state=? WHERE job=?", (CANCELLED, job)
                )
                self._conn.execute(
                    "UPDATE outbox SET status='cancelled' "
                    "WHERE job=? AND status='pending'",
                    (job,),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return {"state": CANCELLED}

    # ---------------------------------------------------------------- budgets
    def budgets(self):
        with self._lock:
            try:
                self._require_initialized()
            except Reject as exc:
                return _err(exc.path)
            self._conn.execute("BEGIN")  # consistent WAL snapshot
            try:
                limits = self._limits()
                out = {}
                for subject in limits:
                    spent = self._spent_fraction(subject)
                    reserved = self._reserved_fraction(subject)
                    out[subject] = {
                        "spent": frac_str(spent),
                        "reserved": frac_str(reserved),
                    }
                self._conn.execute("COMMIT")
            except Exception:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            return out

    # ----------------------------------------------------------- result path
    def _apply_result(self, job, z_num, z_den):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._job_row(job)
                if row is not None and row["state"] == PENDING:
                    self._conn.execute(
                        "UPDATE jobs SET state=?, z_num=?, z_den=? WHERE job=?",
                        (READY, str(z_num), str(z_den), job),
                    )
                    changed = True
                else:
                    changed = False
                self._conn.execute(
                    "UPDATE outbox SET status='done' WHERE job=? AND status='pending'",
                    (job,),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        if changed:
            self._notify()

    # -------------------------------------------------------------- delivery
    def _notify(self):
        with self._wake:
            self._wake.notify_all()

    def _delivery_loop(self):
        while not self._stop.is_set():
            if self.counter_link.down_for_repair:
                # The counter's process died after it may have stored an
                # answer.  Restart it (results survive in its database) before
                # retrying, so we do not re-issue against a dead endpoint.
                try:
                    self.counter_link.repair()
                except Exception:
                    with self._wake:
                        self._wake.wait(0.5)
                    continue
            try:
                rows = self._pending_requests()
            except Exception:
                rows = []
            if not rows:
                with self._wake:
                    self._wake.wait(0.25)
                continue
            progressed = False
            for row in rows:
                if self._stop.is_set():
                    return
                if self._deliver_one(row):
                    progressed = True
            if not progressed:
                # Avoid a hot loop if the counter is down or unresponsive.
                with self._wake:
                    self._wake.wait(0.2)

    def _pending_requests(self):
        with self._lock:
            return self._conn.execute(
                "SELECT req_id,job,payload FROM outbox "
                "WHERE status='pending' ORDER BY seq LIMIT 16"
            ).fetchall()

    def _deliver_one(self, row):
        """Try to deliver one compute request.  True if it made progress."""
        job = row["job"]
        with self._lock:
            jr = self._job_row(job)
            if jr is None or jr["state"] != PENDING:
                self._conn.execute(
                    "UPDATE outbox SET status='done' WHERE req_id=?", (row["req_id"],)
                )
                return True
            payload = json.loads(row["payload"])
        delay = 0.05
        for _ in range(6):
            if self._stop.is_set():
                return False
            with self._lock:
                jr = self._job_row(job)
                if jr is None or jr["state"] != PENDING:
                    self._conn.execute(
                        "UPDATE outbox SET status='done' WHERE req_id=?",
                        (row["req_id"],),
                    )
                    return True
            try:
                result = self.counter_link.call(payload)
            except RpcLost:
                # Unknown outcome: retry the *same* id; the counter dedupes.
                time.sleep(delay)
                delay = min(delay * 1.6, 0.5)
                continue
            except RpcProtocolError:
                time.sleep(0.2)
                continue
            if not isinstance(result, dict) or "z_num" not in result:
                time.sleep(0.2)
                continue
            self._apply_result(job, result["z_num"], result["z_den"])
            return True
        # Leave the outbox entry pending; a later pass (or restart) retries it.
        return False

    def replay_pending(self):
        """Force a delivery pass now (used after restart and by tests)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT req_id,job,payload FROM outbox WHERE status='pending' ORDER BY seq"
            ).fetchall()
        for row in rows:
            if self._stop.is_set():
                return
            self._deliver_one(row)

    # ------------------------------------------------------------------ close
    def close(self):
        self._stop.set()
        self._notify()
        if self._deliverer is not None:
            self._deliverer.join(timeout=5)
        try:
            self.counter_link.stop()
        except Exception:
            pass
        try:
            self._conn.close()
        except Exception:
            pass


def _real(z_num, z_den):
    """Render the exact rational as a JSON-safe float.

    Absurdly large magnitudes (only reachable with an extreme epsilon or an
    injected test value) are clamped to the largest finite double, because
    JSON has no representation for infinity.
    """
    import sys

    f = Fraction(int(z_num), int(z_den))
    try:
        value = float(f)
    except OverflowError:
        value = sys.float_info.max if f > 0 else -sys.float_info.max
    if value != value:  # NaN guard, should be impossible
        value = 0.0
    return value


def _parse_noise(value):
    """TEST: accept a signed rational "p/q", an int, or {"p","q"}."""
    if isinstance(value, bool):
        raise ValueError("bool is not a number")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        f = parse_fraction(value, "$.noise", positive_num_den=False)
        return "%d/%d" % (f.numerator, f.denominator)
    if isinstance(value, dict):
        p = value.get("p")
        q = value.get("q")
        if not is_int(p) or not is_int(q) or q <= 0:
            raise ValueError("bad noise fraction")
        f = Fraction(p, q)
        return "%d/%d" % (f.numerator, f.denominator)
    raise ValueError("bad noise")


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------
def validate_config(config):
    """Validate the one-shot configuration and return a normalized copy.

    Normal form::

        {"bits":   {subject: 0|1},
         "groups": {name: [subject, ...]},        # members must exist, unique
         "limits": {subject: "p/q", ...}}         # one per subject, p,q > 0

    Raises :class:`Reject` carrying the zero-based JSONPath of the first
    offending value found.
    """
    if not isinstance(config, dict):
        raise Reject("$")
    for key in ("bits", "groups", "limits"):
        if key not in config:
            raise Reject("$" + "." + key)

    bits_in = config["bits"]
    if not isinstance(bits_in, dict):
        raise Reject("$.bits")
    bits = {}
    for key, value in bits_in.items():
        path = "$.bits" + quote(key)
        ident(key, path)
        if not is_int(value) or value not in (0, 1):
            raise Reject(path)
        bits[key] = value

    groups_in = config["groups"]
    if not isinstance(groups_in, dict):
        raise Reject("$.groups")
    groups = {}
    for name, members in groups_in.items():
        path = "$.groups" + quote(name)
        ident(name, path)
        if not isinstance(members, list):
            raise Reject(path)
        seen = set()
        clean_members = []
        for index, member in enumerate(members):
            mpath = path + "[%d]" % index
            ident(member, mpath)
            if member not in bits:
                raise Reject(mpath)
            if member in seen:
                raise Reject(mpath)
            seen.add(member)
            clean_members.append(member)
        groups[name] = clean_members

    limits_in = config["limits"]
    if not isinstance(limits_in, dict):
        raise Reject("$.limits")
    limits = {}
    for key, value in limits_in.items():
        path = "$.limits" + quote(key)
        ident(key, path)
        if key not in bits:
            raise Reject(path)
        limits[key] = parse_positive_fraction(value, path)
    for subject in bits:
        if subject not in limits:
            raise Reject("$.limits")

    return {
        "bits": {k: bits[k] for k in bits},
        "groups": groups,
        "limits": {k: frac_str(limits[k]) for k in bits},
    }
