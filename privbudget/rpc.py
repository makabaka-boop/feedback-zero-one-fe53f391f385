"""HTTP transport to the independent counting process.

The counter is a separate OS process listening on loopback.  Every request
must carry the shared bearer token that the budget service generated when it
started the counter, so no other local process can obtain tentative results.

A *lost response* (connection reset, timeout, process death) surfaces as
:class:`RpcLost`.  Callers must treat that as "unknown outcome" and retry the
same request id; the counter's durable dedupe table makes retries safe.
"""

from __future__ import annotations

import http.client
import json
import os
import secrets
import select
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit


class RpcLost(Exception):
    """The response to a request was lost; the outcome is unknown."""


class RpcProtocolError(Exception):
    """The counter answered with something uninterpretable."""


_HERE = os.path.dirname(os.path.abspath(__file__))


def _counter_script():
    return os.path.join(_HERE, "counter_worker.py")


class HttpTransport:
    def __init__(self, host, port, token, timeout=10.0):
        self.host = host
        self.port = port
        self.token = token
        self.timeout = timeout

    def call(self, payload, timeout=None):
        body = json.dumps(payload, separators=(",", ":")).encode()
        conn = http.client.HTTPConnection(
            self.host, self.port, timeout=self.timeout if timeout is None else timeout
        )
        status = None
        data = b""
        try:
            conn.request(
                "POST",
                "/compute",
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + self.token,
                },
            )
            resp = conn.getresponse()
            status = resp.status
            data = resp.read()
        except (OSError, http.client.HTTPException) as exc:
            # The outcome may still have been durably recorded by the counter;
            # the caller retries with the same id, which is idempotent.
            raise RpcLost(str(exc))
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if status == 401:
            raise RpcProtocolError("unauthorized")
        if status != 200:
            # 5xx / 408: the counter may have committed; retry the same id.
            if status >= 500 or status == 408:
                raise RpcLost("http status %s" % status)
            raise RpcProtocolError("http status %s" % status)
        try:
            msg = json.loads(data)
        except ValueError:
            raise RpcProtocolError("malformed counter reply")
        if not msg.get("ok"):
            raise RpcProtocolError(str(msg.get("error")))
        result = msg.get("result")
        if not isinstance(result, dict) or "z_num" not in result:
            raise RpcProtocolError("counter reply lacks a result")
        return result

    def health(self, timeout=2.0):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            conn.request("GET", "/health",
                         headers={"Authorization": "Bearer " + self.token})
            resp = conn.getresponse()
            resp.read()
            return resp.status == 200
        except (OSError, http.client.HTTPException):
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass


class CounterLink:
    """Starts (and restarts) the counter process and talks to it over HTTP."""

    def __init__(self, db_path, python=None, script=None, url=None, token=None,
                 startup_timeout=15.0, request_timeout=10.0, argv_extra=None):
        """``url`` attaches to an already running counter instead of spawning."""
        self.db_path = db_path
        self.python = python or sys.executable
        self.script = script or _counter_script()
        self._fixed_url = url
        self._token = token
        self.startup_timeout = startup_timeout
        self.request_timeout = request_timeout
        self.argv_extra = list(argv_extra or [])
        self.test_noise = False
        self._proc = None
        self._transport = None
        self._start_lock = threading.Lock()
        if self._fixed_url is not None:
            parsed = urlsplit(self._fixed_url)
            self._transport = HttpTransport(
                parsed.hostname, parsed.port or 80, token or "", request_timeout
            )

    @property
    def transport(self):
        return self._transport

    def ensure(self):
        with self._start_lock:
            if self._transport is not None:
                if self._fixed_url is not None:
                    return self._transport
                if self._proc is not None and self._proc.poll() is None:
                    return self._transport
            if self._proc is not None:
                # Child exited; reap it and start a fresh one on the same DB so
                # the results it already stored remain valid.
                self.stop()
            self._start()
            return self._transport

    def _start(self):
        token = secrets.token_hex(16)
        port = os.environ.get("PRIVBUDGET_COUNTER_PORT", "0")
        argv = [
            self.python, "-I", self.script,
            "--db", self.db_path,
            "--port", str(port),
            "--parent-pid", str(os.getpid()),
        ] + list(self.argv_extra)
        env = dict(os.environ)
        # The bearer token is secret: hand it over through the environment,
        # not argv, so it does not show up in the process table.
        env["PRIVBUDGET_COUNTER_TOKEN"] = token
        if self.test_noise:
            env["PRIVBUDGET_TEST_NOISE"] = "1"
        self._proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            env=env,
        )
        deadline = time.monotonic() + self.startup_timeout
        buf = b""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.stop()
                raise RpcLost("counter did not become ready")
            ready, _, _ = select.select([self._proc.stdout], [], [], remaining)
            if not ready:
                continue
            chunk = os.read(self._proc.stdout.fileno(), 4096)
            if not chunk:
                self.stop()
                raise RpcLost("counter exited during startup")
            buf += chunk
            if b"\n" in buf:
                line = buf.split(b"\n", 1)[0]
                try:
                    info = json.loads(line)
                except ValueError:
                    self.stop()
                    raise RpcLost("bad counter handshake")
                self._transport = HttpTransport(
                    "127.0.0.1", int(info["port"]), token, self.request_timeout
                )
                self._token = token
                return self._transport

    def call(self, payload, timeout=None):
        transport = self.ensure()
        return transport.call(payload, timeout=timeout)

    def stop(self):
        if self._proc is not None:
            try:
                if self._proc.poll() is None:
                    self._proc.terminate()
                    try:
                        self._proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self._proc.kill()
                        self._proc.wait(timeout=5)
            except OSError:
                pass
            try:
                self._proc.stdout.close()
            except Exception:
                pass
            self._proc = None
        self._transport = None

    def counter_port(self):
        """The port the currently running counter listens on, if known."""
        with self._start_lock:
            if self._transport is None:
                return None
            return self._transport.port

    @property
    def down_for_repair(self):
        """True when the child died and results may exist in its database."""
        if self._fixed_url is not None:
            return False
        return self._proc is not None and self._proc.poll() is not None

    def repair(self):
        """Restart a dead counter so retries hit a live, deduplicating peer."""
        self.ensure()

    def alive(self):
        if self._fixed_url is not None:
            return self._transport is not None
        return self._proc is not None and self._proc.poll() is None

    def kill_process(self):
        """TEST: hard-kill the counter (like a machine failure)."""
        if self._proc is not None and self._proc.poll() is None:
            self._proc.kill()
            try:
                self._proc.wait(timeout=5)
            except Exception:
                pass
        self._transport = None
