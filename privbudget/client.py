"""A small blocking HTTP client for the budget service API.

Kept dependency-free so the acceptance tests exercise exactly the same
JSON/HTTP surface an external caller would see.
"""

from __future__ import annotations

import http.client
import json


class Client:
    def __init__(self, host="127.0.0.1", port=None, url=None, timeout=30.0):
        if url:
            from urllib.parse import urlsplit

            parsed = urlsplit(url)
            self.host = parsed.hostname
            self.port = parsed.port or 80
        else:
            self.host = host
            self.port = port
        self.timeout = timeout

    def _request(self, method, path, body=None):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            data = None
            headers = {}
            if body is not None:
                data = json.dumps(body).encode()
                headers["Content-Type"] = "application/json"
            conn.request(method, path, body=data, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            status = resp.status
        finally:
            conn.close()
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = raw.decode("utf-8", "replace")
        return status, payload

    def _post(self, name, body):
        status, payload = self._request("POST", "/" + name, body)
        if status != 200:
            raise RuntimeError("HTTP %s: %r" % (status, payload))
        return payload

    def initialize(self, config):
        return self._post("initialize", config)

    def start(self, job, group, epsilon, **extra):
        body = {"job": job, "group": group, "epsilon": epsilon}
        body.update(extra)
        return self._post("start", body)

    def status(self, job, **extra):
        body = {"job": job}
        body.update(extra)
        return self._post("status", body)

    def read(self, job):
        return self._post("read", {"job": job})

    def cancel(self, job):
        return self._post("cancel", {"job": job})

    def budgets(self):
        status, payload = self._request("GET", "/budgets")
        if status != 200:
            raise RuntimeError("HTTP %s: %r" % (status, payload))
        return payload

    def flush(self):
        return self._post("flush", {})
