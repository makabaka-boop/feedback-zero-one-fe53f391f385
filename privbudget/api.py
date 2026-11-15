"""Optional in-process Python facade.

The canonical interface is HTTP (see :mod:`privbudget.server`), but callers who
embed the service in a Python program can use this thin facade instead.  The
counter is still a *separate* OS process reached over HTTP; only the client
side is in-process.

Usage::

    from privbudget.api import PrivateBudget

    budget = PrivateBudget("/var/lib/privbudget")
    budget.initialize({"bits": {...}, "groups": {...}, "limits": {...}})
    budget.start("job1", "groupA", "1/2")
    budget.status("job1")       # {"state": "pending"|"ready"|...}
    budget.read("job1")         # {"value": 1.75}
    budget.cancel("job1")
    budget.budgets()            # {"alice": {"spent": "0/1", "reserved": "1/2"}}

Each method returns exactly the same JSON-ready dict as the HTTP endpoints,
including ``{"error": "<jsonpath>"}`` on rejection.  ``close()`` stops the
counter and closes the databases; the data directory can be reopened later.

The facade is safe to use from multiple threads: the underlying service
serializes all state transitions.
"""

from __future__ import annotations

from .core import BudgetService


class PrivateBudget:
    def __init__(self, data_dir, deliver=True):
        self._service = BudgetService(data_dir, deliver=deliver)

    # -- configuration ------------------------------------------------------
    def initialize(self, config):
        """Configure the service.  Only the first successful call wins."""
        return self._service.initialize(config)

    def is_initialized(self):
        return self._service.is_initialized()

    # -- operations ---------------------------------------------------------
    def start(self, job, group, epsilon):
        return self._service.start(job, group, epsilon)

    def status(self, job):
        return self._service.status(job)

    def read(self, job):
        return self._service.read(job)

    def cancel(self, job):
        return self._service.cancel(job)

    def budgets(self):
        return self._service.budgets()

    def flush(self):
        """Deliver any pending compute requests now (mostly for tests)."""
        self._service.replay_pending()

    # -- lifecycle ----------------------------------------------------------
    def close(self):
        self._service.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False
