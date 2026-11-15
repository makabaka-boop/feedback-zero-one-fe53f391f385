"""Command line entry point: ``python -m privbudget``.

Usage::

    python -m privbudget --data DIR [--host H] [--port P] [--test-noise]

The service prints one JSON handshake line to stdout once it is listening::

    {"port": 47123}

and serves the HTTP API described in :mod:`privbudget.server`.  All persistent
state (service database, counter database) lives under ``DIR``; restarting the
process resumes with the same budgets, jobs and pending deliveries.  SIGTERM
or SIGINT shuts down cleanly.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading

from .core import BudgetService
from .server import Server


def main(argv=None):
    ap = argparse.ArgumentParser(prog="privbudget")
    ap.add_argument("--data", required=True, help="data directory")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--test-noise", action="store_true",
                    default=os.environ.get("PRIVBUDGET_TEST_NOISE") == "1",
                    help="let callers inject deterministic noise (tests only)")
    ap.add_argument("--hold-delivery", action="store_true",
                    help="do not contact the counter automatically (tests)")
    args = ap.parse_args(argv)

    if args.test_noise:
        os.environ["PRIVBUDGET_TEST_NOISE"] = "1"
    if args.hold_delivery:
        os.environ["PRIVBUDGET_TEST_HOLD_DELIVERY"] = "1"

    service = BudgetService(args.data)
    server = Server(service, host=args.host, port=args.port)
    sys.stdout.write(json.dumps({"port": server.port}) + "\n")
    sys.stdout.flush()

    done = threading.Event()

    def _stop(signum, frame):  # noqa: ARG001
        done.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        done.wait()
    finally:
        server.close()
        service.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
