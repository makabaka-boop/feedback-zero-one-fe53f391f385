"""Exact validation / parsing helpers and JSONPath error reporting.

Every rejection is reported as ``{"error": "<JSONPath>"}``.  Paths are
zero-based and point at the offending value.  When several problems exist,
reporting any one of them is acceptable; the order below is deterministic.
"""

from __future__ import annotations

import re
from fractions import Fraction

ALNUM = re.compile(r"^[A-Za-z0-9]+$")


class Reject(Exception):
    """Raised internally; carries the JSONPath of the offending value."""

    def __init__(self, path):
        super().__init__(path)
        self.path = path


def err(path):
    return {"error": path}


def ident(value, path):
    if not isinstance(value, str) or not ALNUM.match(value):
        raise Reject(path)
    return value


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def pos_int(value, path):
    if not is_int(value) or value <= 0:
        raise Reject(path)
    return value


def parse_fraction(value, path, positive_num_den=True):
    """Parse a ``"p/q"`` string into an exact :class:`Fraction`.

    ``p`` and ``q`` must be integers written in plain decimal form (no
    whitespace, no exponent, no leading ``+``).  Unless ``positive_num_den``
    is false, both must be strictly positive.
    """
    if not isinstance(value, str):
        raise Reject(path)
    m = re.match(r"^(-?)([0-9]+)/([0-9]+)$", value)
    if not m:
        raise Reject(path)
    neg, p_s, q_s = m.groups()
    p = int(p_s)
    q = int(q_s)
    if q <= 0:
        raise Reject(path)
    if positive_num_den:
        if neg or p <= 0:
            raise Reject(path)
    else:
        if neg and p == 0:
            raise Reject(path)
        p = -p if neg else p
    return Fraction(p, q)


def parse_positive_fraction(value, path):
    """Strictly positive p/q, both p and q positive integers."""
    return parse_fraction(value, path, positive_num_den=True)


def frac_str(value):
    """Render a Fraction as a reduced ``"p/q"`` string."""
    f = Fraction(value)
    return "%d/%d" % (f.numerator, f.denominator)


def index_path(base, index):
    return "%s[%d]" % (base, index)


def quote(key):
    """JSONPath member access; identifiers use dot, others bracket quoting."""
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
        return "." + key
    escaped = key.replace("\\", "\\\\").replace("'", "\\'")
    return "['%s']" % escaped
