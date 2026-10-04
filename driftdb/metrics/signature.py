"""
driftdb.metrics.signature
==========================
Turns a query's analyzed logical plan text into a stable "template" hash,
so that the *same recurring query* (e.g. the same reporting query run daily
with a different date literal) maps to the same signature across
executions, while genuinely different queries map to different signatures.

This is what the proposal calls a "recurring query signature" in C1.
"""
from __future__ import annotations

import hashlib
import json
import re

# Matches: numeric literals, quoted string literals, and date-like literals.
# We collapse all of these to a placeholder so that predicate value drift
# (which is a workload-drift *signal*, not something that should fragment
# the signature) doesn't create a new signature per run.
_NUM_LITERAL = re.compile(r"(?<![A-Za-z0-9_])-?\d+(\.\d+)?[LDF]?(?![A-Za-z0-9_])")
_STR_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'")
_ATTR_EXPR_ID = re.compile(r"#\d+")
_WHITESPACE = re.compile(r"\s+")

# column#id <op> literal   e.g.  i_manufact_id#115 = 345
_PRED_LITERAL = re.compile(
    r"(\w+)#\d+\s*(<=>|<>|!=|<=|>=|=|<|>)\s*"
    r"(-?\d+(?:\.\d+)?|'(?:[^'\\]|\\.)*')"
)


def normalize_plan_text(plan_text: str) -> str:
    """Collapse literals, Spark's per-compilation attribute expression IDs
    (e.g. `amount#123L` -> `amount#?`), and whitespace so structurally
    identical queries -- including the *same* recurring query re-analyzed
    on a later execution, which always gets fresh attribute IDs from
    Spark's analyzer -- produce identical text."""
    text = _STR_LITERAL.sub("?", plan_text)
    text = _ATTR_EXPR_ID.sub("#?", text)
    text = _NUM_LITERAL.sub("?", text)
    text = _WHITESPACE.sub(" ", text).strip()
    return text


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def query_signature(analyzed_plan_text: str) -> str:
    """Stable signature for a *recurring* query template."""
    return digest(normalize_plan_text(analyzed_plan_text))


def raw_plan_digest(plan_text: str) -> str:
    """Non-normalized digest, kept for debugging/audit trails."""
    return digest(plan_text)


def extract_literals(analyzed_plan_text: str) -> str:
    """Predicate constants of the form `column <op> literal`, as JSON.

    Example: '{"d_moy =": 11, "i_manufact_id =": 345}'
    Best effort: predicates wrapped in casts/functions are not captured.
    """
    out = {}
    for col, op, val in _PRED_LITERAL.findall(analyzed_plan_text):
        if val.startswith("'"):
            v = val[1:-1]
        elif "." in val:
            v = float(val)
        else:
            v = int(val)
        out.setdefault(f"{col} {op}", v)
    return json.dumps(out, sort_keys=True)