#!/usr/bin/env python3
"""Latency percentiles split the way docs/performance.md reports them.

That table has two webhook rows - "unlimited" and "rate-limited (by design)" -
and folding them together is what made an end-to-end p50 of ~550 ms look like a
platform regression when the platform's own attempt latency was p50=3 ms. Three
subscriptions are rate-limited to 50/s by the seed; they take a minute *by
design*, so they get their own row.

  usage: scripts/latency.py <seed.json> <pub.jsonl> <recv.jsonl> [sse.jsonl ...]

Called by scripts/loadtest.sh; also usable standalone on a finished run's
work directory.
"""
import json
import sys
from datetime import datetime


def load(path):
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def ts(v):
    return datetime.fromisoformat(v.replace("Z", "+00:00")) if v else None


def pct(vals, p):
    if not vals:
        return 0.0
    k = (len(vals) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def row(name, vals):
    if not vals:
        print(f"  {name:<34} (no samples)")
        return
    vals.sort()
    print(
        f"  {name:<34} n={len(vals):<6} p50={pct(vals,.50):8.1f}ms"
        f"  p90={pct(vals,.90):8.1f}  p99={pct(vals,.99):8.1f}  max={vals[-1]:8.1f}"
    )


def main():
    seed_path, pub_path, recv_path, *sse_paths = sys.argv[1:]

    rate_limited = {
        s["id"]
        for s in json.load(open(seed_path))
        if s.get("rate_limit_per_s")
    }

    sent = {}
    for rec in load(pub_path):
        ref, t = rec.get("event_ref"), ts(rec.get("time"))
        if ref and t:
            sent[ref] = t

    def delta(rec):
        t = ts(rec.get("received_at"))
        s = sent.get(rec.get("event_ref"))
        return (t - s).total_seconds() * 1000.0 if (t and s) else None

    unlim, limited = [], []
    for rec in load(recv_path):
        d = delta(rec)
        if d is None:
            continue
        (limited if rec.get("subscription_id") in rate_limited else unlim).append(d)

    sse = []
    for path in sse_paths:
        for rec in load(path):
            d = delta(rec)
            if d is not None:
                sse.append(d)

    print(f"  rate-limited subscriptions excluded: {len(rate_limited)}")
    row("webhook (unlimited)", unlim)
    row("webhook (rate-limited, by design)", limited)
    row("sse", sse)


if __name__ == "__main__":
    main()
