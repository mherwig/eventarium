#!/usr/bin/env python3
"""Per-delivery decomposition of end-to-end latency, in segments.

  publish -> pending     the events topic: publish-api's produce, the router's
                         fetch, its processing and its transaction
  pending  -> in_flight  the deliveries topic: the dispatcher picking the
                         record up and reaching the attempt
  in_flight -> delivered the HTTP call itself

Segment 1 is the same number for every delivery of one event, so it is
per-event; the other two are per-delivery. Everything comes from one join
against ClickHouse plus the generator's own file, which is what makes the
three add up to the end-to-end figure instead of being three medians from
three different populations.

  usage: scripts/decompose.py <owner> <seed.json> <pub.jsonl>

Needs python3 (already the one non-shell dependency of scripts/chaos.sh) and
the ClickHouse node the status rows were written to - CH_CONTAINER (default
pubsub-clickhouse-1) and DOCKER select it.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

DOCKER = os.environ.get("DOCKER", "docker")
CH_CONTAINER = os.environ.get("CH_CONTAINER", "pubsub-clickhouse-1")


def pct(vals, p):
    if not vals:
        return 0.0
    v = sorted(vals)
    k = (len(v) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def row(name, vals):
    if not vals:
        print(f"  {name:<30} (no samples)")
        return
    v = sorted(vals)
    print(f"  {name:<30} n={len(v):<7} p50={pct(v,.50):8.1f}  "
          f"p90={pct(v,.90):8.1f}  p99={pct(v,.99):8.1f}  max={v[-1]:9.1f} ms")


def ts(v):
    """ClickHouse returns DateTime64 without a zone (it was stored as UTC);
    the generator's file is RFC3339 with one. Normalise both to UTC or the
    subtraction is naive-vs-aware."""
    if not v:
        return None
    v = v.replace("Z", "+00:00") if v.endswith("Z") else v
    d = datetime.fromisoformat(v)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def ch(sql):
    out = subprocess.run(
        [DOCKER, "exec", CH_CONTAINER, "clickhouse-client",
         "-u", "pubsub", "--password", "pubsub", "-d", "pubsub", "-q", sql],
        capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit("clickhouse: " + out.stderr.strip()[:400])
    return out.stdout


def ids(seed, pred):
    return [s["id"] for s in json.load(open(seed)) if pred(s)]


def fetch(owner, subs, selects, having):
    """One grouped query; returns rows of [delivery_id, event_ref, ts...]."""
    if not subs:
        return []
    lst = ",".join("'" + s + "'" for s in subs)
    sql = (f"SELECT delivery_id, event_ref, {selects} "
           f"FROM delivery_status WHERE owner='{owner}' "
           f"AND subscription_id IN ({lst}) "
           f"GROUP BY delivery_id, event_ref HAVING {having} FORMAT TSV")
    rows = []
    for line in ch(sql).splitlines():
        f = line.split("\t")
        if len(f) >= 4:
            rows.append(f)
    return rows


def report(name, rows, boundaries):
    """boundaries: [(label, column_index), ...] in order after pending."""
    # columns: 0 delivery_id, 1 event_ref, 2 pending, then the rest
    idx = [2] + [j for _, j in boundaries]
    segs = [[] for _ in idx]
    total = []
    for f in rows:
        t0 = sent.get(f[1])
        if t0 is None:
            continue
        pts = []
        ok = True
        for i in idx:
            if i >= len(f):
                ok = False
                break
            v = ts(f[i])
            if v is None:
                ok = False
                break
            pts.append(v)
        if not ok:
            continue
        deltas = [(pts[n] - pts[n - 1]).total_seconds() * 1000 for n in range(1, len(pts))]
        deltas.insert(0, (pts[0] - t0).total_seconds() * 1000)
        for n, d in enumerate(deltas):
            segs[n].append(d)
        total.append(sum(deltas))

    print(f"\n  -- {name} --")
    row("1. publish -> pending", segs[0])
    for n, (label, _) in enumerate(boundaries, start=1):
        row(label, segs[n])
    row("sum of the segments", total)


sent = {}
owner, seed, pub = sys.argv[1:4]
for line in open(pub):
    line = line.strip()
    if line:
        rec = json.loads(line)
        ref, t = rec.get("event_ref"), ts(rec.get("time"))
        if ref and t:
            sent[ref] = t

wh = ids(seed, lambda s: s.get("channel") == "webhook" and not s.get("rate_limit_per_s"))
sse = ids(seed, lambda s: s.get("channel") == "sse")
rl = ids(seed, lambda s: bool(s.get("rate_limit_per_s")))
print(f"  subscriptions: webhook-unlimited={len(wh)} sse={len(sse)} rate-limited={len(rl)}")

report("webhook (unlimited)",
       fetch(owner, wh,
             "minIf(ts,status='pending') t_p, minIf(ts,status='in_flight') t_i, "
             "minIf(ts,status='delivered') t_d",
             "t_p > toDateTime(0) AND t_i > toDateTime(0) AND t_d > toDateTime(0)"),
       [("2. pending  -> in_flight", 3), ("3. in_flight -> delivered", 4)])

report("sse",
       fetch(owner, sse,
             "minIf(ts,status='pending') t_p, minIf(ts,status='sent') t_s",
             "t_p > toDateTime(0) AND t_s > toDateTime(0)"),
       [("2. pending  -> sent", 3)])
