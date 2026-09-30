"""End-to-end guarantees of the pipeline, each tested against real Delta tables."""

from __future__ import annotations

import json
import random

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from conftest import snapshot
from lakehouse.pipeline import BatchConflictError, ReconciliationError, SimulatedCrash


def _raw_events(landing_root, ids):
    events = []
    for b in ids:
        with open(landing_root / b / "events.jsonl") as f:
            events.extend(json.loads(line) for line in f)
    return events


def test_matches_independent_count(spark, landing_template, full_run):
    """Gold totals equal a plain-Python count of distinct landed events."""
    root, ids = landing_template
    pipe, _, _ = full_run
    distinct = {e["event_id"]: e for e in _raw_events(root, ids)}
    per_day = {}
    for e in distinct.values():
        per_day[e["event_time"][:10]] = per_day.get(e["event_time"][:10], 0) + 1

    gold = spark.read.format("delta").load(pipe.lake.gold("app_user_daily"))
    got = {
        r["event_date"].isoformat(): r["n"]
        for r in gold.groupBy("event_date").agg(F.sum("events").alias("n")).collect()
    }
    assert got == per_day
    # the generator really does redeliver and deliver late, so this is not vacuous
    assert len(_raw_events(root, ids)) > len(distinct)
    assert any(e["event_time"][:10] < b for b in ids for e in _raw_events(root, [b]))


def test_rerun_is_a_noop(spark, full_run):
    pipe, first, before = full_run
    assert all(r.status == "committed" for r in first)
    second = pipe.run(batch_ids=[r.batch_id for r in first])
    assert all(r.status == "skipped" for r in second)
    assert pipe.pending_batches() == []
    assert snapshot(spark, pipe) == before


@pytest.mark.parametrize("stage", ["bronze", "silver", "gold_daily", "gold_rolling", "reconcile"])
def test_crash_then_retry_equals_clean_run(spark, landing, make_pipeline, first_two, stage):
    """Crash while processing the second batch (which carries late data for
    the first day), retry, and compare with a run that never crashed."""
    _, ids = landing
    pipe = make_pipeline()
    pipe.process_batch(ids[0])
    with pytest.raises(SimulatedCrash):
        pipe.process_batch(ids[1], crash_after=stage)
    assert ids[1] in pipe.pending_batches()  # a crashed batch is never committed
    assert pipe.process_batch(ids[1]).status == "committed"
    assert snapshot(spark, pipe) == first_two


def test_order_independence(spark, landing, make_pipeline, full_run):
    """Late and redelivered data make arrival order irrelevant to the result."""
    _, ids = landing
    shuffled = ids[:]
    random.Random(3).shuffle(shuffled)
    assert shuffled != ids
    pipe = make_pipeline()
    pipe.run(shuffled)
    assert snapshot(spark, pipe) == full_run[2]


def test_late_batch_rewrites_only_affected_partitions(spark, landing, make_pipeline, full_run):
    root, ids = landing
    pipe = make_pipeline()
    pipe.run(ids)
    assert snapshot(spark, pipe) == full_run[2]

    def daily():
        rows = spark.read.format("delta").load(pipe.lake.gold("app_user_daily")).collect()
        return sorted((r["event_date"].isoformat(), r["app_id"], r["user_id"], r["events"]) for r in rows)

    before = daily()

    late_dir = root / "9999-late"  # a late batch holding one event from the 2nd day
    late_dir.mkdir()
    ev = {
        "event_id": "late-0001",
        "event_time": f"{ids[1]}T12:00:00Z",
        "user_id": "user-0001",
        "app_id": "app-000",
        "server_ip": "10.0.0.1",
        "port": 443,
        "protocol": "TCP",
        "bytes_sent": 10,
        "bytes_recv": 20,
        "action": "allow",
    }
    (late_dir / "events.jsonl").write_text(json.dumps(ev) + "\n")
    res = pipe.process_batch("9999-late")

    assert [d.isoformat() for d in res.affected_dates] == [ids[1]]
    # every as-of date whose 30-day window covers the late day is rebuilt
    assert [d.isoformat() for d in res.rolling_dates] == ids[1:]
    after = daily()
    assert [r for r in after if r[0] != ids[1]] == [r for r in before if r[0] != ids[1]]
    assert sum(r[3] for r in after if r[0] == ids[1]) == sum(r[3] for r in before if r[0] == ids[1]) + 1


def test_relanded_batch_with_new_content_is_refused(landing, make_pipeline):
    root, ids = landing
    pipe = make_pipeline()
    pipe.process_batch(ids[0])
    with open(root / ids[0] / "events.jsonl", "a") as f:
        f.write(open(root / ids[1] / "events.jsonl").readline())
    with pytest.raises(BatchConflictError):
        pipe.process_batch(ids[0])


def test_reconciliation_catches_silent_loss(spark, landing, make_pipeline):
    _, ids = landing
    pipe = make_pipeline()
    pipe.run(ids[:2])
    # corrupt silver behind the pipeline's back: drop 5 rows from one day
    silver = spark.read.format("delta").load(pipe.lake.silver)
    day = silver.select("event_date").first()["event_date"]
    victims = [r["event_id"] for r in silver.where(F.col("event_date") == day).limit(5).collect()]
    DeltaTable.forPath(spark, pipe.lake.silver).delete(F.col("event_id").isin(victims))
    with pytest.raises(ReconciliationError, match="silver_vs_bronze"):
        pipe.reconcile([day], [])


def test_ledger_records_every_check(spark, full_run):
    pipe, _, _ = full_run
    led = spark.read.format("delta").load(pipe.lake.ledger)
    assert led.where("status NOT IN ('ok', 'committed')").count() == 0
    stages = {r["stage"] for r in led.select("stage").distinct().collect()}
    assert {"commit", "silver_vs_bronze", "app_user_daily_vs_silver", "app_user_30d_vs_silver"} <= stages
