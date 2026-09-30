"""Unit tests for the pure transforms, on hand-written rows."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from lakehouse import features as feat
from lakehouse.generate import GenConfig, fingerprint, generate


def _silver(spark, rows):
    """rows: (event_id, 'YYYY-MM-DD HH:MM:SS', user, app, server, port, bytes)"""
    data = [
        (eid, datetime.fromisoformat(ts), datetime.fromisoformat(ts).date(), u, a, s, p, "TCP", b, "allow")
        for eid, ts, u, a, s, p, b in rows
    ]
    schema = (
        "event_id string, event_time timestamp, event_date date, user_id string, app_id string, "
        "server_ip string, port int, protocol string, bytes_total long, action string"
    )
    return spark.createDataFrame(data, schema)


def test_silver_keeps_earliest_copy(spark):
    raw = spark.createDataFrame(
        [("e1", "2026-01-01T10:00:00Z", "u", "a", "s", 443, "TCP", 1, 2, "allow")], feat.RAW_SCHEMA
    )
    b1 = feat.to_bronze(raw, "2026-01-02")
    b0 = feat.to_bronze(raw, "2026-01-01")
    out = feat.to_silver(b1.unionByName(b0)).collect()
    assert len(out) == 1
    assert out[0]["_batch_id"] == "2026-01-01"
    assert out[0]["bytes_total"] == 3
    assert out[0]["event_date"] == date(2026, 1, 1)


def test_pairs_within_same_user_hour_only(spark):
    s = _silver(
        spark,
        [
            ("1", "2026-01-01 10:05:00", "u1", "a", "s", 1, 1),
            ("2", "2026-01-01 10:40:00", "u1", "b", "s", 1, 1),
            ("3", "2026-01-01 11:10:00", "u1", "c", "s", 1, 1),  # next hour: no pair with a/b
            ("4", "2026-01-01 10:10:00", "u2", "c", "s", 1, 1),  # other user: no pair with a/b
        ],
    )
    rows = feat.app_pair_daily(s, max_apps_per_window=10, min_support=1).collect()
    assert [(r["app_a"], r["app_b"], r["windows"]) for r in rows] == [("a", "b", 1)]


def test_pair_pruning_caps_apps_per_window(spark):
    # one user touches 6 apps in one hour; app a is used 3x, b 2x, the rest once
    evs = [
        ("a1", "a"),
        ("a2", "a"),
        ("a3", "a"),
        ("b1", "b"),
        ("b2", "b"),
        ("c", "c"),
        ("d", "d"),
        ("e", "e"),
        ("f", "f"),
    ]
    s = _silver(spark, [(eid, "2026-01-01 09:00:00", "u", app, "s", 1, 1) for eid, app in evs])
    unpruned = feat.app_pair_daily(s, max_apps_per_window=100, min_support=1).count()
    pruned = feat.app_pair_daily(s, max_apps_per_window=3, min_support=1).collect()
    assert unpruned == 15  # C(6, 2)
    # busiest two (a, b) plus the alphabetically first of the ties (c)
    assert sorted((r["app_a"], r["app_b"]) for r in pruned) == [("a", "b"), ("a", "c"), ("b", "c")]


def test_pair_min_support(spark):
    s = _silver(
        spark,
        [
            ("1", "2026-01-01 09:00:00", "u1", "a", "s", 1, 1),
            ("2", "2026-01-01 09:30:00", "u1", "b", "s", 1, 1),
            ("3", "2026-01-01 14:00:00", "u2", "a", "s", 1, 1),
            ("4", "2026-01-01 14:30:00", "u2", "b", "s", 1, 1),
            ("5", "2026-01-01 15:00:00", "u3", "a", "s", 1, 1),
            ("6", "2026-01-01 15:30:00", "u3", "c", "s", 1, 1),
        ],
    )
    rows = feat.app_pair_daily(s, max_apps_per_window=10, min_support=2).collect()
    assert [(r["app_a"], r["app_b"], r["windows"], r["distinct_users"]) for r in rows] == [("a", "b", 2, 2)]


def test_rolling_window_is_30_days_inclusive(spark):
    days = [date(2026, 1, 1), date(2026, 1, 30), date(2026, 1, 31)]
    s = _silver(spark, [(str(i), f"{d.isoformat()} 12:00:00", "u", "a", "s", 1, 10) for i, d in enumerate(days)])
    daily = feat.app_user_daily(s)
    as_of = spark.createDataFrame([(date(2026, 1, 30),), (date(2026, 1, 31),)], "as_of_date date")
    got = {r["as_of_date"]: (r["active_days"], r["events_30d"]) for r in feat.app_user_30d(daily, as_of).collect()}
    # Jan 30 window = Jan 1..Jan 30 (3 events? no: Jan 31 is in the future) -> 2 days
    assert got[date(2026, 1, 30)] == (2, 2)
    # Jan 31 window = Jan 2..Jan 31 -> Jan 1 has dropped out
    assert got[date(2026, 1, 31)] == (2, 2)


def test_generator_is_deterministic(tmp_path):
    cfg = GenConfig(days=3, users=10, apps=5, events_per_day=100, seed=5)
    a, b = tmp_path / "a", tmp_path / "b"
    ids = generate(a, cfg)
    assert generate(b, cfg) == ids
    assert all(fingerprint(a / i) == fingerprint(b / i) for i in ids)


@pytest.mark.parametrize("field", ["late_fraction", "redelivery_fraction"])
def test_generator_produces_messy_data(tmp_path, field):
    import json

    cfg = GenConfig(days=4, users=10, apps=5, events_per_day=500, seed=5)
    ids = generate(tmp_path, cfg)
    late = dup = 0
    seen = set()
    for b in ids:
        for line in open(tmp_path / b / "events.jsonl"):
            e = json.loads(line)
            late += e["event_time"][:10] < b
            dup += e["event_id"] in seen
            seen.add(e["event_id"])
    assert (late if field == "late_fraction" else dup) > 0
