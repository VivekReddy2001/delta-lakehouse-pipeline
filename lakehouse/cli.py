"""``python -m lakehouse <command>``

generate   write synthetic telemetry batches into the landing zone
run        process every landed batch that is not yet committed
reconcile  re-check row counts for all partitions (read-only)
show       print row counts per table and the latest ledger entries
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .generate import GenConfig, generate
from .pipeline import Pipeline
from .spark import get_spark
from .tables import GOLD_DAILY, GOLD_ROLLING


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="lakehouse")
    p.add_argument("--lake", default="lake/tables")
    p.add_argument("--landing", default="lake/landing")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate")
    g.add_argument("--start", default="2026-01-01")
    g.add_argument("--days", type=int, default=10)
    g.add_argument("--users", type=int, default=200)
    g.add_argument("--apps", type=int, default=40)
    g.add_argument("--events-per-day", type=int, default=5000)
    g.add_argument("--seed", type=int, default=7)

    r = sub.add_parser("run")
    r.add_argument("--batch", action="append", help="process only these batch ids")
    r.add_argument(
        "--crash-after",
        choices=["bronze", "silver", "gold_daily", "gold_rolling", "reconcile"],
        help="raise mid-batch to demonstrate recovery on the next run",
    )

    sub.add_parser("reconcile")
    sub.add_parser("show")
    args = p.parse_args(argv)

    if args.cmd == "generate":
        cfg = GenConfig(
            start=date.fromisoformat(args.start),
            days=args.days,
            users=args.users,
            apps=args.apps,
            events_per_day=args.events_per_day,
            seed=args.seed,
        )
        ids = generate(Path(args.landing), cfg)
        print(f"landed {len(ids)} batches in {args.landing}: {ids[0]} .. {ids[-1]}")
        return

    spark = get_spark()
    pipe = Pipeline(spark, Path(args.lake), Path(args.landing))

    if args.cmd == "run":
        batches = args.batch or pipe.pending_batches()
        if not batches:
            print("nothing to do: every landed batch is committed")
        for b in batches:
            res = pipe.process_batch(b, crash_after=args.crash_after)
            print(
                json.dumps(
                    {
                        "batch": res.batch_id,
                        "status": res.status,
                        "rows_in": res.rows_in,
                        "affected_dates": [d.isoformat() for d in res.affected_dates],
                        "rolling_dates": len(res.rolling_dates),
                        "seconds": round(res.seconds, 1),
                    }
                )
            )
    elif args.cmd == "reconcile":
        silver = spark.read.format("delta").load(pipe.lake.silver)
        dates = sorted(r["event_date"] for r in silver.select("event_date").distinct().collect())
        checks = pipe.reconcile(dates, dates)
        print(f"{len(checks)} checks passed across {len(dates)} dates")
    elif args.cmd == "show":
        for name in ["bronze/events", "silver/events"] + [f"gold/{t}" for t in GOLD_DAILY + GOLD_ROLLING]:
            path = pipe.lake.path(name)
            n = spark.read.format("delta").load(path).count()
            print(f"{name:24s} {n:>10,d} rows")
        led = spark.read.format("delta").load(pipe.lake.ledger)
        led.where("stage = 'commit'").orderBy("batch_id").select("batch_id", "expected", "status", "recorded_at").show(
            50, truncate=False
        )


if __name__ == "__main__":
    main()
