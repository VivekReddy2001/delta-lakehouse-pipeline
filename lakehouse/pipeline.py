"""Batch orchestration: ledger, partition-scoped recomputation, reconciliation.

``process_batch`` is safe to call any number of times, in any order, and to
kill at any point:

1. A batch whose ledger entry is ``committed`` with the same content hash is
   skipped. The same batch id with *different* content is refused - the
   source changed under us, and silently accepting it would rewrite history.
2. Every write replaces whole partitions computed from the layer below, so a
   retry after a crash rewrites the same partitions with the same rows.
3. Before a batch is marked committed, row counts are reconciled across
   bronze -> silver -> gold for every partition the batch touched. A
   mismatch aborts the batch *without* committing it, so the next run
   retries it.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import features as feat
from .generate import fingerprint
from .tables import Lake, exists, replace_partitions

LEDGER_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("batch_id", T.StringType()),
        T.StructField("stage", T.StringType()),
        T.StructField("partition", T.StringType()),
        T.StructField("expected", T.LongType()),
        T.StructField("actual", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("fingerprint", T.StringType()),
        T.StructField("recorded_at", T.TimestampType()),
    ]
)

STAGES = ["bronze", "silver", "gold_daily", "gold_rolling", "reconcile"]


class BatchConflictError(RuntimeError):
    """A committed batch id re-landed with different content."""


class ReconciliationError(RuntimeError):
    """Row counts disagree between layers; the batch is not committed."""


class SimulatedCrash(RuntimeError):
    """Raised by ``crash_after`` to test recovery."""


@dataclass
class BatchResult:
    batch_id: str
    status: str  # committed | skipped
    affected_dates: list[date] = field(default_factory=list)
    rolling_dates: list[date] = field(default_factory=list)
    rows_in: int = 0
    seconds: float = 0.0


class Pipeline:
    def __init__(
        self,
        spark: SparkSession,
        lake_root: Path,
        landing: Path,
        max_apps_per_window: int = 10,
        min_pair_support: int = 2,
    ):
        self.spark = spark
        self.lake = Lake(Path(lake_root))
        self.landing = Path(landing)
        self.max_apps_per_window = max_apps_per_window
        self.min_pair_support = min_pair_support

    # ------------------------------------------------------------ ledger
    def _ledger_rows(self) -> DataFrame | None:
        if not exists(self.spark, self.lake.ledger):
            return None
        return self.spark.read.format("delta").load(self.lake.ledger)

    def committed_fingerprint(self, batch_id: str) -> str | None:
        led = self._ledger_rows()
        if led is None:
            return None
        row = (
            led.where((F.col("batch_id") == batch_id) & (F.col("stage") == "commit"))
            .orderBy(F.desc("recorded_at"))
            .select("fingerprint")
            .first()
        )
        return row["fingerprint"] if row else None

    def _record(self, rows: list[tuple]) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        df = self.spark.createDataFrame([(*r, now) for r in rows], LEDGER_SCHEMA)
        df.write.format("delta").mode("append").save(self.lake.ledger)

    # ------------------------------------------------------------- stages
    def _read(self, path: str) -> DataFrame:
        return self.spark.read.format("delta").load(path)

    def _ingest_bronze(self, batch_id: str) -> tuple[int, list[date]]:
        raw = self.spark.read.schema(feat.RAW_SCHEMA).json(str(self.landing / batch_id))
        bronze = feat.to_bronze(raw, batch_id)
        replace_partitions(bronze, self.lake.bronze, "_batch_id", [batch_id])
        written = self._read(self.lake.bronze).where(F.col("_batch_id") == batch_id)
        dates = sorted(r["event_date"] for r in written.select("event_date").distinct().collect())
        return written.count(), dates

    def _rebuild_silver(self, dates: list[date]) -> None:
        bronze = self._read(self.lake.bronze).where(F.col("event_date").isin(dates))
        replace_partitions(feat.to_silver(bronze), self.lake.silver, "event_date", dates)

    def _rebuild_gold_daily(self, dates: list[date]) -> None:
        silver = self._read(self.lake.silver).where(F.col("event_date").isin(dates))
        for name, build in feat.DAILY_BUILDERS.items():
            if name == "app_pair_daily":
                df = build(silver, self.max_apps_per_window, self.min_pair_support)
            else:
                df = build(silver)
            replace_partitions(df, self.lake.gold(name), "event_date", dates)

    def _rolling_dates(self, dates: list[date]) -> list[date]:
        """As-of dates whose 30-day window contains any affected date,
        restricted to dates that exist in silver."""
        present = {r["event_date"] for r in self._read(self.lake.silver).select("event_date").distinct().collect()}
        out = set()
        for d in dates:
            out.update(d + timedelta(k) for k in range(feat.ROLLING_DAYS))
        return sorted(out & present)

    def _rebuild_gold_rolling(self, as_of_dates: list[date]) -> None:
        if not as_of_dates:
            return
        lo = min(as_of_dates) - timedelta(feat.ROLLING_DAYS - 1)
        hi = max(as_of_dates)
        as_of = self.spark.createDataFrame([(d,) for d in as_of_dates], "as_of_date DATE")
        in_range = F.col("event_date").between(lo, hi)
        app_user = self._read(self.lake.gold("app_user_daily")).where(in_range)
        app_server = self._read(self.lake.gold("app_server_daily")).where(in_range)
        replace_partitions(
            feat.app_user_30d(app_user, as_of), self.lake.gold("app_user_30d"), "as_of_date", as_of_dates
        )
        replace_partitions(
            feat.app_30d(app_user, app_server, as_of), self.lake.gold("app_30d"), "as_of_date", as_of_dates
        )

    # ------------------------------------------------------ reconciliation
    def reconcile(self, dates: list[date], as_of_dates: list[date]) -> list[tuple]:
        """Compare row counts across layers; return ledger rows or raise."""
        checks: list[tuple[str, str, int, int]] = []

        bronze = self._read(self.lake.bronze).where(F.col("event_date").isin(dates))
        expected = {
            r["event_date"]: r["n"]
            for r in bronze.groupBy("event_date").agg(F.countDistinct("event_id").alias("n")).collect()
        }
        silver = self._read(self.lake.silver).where(F.col("event_date").isin(dates))
        actual = {
            r["event_date"]: r["n"]
            for r in silver.groupBy("event_date").count().withColumnRenamed("count", "n").collect()
        }
        dup = silver.groupBy("event_id").count().where("count > 1").count()
        checks.append(("silver_unique_event_id", "*", 0, dup))
        for d in dates:
            checks.append(("silver_vs_bronze", d.isoformat(), expected.get(d, 0), actual.get(d, 0)))

        # every event lands in exactly one row of each additive daily table
        for name in ["app_user_daily", "app_server_daily", "app_port_daily"]:
            g = self._read(self.lake.gold(name)).where(F.col("event_date").isin(dates))
            sums = {r["event_date"]: r["n"] for r in g.groupBy("event_date").agg(F.sum("events").alias("n")).collect()}
            for d in dates:
                checks.append((f"{name}_vs_silver", d.isoformat(), actual.get(d, 0), sums.get(d, 0)))

        # rolling windows add up to the silver rows they cover
        if as_of_dates:
            per_day = {
                r["event_date"]: r["count"]
                for r in self._read(self.lake.silver).groupBy("event_date").count().collect()
            }
            roll = self._read(self.lake.gold("app_user_30d")).where(F.col("as_of_date").isin(as_of_dates))
            sums = {
                r["as_of_date"]: r["n"]
                for r in roll.groupBy("as_of_date").agg(F.sum("events_30d").alias("n")).collect()
            }
            for a in as_of_dates:
                exp = sum(n for d, n in per_day.items() if a - timedelta(feat.ROLLING_DAYS) < d <= a)
                checks.append(("app_user_30d_vs_silver", a.isoformat(), exp, sums.get(a, 0)))

        bad = [c for c in checks if c[2] != c[3]]
        if bad:
            detail = "; ".join(f"{n}[{p}] expected {e} got {a}" for n, p, e, a in bad[:10])
            raise ReconciliationError(f"{len(bad)} check(s) failed: {detail}")
        return checks

    # ------------------------------------------------------------- driver
    def process_batch(self, batch_id: str, crash_after: str | None = None) -> BatchResult:
        t0 = time.time()
        fp = fingerprint(self.landing / batch_id)
        prior = self.committed_fingerprint(batch_id)
        if prior is not None:
            if prior != fp:
                raise BatchConflictError(f"batch {batch_id} was committed with different content")
            return BatchResult(batch_id, "skipped", seconds=time.time() - t0)

        run_id = uuid.uuid4().hex[:12]

        def checkpoint(stage: str) -> None:
            if crash_after == stage:
                raise SimulatedCrash(f"simulated crash after {stage}")

        rows_in, dates = self._ingest_bronze(batch_id)
        checkpoint("bronze")
        self._rebuild_silver(dates)
        checkpoint("silver")
        self._rebuild_gold_daily(dates)
        checkpoint("gold_daily")
        as_of_dates = self._rolling_dates(dates)
        self._rebuild_gold_rolling(as_of_dates)
        checkpoint("gold_rolling")

        try:
            checks = self.reconcile(dates, as_of_dates)
        except ReconciliationError as e:
            self._record([(run_id, batch_id, "reconcile", "*", None, None, f"failed: {e}"[:500], fp)])
            raise
        checkpoint("reconcile")
        self._record(
            [(run_id, batch_id, name, part, exp, act, "ok", fp) for name, part, exp, act in checks]
            + [(run_id, batch_id, "commit", "*", rows_in, rows_in, "committed", fp)]
        )
        return BatchResult(batch_id, "committed", dates, as_of_dates, rows_in, time.time() - t0)

    def pending_batches(self) -> list[str]:
        landed = sorted(p.name for p in self.landing.iterdir() if p.is_dir())
        led = self._ledger_rows()
        if led is None:
            return landed
        done = {r["batch_id"] for r in led.where("stage = 'commit'").select("batch_id").collect()}
        return [b for b in landed if b not in done]

    def run(self, batch_ids: list[str] | None = None) -> list[BatchResult]:
        return [self.process_batch(b) for b in (batch_ids or self.pending_batches())]
