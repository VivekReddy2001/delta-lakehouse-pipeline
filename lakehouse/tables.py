"""Table locations and the one write primitive every stage uses.

No stage in this pipeline ever *appends* derived data. Every derived table is
written by recomputing whole partitions from the layer below and replacing
exactly those partitions (Delta ``replaceWhere``). A retry therefore
reproduces the same bytes instead of adding a second copy of them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class Lake:
    root: Path

    def path(self, name: str) -> str:
        return str(Path(self.root) / name)

    @property
    def bronze(self) -> str:
        return self.path("bronze/events")

    @property
    def silver(self) -> str:
        return self.path("silver/events")

    @property
    def ledger(self) -> str:
        return self.path("_ledger")

    def gold(self, name: str) -> str:
        return self.path(f"gold/{name}")


GOLD_DAILY = ["app_user_daily", "app_server_daily", "app_port_daily", "app_pair_daily"]
GOLD_ROLLING = ["app_user_30d", "app_30d"]


def exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def _literal(v) -> str:
    if isinstance(v, date):
        return f"DATE'{v.isoformat()}'"
    return "'" + str(v).replace("'", "''") + "'"


def replace_partitions(df: DataFrame, path: str, column: str, values) -> None:
    """Atomically replace the partitions ``column IN values`` with ``df``.

    ``df`` must only contain rows for those partitions. A value with no rows
    in ``df`` ends up empty, which is the correct result of a recomputation.
    """
    values = sorted(set(values))
    if not values:
        return
    spark = df.sparkSession
    if not exists(spark, path):
        df.write.format("delta").partitionBy(column).save(path)
        return
    predicate = f"{column} IN ({', '.join(_literal(v) for v in values)})"
    (df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).save(path))
