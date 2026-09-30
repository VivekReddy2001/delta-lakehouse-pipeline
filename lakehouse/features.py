"""Pure DataFrame transforms: bronze -> silver -> gold.

Nothing here reads or writes storage, so every transform is unit-testable on
a few hand-written rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAW_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_time", T.StringType(), False),
        T.StructField("user_id", T.StringType(), False),
        T.StructField("app_id", T.StringType(), False),
        T.StructField("server_ip", T.StringType(), False),
        T.StructField("port", T.IntegerType(), False),
        T.StructField("protocol", T.StringType(), False),
        T.StructField("bytes_sent", T.LongType(), False),
        T.StructField("bytes_recv", T.LongType(), False),
        T.StructField("action", T.StringType(), False),
    ]
)

ROLLING_DAYS = 30


def to_bronze(raw: DataFrame, batch_id: str) -> DataFrame:
    """Type the raw records and stamp them with their arrival batch."""
    return (
        raw.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def to_silver(bronze: DataFrame) -> DataFrame:
    """One row per ``event_id``: the copy from the earliest batch wins.

    Redelivered events are byte-identical, so the choice only matters for
    determinism - the same input must always give the same output.
    """
    w = Window.partitionBy("event_id").orderBy(F.col("_batch_id").asc())
    return (
        bronze.withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .withColumn("bytes_total", F.col("bytes_sent") + F.col("bytes_recv"))
    )


# ---------------------------------------------------------------- daily gold
def app_user_daily(silver: DataFrame) -> DataFrame:
    return silver.groupBy("event_date", "app_id", "user_id").agg(
        F.count("*").alias("events"),
        F.sum("bytes_total").alias("bytes"),
        F.sum(F.when(F.col("action") == "block", 1).otherwise(0)).alias("blocked"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
    )


def app_server_daily(silver: DataFrame) -> DataFrame:
    return silver.groupBy("event_date", "app_id", "server_ip").agg(
        F.count("*").alias("events"),
        F.sum("bytes_total").alias("bytes"),
        F.countDistinct("user_id").alias("distinct_users"),
    )


def app_port_daily(silver: DataFrame) -> DataFrame:
    return silver.groupBy("event_date", "app_id", "port", "protocol").agg(
        F.count("*").alias("events"), F.sum("bytes_total").alias("bytes")
    )


def app_pair_daily(silver: DataFrame, max_apps_per_window: int = 10, min_support: int = 2) -> DataFrame:
    """Applications used together by the same user within the same hour.

    The number of pairs grows quadratically with the apps a user touches in
    a window, so two pruning rules keep the table tractable:

    * ``max_apps_per_window`` - only the user's K busiest apps in the window
      take part (a scanner touching 300 apps would otherwise emit ~45k pairs);
    * ``min_support`` - a pair must co-occur in at least this many
      user-hour windows that day to be kept.
    """
    per_app = (
        silver.withColumn("window_start", F.date_trunc("hour", "event_time"))
        .groupBy("event_date", "user_id", "window_start", "app_id")
        .agg(F.count("*").alias("n"))
    )
    rank = Window.partitionBy("event_date", "user_id", "window_start").orderBy(F.desc("n"), F.asc("app_id"))
    top = per_app.withColumn("_r", F.row_number().over(rank)).where(F.col("_r") <= max_apps_per_window).drop("_r", "n")
    a, b = top.alias("a"), top.alias("b")
    pairs = a.join(
        b,
        on=[
            F.col("a.event_date") == F.col("b.event_date"),
            F.col("a.user_id") == F.col("b.user_id"),
            F.col("a.window_start") == F.col("b.window_start"),
            F.col("a.app_id") < F.col("b.app_id"),
        ],
    ).select(
        F.col("a.event_date").alias("event_date"),
        F.col("a.user_id").alias("user_id"),
        F.col("a.app_id").alias("app_a"),
        F.col("b.app_id").alias("app_b"),
    )
    return (
        pairs.groupBy("event_date", "app_a", "app_b")
        .agg(F.count("*").alias("windows"), F.countDistinct("user_id").alias("distinct_users"))
        .where(F.col("windows") >= min_support)
    )


DAILY_BUILDERS = {
    "app_user_daily": app_user_daily,
    "app_server_daily": app_server_daily,
    "app_port_daily": app_port_daily,
    "app_pair_daily": app_pair_daily,
}


# -------------------------------------------------------------- rolling gold
def _windows(daily: DataFrame, as_of: DataFrame) -> DataFrame:
    """Attach every daily row to each as-of date whose 30-day window covers it."""
    d = daily.alias("d")
    return d.join(
        as_of.alias("w"),
        (F.col("d.event_date") <= F.col("w.as_of_date"))
        & (F.col("d.event_date") > F.date_sub(F.col("w.as_of_date"), ROLLING_DAYS)),
    )


def app_user_30d(app_user: DataFrame, as_of: DataFrame) -> DataFrame:
    return (
        _windows(app_user, as_of)
        .groupBy("as_of_date", "app_id", "user_id")
        .agg(
            F.countDistinct("event_date").alias("active_days"),
            F.sum("events").alias("events_30d"),
            F.sum("bytes").alias("bytes_30d"),
            F.sum("blocked").alias("blocked_30d"),
            F.max("last_seen").alias("last_seen"),
        )
    )


def app_30d(app_user: DataFrame, app_server: DataFrame, as_of: DataFrame) -> DataFrame:
    users = (
        _windows(app_user, as_of)
        .groupBy("as_of_date", "app_id")
        .agg(F.countDistinct("user_id").alias("distinct_users_30d"), F.sum("events").alias("events_30d"))
    )
    servers = (
        _windows(app_server, as_of)
        .groupBy("as_of_date", "app_id")
        .agg(F.countDistinct("server_ip").alias("distinct_servers_30d"))
    )
    return users.join(servers, ["as_of_date", "app_id"], "full_outer").fillna(0, ["distinct_servers_30d"])
