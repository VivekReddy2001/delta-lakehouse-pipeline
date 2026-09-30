"""SparkSession factory with Delta Lake configured."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse", shuffle_partitions: int | None = None) -> SparkSession:
    partitions = shuffle_partitions or int(os.environ.get("LAKEHOUSE_SHUFFLE_PARTITIONS", "8"))
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(partitions))
        # Delta rebuilds table state with 50 tasks by default; far too many locally.
        .config("spark.databricks.delta.snapshotPartitions", str(partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Only overwrite partitions that the replaceWhere predicate names.
        .config("spark.databricks.delta.replaceWhere.dataColumns.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
