from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from lakehouse.generate import GenConfig, generate
from lakehouse.pipeline import Pipeline
from lakehouse.spark import get_spark

SMALL = GenConfig(
    start=date(2026, 1, 1),
    days=4,
    users=30,
    apps=12,
    events_per_day=300,
    late_fraction=0.15,
    max_lag_days=3,
    redelivery_fraction=0.08,
    seed=11,
)


@pytest.fixture(scope="session")
def spark():
    s = get_spark("lakehouse-tests", shuffle_partitions=2)
    yield s
    s.stop()


@pytest.fixture(scope="session")
def landing_template(tmp_path_factory) -> tuple[Path, list[str]]:
    root = tmp_path_factory.mktemp("landing")
    return root, generate(root, SMALL)


@pytest.fixture
def landing(tmp_path, landing_template) -> tuple[Path, list[str]]:
    """A private copy of the landed batches, so tests may tamper with them."""
    src, ids = landing_template
    dst = tmp_path / "landing"
    shutil.copytree(src, dst)
    return dst, ids


@pytest.fixture(scope="session")
def full_run(spark, tmp_path_factory, landing_template):
    """One clean, in-order run over all batches, shared by read-only tests."""
    root, ids = landing_template
    pipe = Pipeline(spark, tmp_path_factory.mktemp("full") / "lake", root)
    results = pipe.run(ids)
    return pipe, results, snapshot(spark, pipe)


@pytest.fixture(scope="session")
def first_two(spark, tmp_path_factory, landing_template):
    """Snapshot after a clean run of only the first two batches."""
    root, ids = landing_template
    pipe = Pipeline(spark, tmp_path_factory.mktemp("two") / "lake", root)
    pipe.run(ids[:2])
    return snapshot(spark, pipe)


@pytest.fixture
def make_pipeline(spark, tmp_path, landing):
    root, _ = landing

    def _make(name: str = "lake") -> Pipeline:
        return Pipeline(spark, tmp_path / name, root)

    return _make


def snapshot(spark, pipe: Pipeline) -> dict[str, list]:
    """Sorted contents of every derived table, for exact comparison."""
    from lakehouse.tables import GOLD_DAILY, GOLD_ROLLING

    out = {}
    names = {"silver": pipe.lake.silver} | {t: pipe.lake.gold(t) for t in GOLD_DAILY + GOLD_ROLLING}
    for name, path in names.items():
        df = spark.read.format("delta").load(path)
        cols = sorted(df.columns)
        out[name] = sorted(tuple(r) for r in df.select(*cols).collect())
    return out
