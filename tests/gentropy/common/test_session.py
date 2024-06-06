"""Tests Gentropy session."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.common.session import Log4j, Session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_session_creation() -> None:
    """Test sessio creation with mock data."""
    assert isinstance(Session(spark_uri="local[1]"), Session)


def test_hail_configuration(hail_home: str) -> None:
    """Assert that Hail configuration is set when start_hail is True."""
    session = Session(spark_uri="local[1]", hail_home=hail_home, start_hail=True)

    expected_hail_conf = {
        "spark.jars": f"{hail_home}/backend/hail-all-spark.jar",
        "spark.driver.extraClassPath": f"{hail_home}/backend/hail-all-spark.jar",
        "spark.executor.extraClassPath": "./hail-all-spark.jar",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
    }

    observed_conf = dict(session.spark.sparkContext.getConf().getAll())
    # sourcery skip: no-loop-in-tests
    for key, value in expected_hail_conf.items():
        assert observed_conf.get(key) == value, f"Expected {key} to be set to {value}"


def test_log4j_creation(spark: SparkSession) -> None:
    """Test session log4j."""
    assert isinstance(Log4j(spark=spark), Log4j)
