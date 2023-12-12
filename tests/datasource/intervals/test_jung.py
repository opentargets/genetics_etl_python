"""Test Jung Intervals."""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from otg.common.Liftover import LiftOverSpark
from otg.dataset.gene_index import GeneIndex
from otg.dataset.intervals import Intervals
from otg.datasource.intervals.jung import IntervalsJung


@pytest.fixture(scope="module")
def sample_intervals_jung(spark: SparkSession) -> DataFrame:
    """Sample Jung intervals."""
    return IntervalsJung.read(spark, "tests/data_samples/jung_sample.bed")


def test_read_jung(sample_intervals_jung: DataFrame) -> None:
    """Test read Jung data."""
    assert isinstance(sample_intervals_jung, DataFrame)


def test_jung_intervals_from_source(
    sample_intervals_jung: DataFrame,
    mock_gene_index: GeneIndex,
    liftover_chain_37_to_38: LiftOverSpark,
) -> None:
    """Test JungIntervals creation with mock data."""
    assert isinstance(
        IntervalsJung.parse(
            sample_intervals_jung, mock_gene_index, liftover_chain_37_to_38
        ),
        Intervals,
    )
