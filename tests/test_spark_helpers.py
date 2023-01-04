"""Tests on helper spark functions."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from etl.common.spark_helpers import (
    get_record_with_maximum_value,
    get_record_with_minimum_value,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture
def mock_variant_df(spark: SparkSession) -> DataFrame:
    """Mock Dataframe with info of a variant ID."""
    return spark.createDataFrame(
        [
            [16, 5738837, "snv", "16_5738837_G_C"],
            [16, 10116, "del", "16_10116_TTG_T"],
            [17, 5738922, "del", "16_5738922_CA_C"],
        ],
        ["chromosome", "position", "alleleType", "id"],
    )


def test_get_record_with_minimum_value_group_one_col(
    mock_variant_df: DataFrame,
) -> None:
    """Test the util that return the row with the minimum value in a window by grouping per one column."""
    grouping_col, sorting_col = "chromosome", "position"
    df = mock_variant_df.transform(
        lambda df: get_record_with_minimum_value(df, grouping_col, sorting_col)
    )
    assert (
        df.filter(f.col("chromosome") == 16).collect()[0].__getitem__("position")
    ), 10116


def test_get_record_with_maximum_value_group_two_cols(
    mock_variant_df: DataFrame,
) -> None:
    """Test the util that return the row with the minimum value in a window by grouping per two columns."""
    grouping_col, sorting_col = ["chromosome", "alleleType"], "position"
    df = mock_variant_df.transform(
        lambda df: get_record_with_maximum_value(df, grouping_col, sorting_col)
    )
    assert df.count(), 3
