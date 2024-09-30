"""Tests for experiment."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.experiment import DatasetWithId, DatasetWithoutId


class TestDatasetWithId:
    """Tests for DatasetWithId class."""

    @pytest.fixture(autouse=True)
    def _setup(self: TestDatasetWithId, spark: SparkSession) -> None:
        """Setup DatasetWithId for testing."""
        self.df = spark.createDataFrame(
            data=[("x", 1, 1.0, ["failed"])],
            schema=StructType(
                [
                    StructField("a", StringType(), True),
                    StructField("b", LongType(), True),
                    StructField("c", DoubleType(), True),
                    StructField("qualityControls", ArrayType(StringType(), True), True),
                ]
            ),
        )
        self.df_with_duplicates = spark.createDataFrame(
            data=[("x", 1, 1.0, []), ("x", 1, 2.0, [])],
            schema=StructType(
                [
                    StructField("a", StringType(), False),
                    StructField("b", LongType(), False),
                    StructField("c", DoubleType(), False),
                    StructField("qualityControls", ArrayType(StringType(), True), True),
                ]
            ),
        )
        self.df_invalid_schema = spark.createDataFrame(
            data=[("x", 1, 1.0)],
            schema=StructType(
                [
                    StructField("a", StringType(), False),
                    StructField("b", LongType(), False),
                    StructField("x", DoubleType(), False),
                ]
            ),
        )

    def test_id_column_in_schema(self: TestDatasetWithId) -> None:
        """Test the specified identifier column belongs to the dataset schema."""
        assert DatasetWithId.id_column() in DatasetWithId.schema().fieldNames()

    def test_quality_controls_column_in_schema(self: TestDatasetWithId) -> None:
        """Test the specified quality controls column belongs to the dataset schema."""
        assert (
            DatasetWithId.quality_controls_column()
            in DatasetWithId.schema().fieldNames()
        )

    def test_init_return_type(self: TestDatasetWithId) -> None:
        """Test dataset."""
        assert isinstance(DatasetWithId(df=self.df), DatasetWithId)

    def test_id_column_presence(self: TestDatasetWithId) -> None:
        """Test the id column is created during initalisation."""
        assert DatasetWithId.id_column() in DatasetWithId(df=self.df).df.columns

    def test_invalid_schema(self: TestDatasetWithId) -> None:
        """Test dataset."""
        with pytest.raises(ValueError, match="schema"):
            DatasetWithId(df=self.df_invalid_schema)

    def test_no_duplicates(self: TestDatasetWithId) -> None:
        """Test dataset without duplicates."""
        DatasetWithId(df=self.df).check_duplicates()

    def test_duplicates(self: TestDatasetWithId) -> None:
        """Test dataset with duplicates."""
        with pytest.raises(ValueError, match="Duplicate"):
            DatasetWithId(df=self.df_with_duplicates).check_duplicates()


class TestDatasetWithoutId:
    """Tests for DatasetWithId class."""

    @pytest.fixture(autouse=True)
    def _setup(self: TestDatasetWithoutId, spark: SparkSession) -> None:
        """Setup DatasetWithId for testing."""
        self.df = spark.createDataFrame(
            data=[("x", 1, 1.0)],
            schema=StructType(
                [
                    StructField("a", StringType(), True),
                    StructField("b", LongType(), True),
                    StructField("c", DoubleType(), True),
                ]
            ),
        )
        self.df_with_duplicates = spark.createDataFrame(
            data=[("x", 1, 1.0), ("x", 1, 2.0)],
            schema=StructType(
                [
                    StructField("a", StringType(), False),
                    StructField("b", LongType(), False),
                    StructField("c", DoubleType(), False),
                ]
            ),
        )
        self.df_invalid_schema = spark.createDataFrame(
            data=[("x", 1, 1.0)],
            schema=StructType(
                [
                    StructField("a", StringType(), False),
                    StructField("b", LongType(), False),
                    StructField("x", DoubleType(), False),
                ]
            ),
        )

    def test_init_return_type(self: TestDatasetWithoutId) -> None:
        """Test dataset."""
        assert isinstance(DatasetWithoutId(df=self.df), DatasetWithoutId)

    def test_invalid_schema(self: TestDatasetWithoutId) -> None:
        """Test dataset."""
        with pytest.raises(ValueError, match="schema"):
            DatasetWithoutId(df=self.df_invalid_schema)

    def test_no_duplicates(self: TestDatasetWithoutId) -> None:
        """Test dataset without duplicates."""
        DatasetWithoutId(df=self.df).check_duplicates()

    def test_duplicates(self: TestDatasetWithoutId) -> None:
        """Test dataset with duplicates."""
        with pytest.raises(ValueError, match="Duplicate"):
            DatasetWithoutId(df=self.df_with_duplicates).check_duplicates()
