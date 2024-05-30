"""Dataset class for gentropy."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType
from typing_extensions import Self

from gentropy.common.schemas import flatten_schema

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.common.session import Session


@dataclass
class Dataset(ABC):
    """Open Targets Gentropy Dataset.

    `Dataset` is a wrapper around a Spark DataFrame with a predefined schema. Schemas for each child dataset are described in the `schemas` module.
    """

    _df: DataFrame
    _schema: StructType

    def __post_init__(self: Dataset) -> None:
        """Post init."""
        self.validate_schema()

    @property
    def df(self: Dataset) -> DataFrame:
        """Dataframe included in the Dataset.

        Returns:
            DataFrame: Dataframe included in the Dataset
        """
        return self._df

    @df.setter
    def df(self: Dataset, new_df: DataFrame) -> None:  # noqa: CCE001
        """Dataframe setter.

        Args:
            new_df (DataFrame): New dataframe to be included in the Dataset
        """
        self._df: DataFrame = new_df
        self.validate_schema()

    @property
    def schema(self: Dataset) -> StructType:
        """Dataframe expected schema.

        Returns:
            StructType: Dataframe expected schema
        """
        return self._schema

    @classmethod
    @abstractmethod
    def get_schema(cls: type[Self]) -> StructType:
        """Abstract method to get the schema. Must be implemented by child classes.

        Returns:
            StructType: Schema for the Dataset
        """
        pass

    @classmethod
    def from_parquet(
        cls: type[Self],
        session: Session,
        path: str | list[str],
        **kwargs: bool | float | int | str | None,
    ) -> Self:
        """Reads parquet into a Dataset with a given schema.

        Args:
            session (Session): Spark session
            path (str | list[str]): Path to the parquet dataset
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.parquet

        Returns:
            Self: Dataset with the parquet file contents

        Raises:
            ValueError: Parquet file is empty
        """
        schema = cls.get_schema()
        df = session.read_parquet(path, schema=schema, **kwargs)
        if df.isEmpty():
            raise ValueError(f"Parquet file is empty: {path}")
        return cls(_df=df, _schema=schema)

    def filter(self: Self, condition: Column) -> Self:
        """Creates a new instance of a Dataset with the DataFrame filtered by the condition.

        Args:
            condition (Column): Condition to filter the DataFrame

        Returns:
            Self: Filtered Dataset
        """
        df = self._df.filter(condition)
        class_constructor = self.__class__
        return class_constructor(_df=df, _schema=class_constructor.get_schema())

    def validate_schema(self: Dataset) -> None:
        """Validate DataFrame schema against expected class schema.

        Raises:
            ValueError: DataFrame schema is not valid
        """
        expected_schema = self._schema
        expected_fields = flatten_schema(expected_schema)
        observed_schema = self._df.schema
        observed_fields = flatten_schema(observed_schema)

        # Unexpected fields in dataset
        if unexpected_field_names := [
            x.name
            for x in observed_fields
            if x.name not in [y.name for y in expected_fields]
        ]:
            raise ValueError(
                f"The {unexpected_field_names} fields are not included in DataFrame schema: {expected_fields}"
            )

        # Required fields not in dataset
        required_fields = [x.name for x in expected_schema if not x.nullable]
        if missing_required_fields := [
            req
            for req in required_fields
            if not any(field.name == req for field in observed_fields)
        ]:
            raise ValueError(
                f"The {missing_required_fields} fields are required but missing: {required_fields}"
            )

        # Fields with duplicated names
        if duplicated_fields := [
            x for x in set(observed_fields) if observed_fields.count(x) > 1
        ]:
            raise ValueError(
                f"The following fields are duplicated in DataFrame schema: {duplicated_fields}"
            )

        # Fields with different datatype
        observed_field_types = {
            field.name: type(field.dataType) for field in observed_fields
        }
        expected_field_types = {
            field.name: type(field.dataType) for field in expected_fields
        }
        if fields_with_different_observed_datatype := [
            name
            for name, observed_type in observed_field_types.items()
            if name in expected_field_types
            and observed_type != expected_field_types[name]
        ]:
            raise ValueError(
                f"The following fields present differences in their datatypes: {fields_with_different_observed_datatype}."
            )

    def drop_infinity_values(self: Self, *cols: str) -> Self:
        """Drop infinity values from Double typed column.

        Infinity type reference - https://spark.apache.org/docs/latest/sql-ref-datatypes.html#floating-point-special-values
        The implementation comes from https://stackoverflow.com/questions/34432998/how-to-replace-infinity-in-pyspark-dataframe

        Args:
            *cols (str): names of the columns to check for infinite values, these should be of DoubleType only!

        Returns:
            Self: Dataset after removing infinite values
        """
        if len(cols) == 0:
            return self
        inf_strings = ("Inf", "+Inf", "-Inf", "Infinity", "+Infinity", "-Infinity")
        inf_values = [f.lit(v).cast(DoubleType()) for v in inf_strings]
        conditions = [f.col(c).isin(inf_values) for c in cols]
        # reduce individual filter expressions with or statement
        # to col("beta").isin([lit(Inf)]) | col("beta").isin([lit(Inf)])...
        condition = reduce(lambda a, b: a | b, conditions)
        self.df = self._df.filter(~condition)
        return self

    def persist(self: Self) -> Self:
        """Persist in memory the DataFrame included in the Dataset.

        Returns:
            Self: Persisted Dataset
        """
        self.df = self._df.persist()
        return self

    def unpersist(self: Self) -> Self:
        """Remove the persisted DataFrame from memory.

        Returns:
            Self: Unpersisted Dataset
        """
        self.df = self._df.unpersist()
        return self

    def coalesce(self: Self, num_partitions: int, **kwargs: Any) -> Self:
        """Coalesce the DataFrame included in the Dataset.

        Coalescing is efficient for decreasing the number of partitions because it avoids a full shuffle of the data.

        Args:
            num_partitions (int): Number of partitions to coalesce to
            **kwargs (Any): Arguments to pass to the coalesce method

        Returns:
            Self: Coalesced Dataset
        """
        self.df = self._df.coalesce(num_partitions, **kwargs)
        return self

    def repartition(self: Self, num_partitions: int, **kwargs: Any) -> Self:
        """Repartition the DataFrame included in the Dataset.

        Repartitioning creates new partitions with data that is distributed evenly.

        Args:
            num_partitions (int): Number of partitions to repartition to
            **kwargs (Any): Arguments to pass to the repartition method

        Returns:
            Self: Repartitioned Dataset
        """
        self.df = self._df.repartition(num_partitions, **kwargs)
        return self
