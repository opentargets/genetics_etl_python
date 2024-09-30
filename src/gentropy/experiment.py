"""Experimental dataclass inheritance."""

from abc import ABC, abstractmethod

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from typing_extensions import Self

from gentropy.common.schemas import flatten_schema


class Dataset(ABC):
    """Generic dataset class."""

    __schema: StructType
    __df: DataFrame

    @classmethod
    @abstractmethod
    def unique_defining_columns(cls: type["Dataset"]) -> list[str]:
        """Read-only unique fields."""
        pass

    @classmethod
    def id_column(cls: type["Dataset"]) -> str | None:
        """Read-only identifier column. Returns None unless overriden in the child class."""
        return None

    @classmethod
    def quality_controls_column(cls: type["Dataset"]) -> str | None:
        """Read-only quality control column. Returns None unless overriden in the child class."""
        return None

    @classmethod
    @abstractmethod
    def schema(cls: type["Dataset"]) -> StructType:
        """Read-only schema."""
        return cls.__schema

    @property
    def df(self: Self) -> DataFrame:
        """Read-only DataFrame."""
        return self.__df

    def __init__(self, df: DataFrame) -> None:
        """Constructor.

        Args:
            df (DataFrame): a dataframe
        """
        # Updating id column if available
        # TODO: protect against misssing `unique_defining_columns` by intersecting self.id_column() and self.df.columns
        id_col = self.id_column()
        if id_col:
            self.__df = df.withColumn(
                id_col, f.hash(*self.unique_defining_columns()).cast(StringType())
            )
        else:
            self.__df = df
        # Schema validation
        self.validate_schema()
        super().__init__()

    def check_duplicates(self: Self) -> None:
        """Check for the presence of duplicate records."""
        if (
            self.df.select(self.unique_defining_columns()).distinct().count()
            != self.df.count()
        ):
            raise ValueError(
                f"Duplicate records found based on unique defining columns: {self.unique_defining_columns()}"
            )

    def validate_schema(self: Self) -> None:
        """Validate DataFrame schema against expected class schema.

        Raises:
            ValueError: DataFrame schema is not valid
        """
        expected_schema = self.schema()
        expected_fields = flatten_schema(expected_schema)
        observed_schema = self.df.schema
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

    def write_metadata_file(self: Self) -> None:
        """Write metadata file similar to Platform-etl."""
        raise NotImplementedError


class DatasetWithId(Dataset):
    """Sub-class containing an ID column."""

    __id_column: str = "testId"
    __quality_controls_column: str = "qualityControls"
    __unique_defining_columns: list[str] = ["a", "b"]
    __schema: StructType = StructType(
        [
            StructField("testId", StringType(), False),
            StructField("a", StringType(), False),
            StructField("b", LongType(), False),
            StructField("c", DoubleType(), False),
            StructField("qualityControls", ArrayType(StringType()), False),
        ]
    )

    @classmethod
    def id_column(cls: type["DatasetWithId"]) -> str:
        """Read-only id column."""
        return cls.__id_column

    @classmethod
    def quality_controls_column(cls: type["DatasetWithId"]) -> str:
        """Quality controls column."""
        return cls.__quality_controls_column

    @classmethod
    def unique_defining_columns(cls: type["DatasetWithId"]) -> list[str]:
        """Read-only unique fields."""
        return cls.__unique_defining_columns

    @classmethod
    def schema(cls: type["DatasetWithId"]) -> StructType:
        """Read-only schema."""
        return cls.__schema


class DatasetWithoutId(Dataset):
    """Sub-class containing an ID column."""

    __unique_defining_columns: list[str] = ["a", "b"]
    __schema: StructType = StructType(
        [
            StructField("a", StringType(), False),
            StructField("b", LongType(), False),
            StructField("c", DoubleType(), False),
        ]
    )

    @classmethod
    def unique_defining_columns(cls: type["DatasetWithoutId"]) -> list[str]:
        """Read-only unique fields."""
        return cls.__unique_defining_columns

    @classmethod
    def schema(cls: type["DatasetWithoutId"]) -> StructType:
        """Read-only schema."""
        return cls.__schema
