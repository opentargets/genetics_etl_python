"""LiftOver support."""

from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from google.cloud import storage
from pyliftover import LiftOver

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class LiftOverSpark:
    """LiftOver class for mapping genomic coordinates to an other genome build.

    The input is a Spark DataFrame with a chromosome and position column. This classs can
    also map regions, if a start and end positions are provided.

    **Logic**:

    - The mapping is dropped if the mapped chromosome is not on the same as the source.
    - The mapping is dropped if the mapping is ambiguous (more than one mapping is available).
    - If regions are provided, the mapping is dropped if the new region is reversed (mapped_start > mapped_end).
    - If regions are provided, the mapping is dropped if the difference of the length of the mapped region and original is larger than a threshold.
    - When lifting over intervals, only unique coordinates are lifted, they joined back to the original dataframe.
    """

    def __init__(
        self: LiftOverSpark, chain_file: str, max_difference: int = 100
    ) -> None:
        """Initialize the LiftOver object.

        Args:
            chain_file (str): Path to the chain file
            max_difference (int, optional): The maximum tolerated difference in the resulting length. Defaults to 100.
        """
        self.chain_file = chain_file
        self.max_difference = max_difference

        # Initializing liftover object by opening the chain file - LiftOver only supports local files:
        if chain_file.startswith("gs://"):
            bucket_name = chain_file.split("/")[2]
            blob_name = "/".join(chain_file.split("/")[3:])
            with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                storage.Client().bucket(bucket_name).blob(
                    blob_name
                ).download_to_filename(temp_file.name)
                self.lo = LiftOver(temp_file.name)
        else:
            self.lo = LiftOver(chain_file)

        # UDF to do map genomic coordinates to liftover coordinates:
        self.liftover_udf = f.udf(
            lambda chrom, pos: self.lo.convert_coordinate(chrom, pos),
            t.ArrayType(t.ArrayType(t.StringType())),
        )

    def convert_intervals(
        self: LiftOverSpark,
        df: DataFrame,
        chrom_col: str,
        start_col: str,
        end_col: str,
        filter: bool = True,
    ) -> DataFrame:
        """Convert genomic intervals to liftover coordinates.

        Args:
            df (DataFrame): spark Dataframe with chromosome, start and end columns.
            chrom_col (str): Name of the chromosome column.
            start_col (str): Name of the start column.
            end_col (str): Name of the end column.
            filter (bool): If True, filter is applied on the mapped data, otherwise return everything. Defaults to True.

        Returns:
            DataFrame: Liftovered intervals
        """
        # Lift over start coordinates, changing to 1-based coordinates:
        start_df = (
            df.withColumn(start_col, f.col(start_col) + 1)
            .select(chrom_col, start_col)
            .distinct()
        )
        start_df = self.convert_coordinates(
            start_df, chrom_col, start_col
        ).withColumnRenamed("mapped_pos", f"mapped_{start_col}")

        # Lift over end coordinates:
        end_df = df.select(chrom_col, end_col).distinct()
        end_df = self.convert_coordinates(end_df, chrom_col, end_col).withColumnRenamed(
            "mapped_pos", f"mapped_{end_col}"
        )

        # Join dataframe with mappings (we have to account for the +1 position shift of the start coordinates):
        mapped_df = df.join(
            start_df.withColumn(start_col, f.col(start_col) - 1),
            on=[chrom_col, start_col],
            how="left",
        ).join(end_df, on=[chrom_col, end_col], how="left")

        # The filter option allows to get all the data and filter it afterwards.
        if filter:
            return (
                mapped_df
                # Select only rows where the start is smaller than the end:
                .filter(
                    # Drop rows with no mappings:
                    f.col(f"mapped_{start_col}").isNotNull()
                    & f.col(f"mapped_{end_col}").isNotNull()
                    # Drop rows where the start is larger than the end:
                    & (f.col(f"mapped_{end_col}") >= f.col(f"mapped_{start_col}"))
                    # Drop rows where the difference of the length of the regions are larger than the threshold:
                    & (
                        f.abs(
                            (f.col(end_col) - f.col(start_col))
                            - (
                                f.col(f"mapped_{end_col}")
                                - f.col(f"mapped_{start_col}")
                            )
                        )
                        <= self.max_difference
                    )
                ).persist()
            )
        else:
            return mapped_df.persist()

    def convert_coordinates(
        self: LiftOverSpark, df: DataFrame, chrom_name: str, pos_name: str
    ) -> DataFrame:
        """Converts genomic coordinates to coordinates on an other build.

        Args:
            df (DataFrame): Spark Dataframe with chromosome and position columns.
            chrom_name (str): Name of the chromosome column.
            pos_name (str): Name of the position column.

        Returns:
            DataFrame: Spark Dataframe with the mapped position column.
        """
        mapped = (
            df.withColumn(
                "mapped", self.liftover_udf(f.col(chrom_name), f.col(pos_name))
            )
            .filter((f.col("mapped").isNotNull()) & (f.size(f.col("mapped")) == 1))
            # Extracting mapped corrdinates:
            .withColumn("mapped_" + chrom_name, f.col("mapped")[0][0])
            .withColumn("mapped_" + pos_name, f.col("mapped")[0][1])
            # Drop rows that mapped to the other chromosomes:
            .filter(
                f.col("mapped_" + chrom_name)
                == f.concat(f.lit("chr"), f.col(chrom_name))
            )
            # Dropping unused columns:
            .drop("mapped", "mapped_" + chrom_name)
            .persist()
        )

        return mapped
