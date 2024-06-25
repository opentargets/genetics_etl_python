"""Study Index for Finngen data source."""
from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from gentropy.dataset.study_index import StudyIndex


class UkbPppEurStudyIndex(StudyIndex):
    """Study index dataset from UKB PPP (EUR)."""

    @classmethod
    def from_source(
        cls: type[UkbPppEurStudyIndex],
        spark: SparkSession,
        raw_study_index_path: str,
        raw_summary_stats_path: str,
    ) -> StudyIndex:
        """This function ingests study level metadata from UKB PPP (EUR).

        Args:
            spark (SparkSession): Spark session object.
            raw_study_index_path (str): Raw study index path.
            raw_summary_stats_path (str): Raw summary stats path.

        Returns:
            StudyIndex: Parsed and annotated UKB PPP (EUR) study table.
        """
        # In order to populate the nSamples column, we need to peek inside the summary stats dataframe.
        num_of_samples = (
            spark
            .read
            .parquet(raw_summary_stats_path)
            .filter(f.col("chromosome") == "22")
            .groupBy("studyId")
            .agg(f.first("N").cast("integer").alias("nSamples"))
            .select("*")
        )
        # Now we can read the raw study index and complete the processing.
        study_index_df = (
            spark.read.csv(raw_study_index_path, sep="\t", header=True)
            .select(
                f.lit("pqtl").alias("studyType"),
                f.lit("UKB_PPP_EUR").alias("projectId"),
                f.col("_gentropy_study_id").alias("studyId"),
                f.col("UKBPPP_ProteinID").alias("traitFromSource"),
                f.lit("UBERON_0001969").alias("tissueFromSourceId"),
                f.col("ensembl_id").alias("geneId"),
                f.lit(True).alias("hasSumstats"),
                f.col("_gentropy_summary_stats_link").alias("summarystatsLocation"),
            )
            .join(num_of_samples, "studyId", "inner")
        )
        # Add population structure.
        study_index_df = (
            study_index_df
            .withColumn(
                "discoverySamples",
                f.array(
                    f.struct(
                        f.col("nSamples").cast("integer").alias("sampleSize"),
                        f.lit("European").alias("ancestry"),
                    )
                )
            )
            .withColumn(
                "ldPopulationStructure",
                cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
            )
        )

        return StudyIndex(
            _df=study_index_df,
            _schema=StudyIndex.get_schema(),
        )
