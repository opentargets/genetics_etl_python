"""Locus-breaker clumping method."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

from gentropy.common.spark_helpers import calculate_neglog_pvalue
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:

    from gentropy.dataset.summary_statistics import SummaryStatistics


def locus_breaker(
    summary_statistics: SummaryStatistics,
    baseline_pvalue_cutoff: float,
    distance_cutoff: int,
    pvalue_cutoff: float,
    flankig_distance: int,
) -> StudyLocus:
    """Identify GWAS associated loci based on the provided p-value and distance cutoff.

    - The GWAS associated loci identified by this method have a varying width, and are separated by a distance greater than the provided distance cutoff.
    - The distance is only calculted between single point associations that reach the baseline p-value cutoff.
    - As the width of the selected genomic region dynamically depends on the loci, the resulting StudyLocus object will contain the locus start and end position.
    - To ensure completeness, the locus is extended by a flanking distance in both ends.

    Args:
        summary_statistics (SummaryStatistics): Input summary statistics dataset.
        baseline_pvalue_cutoff (float): baseline significance we consider for the locus.
        distance_cutoff (int): minimum distance that separates two loci.
        pvalue_cutoff (float): the minimum significance the locus should have.
        flankig_distance (int): the distance to extend the locus in both directions.

    Returns:
        StudyLocus: clumped study loci with locus start and end positions + lead variant from the locus.
    """
    # Extract columns from the summary statistics:
    columns_sumstats_columns = summary_statistics.df.columns
    # Convert pvalue_cutoff to neglog scale:
    neglog_pv_cutoff = -np.log10(pvalue_cutoff)

    # First window to calculate the distance between consecutive positions:
    w1 = Window.partitionBy("studyId", "chromosome").orderBy("position")

    # Second window to calculate the locus start and end:
    w2 = (
        Window.partitionBy("studyId", "chromosome", "locusStart")
        .orderBy("position")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    # Third window to rank the variants within the locus based on neglog p-value to find top loci:
    w3 = Window.partitionBy("studyId", "chromosome", "locusStart", "locusEnd").orderBy(
        f.col("negLogPValue").desc()
    )

    return StudyLocus(
        _df=(
            # Applying the baseline p-value cutoff:
            summary_statistics.pvalue_filter(baseline_pvalue_cutoff)
            # Calculating the neglog p-value for easier sorting:
            .df.withColumn(
                "negLogPValue",
                calculate_neglog_pvalue(
                    f.col("pValueMantissa"), f.col("pValueExponent")
                ),
            )
            # Calculating the distance between consecutive positions, then identifying the locus start and end:
            .withColumn("next_position", f.lag(f.col("position")).over(w1))
            .withColumn("distance", f.col("position") - f.col("next_position"))
            .withColumn(
                "locusStart",
                f.when(
                    (f.col("distance") > distance_cutoff) | f.col("distance").isNull(),
                    f.col("position"),
                ),
            )
            .withColumn(
                "locusStart",
                f.when(
                    f.last(f.col("locusStart") - flankig_distance, True).over(
                        w1.rowsBetween(-sys.maxsize, 0)
                    )
                    > 0,
                    f.last(f.col("locusStart") - flankig_distance, True).over(
                        w1.rowsBetween(-sys.maxsize, 0)
                    ),
                ).otherwise(f.lit(0)),
            )
            .withColumn(
                "locusEnd", f.max(f.col("position") + flankig_distance).over(w2)
            )
            .withColumn("rank", f.rank().over(w3))
            .filter((f.col("rank") == 1) & (f.col("negLogPValue") > neglog_pv_cutoff))
            .select(
                *columns_sumstats_columns,
                # To make sure that the type of locusStart and locusEnd follows schema of StudyLocus:
                f.col("locusStart").cast(t.IntegerType()).alias("locusStart"),
                f.col("locusEnd").cast(t.IntegerType()).alias("locusEnd"),
                StudyLocus.assign_study_locus_id(
                    f.col("studyId"), f.col("variantId")
                ).alias("studyLocusId"),
            )
        ),
        _schema=StudyLocus.get_schema(),
    )
