"""Summary satistics dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.utils import parse_region, split_pvalue
from gentropy.config import LocusBreakerClumpingConfig, WindowBasedClumpingStepConfig
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatistics dataset.

        Returns:
            StructType: Schema for the SummaryStatistics dataset
        """
        return parse_spark_schema("summary_statistics.json")

    def pvalue_filter(self: SummaryStatistics, pvalue: float) -> SummaryStatistics:
        """Filter summary statistics based on the provided p-value threshold.

        Args:
            pvalue (float): upper limit of the p-value to be filtered upon.

        Returns:
            SummaryStatistics: summary statistics object containing single point associations with p-values at least as significant as the provided threshold.
        """
        # Converting p-value to mantissa and exponent:
        (mantissa, exponent) = split_pvalue(pvalue)

        # Applying filter:
        df = self._df.filter(
            (f.col("pValueExponent") < exponent)
            | (
                (f.col("pValueExponent") == exponent)
                & (f.col("pValueMantissa") <= mantissa)
            )
        )
        return SummaryStatistics(_df=df, _schema=self._schema)

    def window_based_clumping(
        self: SummaryStatistics,
        distance: int = WindowBasedClumpingStepConfig().distance,
        gwas_significance: float = WindowBasedClumpingStepConfig().gwas_significance,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics using window-based clumping.

        For more info, see [`WindowBasedClumping`][gentropy.method.window_based_clumping.WindowBasedClumping]

        Args:
            distance (int): Distance in base pairs to be used for clumping. Defaults to 500_000.
            gwas_significance (float, optional): GWAS significance threshold. Defaults to 5e-8.

        Returns:
            StudyLocus: Clumped study-locus optionally containing variants based on window.
            Check WindowBasedClumpingStepConfig object for default values.
        """
        from gentropy.method.window_based_clumping import WindowBasedClumping

        return WindowBasedClumping.clump(
            self,
            distance=distance,
            gwas_significance=gwas_significance,
        )

    def locus_breaker_clumping(
        self: SummaryStatistics,
        baseline_pvalue_cutoff: float = LocusBreakerClumpingConfig.lbc_baseline_pvalue,
        distance_cutoff: int = LocusBreakerClumpingConfig.lbc_distance_cutoff,
        pvalue_cutoff: float = LocusBreakerClumpingConfig.lbc_pvalue_threshold,
        flanking_distance: int = LocusBreakerClumpingConfig.lbc_flanking_distance,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics using locus-breaker clumping method with locus boundaries.

        For more info, see [`locus_breaker`][gentropy.method.locus_breaker_clumping.locus_breaker]

        Args:
            baseline_pvalue_cutoff (float, optional): Baseline significance we consider for the locus.
            distance_cutoff (int, optional): Distance in base pairs to be used for clumping.
            pvalue_cutoff (float, optional): GWAS significance threshold.
            flanking_distance (int, optional): Flank distance in base pairs to be used for clumping.

        Returns:
            StudyLocus: Clumped study-locus optionally containing variants based on window.
            Check LocusBreakerClumpingConfig object for default values.
        """
        from gentropy.method.locus_breaker_clumping import LocusBreakerClumping

        return LocusBreakerClumping.locus_breaker(
            self,
            baseline_pvalue_cutoff,
            distance_cutoff,
            pvalue_cutoff,
            flanking_distance,
        )

    def exclude_region(self: SummaryStatistics, region: str) -> SummaryStatistics:
        """Exclude a region from the summary stats dataset.

        Args:
            region (str): region given in "chr##:#####-####" format

        Returns:
            SummaryStatistics: filtered summary statistics.
        """
        (chromosome, start_position, end_position) = parse_region(region)

        return SummaryStatistics(
            _df=(
                self.df.filter(
                    ~(
                        (f.col("chromosome") == chromosome)
                        & (
                            (f.col("position") >= start_position)
                            & (f.col("position") <= end_position)
                        )
                    )
                )
            ),
            _schema=SummaryStatistics.get_schema(),
        )

    def sanity_filter(self: SummaryStatistics) -> SummaryStatistics:
        """The function filters the summary statistics by sanity filters.

        The function filters the summary statistics by the following filters:
            - The p-value should be less than 1.
            - The pValueMantissa should be greater than 0.
            - The beta should not be equal 0.
            - The p-value, beta and se should not be NaN.
            - The se should be positive.
            - The beta and se should not be infinite.

        Returns:
            SummaryStatistics: The filtered summary statistics.
        """
        gwas_df = self._df
        gwas_df = gwas_df.dropna(
            subset=["beta", "standardError", "pValueMantissa", "pValueExponent"]
        )
        gwas_df = gwas_df.filter((f.col("beta") != 0) & (f.col("standardError") > 0))
        gwas_df = gwas_df.filter(
            (f.col("pValueMantissa") * 10 ** f.col("pValueExponent") < 1)
            & (f.col("pValueMantissa") > 0)
        )
        cols = ["beta", "standardError"]
        summary_stats = SummaryStatistics(
            _df=gwas_df,
            _schema=SummaryStatistics.get_schema(),
        ).drop_infinity_values(*cols)

        return summary_stats
