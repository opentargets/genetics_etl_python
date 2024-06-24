"""Step to apply linkageg based clumping on study-locus dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.locus_breaker_clumping import LocusBreakerClumping


class LocusBreakerClumpingStep:
    """Step to perform locus-breaker clumping on a study."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        clumped_study_locus_output_path: str,
        lbc_baseline_pvalue: float,
        lbc_distance_cutoff: int,
        lbc_pvalue_threshold: float,
        lbc_flanking_distance: int,
        large_loci_size: int,
        wbc_pvalue_threshold: float,
        collect_locus_distance: int,
        collect_locus: bool = False,
        remove_mhc: bool = True,
    ) -> None:
        """Run locus-breaker clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the input study locus.
            clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
            lbc_baseline_pvalue (float): Baseline p-value for locus breaker clumping.
            lbc_distance_cutoff (int): Distance cutoff for locus breaker clumping.
            lbc_pvalue_threshold (float): P-value threshold for locus breaker clumping.
            lbc_flanking_distance (int): Flanking distance for locus breaker clumping.
            large_loci_size (int): Size of large loci.
            wbc_pvalue_threshold (float): P-value threshold for window breaker clumping.
            collect_locus_distance (int): Distance to collect locus.
            collect_locus (bool, optional): Whether to collect locus. Defaults to False.
            remove_mhc (bool, optional): If true will use exclude_region() to remove the MHC region.
        """
        sum_stats = SummaryStatistics.from_parquet(
            session, summary_statistics_input_path
        )
        lbc = sum_stats.locus_breaker_clumping(
            lbc_baseline_pvalue,
            lbc_distance_cutoff,
            lbc_pvalue_threshold,
            lbc_flanking_distance,
        )
        clumped_result = LocusBreakerClumping._process_locus_breaker(
            lbc,
            sum_stats,
            large_loci_size,
            wbc_pvalue_threshold,
        )
        if collect_locus:
            clumped_result = clumped_result.annotate_locus_statistics(
                sum_stats, collect_locus_distance
            )
        if remove_mhc:
            clumped_result = clumped_result.exclude_region("chr6:25726063-33400556")

        clumped_result.df.write.mode(session.write_mode).parquet(
            clumped_study_locus_output_path
        )
