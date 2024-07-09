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
        wbc_clump_distance: int,
        wbc_pvalue_threshold: float,
        collect_locus: bool = False,
        remove_mhc: bool = True,
    ) -> None:
        """Run locus-breaker clumping step.

        This step will perform locus-breaker clumping on the full set of summary statistics.
        StudyLocus larger than the large_loci_size, by distance, will be further clumped with window-based
        clumping.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the input study locus.
            clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
            lbc_baseline_pvalue (float): Baseline p-value for locus breaker clumping.
            lbc_distance_cutoff (int): Distance cutoff for locus breaker clumping.
            lbc_pvalue_threshold (float): P-value threshold for locus breaker clumping.
            lbc_flanking_distance (int): Flanking distance for locus breaker clumping.
            large_loci_size (int): Threshold distance to define large loci for window-based clumping.
            wbc_clump_distance (int): Clump distance for window breaker clumping.
            wbc_pvalue_threshold (float): P-value threshold for window breaker clumping.
            collect_locus (bool, optional): Whether to collect locus. Defaults to False.
            remove_mhc (bool, optional): If true will use exclude_region() to remove the MHC region.
        """
        sum_stats = SummaryStatistics.from_parquet(
            session, summary_statistics_input_path, recursiveFileLookup=True
        )
        lbc = sum_stats.locus_breaker_clumping(
            lbc_baseline_pvalue,
            lbc_distance_cutoff,
            lbc_pvalue_threshold,
            lbc_flanking_distance,
        )
        wbc = sum_stats.window_based_clumping(wbc_clump_distance, wbc_pvalue_threshold)

        clumped_result = LocusBreakerClumping.process_locus_breaker_output(
            lbc,
            wbc,
            large_loci_size,
        )
        if remove_mhc:
            clumped_result = clumped_result.exclude_region(
                "chr6:25726063-33400556", exclude_overlap=True
            )

        if collect_locus:
            clumped_result = clumped_result.annotate_locus_statistics_boundaries(
                sum_stats
            )
        clumped_result.df.write.partitionBy("studyLocusId").mode(
            session.write_mode
        ).parquet(clumped_study_locus_output_path)
