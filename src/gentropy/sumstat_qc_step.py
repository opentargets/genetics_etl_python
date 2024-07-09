"""Step to calculate quality control metrics on the provided GWAS study."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.sumstat_quality_controls import SummaryStatisticsQC


class SummaryStatisticsQCStep:
    """Step to run GWAS QC."""

    def __init__(
        self,
        session: Session,
        gwas_path: str,
        output_path: str,
        studyid: str,
    ) -> None:
        """Calculating quality control metrics on the provided GWAS study.

        Args:
            session (Session): Spark session
            gwas_path (str): Path to the GWAS summary statistics.
            output_path (str): Output path for the QC results.
            studyid (str): Study ID for the QC.

        """
        gwas = SummaryStatistics.from_parquet(session, path=gwas_path)

        (
            SummaryStatisticsQC.get_quality_control_metrics(
                gwas=gwas, limit=100_000_000, min_count=100, n_total=100000
            )
            .write.mode(session.write_mode)
            .parquet(output_path + "/qc_results_" + studyid)
        )
