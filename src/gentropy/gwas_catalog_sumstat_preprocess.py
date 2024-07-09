"""Step to generate variant annotation dataset."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.gwas_catalog.summary_statistics import (
    GWASCatalogSummaryStatistics,
)


class GWASCatalogSumstatsPreprocessStep:
    """Step to preprocess GWAS Catalog harmonised summary stats.

    It additionally performs sanity filter of GWAS before saving it.
    """

    def __init__(
        self, session: Session, raw_sumstats_path: str, out_sumstats_path: str
    ) -> None:
        """Run step to preprocess GWAS Catalog harmonised summary stats and produce SummaryStatistics dataset.

        Args:
            session (Session): Session object.
            raw_sumstats_path (str): Input GWAS Catalog harmonised summary stats path.
            out_sumstats_path (str): Output SummaryStatistics dataset path.
        """
        # Processing dataset:
        GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            session.spark, raw_sumstats_path
        ).sanity_filter().df.write.mode(session.write_mode).parquet(out_sumstats_path)
        session.logger.info("Processing dataset successfully completed.")
