"""Test of the qc of summary statistics."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import pytest
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.sumstat_quality_controls import SummaryStatisticsQC
from pyspark.sql.functions import rand, when


def test_qc_functions(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test all sumstat qc functions."""
    gwas = sample_summary_statistics.sanity_filter()
    QC = SummaryStatisticsQC.get_quality_control_metrics(
        gwas=gwas, limit=100000, min_count=100, n_total=100000
    )
    QC = QC.toPandas()

    assert QC["n_variants"].iloc[0] == 1663
    assert QC["n_variants_sig"].iloc[0] == 29
    assert np.round(QC["gc_lambda"].iloc[0], 4) == 1.916
    assert np.round(QC["mean_beta"].iloc[0], 4) == 0.0013
    assert np.round(QC["mean_diff_pz"].iloc[0], 6) == 0
    assert np.round(QC["se_diff_pz"].iloc[0], 6) == 0
    assert pd.isna(QC["se_N"].iloc[0])


def test_neff_check_eaf(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test N_eff check using mock EAFs."""
    gwas = sample_summary_statistics.sanity_filter()
    gwas_df = gwas._df
    gwas_df = gwas_df.withColumn("effectAlleleFrequencyFromSource", f.lit(0.5))
    gwas._df = gwas_df

    QC = SummaryStatisticsQC.get_quality_control_metrics(
        gwas=gwas, limit=100000, min_count=100, n_total=100000
    )
    QC = QC.toPandas()
    assert np.round(QC["se_N"].iloc[0], 4) == 0.5586


def test_several_studyid(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test stability when several studyIds are present."""
    gwas = sample_summary_statistics.sanity_filter()
    gwas_df = gwas._df
    gwas_df = gwas_df.withColumn(
        "studyId", when(rand() < 0.5, "new_value").otherwise(gwas_df["studyId"])
    )
    gwas._df = gwas_df

    QC = SummaryStatisticsQC.get_quality_control_metrics(
        gwas=gwas, limit=100000, min_count=100, n_total=100000
    )
    QC = QC.toPandas()
    assert QC.shape == (2, 8)


def test_sanity_filter_remove_inf_values(
    session: Session,
) -> None:
    """Sanity filter remove inf value from standardError field."""
    data = [
        (
            "GCST012234",
            "10_73856419_C_A",
            10,
            73856419,
            np.Infinity,
            1,
            3.1324,
            -650,
            None,
            0.4671,
        ),
        (
            "GCST012234",
            "14_98074714_G_C",
            14,
            98074714,
            6.697,
            2,
            5.4275,
            -2890,
            None,
            0.4671,
        ),
    ]
    input_df = session.spark.createDataFrame(
        data=data, schema=SummaryStatistics.get_schema()
    )
    summary_stats = SummaryStatistics(
        _df=input_df, _schema=SummaryStatistics.get_schema()
    )
    stats_after_filter = summary_stats.sanity_filter().df.collect()
    assert input_df.count() == 2
    assert len(stats_after_filter) == 1
    assert stats_after_filter[0]["beta"] - 6.697 == pytest.approx(0)
