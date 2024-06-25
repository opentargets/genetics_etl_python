"""Airflow DAG to ingest and harmonise UKB PPP (EUR) data."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG

CLUSTER_NAME = "otg-ukb-ppp-eur"

# Input location.
UKB_PPP_EUR_STUDY_INDEX = "gs://gentropy-tmp/batch/output/ukb_ppp_eur/study_index.tsv"
UKB_PPP_EUR_SUMMARY_STATS = "gs://gentropy-tmp/batch/output/ukb_ppp_eur/summary_stats.parquet"
VARIANT_ANNOTATION = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/variant_annotation"

# Output locations.
TMP_VARIANT_ANNOTATION = "gs://gentropy-tmp/variant_annotation"
UKB_PPP_EUR_OUTPUT_STUDY_INDEX = "gs://ukb_ppp_eur_data/study_index"
UKB_PPP_EUR_OUTPUT_SUMMARY_STATS = "gs://ukb_ppp_eur_data/summary_stats"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Ingest UKB PPP (EUR)",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    dag = common.generate_dag(
        cluster_name=CLUSTER_NAME,
        tasks=[
            common.submit_step(
                cluster_name=CLUSTER_NAME,
                step_id="ot_ukb_ppp_eur_sumstat_preprocess",
                other_args=[
                    f"step.raw_study_index_path={UKB_PPP_EUR_STUDY_INDEX}",
                    f"step.raw_summary_stats_path={UKB_PPP_EUR_SUMMARY_STATS}",
                    f"step.variant_annotation_path={VARIANT_ANNOTATION}",
                    f"step.tmp_variant_annotation_path={TMP_VARIANT_ANNOTATION}",
                    f"step.study_index_output_path={UKB_PPP_EUR_OUTPUT_STUDY_INDEX}",
                    f"step.summary_stats_output_path={UKB_PPP_EUR_OUTPUT_SUMMARY_STATS}",
                ]
            )
        ]
    )
