"""Airflow DAG for the Preprocess part of the pipeline."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-finngen"
AUTOSCALING = "finngen-preprocess"

# Get all parameters for the DAG:
FINNGEN_VERSION = "r10"
FINNGEN_BUCKET = f"gs://finngen_data/{FINNGEN_VERSION}"

STUDY_INDEX = f"{FINNGEN_BUCKET}/study_index"
SUMMARY_STATISTICS = f"{FINNGEN_BUCKET}/harmonised_summary_statistics"
WINDOW_BASED_CLUMPED = f"{FINNGEN_BUCKET}/study_locus_datasets/finngen_window_clumped"
LD_CLUMPED = f"{FINNGEN_BUCKET}/study_locus_datasets/finngen_ld_clumped"
PICSED_CREDIBLE_SET = f"{FINNGEN_BUCKET}/credible_set_datasets/finngen_pics"

FINNGEN_FINEMAPPING = (
    "gs://genetics_etl_python_playground/input/Finngen_susie_finemapping_r10/full"
)
FINNGEN_FM_SUMMARIES = "gs://genetics_etl_python_playground/input/Finngen_susie_finemapping_r10/Finngen_susie_credset_summary_r10.tsv"
FINNGEN_PREFIX = "FINNGEN_R10_"
FINNGEN_FM_OUT = f"{FINNGEN_BUCKET}/credible_set_datasets/finngen_susie"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    finngen_finemapping_ingestion = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ot_finngen_finemapping_ingestion",
        task_id="finngen_finemapping_ingestion",
        other_args=[
            f"step.finngen_finemapping_out={FINNGEN_FM_OUT}",
            f"step.finngen_release_prefix={FINNGEN_PREFIX}",
            f"step.finngen_finemapping_results_path={FINNGEN_FINEMAPPING}",
            f"step.finngen_finemapping_summaries_path={FINNGEN_FM_SUMMARIES}",
        ],
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )
    with TaskGroup(
        group_id="finngen_summary_stats_preprocess"
    ) as finngen_summary_stats_preprocess:
        study_index = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="finngen_studies",
            task_id="finngen_studies",
            other_args=[
                f"step.finngen_study_index_out={STUDY_INDEX}",
            ],
        )

        # Define order of steps:
        (study_index)
    (
        common.create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            master_disk_size=2000,
            num_workers=6,
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> [finngen_summary_stats_preprocess, finngen_finemapping_ingestion]
        >> common.delete_cluster(CLUSTER_NAME)
    )
