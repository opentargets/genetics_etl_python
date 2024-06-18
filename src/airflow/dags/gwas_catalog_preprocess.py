"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.task_group import TaskGroup

CLUSTER_NAME = "otg-preprocess-gwascatalog"
AUTOSCALING = "otg-preprocess-gwascatalog"

# Setting up bucket name and output object names:
GWAS_CATALOG_BUCKET_NAME = "gwas_catalog_data"
HARMONISED_SUMSTATS_PREFIX = "harmonised_summary_statistics"

# Manifest paths:
MANIFESTS_PATH = f"gs://{GWAS_CATALOG_BUCKET_NAME}/manifests/"

# The name of the manifest files have to be consistent with the config file:
HARMONISED_SUMSTATS_LIST_OBJECT_NAME = (
    "manifests/gwas_catalog_harmonised_summary_statistics_list.txt"
)
HARMONISED_SUMSTATS_LIST_FULL_NAME = (
    f"gs://{GWAS_CATALOG_BUCKET_NAME}/{HARMONISED_SUMSTATS_LIST_OBJECT_NAME}"
)
CURATION_INCLUSION_NAME = f"{MANIFESTS_PATH}/gwas_catalog_curation_included_studies"
CURATION_EXCLUSION_NAME = f"{MANIFESTS_PATH}/gwas_catalog_curation_excluded_studies"
SUMMARY_STATISTICS_INCLUSION_NAME = (
    f"{MANIFESTS_PATH}/gwas_catalog_summary_statistics_included_studies"
)
SUMMARY_STATISTICS_EXCLUSION_NAME = (
    f"{MANIFESTS_PATH}/gwas_catalog_summary_statistics_excluded_studies"
)

# Study index:
STUDY_INDEX = f"gs://{GWAS_CATALOG_BUCKET_NAME}/study_index"

# Study loci:
CURATED_STUDY_LOCI = f"gs://{GWAS_CATALOG_BUCKET_NAME}/study_locus_datasets/gwas_catalog_curated_associations"
CURATED_LD_CLUMPED = f"gs://{GWAS_CATALOG_BUCKET_NAME}/study_locus_datasets/gwas_catalog_curated_associations_ld_clumped"
WINDOW_BASED_CLUMPED = f"gs://{GWAS_CATALOG_BUCKET_NAME}/study_locus_datasets/gwas_catalog_summary_stats_window_clumped"
LD_BASED_CLUMPED = f"gs://{GWAS_CATALOG_BUCKET_NAME}/study_locus_datasets/gwas_catalog_summary_stats_ld_clumped"
# Credible sets:
CURATED_CREDIBLE_SETS = f"gs://{GWAS_CATALOG_BUCKET_NAME}/credible_set_datasets/gwas_catalog_PICSed_curated_associations"
SUMMARY_STATISTICS_CREDIBLE_SETS = f"gs://{GWAS_CATALOG_BUCKET_NAME}/credible_set_datasets/gwas_catalog_PICSed_summary_statistics"


def upload_harmonized_study_list(
    concatenated_studies: str, bucket_name: str, object_name: str
) -> None:
    """This function uploads file to GCP.

    Args:
        concatenated_studies (str): Concatenated list of harmonized summary statistics.
        bucket_name (str): Bucket name
        object_name (str): Name of the object
    """
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=concatenated_studies,
        encoding="utf-8",
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # Getting list of folders (each a gwas study with summary statistics)
    list_harmonised_sumstats = GCSListObjectsOperator(
        task_id="list_harmonised_parquet",
        bucket=GWAS_CATALOG_BUCKET_NAME,
        prefix=HARMONISED_SUMSTATS_PREFIX,
        match_glob="**/_SUCCESS",
    )

    # Upload resuling list to a bucket:
    upload_task = PythonOperator(
        task_id="uploader",
        python_callable=upload_harmonized_study_list,
        op_kwargs={
            "concatenated_studies": '{{ "\n".join(ti.xcom_pull( key="return_value", task_ids="list_harmonised_parquet")) }}',
            "bucket_name": GWAS_CATALOG_BUCKET_NAME,
            "object_name": HARMONISED_SUMSTATS_LIST_OBJECT_NAME,
        },
    )

    # Processing curated GWAS Catalog top-bottom:
    with TaskGroup(group_id="curation_processing") as curation_processing:
        # Generate inclusion list:
        curation_calculate_inclusion_list = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_gwas_catalog_study_inclusion",
            task_id="catalog_curation_inclusion_list",
            other_args=[
                "step.criteria=curation",
                f"step.inclusion_list_path={CURATION_INCLUSION_NAME}",
                f"step.exclusion_list_path={CURATION_EXCLUSION_NAME}",
                f"step.harmonised_study_file={HARMONISED_SUMSTATS_LIST_FULL_NAME}",
            ],
        )

        # Ingest curated associations from GWAS Catalog:
        curation_ingest_data = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_gwas_catalog_ingestion",
            task_id="ingest_curated_gwas_catalog_data",
            other_args=[f"step.inclusion_list_path={CURATION_INCLUSION_NAME}"],
        )

        # Run LD-annotation and clumping on curated data:
        curation_ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_ld_based_clumping",
            task_id="catalog_curation_ld_clumping",
            other_args=[
                f"step.study_locus_input_path={CURATED_STUDY_LOCI}",
                f"step.study_index_path={STUDY_INDEX}",
                f"step.clumped_study_locus_output_path={CURATED_LD_CLUMPED}",
            ],
        )

        # Do PICS based finemapping:
        curation_pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="pics",
            task_id="catalog_curation_pics",
            other_args=[
                f"step.study_locus_ld_annotated_in={CURATED_LD_CLUMPED}",
                f"step.picsed_study_locus_out={CURATED_CREDIBLE_SETS}",
            ],
        )

        # Define order of steps:
        (
            curation_calculate_inclusion_list
            >> curation_ingest_data
            >> curation_ld_clumping
            >> curation_pics
        )

    # Processing summary statistics from GWAS Catalog:
    with TaskGroup(
        group_id="summary_statistics_processing"
    ) as summary_statistics_processing:
        # Generate inclusion study lists:
        summary_stats_calculate_inclusion_list = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_gwas_catalog_study_inclusion",
            task_id="catalog_sumstats_inclusion_list",
            other_args=[
                "step.criteria=summary_stats",
                f"step.inclusion_list_path={SUMMARY_STATISTICS_INCLUSION_NAME}",
                f"step.exclusion_list_path={SUMMARY_STATISTICS_EXCLUSION_NAME}",
                f"step.harmonised_study_file={HARMONISED_SUMSTATS_LIST_FULL_NAME}",
            ],
        )

        # Run window-based clumping:
        summary_stats_window_based_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="window_based_clumping",
            task_id="catalog_sumstats_window_clumping",
            other_args=[
                f"step.summary_statistics_input_path=gs://{GWAS_CATALOG_BUCKET_NAME}/{HARMONISED_SUMSTATS_PREFIX}",
                f"step.inclusion_list_path={SUMMARY_STATISTICS_INCLUSION_NAME}",
                f"step.study_locus_output_path={WINDOW_BASED_CLUMPED}",
            ],
        )

        # Run LD based clumping:
        summary_stats_ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_ld_based_clumping",
            task_id="catalog_sumstats_ld_clumping",
            other_args=[
                f"step.study_locus_input_path={WINDOW_BASED_CLUMPED}",
                f"step.study_index_path={STUDY_INDEX}",
                f"step.clumped_study_locus_output_path={LD_BASED_CLUMPED}",
            ],
        )

        # Run PICS finemapping:
        summary_stats_pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="pics",
            task_id="catalog_sumstats_pics",
            other_args=[
                f"step.study_locus_ld_annotated_in={LD_BASED_CLUMPED}",
                f"step.picsed_study_locus_out={SUMMARY_STATISTICS_CREDIBLE_SETS}",
            ],
        )

        # Order of steps within the group:
        (
            summary_stats_calculate_inclusion_list
            >> summary_stats_window_based_clumping
            >> summary_stats_ld_clumping
            >> summary_stats_pics
        )

    # DAG description:
    (
        common.create_cluster(
            CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> list_harmonised_sumstats
        >> upload_task
        >> curation_processing
        >> summary_statistics_processing
        >> common.delete_cluster(CLUSTER_NAME)
    )
