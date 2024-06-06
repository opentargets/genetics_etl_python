"""Test DAG to prototype data transfer."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup

CLUSTER_NAME = "otg-etl"
SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "dag.yaml"

# Release specific variables:
RELEASE_VERSION = "24.06"
RELEASE_BUCKET_NAME = "genetics_etl_python_playground"

# Datasource paths:
GWAS_CATALOG_BUCKET_NAME = "gwas_catalog_data"
EQTL_BUCKET_NAME = "eqtl_catalogue_data"
FINNGEN_BUCKET_NAME = "finngen_data"
FINNGEN_RELEASE = "r10"

# Files to move:
DATA_TO_MOVE = {
    # GWAS Catalog summary study index:
    "gwas_catalog_study_index": {
        "source_bucket": GWAS_CATALOG_BUCKET_NAME,
        "source_object": "study_index",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/study_index/gwas_catalog",
    },
    # PICS credible sets from GWAS Catalog curated associations:
    "gwas_catalog_curated_credible_set": {
        "source_bucket": GWAS_CATALOG_BUCKET_NAME,
        "source_object": "credible_set_datasets/gwas_catalog_PICSed_curated_associations",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/credible_set/gwas_catalog_PICSed_curated_associations",
    },
    # PICS credible sets from GWAS Catalog summary statistics:
    "gwas_catalog_sumstats_credible_set": {
        "source_bucket": GWAS_CATALOG_BUCKET_NAME,
        "source_object": "credible_set_datasets/gwas_catalog_PICSed_summary_statistics",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/credible_set/gwas_catalog_PICSed_summary_statistics",
    },
    # GWAS Catalog manifest files:
    "gwas_catalog_manifests": {
        "source_bucket": GWAS_CATALOG_BUCKET_NAME,
        "source_object": "manifests",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/manifests",
    },
    # eQTL Catalog study index:
    "eqtl_catalogue_study_index": {
        "source_bucket": EQTL_BUCKET_NAME,
        "source_object": "study_index",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/study_index/eqtl_catalogue",
    },
    # eQTL Catalog SuSiE credible sets:
    "eqtl_catalogue_susie_credible_set": {
        "source_bucket": EQTL_BUCKET_NAME,
        "source_object": "credible_set_datasets/susie",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/credible_set/eqtl_catalogue_susie",
    },
    # Finngen study index:
    "finngen_study_index": {
        "source_bucket": FINNGEN_BUCKET_NAME,
        "source_object": f"{FINNGEN_RELEASE}/study_index",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/study_index/finngen",
    },
    # Finngen SuSiE credible sets:
    "finngen_susie_credible_set": {
        "source_bucket": FINNGEN_BUCKET_NAME,
        "source_object": f"{FINNGEN_RELEASE}/credible_set_datasets/finngen_susie_processed",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/credible_set/finngen_susie",
    },
    # L2G gold standard:
    "gold_standard": {
        "source_bucket": "genetics_etl_python_playground",
        "source_object": "input/l2g/gold_standard/curation.json",
        "destination_bucket": RELEASE_BUCKET_NAME,
        "destination_object": f"releases/{RELEASE_VERSION}/locus_to_gene_gold_standard.json",
    },
}


# This operator meant to fail the DAG if the release folder exists:
ensure_release_folder_not_exists = ShortCircuitOperator(
    task_id="test_release_folder_exists",
    python_callable=lambda bucket, path: not common.check_gcp_folder_exists(
        bucket, path
    ),
    op_kwargs={
        "bucket": RELEASE_BUCKET_NAME,
        "path": f"releases/{RELEASE_VERSION}",
    },
)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # Compiling tasks for moving data to the right place:
    with TaskGroup(group_id="data_transfer") as data_transfer:
        # Defining the tasks to execute in the task group:
        [
            GCSToGCSOperator(
                task_id=f"move_{data_name}",
                source_bucket=data["source_bucket"],
                source_object=data["source_object"],
                destination_bucket=data["destination_bucket"],
                destination_object=data["destination_object"],
            )
            for data_name, data in DATA_TO_MOVE.items()
        ]

    with TaskGroup(group_id="genetics_etl") as genetics_etl:
        # Parse and define all steps and their prerequisites.
        tasks = {}
        steps = common.read_yaml_config(SOURCE_CONFIG_FILE_PATH)
        for step in steps:
            # Define task for the current step.
            step_id = step["id"]
            this_task = common.submit_step(
                cluster_name=CLUSTER_NAME,
                step_id=step_id,
                task_id=step_id,
            )
            # Chain prerequisites.
            tasks[step_id] = this_task
            for prerequisite in step.get("prerequisites", []):
                this_task.set_upstream(tasks[prerequisite])

        common.generate_dag(cluster_name=CLUSTER_NAME, tasks=list(tasks.values()))

    # DAG description:
    (
        # Test that the release folder doesn't exist:
        ensure_release_folder_not_exists
        # Run data transfer:
        >> data_transfer
        # Once datasets are transferred, run the rest of the steps:
        >> genetics_etl
    )
