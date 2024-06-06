"""Airflow DAG to extract credible sets and a study index from eQTL Catalogue's finemapping results."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

CLUSTER_NAME = "otg-preprocess-eqtl"
AUTOSCALING = "eqtl-preprocess"
PROJECT_ID = "open-targets-genetics-dev"

EQTL_CATALOGUE_SUSIE_LOCATION = "gs://eqtl_catalogue_data/ebi_ftp/susie"
TEMP_DECOMPRESS_LOCATION = f"{EQTL_CATALOGUE_SUSIE_LOCATION}_decompressed_tmp"
DECOMPRESS_FAILED_LOG = f"{TEMP_DECOMPRESS_LOCATION}/logs.log"
STUDY_INDEX_PATH = "gs://eqtl_catalogue_data/study_index"
CREDIBLE_SET_PATH = "gs://eqtl_catalogue_data/credible_set_datasets/susie"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — eQTL preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # SuSIE fine mapping results are stored as gzipped files in a GCS bucket.
    # To improve processing performance, we decompress the files before processing to a temporary location in GCS.
    decompression_job = DataflowTemplatedJobStartOperator(
        task_id="decompress_susie_outputs",
        template="gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files",
        location="europe-west1",
        project_id=PROJECT_ID,
        parameters={
            "inputFilePattern": f"{EQTL_CATALOGUE_SUSIE_LOCATION}/**/*.gz",
            "outputDirectory": TEMP_DECOMPRESS_LOCATION,
            "outputFailureFile": DECOMPRESS_FAILED_LOG,
        },
    )

    ingestion_job = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ot_eqtl_catalogue",
        task_id="ot_eqtl_ingestion",
        other_args=[
            f"step.eqtl_catalogue_paths_imported={TEMP_DECOMPRESS_LOCATION}",
            f"step.eqtl_catalogue_study_index_out={STUDY_INDEX_PATH}",
            f"step.eqtl_catalogue_credible_sets_out={CREDIBLE_SET_PATH}",
        ],
    )

    delete_decompressed_job = GCSDeleteObjectsOperator(
        task_id="delete_decompressed_files",
        bucket_name=TEMP_DECOMPRESS_LOCATION.split("/")[2],
        prefix=f"{TEMP_DECOMPRESS_LOCATION.split('/')[-1]}/",
    )

    (
        decompression_job
        >> common.create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            num_workers=4,
            worker_machine_type="n1-highmem-8",
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> ingestion_job
        >> [delete_decompressed_job, common.delete_cluster(CLUSTER_NAME)]
    )
