"""Airflow DAG to extract credible sets and a study index from eQTL Catalogue's finemapping results."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteBucketOperator

CLUSTER_NAME = "otg-preprocess-eqtl"
AUTOSCALING = "otg-etl"
PROJECT_ID = "open-targets-genetics-dev"

EQTL_CATALOG_SUSIE_LOCATION = "gs://eqtl_catalog_data/ebi_ftp/susie"
TEMP_DECOMPRESS_LOCATION = "gs://eqtl_catalog_data/tmp_susie_decompressed"
DECOMPRESS_FAILED_LOG = "gs://eqtl_catalog_data/tmp_susie_decompressed.log"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — eQTL preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # SuSIE fine mapping results are stored as gzipped files in a GCS bucket.
    # To improve processing performance, we decompress the files before processing to a temporary location in GCS.
    decompression_job = DataflowTemplatedJobStartOperator(
        task_id="decompress_sussie_outputs",
        template="gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files",
        location="europe-west1",
        project_id=PROJECT_ID,
        parameters={
            "inputFilePattern": f"{EQTL_CATALOG_SUSIE_LOCATION}/**/*.gz",
            "outputDirectory": TEMP_DECOMPRESS_LOCATION,
            "outputFailureFile": DECOMPRESS_FAILED_LOG,
        },
    )

    ingestion_job = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="eqtl_catalogue",
        task_id="ot_eqtl_ingestion",
    )

    delete_decompressed_job = GCSDeleteBucketOperator(
        task_id="delete_decompressed_files",
        bucket_name=TEMP_DECOMPRESS_LOCATION,
        force=True,
        user_project=PROJECT_ID,
    )

    (
        decompression_job
        >> common.create_cluster(
            CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> ingestion_job
        >> delete_decompressed_job
        >> common.delete_cluster(CLUSTER_NAME)
    )
