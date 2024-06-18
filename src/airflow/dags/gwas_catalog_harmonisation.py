"""Airflow DAG for the harmonisation part of the pipeline."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

import common_airflow as common
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

CLUSTER_NAME = "otg-gwascatalog-harmonisation"
AUTOSCALING = "gwascatalog-harmonisation"

SUMMARY_STATS_BUCKET_NAME = "gwas_catalog_data"
RAW_SUMMARY_STATISTICS_PREFIX = "raw_summary_statistics"
HARMONISED_SUMMARY_STATISTICS_PREFIX = "harmonised_summary_statistics"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog harmonisation",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # List raw harmonised files from GWAS Catalog
    list_inputs = GCSListObjectsOperator(
        task_id="list_raw_harmonised",
        bucket=SUMMARY_STATS_BUCKET_NAME,
        prefix=RAW_SUMMARY_STATISTICS_PREFIX,
        match_glob="**/*.h.tsv.gz",
    )
    # List parquet files that have been previously processed
    list_outputs = GCSListObjectsOperator(
        task_id="list_harmonised_parquet",
        bucket=SUMMARY_STATS_BUCKET_NAME,
        prefix=HARMONISED_SUMMARY_STATISTICS_PREFIX,
        match_glob="**/_SUCCESS",
    )

    # Create list of pending jobs
    @task(task_id="create_to_do_list")
    def create_to_do_list(**kwargs: Any) -> Any:
        """Create the to-do list of studies.

        Args:
            **kwargs (Any): Keyword arguments.

        Returns:
            Any: To-do list.
        """
        ti = kwargs["ti"]
        raw_harmonised = ti.xcom_pull(
            task_ids="list_raw_harmonised", key="return_value"
        )
        print("Number of raw harmonised files: ", len(raw_harmonised))  # noqa: T201
        to_do_list = []
        # Remove the ones that have been processed
        parquets = ti.xcom_pull(task_ids="list_harmonised_parquet", key="return_value")
        print("Number of parquet files: ", len(parquets))  # noqa: T201
        for path in raw_harmonised:
            match_result = re.search(
                rf"{RAW_SUMMARY_STATISTICS_PREFIX}/(.*)/(GCST\d+)/harmonised/(.*)\.h\.tsv\.gz",
                path,
            )
            if match_result:
                study_id = match_result.group(2)
                if (
                    f"{HARMONISED_SUMMARY_STATISTICS_PREFIX}/{study_id}.parquet/_SUCCESS"
                    not in parquets
                ):
                    to_do_list.append(path)
        print("Number of jobs to submit: ", len(to_do_list))  # noqa: T201
        ti.xcom_push(key="to_do_list", value=to_do_list)

    # Submit jobs to dataproc
    @task(task_id="submit_jobs")
    def submit_jobs(**kwargs: Any) -> None:
        """Submit jobs to dataproc.

        Args:
            **kwargs (Any): Keyword arguments.
        """
        ti = kwargs["ti"]
        todo = ti.xcom_pull(task_ids="create_to_do_list", key="to_do_list")
        print("Number of jobs to submit: ", len(todo))  # noqa: T201
        for i in range(len(todo)):
            # Not to exceed default quota 400 jobs per minute
            if i > 0 and i % 399 == 0:
                time.sleep(60)
            input_path = todo[i]
            match_result = re.search(
                rf"{RAW_SUMMARY_STATISTICS_PREFIX}/(.*)/(GCST\d+)/harmonised/(.*)\.h\.tsv\.gz",
                input_path,
            )
            if match_result:
                study_id = match_result.group(2)
            print("Submitting job for study: ", study_id)  # noqa: T201
            common.submit_pyspark_job_no_operator(
                cluster_name=CLUSTER_NAME,
                step_id="gwas_catalog_sumstat_preprocess",
                other_args=[
                    f"step.raw_sumstats_path=gs://{SUMMARY_STATS_BUCKET_NAME}/{input_path}",
                    f"step.out_sumstats_path=gs://{SUMMARY_STATS_BUCKET_NAME}/{HARMONISED_SUMMARY_STATISTICS_PREFIX}/{study_id}.parquet",
                ],
            )

    (
        [list_inputs, list_outputs]
        >> create_to_do_list()
        >> common.create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            num_workers=8,
            num_preemptible_workers=8,
            master_machine_type="n1-highmem-64",
            worker_machine_type="n1-standard-2",
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> submit_jobs()
        # >> common.delete_cluster(CLUSTER_NAME)
    )
