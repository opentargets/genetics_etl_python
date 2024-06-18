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

CLUSTER_NAME = "otg-finngen-harmonisation"
AUTOSCALING = "gwascatalog-harmonisation"  # same as GWAS Catalog harmonisation
SUMMARY_STATS_BUCKET_NAME = "finngen-public-data-r10"
RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"
SUMSTATS_PARQUET = f"{RELEASEBUCKET}/summary_statistics/finngen"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen harmonisation",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # List raw harmonised files from GWAS Catalog
    list_inputs = GCSListObjectsOperator(
        task_id="list_raw_sumstats",
        bucket=SUMMARY_STATS_BUCKET_NAME,
        prefix="summary_stats",
        match_glob="**/*.gz",
    )

    # Submit jobs to dataproc
    @task(task_id="submit_jobs")
    def submit_jobs(**kwargs: Any) -> None:
        """Submit jobs to dataproc.

        Args:
            **kwargs (Any): Keyword arguments.
        """
        ti = kwargs["ti"]
        todo = ti.xcom_pull(task_ids="list_raw_sumstats", key="return_value")
        print("Number of jobs to submit: ", len(todo))  # noqa: T201
        for i in range(len(todo)):
            # Not to exceed default quota 400 jobs per minute
            if i > 0 and i % 399 == 0:
                time.sleep(60)
            input_path = todo[i]
            match_result = re.search(r"summary_stats/finngen_(.*).gz", input_path)
            if match_result:
                study_id = match_result.group(1)
            print("Submitting job for study: ", study_id)  # noqa: T201
            common.submit_pyspark_job_no_operator(
                cluster_name=CLUSTER_NAME,
                step_id="finngen_sumstat_preprocess",
                other_args=[
                    f"step.raw_sumstats_path=gs://{SUMMARY_STATS_BUCKET_NAME}/{input_path}",
                    f"step.out_sumstats_path={SUMSTATS_PARQUET}/{study_id}.parquet",
                ],
            )

    # list_inputs >>
    (
        list_inputs
        >> common.create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            num_workers=8,
            # num_preemptible_workers=8,
            master_machine_type="n1-highmem-32",
            worker_machine_type="n1-standard-2",
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> submit_jobs()
        >> common.delete_cluster(CLUSTER_NAME)
    )
