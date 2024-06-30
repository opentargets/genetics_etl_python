"""Airflow DAG for the Preprocess GnomAD datasets - LD index and GnomAD variant set."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG

CLUSTER_NAME = "gnomad-preprocess"

ALL_STEPS = [
    "ot_ld_index",
    "ot_gnomad_variants",
]


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GnomAD Preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    all_tasks = [
        common.submit_step(cluster_name=CLUSTER_NAME, step_id=step, task_id=step)
        for step in ALL_STEPS
    ]
    dag = common.generate_dag(cluster_name=CLUSTER_NAME, tasks=all_tasks)
