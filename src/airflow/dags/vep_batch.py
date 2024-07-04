"""Airflow DAG for the harmonisation part of the pipeline."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

import common_airflow as common
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from google.cloud import batch_v1

PROJECT_ID = "open-targets-genetics-dev"
REGION = "europe-west1"

# Required parameters:
VEP_DOCKER_IMAGE = "europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/custom_ensembl_vep:dev"

VCF_INPUT_BUCKET = "gs://genetics_etl_python_playground/vep/test_vep_input"
VEP_OUTPUT_BUCKET = "gs://genetics_etl_python_playground/vep/test_vep_output"
VEP_CACHE_BUCKET = "gs://genetics_etl_python_playground/vep/cache"

# Internal parameters for the docker image:
MOUNT_DIR = "/mnt/disks/share"

# Configuration for the machine types:
MACHINES = {
    "VEPMACHINE": {
        "machine_type": "e2-standard-4",
        "cpu_milli": 2000,
        "memory_mib": 2000,
        "boot_disk_mib": 10000,
    },
}


@dataclass
class PathManager:
    """It is quite complicated to keep track of all the input/output buckets, the corresponding mounting points prefixes etc..."""

    VCF_INPUT_BUCKET: str
    VEP_OUTPUT_BUCKET: str
    VEP_CACHE_BUCKET: str
    MOUNT_DIR_ROOT: str

    # Derived parameters to find the list of files to process:
    input_path: str | None = None
    input_bucket: str | None = None

    # Derived parameters to initialise the docker image:
    path_dictionary: dict[str, dict[str, str]] | None = None

    # Derived parameters to point to the right mouting points:
    cache_dir: str | None = None
    input_dir: str | None = None
    output_dir: str | None = None

    def __post_init__(self: PathManager) -> None:
        """Build paths based on the input parameters."""
        self.path_dictionary = {
            "input": {
                "remote_path": self.VCF_INPUT_BUCKET.replace("gs://", ""),
                "mount_point": f"{self.MOUNT_DIR_ROOT}/input",
            },
            "output": {
                "remote_path": self.VEP_OUTPUT_BUCKET.replace("gs://", ""),
                "mount_point": f"{self.MOUNT_DIR_ROOT}/output",
            },
            "cache": {
                "remote_path": self.VEP_CACHE_BUCKET.replace("gs://", ""),
                "mount_point": f"{self.MOUNT_DIR_ROOT}/cache",
            },
        }
        # Parameters for fetching files:
        self.input_path = self.VCF_INPUT_BUCKET.replace("gs://", "") + "/"
        self.input_bucket = self.VCF_INPUT_BUCKET.split("/")[2]

        # Parameters for VEP:
        self.cache_dir = f"{self.MOUNT_DIR_ROOT}/cache"
        self.input_dir = f"{self.MOUNT_DIR_ROOT}/input"
        self.output_dir = f"{self.MOUNT_DIR_ROOT}/output"

    def get_mount_config(self) -> list[dict[str, str]]:
        """Return the mount configuration.

        Returns:
            list[dict[str, str]]: The mount configuration.
        """
        assert self.path_dictionary is not None, "Path dictionary not initialized."
        return list(self.path_dictionary.values())


def create_container_runnable(image: str, commands: List[str]) -> batch_v1.Runnable:
    """Create a container runnable for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (List[str]): The commands to run in the container.

    Returns:
        batch_v1.Runnable: The container runnable.
    """
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = image
    runnable.container.entrypoint = "/bin/sh"
    runnable.container.commands = commands
    return runnable


def create_task_spec(image: str, commands: List[str]) -> batch_v1.TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (List[str]): The commands to run in the container.

    Returns:
        batch_v1.TaskSpec: The task specification.
    """
    task = batch_v1.TaskSpec()
    task.runnables = [
        create_container_runnable(image, commands)
        # msg_runnable()
    ]
    return task


def set_up_mouting_points(
    mounting_points: list[dict[str, str]],
) -> list[batch_v1.Volume]:
    """Set up the mounting points for the container.

    Args:
        mounting_points (list[dict[str, str]]): The mounting points.

    Returns:
        list[batch_v1.Volume]: The volumes.
    """
    volumes = []
    for mount in mounting_points:
        gcs_bucket = batch_v1.GCS()
        gcs_bucket.remote_path = mount["remote_path"]
        gcs_volume = batch_v1.Volume()
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = mount["mount_point"]
        volumes.append(gcs_volume)
    return volumes


def create_batch_job(
    task: batch_v1.TaskSpec,
    machine: str,
    task_env: list[batch_v1.Environment],
    mounting_points: list[dict[str, str]],
) -> batch_v1.Job:
    """Create a Google Batch job.

    Args:
        task (batch_v1.TaskSpec): The task specification.
        machine (str): The machine type to use.
        task_env (list[batch_v1.Environment]): The environment variables for the task.
        mounting_points (list[dict[str, str]]): List of mounting points.

    Returns:
        batch_v1.Job: The Batch job.
    """
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = MACHINES[machine]["cpu_milli"]
    resources.memory_mib = MACHINES[machine]["memory_mib"]
    resources.boot_disk_mib = MACHINES[machine]["boot_disk_mib"]
    task.compute_resource = resources

    task.max_retry_count = 3
    task.max_run_duration = "43200s"

    # The mounting points are set up and assigned to the task:
    task.volumes = set_up_mouting_points(mounting_points)

    group = batch_v1.TaskGroup()
    group.task_spec = task
    group.task_environments = task_env

    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = MACHINES[machine]["machine_type"]
    policy.provisioning_model = "SPOT"

    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    return job


@task(task_id="vep_annotation")
def vep_annotation(pm: PathManager, **kwargs: Any) -> None:
    """Submit a Batch job to download cache for VEP.

    Args:
        pm (PathManager): The path manager with all the required path related information.
        **kwargs (Any): Keyword arguments.
    """
    # Get the filenames to process:
    ti = kwargs["ti"]
    filenames = [
        os.path.basename(os.path.splitext(path)[0])
        for path in ti.xcom_pull(task_ids="get_vep_todo_list", key="return_value")
    ]
    # Stop process if no files was found:
    assert filenames, "No files found to process."

    # Based on the filenames, build the environment variables for the batch job:
    task_env = [
        batch_v1.Environment(
            variables={
                "INPUT_FILE": filename + ".tsv",
                "OUTPUT_FILE": filename + ".json",
            }
        )
        for filename in filenames
    ]
    # Build the command to run in the container:
    command = [
        "-c",
        rf"vep --cache --offline --format vcf --force_overwrite \
            --no_stats \
            --dir_cache {pm.cache_dir} \
            --input_file {pm.input_dir}/$INPUT_FILE \
            --output_file {pm.output_dir}/$OUTPUT_FILE --json \
            --dir_plugins {pm.cache_dir}/VEP_plugins \
            --sift b \
            --polyphen b \
            --uniprot \
            --check_existing \
            --exclude_null_alleles \
            --canonical \
            --plugin LoF,loftee_path:{pm.cache_dir}/VEP_plugins,gerp_bigwig:{pm.cache_dir}/gerp_conservation_scores.homo_sapiens.GRCh38.bw,human_ancestor_fa:{pm.cache_dir}/human_ancestor.fa.gz,conservation_file:/opt/vep/loftee.sql \
            --plugin AlphaMissense,file={pm.cache_dir}/AlphaMissense_hg38.tsv.gz,transcript_match=1 \
            --plugin CADD,snv={pm.cache_dir}/CADD_GRCh38_whole_genome_SNVs.tsv.gz",
    ]
    task = create_task_spec(VEP_DOCKER_IMAGE, command)
    batch_task = CloudBatchSubmitJobOperator(
        task_id="vep_batch_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"vep-job-{time.strftime('%Y%m%d-%H%M%S')}",
        job=create_batch_job(task, "VEPMACHINE", task_env, pm.get_mount_config()),
        deferrable=False,
    )
    batch_task.execute(context=kwargs)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Ensembl VEP",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # Initialise parameter manager:
    pm = PathManager(VCF_INPUT_BUCKET, VEP_OUTPUT_BUCKET, VEP_CACHE_BUCKET, MOUNT_DIR)

    # Get a list of files to process from the input bucket:
    get_vep_todo_list = GCSListObjectsOperator(
        task_id="get_vep_todo_list",
        bucket=pm.input_bucket,
        prefix=pm.input_path,
        match_glob="**tsv",
    )

    get_vep_todo_list >> vep_annotation(pm)
