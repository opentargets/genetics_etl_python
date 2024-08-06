"""Ütility functions for the CLI."""

import asyncio
import logging
import sys
import time
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse

import click

logger = logging.getLogger("gentropy")
logger.setLevel(logging.DEBUG)


def set_log_file(ctx: click.Context, param: click.Option, file: str) -> str:
    """Set logging file based on provided `log-file` flag.

    This is a callback function called by the click.Option [--log-file] flag.

    Returns:
        None
    """
    if not file:
        return ""
    ctx.ensure_object(dict)
    upload_to_gcp = False
    if "://" in file:
        upload_to_gcp = True
    if upload_to_gcp:
        parsed_uri = urlparse(file)
        ctx.obj["gcp_log_file"] = file
        if parsed_uri.scheme != "gs":
            raise click.BadParameter("Only GCS is supported for logging upload")
        file = parsed_uri.path.strip("/")
        ctx.obj["local_log_file"] = file
    ctx.obj["upload_to_gcp"] = upload_to_gcp

    local_file = Path(file)
    if local_file.exists() and local_file.is_dir():
        raise click.BadParameter("Log file is a directory")
    if local_file.exists() and local_file.is_file():
        local_file.unlink()
    if not local_file.exists():
        local_file.touch()
    logger.info("Logging to %s", local_file)
    handler = logging.FileHandler(local_file)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return str(local_file)


def teardown_cli(ctx: click.Context) -> None:
    """Teardown the gentropy cli.

    This function is used to upload the log file to GCP bucket once
    the CLI has finished running.
    """
    if "uplaod_to_gcp" in ctx.obj and ctx.obj["upload_to_gcp"]:
        from google.cloud import storage

        gcp_file = ctx.obj["gcp_log_file"]
        local_file = ctx.obj["local_log_file"]
        client = storage.Client()
        bucket_name = urlparse(gcp_file).netloc
        bucket = client.bucket(bucket_name=bucket_name)
        blob = bucket.blob(Path(local_file).name)
        logger.info("Uploading %s to %s", local_file, gcp_file)
        blob.upload_from_filename(local_file)
    logger.info(
        "Finished, elapsed time %s seconds", time.time() - ctx.obj["execution_start"]
    )


def set_log_lvl(ctx: click.Context, param: click.Option, value: int) -> int:
    """Set logging level based on the number of provided `v` flags.

    This is a callback function called by the click.Option [-v] flag.
    For example
    `-vv` - DEBUG
    `-v`  - INFO
    `no flag - ERROR

    Returns:
        int: logging level
    """
    log_lvls = {0: logging.ERROR, 1: logging.INFO, 2: logging.DEBUG}
    log_lvl = log_lvls.get(value, logging.DEBUG)
    logger = logging.getLogger("gentropy")
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    handler.setLevel(log_lvl)
    logger.addHandler(handler)
    return log_lvl


def coro(f):
    """Corutine wrapper for synchronous functions."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


__all__ = ["set_log_file", "set_log_lvl", "coro", "logger", "teardown_cli"]
