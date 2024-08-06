"""Cli for gentropy."""

from __future__ import annotations

import time

import click

from gentropy.cli.step_cli import execute
from gentropy.cli.update_gwas_catalog_data import update_gwas_catalog_data
from gentropy.cli.utils import set_log_file, set_log_lvl, teardown_cli

ascii_art = r"""
 ___  ____  _  _  ____  ____  _____  ____  _  _
/ __)( ___)( \( )(_  _)(  _ \(  _  )(  _ \( \/ )
((_-. )__)  )  (   )(   )   / )(_)(  )___/ \  /
\___/(____)(_)\_) (__) (_)\_)(_____)(__)   (__)
"""


@click.group()
@click.option("-d", "--dry-run", is_flag=True, default=False)
@click.option("-v", "--verbose", count=True, default=0, callback=set_log_lvl)
@click.option("-q", "--log-file", callback=set_log_file, required=False)
@click.pass_context
def cli(ctx: click.Context, **kwargs) -> None:
    r"""Gentropy Coomand line interface."""
    click.echo(click.style(ascii_art, fg="blue"))  # noqa: T201
    # https://click.palletsprojects.com/en/8.1.x/commands/
    ctx.max_content_width = 200
    ctx.ensure_object(dict)
    ctx.obj["dry_run"] = kwargs["dry_run"]
    ctx.obj["execution_start"] = time.time()
    ctx.call_on_close(lambda: teardown_cli(ctx))


cli.add_command(update_gwas_catalog_data)
cli.add_command(execute)
