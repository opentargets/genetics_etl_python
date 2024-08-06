"""Step CLI module."""

from __future__ import annotations

import inspect
import logging
import sys

import click
import hydra
from hydra.utils import instantiate
from omegaconf import OmegaConf

from gentropy.config import Config, register_config

register_config()


logger = logging.getLogger("gentropy")


class HydraConnector:
    """This class implements sys.argv overwrite (hack) to allow Hydra to work with Click."""

    def __init__(self, entry_command: str, cli_name: str) -> None:
        self.entry_command = entry_command
        self.cli_name = cli_name
        self.original_argv = sys.argv

        # construct new sys.argv skipping the commands between the cli_name and entry_command
        # include the cli_name but exclude the entry_command
        # otherwise hydra will try to treat the entry_command as an override

        pre_argv_last_idx = self._find_in_argv(self.cli_name)
        post_argv_first_idx = self._find_in_argv(self.entry_command) + 1
        self.new_argv = self.original_argv[: pre_argv_last_idx + 1]
        self.new_argv.extend(self.original_argv[post_argv_first_idx:])

    def _find_in_argv(self, arg: str) -> int:
        for idx, item in enumerate(self.original_argv):
            if arg in item:
                return idx
        raise ValueError(f"Argument {arg} not found in sys.argv")

    def __enter__(self):
        """Update the sys.argv."""
        sys.argv = self.new_argv

    def __exit__(self, *_):
        """Restore the original sys.argv."""
        sys.argv = self.original_argv


@click.command(context_settings={"ignore_unknown_options": True})
@click.argument("hydra_options", nargs=-1, type=click.UNPROCESSED)
def execute(**hydra_options) -> None:
    """Run the step using hydra CLI interface."""
    logger.info("Running the step using hydra CLI interface.")
    current_frame = inspect.currentframe()
    if current_frame is None:
        raise ValueError("Cannot find current frame.")
    current_fun_name = inspect.getframeinfo(current_frame).function
    with HydraConnector(entry_command=current_fun_name, cli_name="gentropy"):

        @hydra.main(version_base="1.3", config_path=None, config_name="config")
        def run_command(cfg: Config) -> None:
            """Gentropy CLI.

            Args:
                cfg (Config): configuration object.
            """
            logger.info(OmegaConf.to_yaml(cfg))  # noqa: T201
            # Initialise and run step
            instantiate(cfg.step)

        return run_command()


__all__ = ["execute"]
