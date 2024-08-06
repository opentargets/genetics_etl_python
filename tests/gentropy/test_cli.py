"""Test the command-line interface (CLI)."""

from click.testing import CliRunner
from gentropy.cli import cli

# from hydra.errors import ConfigCompositionException


def test_main_no_step() -> None:
    """Test the main function of the CLI without a valid step."""
    override_key = "step"
    expected = f"You must specify '{override_key}', e.g, {override_key}=<OPTION>\nAvailable options:"

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["-vv", "execute"])
    print(result.stdout)
    # print(len(result.))
    # assert expected in result.stderr
    assert 0 == 1


def test_main_step() -> None:
    """Test the main function of the CLI complains about mandatory values."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execute", "step=gene_index"])
    assert "Missing mandatory value: step.target_path" in result.output
