"""Tests version engine class."""

from __future__ import annotations

from pathlib import Path

import pytest
from gentropy.common.version_engine import GnomADVersionSeeker, VersionEngine


@pytest.mark.parametrize(
    ["text", "version"],
    [
        pytest.param(
            "gcp-public-data--gnomad/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.7.vcf",
            "2.1.1",
            id="GnomAD v2.1.1",
        ),
        pytest.param(
            "/gcp-public-data--gnomad/release/3.0/vcf/genomes/gnomad.genomes.r3.0.sites.chr6.vcf",
            "3.0",
            id="GnomAD v3.0",
        ),
        pytest.param(
            "gs://gcp-public-data--gnomad/release/3.1.1/vcf/genomes/gnomad.genomes.v3.1.1.sites.chr1.vcf",
            "3.1.1",
            id="GnomAD v3.1.1",
        ),
        pytest.param(
            "gs://gcp-public-data--gnomad/release/3.1.2/vcf/genomes/gnomad.genomes.v3.1.2.sites.chrY.vcf",
            "3.1.2",
            id="GnomAD v3.1.2",
        ),
        pytest.param(
            "gsa://gcp-public-data--gnomad/release/4.0/vcf/genomes/gnomad.genomes.v4.0.sites.chrY.vcf",
            "4.0",
            id="GnomAD v4.0",
        ),
        pytest.param(
            "gs://gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr18.vcf",
            "4.1",
            id="GnomAD v4.1",
        ),
        pytest.param(
            "/some/path/to/the/version/r20.111.44",
            "20.111.44",
            id="Extreme version number",
        ),
    ],
)
def test_extracting_version_with_gnomad_seeker(text: str, version: str) -> None:
    """Test gnomad version extraction with GnomADVersionSeeker."""
    version_seeker = GnomADVersionSeeker().seek_version
    assert version_seeker(text) == version


def test_not_registered_datasource_raises_error() -> None:
    """Test that unknown datasource raises error."""
    with pytest.raises(ValueError) as e:
        VersionEngine("ClinVar").seek("some/path/to/the/version/v20.111.44")  # type: ignore
        assert e.value.args[0].startswith("Invalid datasource ClinVar")


def test_extracting_version_when_no_version_is_found() -> None:
    """Test that unknown datasource raises error."""
    with pytest.raises(ValueError) as e:
        VersionEngine("ClinVar").seek("some/path/without/version")  # type: ignore
        assert e.value.args[0].startswith(
            "Can not find version in some/path/without/version"
        )


def test_non_string_path_raises_error() -> None:
    """Test that non-string path raises error."""
    with pytest.raises(TypeError) as e:
        VersionEngine("gnomad").seek(123)  # type: ignore
        assert e.value.args[0].startswith("Can not infer version from 123")


@pytest.mark.parametrize(
    ["text", "version"],
    [
        pytest.param(Path("some/file/path/v3.1.1"), "3.1.1", id="Path object"),
        pytest.param("s3://some/file/path/v3.1.1", "3.1.1", id="S3 protocol"),
        pytest.param("gs://some/file/path/v3.1.1", "3.1.1", id="GS protocol"),
    ],
)
def test_extracting_version_with_version_engine(text: str | Path, version: str) -> None:
    """Check if concrete data types and file protocols does not return an error while passed to VersionEngine."""
    assert VersionEngine("gnomad").seek(text) == version


@pytest.mark.parametrize(
    ["input_path", "output_path", "expected_output"],
    [
        pytest.param(
            "input/v20.111.44", "output", "output/20.111.44", id="Append version"
        ),
        pytest.param(
            "input/1.0.0",
            "output/1.0.0",
            "output/1.0.0",
            id="Do not append version, already present",
        ),
        pytest.param(
            Path("input/1.0.0"), Path("output/"), "output/1.0.0", id="Path objects"
        ),
    ],
)
def test_appending_version_to_path(
    input_path: Path | str, output_path: Path | str, expected_output: str
) -> None:
    """Test that the version is ammended at the end of the output path."""
    VersionEngine("gnomad").amend_version(input_path, output_path) == expected_output
