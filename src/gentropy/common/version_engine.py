"""Mechanism to seek version from specific datasource."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable

from gentropy.common.types import DataSourceType


class VersionEngine:
    """Seek version from the datasource."""

    def __init__(self, datasource: DataSourceType) -> None:
        """Initialize VersionEngine.

        Args:
            datasource (DataSourceType): datasource to seek the version from
        """
        self.datasource = datasource

    @staticmethod
    def version_seekers() -> dict[DataSourceType, DatasourceVersionSeeker]:
        """List version seekers.

        Returns:
            dict[DataSourceType, DatasourceVersionSeeker]: list of available data sources.
        """
        return {
            "gnomad": GnomADVersionSeeker(),
        }

    def seek(self, text: str | Path) -> str:
        """Interface for inferring the version from text by using registered data source version iner method.

        Args:
            text (str | Path): text to seek version from

        Returns:
            str: inferred version

        Raises:
            TypeError: if version can not be found in the text

        Examples:
            >>> VersionEngine("gnomad").seek("gs://gcp-public-data--gnomad/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.vcf.bgz")
            '2.1.1'
        """
        match text:
            case Path() | str():
                text = str(text)
            case _:
                msg = f"Can not find version in {text}"
                raise TypeError(msg)
        infer_method = self._get_version_seek_method()
        return infer_method(text)

    def _get_version_seek_method(self) -> Callable[[str], str]:
        """Method that gets the version seeker for the datasource.

        Returns:
            Callable[[str], str]: Method to seek version based on the initialized datasource

        Raises:
            ValueError: if datasource is not registered in the list of version seekers
        """
        if self.datasource not in self.version_seekers():
            raise ValueError(f"Invalid datasource {self.datasource}")
        return self.version_seekers()[self.datasource].seek_version

    def amend_version(
        self, analysis_input_path: str | Path, analysis_output_path: str | Path
    ) -> str:
        """Amend version to the analysis output path if it is not already present.

        Path can be path to g3:// or Path object, absolute or relative.
        The analysis_input_path has to contain the version number.
        If the analysis_output_path contains the same version as inferred from input version already,
        then it will not be appended.

        Args:
            analysis_input_path (str | Path): step input path
            analysis_output_path (str | Path): step output path

        Returns:
            str: Path with the ammended version, does not return Path object!

        Examples:
            >>> VersionEngine("gnomad").amend_version("gs://gcp-public-data--gnomad/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.vcf.bgz", "/some/path/without/version")
            '/some/path/without/version/2.1.1'
        """
        version = self.seek(analysis_input_path)
        output_path = str(analysis_output_path)
        if version in output_path:
            return output_path
        if output_path.endswith("/"):
            return f"{analysis_output_path}{version}"
        return f"{analysis_output_path}/{version}"


class DatasourceVersionSeeker(ABC):
    """Interface for datasource version seeker."""

    @staticmethod
    @abstractmethod
    def seek_version(text: str) -> str:
        """Seek version from text. Implement this method in the subclass.

        Args:
            text (str): text to seek version from

        Returns:
            str: seeked version

        Raises:
            ValueError: if version can not be seeked

        """
        raise NotImplementedError


class GnomADVersionSeeker(DatasourceVersionSeeker):
    """Seek version from GnomAD datasource."""

    @staticmethod
    def seek_version(text: str) -> str:
        """Seek GnomAD version from provided text by using regex.

        Up to 3 digits are allowed in the version number.
        Historically gnomAD version numbers have been in the format
        2.1.1, 3.1, etc. as of 2024-05. GnomAD versions can be found by
        running `"gs://gcp-public-data--gnomad/release/*/*/*"`

        Args:
            text (str): text to seek version from

        Raises:
            ValueError: if version can not be seeked

        Returns:
            str: seeked version

        Examples:
            >>> GnomADVersionSeeker.seek_version("gs://gcp-public-data--gnomad/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.vcf.bgz")
            '2.1.1'
        """
        result = re.search(r"v?((\d+){1}\.(\d+){1}\.?(\d+)?)", text)
        match result:
            case None:
                raise ValueError(f"No GnomAD version found in provided text: {text}")
            case _:
                return result.group(1)
