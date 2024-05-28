"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""

from __future__ import annotations

import hail as hl

from gentropy.common.session import Session
from gentropy.common.types import LD_Population
from gentropy.common.version_engine import VersionEngine
from gentropy.config import LDIndexConfig
from gentropy.datasource.gnomad.ld import GnomADLDMatrix


class LDIndexStep:
    """LD index step.

    !!! warning "This step is resource intensive"

        Suggested params: high memory machine, 5TB of boot disk, no SSDs.

    """

    def __init__(
        self,
        session: Session,
        ld_index_out: str,
        min_r2: float = LDIndexConfig().min_r2,
        ld_matrix_template: str = LDIndexConfig().ld_matrix_template,
        ld_index_raw_template: str = LDIndexConfig().ld_index_raw_template,
        ld_populations: list[LD_Population | str] = LDIndexConfig().ld_populations,
        liftover_ht_path: str = LDIndexConfig().liftover_ht_path,
        use_version_from_input: bool = LDIndexConfig().use_version_from_input,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            ld_index_out (str): Output LD index path. (required)
            min_r2 (float): Minimum r2 to consider when considering variants within a window.
            ld_matrix_template (str): Input path to the gnomAD ld file with placeholder for population
            ld_index_raw_template (str): Input path to the raw gnomAD LD indices file with placeholder for population string
            ld_populations (list[LD_Population | str]): Population names derived from the ld file paths
            liftover_ht_path (str): Path to the liftover ht file
            use_version_from_input (bool): Append version derived from input ld_matrix_template to the output ld_index_out. Defaults to False.

        In case use_version_from_input is set to True,
        data source version inferred from ld_matrix_temolate is appended as the last path segment to the output path.
        Default values are provided in LDIndexConfig.
        """
        if use_version_from_input:
            # amend data source version to output path
            ld_index_out = VersionEngine("gnomad").amend_version(
                ld_matrix_template, ld_index_out
            )
        hl.init(sc=session.spark.sparkContext, log="/dev/null")
        (
            GnomADLDMatrix(
                ld_matrix_template=ld_matrix_template,
                ld_index_raw_template=ld_index_raw_template,
                ld_populations=ld_populations,
                liftover_ht_path=liftover_ht_path,
            )
            .as_ld_index(min_r2)
            .df.write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(ld_index_out)
        )
        session.logger.info(ld_index_out)
