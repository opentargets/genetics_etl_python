"""Step to generate variant annotation dataset."""

from __future__ import annotations

import hail as hl

from gentropy.common.session import Session
from gentropy.common.types import VariantPopulation
from gentropy.common.version_engine import VersionEngine
from gentropy.config import VariantAnnotationConfig
from gentropy.datasource.gnomad.variants import GnomADVariants


class VariantAnnotationStep:
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table.
    This dataset is used to validate variants and as a source of annotation.
    """

    def __init__(
        self,
        session: Session,
        variant_annotation_path: str,
        gnomad_genomes_path: str = VariantAnnotationConfig().gnomad_genomes_path,
        gnomad_variant_populations: list[
            VariantPopulation | str
        ] = VariantAnnotationConfig().gnomad_variant_populations,
        chain_38_37: str = VariantAnnotationConfig().chain_38_37,
        use_version_from_input: bool = VariantAnnotationConfig().use_version_from_input,
    ) -> None:
        """Run Variant Annotation step.

        Args:
            session (Session): Session object.
            variant_annotation_path (str): Variant annotation dataset path.
            gnomad_genomes_path (str): Path to gnomAD genomes hail table, e.g. `gs://gcp-public-data--gnomad/release/4.0/ht/genomes/gnomad.genomes.v4.0.sites.ht/`.
            gnomad_variant_populations (list[VariantPopulation | str]): List of populations to include.
            chain_38_37 (str): Path to GRCh38 to GRCh37 chain file for lifover.
            use_version_from_input (bool): Append version derived from input gnomad_genomes_path to the output variant_annotation_path. Defaults to False.

        In case use_version_from_input is set to True,
        data source version inferred from gnomad_genomes_path is appended as the last path segment to the output path.
        All defaults are stored in the VariantAnnotationConfig.
        """
        # amend data source version to output path
        if use_version_from_input:
            variant_annotation_path = VersionEngine("gnomad").amend_version(
                gnomad_genomes_path, variant_annotation_path
            )

        # Initialise hail session.
        hl.init(sc=session.spark.sparkContext, log="/dev/null")
        # Run variant annotation.
        variant_annotation = GnomADVariants(
            gnomad_genomes_path=gnomad_genomes_path,
            gnomad_variant_populations=gnomad_variant_populations,
            chain_38_37=chain_38_37,
        ).as_variant_annotation()

        # Write data partitioned by chromosome and position.
        (
            variant_annotation.df.write.mode(session.write_mode).parquet(
                variant_annotation_path
            )
        )
