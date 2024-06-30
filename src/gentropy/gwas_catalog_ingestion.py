"""Step to process GWAS Catalog associations and study table."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
)
from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalogParser,
    read_curation_table,
)
from gentropy.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter


class GWASCatalogIngestionStep:
    """GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables.

    !!! note This step currently only processes the GWAS Catalog curated list of top hits.
    """

    def __init__(
        self,
        session: Session,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        catalog_sumstats_lut: str,
        catalog_associations_file: str,
        gnomad_variant_path: str,
        catalog_studies_out: str,
        catalog_associations_out: str,
        gwas_catalog_study_curation_file: str | None = None,
        inclusion_list_path: str | None = None,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
            catalog_associations_file (str): Raw GWAS catalog associations file.
            gnomad_variant_path (str): Path to GnomAD variants.
            catalog_studies_out (str): Output GWAS catalog studies path.
            catalog_associations_out (str): Output GWAS catalog associations path.
            gwas_catalog_study_curation_file (str | None): file of the curation table. Optional.
            inclusion_list_path (str | None): optional inclusion list (parquet)
        """
        # Extract
        gnomad_variants = VariantIndex.from_parquet(session, gnomad_variant_path)
        catalog_studies = session.spark.read.csv(
            list(catalog_study_files), sep="\t", header=True
        )
        ancestry_lut = session.spark.read.csv(
            list(catalog_ancestry_files), sep="\t", header=True
        )
        sumstats_lut = session.spark.read.csv(
            catalog_sumstats_lut, sep="\t", header=False
        )
        catalog_associations = session.spark.read.csv(
            catalog_associations_file, sep="\t", header=True
        ).persist()
        gwas_catalog_study_curation = read_curation_table(
            gwas_catalog_study_curation_file, session
        )

        # Transform
        study_index, study_locus = GWASCatalogStudySplitter.split(
            StudyIndexGWASCatalogParser.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            ).annotate_from_study_curation(gwas_catalog_study_curation),
            GWASCatalogCuratedAssociationsParser.from_source(
                catalog_associations, gnomad_variants
            ),
        )

        # if inclusion list is provided apply filter:
        if inclusion_list_path:
            inclusion_list = session.spark.read.parquet(
                inclusion_list_path, sep="\t", header=True
            )

            study_index = study_index.apply_inclusion_list(inclusion_list)
            study_locus = study_locus.apply_inclusion_list(inclusion_list)

        # Load
        study_index.df.write.mode(session.write_mode).parquet(catalog_studies_out)
        study_locus.df.write.mode(session.write_mode).parquet(catalog_associations_out)
