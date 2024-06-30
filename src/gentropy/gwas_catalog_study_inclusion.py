"""Step to generate an GWAS Catalog study identifier inclusion and exclusion list."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
)
from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalog,
    StudyIndexGWASCatalogParser,
    read_curation_table,
)
from gentropy.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class GWASCatalogStudyInclusionGenerator:
    """GWAS Catalog study eligibility for ingestion based on curation and the provided criteria."""

    @staticmethod
    def flag_eligible_studies(
        study_index: StudyIndexGWASCatalog, criteria: str
    ) -> DataFrame:
        """Apply filter on GWAS Catalog studies based on the provided criteria.

        Args:
            study_index (StudyIndexGWASCatalog): complete study index to be filtered based on the provided filter set
            criteria (str): name of the filter set to be applied.

        Raises:
            ValueError: if the provided filter set is not in the accepted values.

        Returns:
            DataFrame: filtered dataframe containing only eligible studies.
        """
        filters: dict[str, Column] = {
            # Filters applied on studies for ingesting curated associations:
            "curation": (study_index.is_gwas() & study_index.has_mapped_trait()),
            # Filters applied on studies for ingesting summary statistics:
            "summary_stats": (
                study_index.is_gwas()
                & study_index.has_mapped_trait()
                & (~study_index.is_quality_flagged())
                & study_index.has_summarystats()
            ),
        }

        if criteria not in filters:
            raise ValueError(
                f'Wrong value as filter set ({criteria}). Accepted: {",".join(filters.keys())}'
            )

        # Applying the relevant filter to the study:
        return study_index.df.select(
            "studyId",
            "studyType",
            "traitFromSource",
            "traitFromSourceMappedIds",
            "qualityControls",
            "hasSumstats",
            filters[criteria].alias("isEligible"),
        )

    @staticmethod
    def process_harmonised_list(studies: list[str], session: Session) -> DataFrame:
        """Generate spark dataframe from the provided list.

        Args:
            studies (list[str]): list of path pointing to harmonised summary statistics.
            session (Session): session

        Returns:
            DataFrame: column name is consistent with original implementatin
        """
        return session.spark.createDataFrame([{"_c0": path} for path in studies])

    @staticmethod
    def get_gwas_catalog_study_index(
        session: Session,
        gnomad_variant_path: str,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        harmonised_study_file: str,
        catalog_associations_file: str,
        gwas_catalog_study_curation_file: str,
    ) -> StudyIndexGWASCatalog:
        """Return GWAS Catalog study index.

        Args:
            session (Session): Session object.
            gnomad_variant_path (str): Path to GnomAD variant list.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            harmonised_study_file (str): GWAS Catalog summary statistics lookup table.
            catalog_associations_file (str): Raw GWAS catalog associations file.
            gwas_catalog_study_curation_file (str): file of the curation table. Optional.

        Returns:
            StudyIndexGWASCatalog: Completely processed and fully annotated study index.
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
            harmonised_study_file, sep="\t", header=False
        )
        catalog_associations = session.spark.read.csv(
            catalog_associations_file, sep="\t", header=True
        ).persist()
        gwas_catalog_study_curation = read_curation_table(
            gwas_catalog_study_curation_file, session
        )

        # Transform
        study_index, _ = GWASCatalogStudySplitter.split(
            StudyIndexGWASCatalogParser.from_source(
                catalog_studies,
                ancestry_lut,
                sumstats_lut,
            ).annotate_from_study_curation(gwas_catalog_study_curation),
            GWASCatalogCuratedAssociationsParser.from_source(
                catalog_associations, gnomad_variants
            ),
        )

        return study_index

    def __init__(
        self,
        session: Session,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        catalog_associations_file: str,
        gwas_catalog_study_curation_file: str,
        gnomad_variant_path: str,
        harmonised_study_file: str,
        criteria: str,
        inclusion_list_path: str,
        exclusion_list_path: str,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session objecct.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            catalog_associations_file (str): Raw GWAS catalog associations file.
            gwas_catalog_study_curation_file (str): file of the curation table. Optional.
            gnomad_variant_path (str): Path to GnomAD variant list.
            harmonised_study_file (str): GWAS Catalog summary statistics lookup table.
            criteria (str): name of the filter set to be applied.
            inclusion_list_path (str): Output path for the inclusion list.
            exclusion_list_path (str): Output path for the exclusion list.
        """
        # Create study index:
        study_index = self.get_gwas_catalog_study_index(
            session,
            gnomad_variant_path,
            catalog_study_files,
            catalog_ancestry_files,
            harmonised_study_file,
            catalog_associations_file,
            gwas_catalog_study_curation_file,
        )

        # Get study indices for inclusion:
        flagged_studies = self.flag_eligible_studies(study_index, criteria)

        # Output inclusion list:
        eligible = (
            flagged_studies.filter(f.col("isEligible")).select("studyId").persist()
        )
        eligible.write.mode(session.write_mode).parquet(inclusion_list_path)

        # Output exclusion list:
        excluded = flagged_studies.filter(~f.col("isEligible")).persist()
        excluded.write.mode(session.write_mode).parquet(exclusion_list_path)
