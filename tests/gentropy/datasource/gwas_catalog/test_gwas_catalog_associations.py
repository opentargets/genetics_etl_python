"""Test GWAS Catalog associations import."""

from __future__ import annotations

from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
    StudyLocusGWASCatalog,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import LongType


def test_study_locus_gwas_catalog_creation(
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
) -> None:
    """Test study locus creation with mock data."""
    assert isinstance(mock_study_locus_gwas_catalog, StudyLocusGWASCatalog)


def test_qc_all(sample_gwas_catalog_associations: DataFrame) -> None:
    """Test qc all with some hard-coded values."""
    assert isinstance(
        sample_gwas_catalog_associations.withColumn(
            # Perform all quality control checks:
            "qualityControls",
            GWASCatalogCuratedAssociationsParser._qc_all(
                f.array().alias("qualityControls"),
                f.col("CHR_ID"),
                f.col("CHR_POS"),
                f.lit("A").alias("referenceAllele"),
                f.lit("T").alias("referenceAllele"),
                f.col("STRONGEST SNP-RISK ALLELE"),
                *GWASCatalogCuratedAssociationsParser._parse_pvalue(f.col("P-VALUE")),
                5e-8,
            ),
        ),
        DataFrame,
    )


def test_qc_ambiguous_study(
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
) -> None:
    """Test qc ambiguous."""
    assert isinstance(
        mock_study_locus_gwas_catalog.qc_ambiguous_study(), StudyLocusGWASCatalog
    )


def test_study_locus_gwas_catalog_from_source(
    mock_variant_index: VariantIndex,
    sample_gwas_catalog_associations: DataFrame,
) -> None:
    """Test study locus from gwas catalog mock data."""
    assert isinstance(
        GWASCatalogCuratedAssociationsParser.from_source(
            sample_gwas_catalog_associations, mock_variant_index
        ),
        StudyLocusGWASCatalog,
    )


def test_map_variants_to_variant_index(
    sample_gwas_catalog_associations: DataFrame,
    mock_variant_index: VariantIndex,
) -> None:
    """Test mapping to variant annotation variants."""
    assert isinstance(
        GWASCatalogCuratedAssociationsParser._map_variants_to_variant_index(
            sample_gwas_catalog_associations.withColumn(
                "studyLocusId", f.monotonically_increasing_id().cast(LongType())
            ),
            mock_variant_index,
        ),
        DataFrame,
    )
