"""Unit test configuration."""

from __future__ import annotations

from pathlib import Path

import dbldatagen as dg
import hail as hl
import numpy as np
import pandas as pd
import pytest
from gentropy.common.Liftover import LiftOverSpark
from gentropy.common.session import Session
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.v2g import V2G
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from gentropy.datasource.gwas_catalog.associations import StudyLocusGWASCatalog
from gentropy.datasource.gwas_catalog.study_index import StudyIndexGWASCatalog
from pyspark.sql import DataFrame, SparkSession

from utils.spark import get_spark_testing_conf


@pytest.fixture(scope="session", autouse=True)
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    """Local spark session for testing purposes.

    Args:
        tmp_path_factory (pytest.TempPathFactory): pytest fixture

    Returns:
        SparkSession: local spark session
    """
    return (
        SparkSession.builder.config(conf=get_spark_testing_conf())
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )


@pytest.fixture()
def session() -> Session:
    """Return gentropy Session object."""
    return Session()


@pytest.fixture()
def hail_home() -> str:
    """Return the path to the Hail home directory."""
    return Path(hl.__file__).parent.as_posix()


@pytest.fixture()
def mock_colocalisation(spark: SparkSession) -> Colocalisation:
    """Mock colocalisation dataset."""
    coloc_schema = Colocalisation.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(coloc_schema)
        .withColumnSpec("h0", percentNulls=0.1)
        .withColumnSpec("h1", percentNulls=0.1)
        .withColumnSpec("h2", percentNulls=0.1)
        .withColumnSpec("h3", percentNulls=0.1)
        .withColumnSpec("h4", percentNulls=0.1)
        .withColumnSpec("log2h4h3", percentNulls=0.1)
        .withColumnSpec("clpp", percentNulls=0.1)
        .withColumnSpec(
            "colocalisationMethod",
            percentNulls=0.0,
            values=["COLOC", "eCAVIAR"],
        )
    )
    return Colocalisation(_df=data_spec.build(), _schema=coloc_schema)


def mock_study_index_data(spark: SparkSession) -> DataFrame:
    """Mock study index dataset."""
    si_schema = StudyIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(si_schema)
        .withColumnSpec(
            "traitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "backgroundTraitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "discoverySamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "replicationSamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec("geneId", percentNulls=0.1)
        .withColumnSpec("pubmedId", percentNulls=0.1)
        .withColumnSpec("publicationFirstAuthor", percentNulls=0.1)
        .withColumnSpec("publicationDate", percentNulls=0.1)
        .withColumnSpec("publicationJournal", percentNulls=0.1)
        .withColumnSpec("publicationTitle", percentNulls=0.1)
        .withColumnSpec("initialSampleSize", percentNulls=0.1)
        .withColumnSpec("nCases", percentNulls=0.1)
        .withColumnSpec("nControls", percentNulls=0.1)
        .withColumnSpec("nSamples", percentNulls=0.1)
        .withColumnSpec("summarystatsLocation", percentNulls=0.1)
        .withColumnSpec(
            "studyType", percentNulls=0.0, values=["eqtl", "pqtl", "sqtl", "gwas"]
        )
    )
    return data_spec.build()


@pytest.fixture()
def mock_study_index(spark: SparkSession) -> StudyIndex:
    """Mock StudyIndex dataset."""
    return StudyIndex(
        _df=mock_study_index_data(spark),
        _schema=StudyIndex.get_schema(),
    )


@pytest.fixture()
def mock_study_index_gwas_catalog(spark: SparkSession) -> StudyIndexGWASCatalog:
    """Mock GWASCatalogStudyIndex dataset."""
    return StudyIndexGWASCatalog(
        _df=mock_study_index_data(spark),
        _schema=StudyIndexGWASCatalog.get_schema(),
    )


@pytest.fixture()
def mock_study_locus_overlap(spark: SparkSession) -> StudyLocusOverlap:
    """Mock StudyLocusOverlap dataset."""
    overlap_schema = StudyLocusOverlap.get_schema()

    data_spec = dg.DataGenerator(
        spark,
        rows=400,
        partitions=4,
        randomSeedMethod="hash_fieldname",
    ).withSchema(overlap_schema)

    return StudyLocusOverlap(_df=data_spec.build(), _schema=overlap_schema)


def mock_study_locus_data(spark: SparkSession) -> DataFrame:
    """Mock study_locus dataset."""
    sl_schema = StudyLocus.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(sl_schema)
        .withColumnSpec("chromosome", percentNulls=0.1)
        .withColumnSpec("position", minValue=100, percentNulls=0.1)
        .withColumnSpec("beta", percentNulls=0.1)
        .withColumnSpec("effectAlleleFrequencyFromSource", percentNulls=0.1)
        .withColumnSpec("standardError", percentNulls=0.1)
        .withColumnSpec("subStudyDescription", percentNulls=0.1)
        .withColumnSpec("pValueMantissa", minValue=1, percentNulls=0.1)
        .withColumnSpec("pValueExponent", minValue=-10, percentNulls=0.1)
        .withColumnSpec(
            "qualityControls",
            expr="array(cast(rand() as string))",
            percentNulls=0.1,
        )
        .withColumnSpec("finemappingMethod", percentNulls=0.1)
        .withColumnSpec(
            "locus",
            expr='array(named_struct("is95CredibleSet", cast(rand() > 0.5 as boolean), "is99CredibleSet", cast(rand() > 0.5 as boolean), "logBF", rand(), "posteriorProbability", rand(), "variantId", cast(rand() as string), "beta", rand(), "standardError", rand(), "r2Overall", rand(), "pValueMantissa", rand(), "pValueExponent", rand()))',
            percentNulls=0.1,
        )
    )
    return data_spec.build()


@pytest.fixture()
def mock_study_locus(spark: SparkSession) -> StudyLocus:
    """Mock study_locus dataset."""
    return StudyLocus(
        _df=mock_study_locus_data(spark),
        _schema=StudyLocus.get_schema(),
    )


@pytest.fixture()
def mock_study_locus_gwas_catalog(spark: SparkSession) -> StudyLocusGWASCatalog:
    """Mock study_locus dataset."""
    return StudyLocusGWASCatalog(
        _df=mock_study_locus_data(spark),
        _schema=StudyLocusGWASCatalog.get_schema(),
    )


@pytest.fixture()
def mock_intervals(spark: SparkSession) -> Intervals:
    """Mock intervals dataset."""
    interval_schema = Intervals.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(interval_schema)
        .withColumnSpec("pmid", percentNulls=0.1)
        .withColumnSpec("resourceScore", percentNulls=0.1)
        .withColumnSpec("score", percentNulls=0.1)
        .withColumnSpec("biofeature", percentNulls=0.1)
    )

    return Intervals(_df=data_spec.build(), _schema=interval_schema)


@pytest.fixture()
def mock_v2g(spark: SparkSession) -> V2G:
    """Mock v2g dataset."""
    v2g_schema = V2G.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(v2g_schema)
        .withColumnSpec("distance", percentNulls=0.1)
        .withColumnSpec("resourceScore", percentNulls=0.1)
        .withColumnSpec("score", percentNulls=0.1)
        .withColumnSpec("pmid", percentNulls=0.1)
        .withColumnSpec("biofeature", percentNulls=0.1)
        .withColumnSpec("variantFunctionalConsequenceId", percentNulls=0.1)
        .withColumnSpec("isHighQualityPlof", percentNulls=0.1)
    )

    return V2G(_df=data_spec.build(), _schema=v2g_schema)


@pytest.fixture()
def mock_variant_index(spark: SparkSession) -> VariantIndex:
    """Mock variant index."""
    vi_schema = VariantIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(vi_schema)
        .withColumnSpec("mostSevereConsequenceId", percentNulls=0.1)
        # Nested column handling workaround
        # https://github.com/databrickslabs/dbldatagen/issues/135
        # It's a workaround for nested column handling in dbldatagen.
        .withColumnSpec(
            "inSilicoPredictors",
            expr="""
                array(
                    named_struct(
                        "method", cast(rand() as string),
                        "assessment", cast(rand() as string),
                        "score", rand(),
                        "assessmentFlag", cast(rand() as string),
                        "targetId", cast(rand() as string)
                    )
                )
            """,
            percentNulls=0.1,
        )
        .withColumnSpec(
            "alleleFrequencies",
            expr='array(named_struct("alleleFrequency", rand(), "populationName", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec("rsIds", expr="array(cast(rand() AS string))", percentNulls=0.1)
        .withColumnSpec(
            "transcriptConsequences",
            expr="""
                array(
                    named_struct(
                        "variantFunctionalConsequenceIds", array(cast(rand() as string)),
                        "aminoAcidChange", cast(rand() as string),
                        "uniprotAccessions", array(cast(rand() as string)),
                        "isEnsemblCanonical", cast(rand() as boolean),
                        "codons", cast(rand() as string),
                        "distance", cast(rand() as long),
                        "targetId", cast(rand() as string),
                        "impact", cast(rand() as string),
                        "lofteePrediction", cast(rand() as string),
                        "siftPrediction", rand(),
                        "polyphenPrediction", rand(),
                        "transcriptId", cast(rand() as string)
                    )
                )
            """,
            percentNulls=0.1,
        )
        .withColumnSpec(
            "dbXrefs",
            expr="""
                array(
                    named_struct(
                        "id", cast(rand() as string),
                        "source", cast(rand() as string)
                    )
                )
            """,
            percentNulls=0.1,
        )
    )

    return VariantIndex(_df=data_spec.build(), _schema=vi_schema)


@pytest.fixture()
def mock_summary_statistics_data(spark: SparkSession) -> DataFrame:
    """Generating mock summary statistics data.

    Args:
        spark (SparkSession): Spark session

    Returns:
        DataFrame: Mock summary statistics data
    """
    ss_schema = SummaryStatistics.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="summaryStats",
        )
        .withSchema(ss_schema)
        # Allowing missingness in effect allele frequency and enforce upper limit:
        .withColumnSpec(
            "effectAlleleFrequencyFromSource", percentNulls=0.1, maxValue=1.0
        )
        # Allowing missingness:
        .withColumnSpec("standardError", percentNulls=0.1)
        # Making sure p-values are below 1:
    ).build()

    return data_spec


@pytest.fixture()
def mock_summary_statistics(
    mock_summary_statistics_data: DataFrame,
) -> SummaryStatistics:
    """Generating a mock summary statistics dataset."""
    return SummaryStatistics(
        _df=mock_summary_statistics_data, _schema=SummaryStatistics.get_schema()
    )


@pytest.fixture()
def mock_ld_index(spark: SparkSession) -> LDIndex:
    """Mock gene index."""
    ld_schema = LDIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(ld_schema)
        .withColumnSpec(
            "ldSet",
            expr="array(named_struct('tagVariantId', cast(rand() as string), 'rValues', array(named_struct('population', cast(rand() as string), 'r', cast(rand() as double)))))",
        )
    )

    return LDIndex(_df=data_spec.build(), _schema=ld_schema)


@pytest.fixture()
def sample_gwas_catalog_studies(spark: SparkSession) -> DataFrame:
    """Sample GWAS Catalog studies."""
    return spark.read.csv(
        "tests/gentropy/data_samples/gwas_catalog_studies.tsv",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_gwas_catalog_ancestries_lut(spark: SparkSession) -> DataFrame:
    """Sample GWAS ancestries sample data."""
    return spark.read.csv(
        "tests/gentropy/data_samples/gwas_catalog_ancestries.tsv",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_gwas_catalog_harmonised_sumstats_list(spark: SparkSession) -> DataFrame:
    """Sample GWAS harmonised sumstats sample data."""
    return spark.read.csv(
        "tests/gentropy/data_samples/gwas_catalog_harmonised_list.txt",
        sep="\t",
        header=False,
    )


@pytest.fixture()
def sample_gwas_catalog_associations(spark: SparkSession) -> DataFrame:
    """Sample GWAS raw associations sample data."""
    return spark.read.csv(
        "tests/gentropy/data_samples/gwas_catalog_associations.tsv",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_summary_statistics(spark: SparkSession) -> SummaryStatistics:
    """Sample GWAS raw associations sample data."""
    return SummaryStatistics(
        _df=spark.read.parquet("tests/gentropy/data_samples/sumstats_sample"),
        _schema=SummaryStatistics.get_schema(),
    )


@pytest.fixture()
def sample_finngen_studies(spark: SparkSession) -> DataFrame:
    """Sample FinnGen studies."""
    # For reference, the sample file was generated with the following command:
    # curl https://r9.finngen.fi/api/phenos | jq '.[:10]' > tests/gentropy/data_samples/finngen_studies_sample.json
    with open(
        "tests/gentropy/data_samples/finngen_studies_sample.json"
    ) as finngen_studies:
        json_data = finngen_studies.read()
        rdd = spark.sparkContext.parallelize([json_data])
        return spark.read.json(rdd)


@pytest.fixture()
def sample_eqtl_catalogue_finemapping_credible_sets(spark: SparkSession) -> DataFrame:
    """Sample raw eQTL Catalogue credible sets outputted by SuSIE."""
    return spark.read.option("delimiter", "\t").csv(
        "tests/gentropy/data_samples/QTD000584.credible_sets.tsv",
        header=True,
        schema=EqtlCatalogueFinemapping.raw_credible_set_schema,
    )


@pytest.fixture()
def sample_eqtl_catalogue_finemapping_lbf(spark: SparkSession) -> DataFrame:
    """Sample raw eQTL Catalogue table with logBayesFactors outputted by SuSIE."""
    return spark.read.option("delimiter", "\t").csv(
        "tests/gentropy/data_samples/QTD000584.lbf_variable.txt",
        header=True,
        schema=EqtlCatalogueFinemapping.raw_lbf_schema,
    )


@pytest.fixture()
def sample_eqtl_catalogue_studies_metadata(spark: SparkSession) -> DataFrame:
    """Sample raw eQTL Catalogue table with metadata about the QTD000584 study."""
    return spark.read.option("delimiter", "\t").csv(
        "tests/gentropy/data_samples/sample_eqtl_catalogue_studies.tsv",
        header=True,
        schema=EqtlCatalogueStudyIndex.raw_studies_metadata_schema,
    )


@pytest.fixture()
def sample_ukbiobank_studies(spark: SparkSession) -> DataFrame:
    """Sample UKBiobank manifest."""
    # Sampled 10 rows of the UKBB manifest tsv
    return spark.read.csv(
        "tests/gentropy/data_samples/neale2_saige_study_manifest.samples.tsv",
        sep="\t",
        header=True,
        inferSchema=True,
    )


@pytest.fixture()
def sample_target_index(spark: SparkSession) -> DataFrame:
    """Sample target index sample data."""
    return spark.read.parquet(
        "tests/gentropy/data_samples/target_sample.parquet",
    )


@pytest.fixture()
def mock_gene_index(spark: SparkSession) -> GeneIndex:
    """Mock gene index dataset."""
    gi_schema = GeneIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(gi_schema)
        .withColumnSpec("approvedSymbol", percentNulls=0.1)
        .withColumnSpec("biotype", percentNulls=0.1)
        .withColumnSpec("approvedName", percentNulls=0.1)
        .withColumnSpec("tss", percentNulls=0.1)
        .withColumnSpec("start", percentNulls=0.1)
        .withColumnSpec("end", percentNulls=0.1)
        .withColumnSpec("strand", percentNulls=0.1)
    )

    return GeneIndex(_df=data_spec.build(), _schema=gi_schema)


@pytest.fixture()
def liftover_chain_37_to_38(spark: SparkSession) -> LiftOverSpark:
    """Sample liftover chain file."""
    return LiftOverSpark("tests/gentropy/data_samples/grch37_to_grch38.over.chain")


@pytest.fixture()
def sample_l2g_gold_standard(spark: SparkSession) -> DataFrame:
    """Sample L2G gold standard curation."""
    return spark.read.json(
        "tests/gentropy/data_samples/l2g_gold_standard_curation_sample.json.gz",
    )


@pytest.fixture()
def sample_otp_interactions(spark: SparkSession) -> DataFrame:
    """Sample OTP gene-gene interactions dataset."""
    return spark.read.parquet(
        "tests/gentropy/data_samples/otp_interactions_sample.parquet",
    )


@pytest.fixture()
def mock_l2g_feature_matrix(spark: SparkSession) -> L2GFeatureMatrix:
    """Mock l2g feature matrix dataset."""
    schema = L2GFeatureMatrix.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=50,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(schema)
        .withColumnSpec("distanceTssMean", percentNulls=0.1)
        .withColumnSpec("distanceTssMinimum", percentNulls=0.1)
        .withColumnSpec("eqtlColocClppMaximum", percentNulls=0.1)
        .withColumnSpec("eqtlColocClppMaximumNeighborhood", percentNulls=0.1)
        .withColumnSpec("eqtlColocLlrMaximum", percentNulls=0.1)
        .withColumnSpec("eqtlColocLlrMaximumNeighborhood", percentNulls=0.1)
        .withColumnSpec("pqtlColocClppMaximum", percentNulls=0.1)
        .withColumnSpec("pqtlColocClppMaximumNeighborhood", percentNulls=0.1)
        .withColumnSpec("pqtlColocLlrMaximum", percentNulls=0.1)
        .withColumnSpec("pqtlColocLlrMaximumNeighborhood", percentNulls=0.1)
        .withColumnSpec("sqtlColocClppMaximum", percentNulls=0.1)
        .withColumnSpec("sqtlColocClppMaximumNeighborhood", percentNulls=0.1)
        .withColumnSpec("sqtlColocLlrMaximum", percentNulls=0.1)
        .withColumnSpec("sqtlColocLlrMaximumNeighborhood", percentNulls=0.1)
        .withColumnSpec(
            "goldStandardSet", percentNulls=0.0, values=["positive", "negative"]
        )
    )

    return L2GFeatureMatrix(_df=data_spec.build(), _schema=schema)


@pytest.fixture()
def mock_l2g_gold_standard(spark: SparkSession) -> L2GGoldStandard:
    """Mock l2g gold standard dataset."""
    schema = L2GGoldStandard.get_schema()
    data_spec = dg.DataGenerator(
        spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
    ).withSchema(schema)

    return L2GGoldStandard(_df=data_spec.build(), _schema=schema)


@pytest.fixture()
def mock_l2g_predictions(spark: SparkSession) -> L2GPrediction:
    """Mock l2g predictions dataset."""
    schema = L2GPrediction.get_schema()
    data_spec = dg.DataGenerator(
        spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
    ).withSchema(schema)

    return L2GPrediction(_df=data_spec.build(), _schema=schema)


@pytest.fixture()
def sample_data_for_carma() -> list[np.ndarray]:
    """Sample data for fine-mapping by CARMA."""
    ld = pd.read_csv("tests/gentropy/data_samples/01_test_ld.csv", header=None)
    ld = np.array(ld)
    z = pd.read_csv("tests/gentropy/data_samples/01_test_z.csv")
    z = np.array(z.iloc[:, 1])
    pips = pd.read_csv("tests/gentropy/data_samples/01_test_PIPs.txt")
    pips = np.array(pips.iloc[:, 0])
    return [ld, z, pips]


@pytest.fixture()
def sample_data_for_susie_inf() -> list[np.ndarray]:
    """Sample data for fine-mapping by SuSiE-inf."""
    ld = np.loadtxt("tests/gentropy/data_samples/01_test_ld.csv", delimiter=",")
    z = pd.read_csv("tests/gentropy/data_samples/01_test_z.csv")
    z = np.array(z.iloc[:, 1])
    lbf_moments = np.loadtxt("tests/gentropy/data_samples/01_test_lbf_moments.csv")
    lbf_mle = np.loadtxt("tests/gentropy/data_samples/01_test_lbf_mle.csv")
    return [ld, z, lbf_moments, lbf_mle]
