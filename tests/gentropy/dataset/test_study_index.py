"""Test study index dataset."""

from __future__ import annotations

import pytest
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.study_index import StudyIndex
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f


def test_study_index_creation(mock_study_index: StudyIndex) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index, StudyIndex)


def test_study_index_type_lut(mock_study_index: StudyIndex) -> None:
    """Test study index type lut."""
    assert isinstance(mock_study_index.study_type_lut(), DataFrame)


def test_aggregate_and_map_ancestries__correctness(spark: SparkSession) -> None:
    """Test if population are mapped and relative sample sizes are calculated."""
    data = [
        (
            "s1",
            "East Asian",
            100,
        ),
        (
            "s1",
            "Finnish",
            100,
        ),
        (
            "s1",
            "NR",
            100,
        ),
        (
            "s1",
            "European",
            100,
        ),
    ]

    columns = ["studyId", "ancestry", "sampleSize"]

    df = (
        spark.createDataFrame(data, columns)
        .groupBy("studyId")
        .agg(
            f.collect_list(f.struct("ancestry", "sampleSize")).alias("discoverySamples")
        )
        .select(
            StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")).alias(
                "parsedPopulation"
            )
        )
    )

    # Asserting that there are three population (both NR and Europeans are grounded to 'nfe'):
    assert (df.select(f.explode("parsedPopulation")).count()) == 3

    # Asserting that the relative count go to 1.0
    assert (
        (
            df.select(
                f.aggregate(
                    "parsedPopulation",
                    f.lit(0.0),
                    lambda y, x: y + x.relativeSampleSize,
                ).alias("sum")
            ).collect()[0]["sum"]
        )
        == 1.0
    )


def test_aggregate_samples_by_ancestry__correctness(spark: SparkSession) -> None:
    """Test correctness of the ancestry aggregator function."""
    data = [
        (
            "s1",
            "a1",
            100,
        ),
        (
            "s1",
            "a1",
            100,
        ),
        (
            "s1",
            "a2",
            100,
        ),
    ]

    columns = ["studyId", "ancestry", "sampleSize"]

    df = (
        spark.createDataFrame(data, columns)
        .groupBy("studyId")
        .agg(
            f.collect_list(f.struct("ancestry", "sampleSize")).alias("discoverySamples")
        )
        .select(
            f.aggregate(
                "discoverySamples",
                f.array_distinct(
                    f.transform(
                        "discoverySamples",
                        lambda x: f.struct(
                            x.ancestry.alias("ancestry"), f.lit(0.0).alias("sampleSize")
                        ),
                    )
                ),
                StudyIndex._aggregate_samples_by_ancestry,
            ).alias("test_output")
        )
        .persist()
    )

    # Asserting the number of aggregated population:
    assert (
        df.filter(f.col("studyId") == "s1").select(f.explode("test_output")).count()
    ) == 2

    # Asserting the number of aggregated sample size:
    assert (
        (
            df.filter(f.col("studyId") == "s1")
            .select(
                f.aggregate(
                    "test_output", f.lit(0.0), lambda y, x: x.sampleSize + y
                ).alias("totalSamples")
            )
            .collect()[0]["totalSamples"]
        )
        == 300.0
    )


class TestGeneValidation:
    """A small test suite to ensure the gene validation works as intended."""

    GENE_DATA = [
        ("ENSG00000102021", "1"),
        ("ENSG000001020", "1"),
    ]

    GENE_COLUMNS = ["geneId", "chromosome"]

    STUDY_DATA = [
        ("s1", "eqtl", "p", "ENSG00000102021"),
        # This is the only study to be flagged: QTL + Wrong gene
        ("s2", "eqtl", "p", "cicaful"),
        ("s3", "gwas", "p", None),
        ("s4", "gwas", "p", "pocok"),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId", "geneId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestGeneValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array()
            ),
            _schema=StudyIndex.get_schema(),
        )

        self.study_index_no_gene = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS)
            .withColumn("qualityControls", f.array())
            .drop("geneId"),
            _schema=StudyIndex.get_schema(),
        )

        self.gene_index = GeneIndex(
            _df=spark.createDataFrame(self.GENE_DATA, self.GENE_COLUMNS),
            _schema=GeneIndex.get_schema(),
        )

    def test_gene_validation_type(self: TestGeneValidation) -> None:
        """Testing if the validation runs and returns the expected type."""
        validated = self.study_index.validate_target(self.gene_index)
        assert isinstance(validated, StudyIndex)

    def test_gene_validation_correctness(self: TestGeneValidation) -> None:
        """Testing if the gene validation only flags the expected studies."""
        validated = self.study_index.validate_target(self.gene_index).persist()

        # Make sure there's only one flagged:
        assert validated.df.filter(f.size("qualityControls") != 0).count() == 1

        # Make sure there's only one flagged:
        flagged_study = validated.df.filter(f.size("qualityControls") != 0).collect()[
            0
        ]["studyId"]

        assert flagged_study == "s2"

    def test_gene_validation_no_gene_column(self: TestGeneValidation) -> None:
        """Testing what happens if no geneId column is present."""
        validated = self.study_index_no_gene.validate_target(self.gene_index)

        # Asserty type:
        assert isinstance(validated, StudyIndex)

        # Assert count:
        assert validated.df.count() == self.study_index.df.count()


class TestUniquenessValidation:
    """A small test suite to ensure the gene validation works as intended."""

    STUDY_DATA = [
        # This is the only study to be flagged:
        ("s1", "eqtl", "p"),
        ("s1", "eqtl", "p"),
        ("s3", "gwas", "p"),
        ("s4", "gwas", "p"),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestUniquenessValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array()
            ),
            _schema=StudyIndex.get_schema(),
        )

    def test_uniqueness_return_type(self: TestUniquenessValidation) -> None:
        """Testing if the function returns the right type."""
        assert isinstance(self.study_index.validate_unique_study_id(), StudyIndex)

    def test_uniqueness_correct_data(self: TestUniquenessValidation) -> None:
        """Testing if the function returns the right type."""
        validated = self.study_index.validate_unique_study_id().persist()

        # We have more than one flagged studies:
        assert validated.df.filter(f.size(f.col("qualityControls")) > 0).count() > 1

        # The flagged study identifiers are found more than once:
        flagged_ids = {
            study["studyId"]: study["count"]
            for study in validated.df.filter(f.size(f.col("qualityControls")) > 0)
            .groupBy("studyId")
            .count()
            .collect()
        }

        for _, count in flagged_ids.items():
            assert count > 1

        # the right study is found:
        assert "s1" in flagged_ids


class TestStudyTypeValidation:
    """Testing study type validation."""

    STUDY_DATA = [
        # This study is flagged because of unexpected type:
        ("s1", "cicaful", "p", "gene1"),
        ("s3", "eqtl", "p", "gene1"),
        ("s4", "gwas", "p", None),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId", "geneId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyTypeValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array()
            ),
            _schema=StudyIndex.get_schema(),
        )

    def test_study_type_validation_return_type(self: TestStudyTypeValidation) -> None:
        """Testing if the function returns the expected type."""
        assert isinstance(self.study_index.validate_study_type(), StudyIndex)

    def test_study_type_validation_correctness(self: TestStudyTypeValidation) -> None:
        """Test if the correct study is flagged."""
        flagged_study_ids = [
            study["studyId"]
            for study in self.study_index.validate_study_type()
            .df.filter(f.size("qualityControls") > 0)
            .collect()
        ]
        assert "s1" in flagged_study_ids
        # Check if any
        flagged_study_types = [
            study["studyType"]
            for study in self.study_index.validate_study_type()
            .df.filter(f.size("qualityControls") != 0)
            .collect()
        ]
        for study_type in flagged_study_types:
            assert study_type != "gwas"
            assert "qtl" not in study_type


class TestDiseaseValidation:
    """Testing the disease validation."""

    DISEASE_DATA = [
        ("EFO_old", "EFO_new"),
        ("EFO_new", "EFO_new"),
        ("EFO_new2", "EFO_new2"),
    ]

    DISEASE_HEADER = ["efo", "diseaseId"]

    STUDY_DATA = [
        # Old EFO mapped to new:
        ("s1", "gwas", "p", "EFO_old"),
        # List of EFOs some mapped, some not:
        ("s2", "gwas", "p", "EFO_old"),
        ("s2", "gwas", "p", "EFO_new2"),
        ("s2", "gwas", "p", "EFO_invalid"),
        # single EFO mapped as the same:
        ("s3", "gwas", "p", "EFO_new2"),
        # Invalid study:
        ("s4", "gwas", "p", "EFO_invalid"),
        # Invalid study - no EFO:
        ("s5", "gwas", "p", None),
        # Valid study, missing efo, not gwas:
        ("s6", "eqtl", "p2", None),
    ]

    STUDY_COLUMNS = ["studyId", "studyType", "projectId", "efo"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestDiseaseValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        study_df = (
            spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS)
            .groupBy("studyId", "studyType", "projectId")
            .agg(f.collect_set("efo").alias("traitFromSourceMappedIds"))
            .withColumn("qualityControls", f.array())
            .withColumn("backgroundTraitFromSourceMappedIds", f.array())
        )
        study_df.show()
        # Mock study index:
        self.study_index = StudyIndex(
            _df=study_df,
            _schema=StudyIndex.get_schema(),
        )

        # Disease mapping:
        self.disease = spark.createDataFrame(self.DISEASE_DATA, self.DISEASE_HEADER)

        # Validated data:
        self.validated = self.study_index.validate_disease(self.disease).persist()

    def test_disease_validation_return_type(self: TestDiseaseValidation) -> None:
        """Testing if the disease validation returns the right type."""
        assert isinstance(self.validated, StudyIndex)

    def test_disease_validation_right_flag(self: TestDiseaseValidation) -> None:
        """Testing if the right studies are flagged in the validation step."""
        # Testing flagged studies:
        for study in self.validated.df.filter(f.size("qualityControls") > 0).collect():
            # All flagged studies are from gwas:
            assert study["studyType"] == "gwas"
            # None of the flagged studies have assigned valid disease:
            assert len(study["diseaseIds"]) == 0

        # Testing unflagged studies:
        for study in self.validated.df.filter(f.size("qualityControls") == 0).collect():
            # If a valid study has no disease, it cannot be gwas:
            if len(study["diseaseIds"]) == 0:
                assert study["studyType"] != "gwas"

    def test_disease_validation_disease_mapping(self: TestDiseaseValidation) -> None:
        """Testing if old disease identifiers can be rescued."""
        example_study_id = "s1"

        test_study = self.validated.df.filter(
            f.col("studyId") == example_study_id
        ).collect()[0]

        # Assert validation:
        assert len(test_study["qualityControls"]) == 0

        # Assert disease mapping:
        assert test_study["traitFromSourceMappedIds"][0] != test_study["diseaseIds"][0]

    def test_disease_validation_disease_removal(self: TestDiseaseValidation) -> None:
        """Testing if not all diseases can be mapped, the study still passes QC."""
        example_study_id = "s2"

        test_study = self.validated.df.filter(
            f.col("studyId") == example_study_id
        ).collect()[0]

        # Assert validation:
        assert len(test_study["qualityControls"]) == 0

        # Assert not all diseases could be mapped to disease index:
        assert len(test_study["traitFromSourceMappedIds"]) > len(
            test_study["diseaseIds"]
        )
