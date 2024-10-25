"""Test study locus dataset."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pyspark.sql import Column, Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import (
    CredibleInterval,
    CredibleSetConfidenceClasses,
    StudyLocus,
    StudyLocusQualityCheck,
)
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader


@pytest.mark.parametrize(
    "has_overlap, expected",
    [
        # Overlap exists
        (
            True,
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "commonTag",
                    "statistics": {
                        "left_posteriorProbability": 0.9,
                        "right_posteriorProbability": 0.6,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "nonCommonTag",
                    "statistics": {
                        "left_posteriorProbability": None,
                        "right_posteriorProbability": 0.6,
                    },
                },
            ],
        ),
        # No overlap
        (False, []),
    ],
)
def test_find_overlaps_semantic(
    spark: SparkSession, has_overlap: bool, expected: list[Any]
) -> None:
    """Test study locus overlaps with and without actual overlap."""
    if has_overlap:
        credset = StudyLocus(
            _df=spark.createDataFrame(
                # 2 associations with a common variant in the locus
                [
                    {
                        "studyLocusId": "1",
                        "variantId": "lead1",
                        "studyId": "study1",
                        "studyType": "gwas",
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": "2",
                        "variantId": "lead2",
                        "studyId": "study2",
                        "studyType": "eqtl",
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.6},
                            {"variantId": "nonCommonTag", "posteriorProbability": 0.6},
                        ],
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )
    else:
        credset = StudyLocus(
            _df=spark.createDataFrame(
                # 2 associations with no common variants in the locus
                [
                    {
                        "studyLocusId": "1",
                        "variantId": "lead1",
                        "studyId": "study1",
                        "studyType": "gwas",
                        "locus": [
                            {"variantId": "var1", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": "2",
                        "variantId": "lead2",
                        "studyId": "study2",
                        "studyType": "eqtl",
                        "locus": None,
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )

    expected_overlaps_df = spark.createDataFrame(
        expected, StudyLocusOverlap.get_schema()
    )
    cols_to_compare = [
        "tagVariantId",
        "statistics.left_posteriorProbability",
        "statistics.right_posteriorProbability",
    ]
    assert (
        credset.find_overlaps().df.select(*cols_to_compare).collect()
        == expected_overlaps_df.select(*cols_to_compare).collect()
    ), "Overlaps differ from expected."


def test_find_overlaps(mock_study_locus: StudyLocus) -> None:
    """Test study locus overlaps."""
    assert isinstance(mock_study_locus.find_overlaps(), StudyLocusOverlap)


@pytest.mark.parametrize(
    "study_type, expected_sl_count", [("gwas", 1), ("eqtl", 1), ("pqtl", 0)]
)
def test_filter_by_study_type(
    spark: SparkSession, study_type: str, expected_sl_count: int
) -> None:
    """Test filter by study type."""
    # Input data
    sl = StudyLocus(
        _df=spark.createDataFrame(
            [
                {
                    # from gwas
                    "studyLocusId": "1",
                    "variantId": "lead1",
                    "studyId": "study1",
                    "studyType": "gwas",
                },
                {
                    # from eqtl
                    "studyLocusId": "2",
                    "variantId": "lead2",
                    "studyId": "study2",
                    "studyType": "eqtl",
                },
            ],
            StudyLocus.get_schema(),
        ),
        _schema=StudyLocus.get_schema(),
    )

    observed = sl.filter_by_study_type(study_type)
    assert observed.df.count() == expected_sl_count


def test_annotate_locus_statistics(
    mock_study_locus: StudyLocus, mock_summary_statistics: SummaryStatistics
) -> None:
    """Test annotate locus statistics returns a StudyLocus."""
    assert isinstance(
        mock_study_locus.annotate_locus_statistics(mock_summary_statistics, 100),
        StudyLocus,
    )


def test_filter_credible_set(mock_study_locus: StudyLocus) -> None:
    """Test credible interval filter."""
    assert isinstance(
        mock_study_locus.filter_credible_set(CredibleInterval.IS95), StudyLocus
    )


def test_qc_abnormal_pips(mock_study_locus: StudyLocus) -> None:
    """Test that the qc_abnormal_pips method returns a StudyLocus object."""
    assert isinstance(mock_study_locus.qc_abnormal_pips(0.99, 1), StudyLocus)


# Used primarily for test_unique_variants_in_locus but also for other tests
test_unique_variants_in_locus_test_data = [
    (
        # Locus is not null, should return union between variants in locus and lead variant
        [
            (
                "1",
                "traitA",
                "22_varA",
                [
                    {"variantId": "22_varA", "posteriorProbability": 0.44},
                    {"variantId": "22_varB", "posteriorProbability": 0.015},
                ],
            ),
        ],
        [
            (
                "22_varA",
                "22",
            ),
            (
                "22_varB",
                "22",
            ),
        ],
    ),
    (
        # locus is null, should return lead variant
        [
            ("1", "traitA", "22_varA", None),
        ],
        [
            (
                "22_varA",
                "22",
            ),
        ],
    ),
]


@pytest.mark.parametrize(
    ("observed", "expected"),
    test_unique_variants_in_locus_test_data,
)
def test_unique_variants_in_locus(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test unique variants in locus."""
    # assert isinstance(mock_study_locus.test_unique_variants_in_locus(), DataFrame)
    schema = StructType(
        [
            StructField("studyLocusId", StringType(), True),
            StructField("studyId", StringType(), True),
            StructField("variantId", StringType(), True),
            StructField(
                "locus",
                ArrayType(
                    StructType(
                        [
                            StructField("variantId", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    data_sl = StudyLocus(
        _df=spark.createDataFrame(observed, schema), _schema=StudyLocus.get_schema()
    )
    expected_df = spark.createDataFrame(
        expected, schema="variantId: string, chromosome: string"
    )
    assert data_sl.unique_variants_in_locus().collect() == expected_df.collect()


def test_neglog_pvalue(mock_study_locus: StudyLocus) -> None:
    """Test neglog pvalue."""
    assert isinstance(mock_study_locus.neglog_pvalue(), Column)


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test clump."""
    assert isinstance(mock_study_locus.clump(), StudyLocus)


# Used primarily for test_annotate_credible_sets but also for other tests
test_annotate_credible_sets_test_data = [
    (
        # Simple case
        [
            # Observed
            (
                "1",
                "traitA",
                "leadB",
                [{"variantId": "tagVariantA", "posteriorProbability": 1.0}],
            ),
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadB",
                [
                    {
                        "variantId": "tagVariantA",
                        "posteriorProbability": 1.0,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    }
                ],
            )
        ],
    ),
    (
        # Unordered credible set
        [
            # Observed
            (
                "1",
                "traitA",
                "leadA",
                [
                    {"variantId": "tagVariantA", "posteriorProbability": 0.44},
                    {"variantId": "tagVariantB", "posteriorProbability": 0.015},
                    {"variantId": "tagVariantC", "posteriorProbability": 0.04},
                    {"variantId": "tagVariantD", "posteriorProbability": 0.005},
                    {"variantId": "tagVariantE", "posteriorProbability": 0.5},
                    {"variantId": "tagVariantNull", "posteriorProbability": None},
                    {"variantId": "tagVariantNull", "posteriorProbability": None},
                ],
            )
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadA",
                [
                    {
                        "variantId": "tagVariantE",
                        "posteriorProbability": 0.5,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantA",
                        "posteriorProbability": 0.44,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantC",
                        "posteriorProbability": 0.04,
                        "is95CredibleSet": True,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantB",
                        "posteriorProbability": 0.015,
                        "is95CredibleSet": False,
                        "is99CredibleSet": True,
                    },
                    {
                        "variantId": "tagVariantD",
                        "posteriorProbability": 0.005,
                        "is95CredibleSet": False,
                        "is99CredibleSet": False,
                    },
                    {
                        "variantId": "tagVariantNull",
                        "posteriorProbability": None,
                        "is95CredibleSet": False,
                        "is99CredibleSet": False,
                    },
                    {
                        "variantId": "tagVariantNull",
                        "posteriorProbability": None,
                        "is95CredibleSet": False,
                        "is99CredibleSet": False,
                    },
                ],
            )
        ],
    ),
    (
        # Null credible set
        [
            # Observed
            (
                "1",
                "traitA",
                "leadB",
                None,
            ),
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadB",
                None,
            )
        ],
    ),
    (
        # Empty credible set
        [
            # Observed
            (
                "1",
                "traitA",
                "leadB",
                [],
            ),
        ],
        [
            # Expected
            (
                "1",
                "traitA",
                "leadB",
                None,
            )
        ],
    ),
]


@pytest.mark.parametrize(
    ("observed", "expected"),
    test_annotate_credible_sets_test_data,
)
def test_annotate_credible_sets(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test annotate_credible_sets."""
    schema = StructType(
        [
            StructField("studyLocusId", StringType(), True),
            StructField("studyId", StringType(), True),
            StructField("variantId", StringType(), True),
            StructField(
                "locus",
                ArrayType(
                    StructType(
                        [
                            StructField("variantId", StringType(), True),
                            StructField("posteriorProbability", DoubleType(), True),
                            StructField("is95CredibleSet", BooleanType(), True),
                            StructField("is99CredibleSet", BooleanType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    data_sl = StudyLocus(
        _df=spark.createDataFrame(observed, schema), _schema=StudyLocus.get_schema()
    )
    expected_sl = StudyLocus(
        _df=spark.createDataFrame(expected, schema), _schema=StudyLocus.get_schema()
    )
    assert data_sl.annotate_credible_sets().df.collect() == expected_sl.df.collect()


def test_qc_abnormal_pips_bad_locus(spark: SparkSession) -> None:
    """Test qc_abnormal_pips with an abnormal locus."""
    # Input data
    sl = StudyLocus(
        _df=spark.createDataFrame(
            test_annotate_credible_sets_test_data[1][0], StudyLocus.get_schema()
        ),
        _schema=StudyLocus.get_schema(),
    )
    assert sl.qc_abnormal_pips(0.99, 1).df.count() == 0, "something"


def test_qc_abnormal_pips_good_locus(spark: SparkSession) -> None:
    """Test qc_abnormal_pips with a well-behaving locus."""
    # Input data
    sl = StudyLocus(
        _df=spark.createDataFrame(
            test_unique_variants_in_locus_test_data[0][0], StudyLocus.get_schema()
        ),
        _schema=StudyLocus.get_schema(),
    )
    assert sl.qc_abnormal_pips(0.99, 1).df.count() == 1, "something"


def test_annotate_ld(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex, mock_ld_index: LDIndex
) -> None:
    """Test annotate_ld."""
    assert isinstance(
        mock_study_locus.annotate_ld(mock_study_index, mock_ld_index), StudyLocus
    )


def test__qc_no_population(mock_study_locus: StudyLocus) -> None:
    """Test _qc_no_population."""
    assert isinstance(mock_study_locus._qc_no_population(), StudyLocus)


def test_qc_MHC_region(mock_study_locus: StudyLocus) -> None:
    """Test qc_MHC_region."""
    assert isinstance(mock_study_locus.qc_MHC_region(), StudyLocus)


def test_ldannotate(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex, mock_ld_index: LDIndex
) -> None:
    """Test ldannotate."""
    assert isinstance(
        mock_study_locus.annotate_ld(mock_study_index, mock_ld_index), StudyLocus
    )


def test_filter_ld_set(spark: SparkSession) -> None:
    """Test filter_ld_set."""
    observed_data = [
        Row(studyLocusId="sl1", ldSet=[{"tagVariantId": "tag1", "r2Overall": 0.4}])
    ]
    observed_df = spark.createDataFrame(
        observed_data, ["studyLocusId", "ldSet"]
    ).withColumn("ldSet", StudyLocus.filter_ld_set(f.col("ldSet"), 0.5))
    expected_tags_in_ld = 0
    assert (
        observed_df.filter(f.size("ldSet") > 1).count() == expected_tags_in_ld
    ), "Expected tags in ld set differ from observed."


def test_annotate_locus_statistics_boundaries(
    mock_study_locus: StudyLocus, mock_summary_statistics: SummaryStatistics
) -> None:
    """Test annotate locus statistics returns a StudyLocus."""
    df = mock_study_locus.df
    df = df.withColumn("locusStart", f.col("position") - 10)
    df = df.withColumn("locusEnd", f.col("position") + 10)
    slt = StudyLocus(df, StudyLocus.get_schema())
    assert isinstance(
        slt.annotate_locus_statistics_boundaries(mock_summary_statistics),
        StudyLocus,
    )


class TestStudyLocusVariantValidation:
    """Collection of tests for StudyLocus variant validation."""

    VARIANT_DATA = [
        ("v1", "c1", 1, "r", "a"),
        ("v2", "c1", 2, "r", "a"),
        ("v3", "c1", 3, "r", "a"),
        ("v4", "c1", 4, "r", "a"),
    ]
    VARIANT_HEADERS = [
        "variantId",
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
    ]

    STUDYLOCUS_DATA = [
        # First studylocus passes qc:
        ("1", "v1", "s1", "v1"),
        ("1", "v1", "s1", "v2"),
        ("1", "v1", "s1", "v3"),
        # Second studylocus passes qc:
        ("2", "v1", "s1", "v1"),
        ("2", "v1", "s1", "v5"),
    ]
    STUDYLOCUS_HEADER = ["studyLocusId", "variantId", "studyId", "tagVariantId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusVariantValidation, spark: SparkSession) -> None:
        """Setup study locus for testing."""
        self.variant_index = VariantIndex(
            _df=spark.createDataFrame(
                self.VARIANT_DATA, self.VARIANT_HEADERS
            ).withColumn("position", f.col("position").cast(t.IntegerType())),
            _schema=VariantIndex.get_schema(),
        )

        self.credible_set = StudyLocus(
            _df=(
                spark.createDataFrame(self.STUDYLOCUS_DATA, self.STUDYLOCUS_HEADER)
                .withColumn("studyLocusId", f.col("studyLocusId").cast(t.StringType()))
                .withColumn("qualityControls", f.array())
                .groupBy("studyLocusId", "variantId", "studyId")
                .agg(
                    f.collect_set(
                        f.struct(f.col("tagVariantId").alias("variantId"))
                    ).alias("locus")
                )
            ),
            _schema=StudyLocus.get_schema(),
        )

    def test_validation_return_type(self: TestStudyLocusVariantValidation) -> None:
        """Testing if the validation returns the right type."""
        assert isinstance(
            self.credible_set.validate_variant_identifiers(self.variant_index),
            StudyLocus,
        )

    def test_validation_no_data_loss(self: TestStudyLocusVariantValidation) -> None:
        """Testing if the validation returns same number of rows."""
        assert (
            self.credible_set.validate_variant_identifiers(
                self.variant_index
            ).df.count()
            == self.credible_set.df.count()
        )

    def test_validation_correctness(self: TestStudyLocusVariantValidation) -> None:
        """Testing if the validation flags the right number of variants."""
        # Execute validation:
        validated = self.credible_set.validate_variant_identifiers(
            self.variant_index
        ).df

        # Make sure there's only one study locus with a failed variants:
        assert validated.filter(f.size("qualityControls") > 0).count() == 1

        # Check that the right one is flagged:
        assert (
            validated.filter(
                (f.size("qualityControls") > 0) & (f.col("studyLocusId") == "2")
            ).count()
            == 1
        )


class TestStudyLocusValidation:
    """Collection of tests for StudyLocus validation."""

    STUDY_LOCUS_DATA = [
        # Won't be flagged:
        ("1", "v1", "s1", 1.0, -8, [], "pics"),
        # Already flagged, needs to be tested if the flag reamins unique:
        (
            "2",
            "v2",
            "s2",
            5.0,
            -4,
            [StudyLocusQualityCheck.SUBSIGNIFICANT_FLAG.value],
            "pics",
        ),
        # To be flagged:
        ("3", "v3", "s3", 1.0, -4, [], "SuSiE-inf"),
        ("4", "v4", "s4", 5.0, -3, [], "unknown"),
    ]

    STUDY_LOCUS_SCHEMA = t.StructType(
        [
            t.StructField("studyLocusId", t.StringType(), False),
            t.StructField("variantId", t.StringType(), False),
            t.StructField("studyId", t.StringType(), False),
            t.StructField("pValueMantissa", t.FloatType(), False),
            t.StructField("pValueExponent", t.IntegerType(), False),
            t.StructField("qualityControls", t.ArrayType(t.StringType()), False),
            t.StructField("finemappingMethod", t.StringType(), False),
        ]
    )

    STUDY_DATA = [
        # Unflagged:
        ("s1", "p1", "gwas", []),
        # Flagged:
        ("s2", "p1", "gwas", ["some_flag"]),
        ("s3", "p1", "gwas", ["some_flag"]),
    ]

    STUDY_SCHEMA = t.StructType(
        [
            t.StructField("studyId", t.StringType(), False),
            t.StructField("projectId", t.StringType(), False),
            t.StructField("studyType", t.StringType(), False),
            t.StructField("qualityControls", t.ArrayType(t.StringType(), True), True),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusValidation, spark: SparkSession) -> None:
        """Setup study locus for testing."""
        self.study_locus = StudyLocus(
            _df=spark.createDataFrame(
                self.STUDY_LOCUS_DATA, schema=self.STUDY_LOCUS_SCHEMA
            ),
            _schema=StudyLocus.get_schema(),
        )

        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, schema=self.STUDY_SCHEMA),
            _schema=StudyIndex.get_schema(),
        )

    @pytest.mark.parametrize(
        "test_pvalues",
        [1e-6, 1e-5],
    )
    def test_return_type_pval_validation(
        self: TestStudyLocusValidation, test_pvalues: float
    ) -> None:
        """Testing if the p-value validation returns the right type."""
        assert isinstance(
            self.study_locus.validate_lead_pvalue(test_pvalues), StudyLocus
        )

    def test_confidence_flag_return_type(self: TestStudyLocusValidation) -> None:
        """Testing if the confidence flagging returns the right type."""
        assert isinstance(self.study_locus.assign_confidence(), StudyLocus)

    def test_confidence_flag_new_column(self: TestStudyLocusValidation) -> None:
        """Testing if the confidence flagging adds a new column."""
        assert (
            self.study_locus.assign_confidence().df.columns
            == self.study_locus.df.columns + ["confidence"]
        )

    def test_confidence_flag_unknown_confidence(self: TestStudyLocusValidation) -> None:
        """Testing if the confidence flagging adds a new column."""
        assert (
            self.study_locus.assign_confidence()
            .df.filter(
                f.col("confidence") == CredibleSetConfidenceClasses.UNKNOWN.value
            )
            .count()
            == 1
        )

    @pytest.mark.parametrize(
        ("test_pvalues", "flagged_count"),
        [(1e-5, 3), (1e-4, 2)],
    )
    def test_flagged_count(
        self: TestStudyLocusValidation, test_pvalues: float, flagged_count: int
    ) -> None:
        """Testing if the p-value validation flags the right number of variants."""
        assert (
            self.study_locus.validate_lead_pvalue(test_pvalues)
            .df.filter(f.size("qualityControls") > 0)
            .count()
            == flagged_count
        )

    def test_flag_uniqueness(self: TestStudyLocusValidation) -> None:
        """Testing if the flag remains unique although the locus should be flagged twice."""
        assert (
            self.study_locus.validate_lead_pvalue(1e-10)
            .df.filter(f.size("qualityControls") > 1)
            .count()
        ) == 0

    def test_study_validation_return_type(self: TestStudyLocusValidation) -> None:
        """Testing if the study validation returns the right type."""
        assert isinstance(self.study_locus.validate_study(self.study_index), StudyLocus)

    def test_study_validation_no_data_loss(self: TestStudyLocusValidation) -> None:
        """Testing if the study validation returns same number of rows."""
        assert (
            self.study_locus.validate_study(self.study_index).df.count()
        ) == self.study_locus.df.count()

    def test_study_validation_correctness(self: TestStudyLocusValidation) -> None:
        """Testing if the study validation flags the right number of studies."""
        assert (
            self.study_locus.validate_study(self.study_index)
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"), StudyLocusQualityCheck.FAILED_STUDY.value
                )
            )
            .count()
        ) == 2

        assert (
            self.study_locus.validate_study(self.study_index)
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"), StudyLocusQualityCheck.MISSING_STUDY.value
                )
            )
            .count()
        ) == 1


class TestStudyLocusWindowClumping:
    """Testing window-based clumping on study locus."""

    TEST_DATASET = [
        ("s1", "c1", 1, -1),
        ("s1", "c1", 2, -2),
        ("s1", "c1", 3, -3),
        ("s2", "c2", 2, -2),
        ("s3", "c2", 2, -2),
    ]

    TEST_SCHEMA = t.StructType(
        [
            t.StructField("studyId", t.StringType(), False),
            t.StructField("chromosome", t.StringType(), False),
            t.StructField("position", t.IntegerType(), False),
            t.StructField("pValueExponent", t.IntegerType(), False),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusWindowClumping, spark: SparkSession) -> None:
        """Setup study locus for testing."""
        self.study_locus = StudyLocus(
            _df=(
                spark.createDataFrame(
                    self.TEST_DATASET, schema=self.TEST_SCHEMA
                ).withColumns(
                    {
                        "studyLocusId": f.monotonically_increasing_id().cast(
                            t.StringType()
                        ),
                        "pValueMantissa": f.lit(1).cast(t.FloatType()),
                        "variantId": f.concat(
                            f.lit("v"),
                            f.monotonically_increasing_id().cast(t.StringType()),
                        ),
                    }
                )
            ),
            _schema=StudyLocus.get_schema(),
        )

    def test_clump_return_type(self: TestStudyLocusWindowClumping) -> None:
        """Testing if the clumping returns the right type."""
        assert isinstance(self.study_locus.window_based_clumping(3), StudyLocus)

    def test_clump_no_data_loss(self: TestStudyLocusWindowClumping) -> None:
        """Testing if the clumping returns same number of rows."""
        assert (
            self.study_locus.window_based_clumping(3).df.count()
            == self.study_locus.df.count()
        )

    def test_correct_flag(self: TestStudyLocusWindowClumping) -> None:
        """Testing if the clumping flags are for variants."""
        assert (
            self.study_locus.window_based_clumping(3)
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"),
                    StudyLocusQualityCheck.WINDOW_CLUMPED.value,
                )
            )
            .count()
        ) == 2


def test_build_feature_matrix(
    mock_study_locus: StudyLocus,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
) -> None:
    """Test building feature matrix with the eQtlColocH4Maximum feature."""
    features_list = ["eQtlColocH4Maximum"]
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation,
        study_index=mock_study_index,
        study_locus=mock_study_locus,
    )
    fm = mock_study_locus.build_feature_matrix(features_list, loader)
    assert isinstance(
        fm, L2GFeatureMatrix
    ), "Feature matrix should be of type L2GFeatureMatrix"


class TestStudyLocusRedundancyFlagging:
    """Collection of tests related to flagging redundant credible sets."""

    STUDY_LOCUS_DATA = [
        ("1", "v1", "s1", "pics", []),
        ("2", "v2", "s1", "pics", [StudyLocusQualityCheck.TOP_HIT.value]),
        ("3", "v3", "s1", "pics", []),
        ("3", "v3", "s1", "pics", []),
        ("1", "v1", "s1", "pics", [StudyLocusQualityCheck.TOP_HIT.value]),
        ("1", "v1", "s2", "pics", [StudyLocusQualityCheck.TOP_HIT.value]),
        ("1", "v1", "s2", "pics", [StudyLocusQualityCheck.TOP_HIT.value]),
        ("1", "v1", "s3", "SuSie", []),
        ("1", "v1", "s3", "pics", [StudyLocusQualityCheck.TOP_HIT.value]),
        ("1", "v1", "s4", "pics", []),
        ("1", "v1", "s4", "SuSie", []),
        ("1", "v1", "s4", "pics", [StudyLocusQualityCheck.TOP_HIT.value]),
    ]

    STUDY_LOCUS_SCHEMA = t.StructType(
        [
            t.StructField("studyLocusId", t.StringType(), False),
            t.StructField("variantId", t.StringType(), False),
            t.StructField("studyId", t.StringType(), False),
            t.StructField("finemappingMethod", t.StringType(), False),
            t.StructField("qualityControls", t.ArrayType(t.StringType()), False),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusRedundancyFlagging, spark: SparkSession) -> None:
        """Setup study locus for testing."""
        self.study_locus = StudyLocus(
            _df=spark.createDataFrame(
                self.STUDY_LOCUS_DATA, schema=self.STUDY_LOCUS_SCHEMA
            ),
            _schema=StudyLocus.get_schema(),
        )

    def test_qc_redundant_top_hits_from_PICS_returntype(
        self: TestStudyLocusRedundancyFlagging,
    ) -> None:
        """Test qc_redundant_top_hits_from_PICS."""
        assert isinstance(
            self.study_locus.qc_redundant_top_hits_from_PICS(), StudyLocus
        )

    def test_qc_redundant_top_hits_from_PICS_no_data_loss(
        self: TestStudyLocusRedundancyFlagging,
    ) -> None:
        """Testing if the redundancy flagging returns the same number of rows."""
        assert (
            self.study_locus.qc_redundant_top_hits_from_PICS().df.count()
            == self.study_locus.df.count()
        )

    def test_qc_redundant_top_hits_from_PICS_correctness(
        self: TestStudyLocusRedundancyFlagging,
    ) -> None:
        """Testing if the study validation flags the right number of studies."""
        assert (
            self.study_locus.qc_redundant_top_hits_from_PICS()
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"),
                    StudyLocusQualityCheck.REDUNDANT_PICS_TOP_HIT.value,
                )
            )
            .count()
        ) == 3


class TestStudyLocusSuSiERedundancyFlagging:
    """Collection of tests related to flagging redundant credible sets."""

    STUDY_LOCUS_DATA: Any = [
        # to be flagged due to v4
        (
            "1",
            "v1",
            "s1",
            "X",
            "pics",
            1,
            3,
            [
                {"variantId": "X_1_A_A"},
                {"variantId": "X_2_A_A"},
                {"variantId": "X_3_A_A"},
            ],
            [],
        ),
        # to be flagged due to v4
        (
            "2",
            "v2",
            "s1",
            "X",
            "pics",
            4,
            5,
            [
                {"variantId": "X_4_A_A"},
                {"variantId": "X_5_A_A"},
            ],
            [],
        ),
        # NOT to be flagged (outside regions)
        (
            "3",
            "v3",
            "s1",
            "X",
            "pics",
            6,
            7,
            [
                {"variantId": "X_6_A_A"},
                {"variantId": "X_7_A_A"},
            ],
            [],
        ),
        # NOT to be flagged (SuSie-Inf credible set)
        (
            "4",
            "v4",
            "s1",
            "X",
            "SuSiE-inf",
            3,
            5,
            [{"variantId": "X_3_A_A"}, {"variantId": "X_5_A_A"}],
            [],
        ),
        # To be flagged (Unresolved LD flag on it)
        (
            "5",
            "v5",
            "s1",
            "X",
            "pics",
            5,
            5,
            [
                {"variantId": "X_5_A_A"},
            ],
            [StudyLocusQualityCheck.UNRESOLVED_LD.value],
        ),
        # NOT to be flagged (different study)
        (
            "6",
            "v6",
            "s2",
            "X",
            "pics",
            3,
            5,
            [
                {"variantId": "X_3_A_A"},
                {"variantId": "X_5_A_A"},
            ],
            [],
        ),
    ]

    STUDY_LOCUS_SCHEMA = t.StructType(
        [
            t.StructField("studyLocusId", t.StringType(), False),
            t.StructField("variantId", t.StringType(), False),
            t.StructField("studyId", t.StringType(), False),
            t.StructField("chromosome", t.StringType(), False),
            t.StructField("finemappingMethod", t.StringType(), False),
            t.StructField("locusStart", t.IntegerType(), False),
            t.StructField("locusEnd", t.IntegerType(), False),
            StructField(
                "locus",
                ArrayType(
                    StructType(
                        [
                            StructField("variantId", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            t.StructField("qualityControls", t.ArrayType(t.StringType()), False),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(
        self: TestStudyLocusSuSiERedundancyFlagging, spark: SparkSession
    ) -> None:
        """Setup study locus for testing."""
        self.study_locus = StudyLocus(
            _df=spark.createDataFrame(
                self.STUDY_LOCUS_DATA, schema=self.STUDY_LOCUS_SCHEMA
            ),
            _schema=StudyLocus.get_schema(),
        )

    def test_qc_qc_explained_by_SuSiE_returntype(
        self: TestStudyLocusSuSiERedundancyFlagging,
    ) -> None:
        """Test qc_explained_by_SuSiE."""
        assert isinstance(self.study_locus.qc_explained_by_SuSiE(), StudyLocus)

    def test_qc_explained_by_SuSiE_no_data_loss(
        self: TestStudyLocusSuSiERedundancyFlagging,
    ) -> None:
        """Test qc_explained_by_SuSiE no data loss."""
        assert (
            self.study_locus.qc_explained_by_SuSiE().df.count()
            == self.study_locus.df.count()
        )

    def test_qc_explained_by_SuSiE_correctness(
        self: TestStudyLocusSuSiERedundancyFlagging,
    ) -> None:
        """Testing if the study validation flags the right number of studies."""
        assert (
            self.study_locus.qc_explained_by_SuSiE()
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"),
                    StudyLocusQualityCheck.EXPLAINED_BY_SUSIE.value,
                )
            )
            .count()
        ) == 3


def test_qc_valid_chromosomes(
    spark: SparkSession,
) -> None:
    """Testing if chredible sets with invalid chromosomes are properly flagged."""
    df = spark.createDataFrame(
        [
            # Chromosome is fine:
            ("1", "v1", "s1", "X", []),
            ("2", "v2", "s1", "1", []),
            # Should be flagged:
            ("3", "v3", "s1", "11325", []),
            ("4", "v4", "s1", "CICAFUL", []),
        ],
        schema=t.StructType(
            [
                t.StructField("studyLocusId", t.StringType(), False),
                t.StructField("variantId", t.StringType(), False),
                t.StructField("studyId", t.StringType(), False),
                t.StructField("chromosome", t.StringType(), False),
                t.StructField("qualityControls", t.ArrayType(t.StringType()), False),
            ]
        ),
    )

    sl = StudyLocus(_df=df, _schema=StudyLocus.get_schema()).validate_chromosome_label()

    # Assert return type:
    assert isinstance(sl, StudyLocus)

    # Assert flagging correctness:
    for row in sl.df.collect():
        if row["chromosome"] in ["1", "X"]:
            assert not row["qualityControls"]
        else:
            assert (
                StudyLocusQualityCheck.INVALID_CHROMOSOME.value
                in row["qualityControls"]
            )
