"""Test study locus dataset."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
import pytest
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.dataset.summary_statistics import SummaryStatistics
from pyspark.sql import Column, Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.mark.parametrize(
    "has_overlap, expected",
    [
        # Overlap exists
        (
            True,
            [
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
                    "chromosome": "1",
                    "tagVariantId": "commonTag",
                    "statistics": {
                        "left_posteriorProbability": 0.9,
                        "right_posteriorProbability": 0.6,
                    },
                },
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
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
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead2",
                        "studyId": "study2",
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
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",
                        "locus": [
                            {"variantId": "var1", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead2",
                        "studyId": "study2",
                        "locus": None,
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )

    studies = StudyIndex(
        _df=spark.createDataFrame(
            [
                {
                    "studyId": "study1",
                    "studyType": "gwas",
                    "traitFromSource": "trait1",
                    "projectId": "project1",
                },
                {
                    "studyId": "study2",
                    "studyType": "eqtl",
                    "traitFromSource": "trait2",
                    "projectId": "project2",
                },
            ]
        ),
        _schema=StudyIndex.get_schema(),
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
        credset.find_overlaps(studies).df.select(*cols_to_compare).collect()
        == expected_overlaps_df.select(*cols_to_compare).collect()
    ), "Overlaps differ from expected."


def test_find_overlaps(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test study locus overlaps."""
    assert isinstance(
        mock_study_locus.find_overlaps(mock_study_index), StudyLocusOverlap
    )


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
                    "studyLocusId": 1,
                    "variantId": "lead1",
                    "studyId": "study1",
                },
                {
                    # from eqtl
                    "studyLocusId": 2,
                    "variantId": "lead2",
                    "studyId": "study2",
                },
            ],
            StudyLocus.get_schema(),
        ),
        _schema=StudyLocus.get_schema(),
    )
    studies = StudyIndex(
        _df=spark.createDataFrame(
            [
                {
                    "studyId": "study1",
                    "studyType": "gwas",
                    "traitFromSource": "trait1",
                    "projectId": "project1",
                },
                {
                    "studyId": "study2",
                    "studyType": "eqtl",
                    "traitFromSource": "trait2",
                    "projectId": "project2",
                },
            ]
        ),
        _schema=StudyIndex.get_schema(),
    )

    observed = sl.filter_by_study_type(study_type, studies)
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


def test_assign_study_locus_id__null_variant_id(spark: SparkSession) -> None:
    """Test assign study locus id when variant id is null for the same study."""
    df = spark.createDataFrame(
        [("GCST000001", None), ("GCST000001", None)],
        schema="studyId: string, variantId: string",
    ).withColumn(
        "studyLocusId",
        StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
    )
    assert (
        df.select("studyLocusId").distinct().count() == 2
    ), "studyLocusId is not unique when variantId is null"


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # Locus is not null, should return union between variants in locus and lead variant
            [
                (
                    1,
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
                (1, "traitA", "22_varA", None),
            ],
            [
                (
                    "22_varA",
                    "22",
                ),
            ],
        ),
    ],
)
def test_unique_variants_in_locus(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test unique variants in locus."""
    # assert isinstance(mock_study_locus.test_unique_variants_in_locus(), DataFrame)
    schema = StructType(
        [
            StructField("studyLocusId", LongType(), True),
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


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # Simple case
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [{"variantId": "tagVariantA", "posteriorProbability": 1.0}],
                ),
            ],
            [
                # Expected
                (
                    1,
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
                    1,
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
                    1,
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
                    1,
                    "traitA",
                    "leadB",
                    None,
                ),
            ],
            [
                # Expected
                (
                    1,
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
                    1,
                    "traitA",
                    "leadB",
                    [],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                )
            ],
        ),
    ],
)
def test_annotate_credible_sets(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test annotate_credible_sets."""
    schema = StructType(
        [
            StructField("studyLocusId", LongType(), True),
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
