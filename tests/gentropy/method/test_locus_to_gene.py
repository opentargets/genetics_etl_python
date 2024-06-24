"""Test locus-to-gene model training."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature import L2GFeature
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.l2g.feature_factory import ColocalisationFactory, StudyLocusFactory
from gentropy.method.l2g.model import LocusToGeneModel
from sklearn.ensemble import RandomForestClassifier

if TYPE_CHECKING:
    from gentropy.dataset.v2g import V2G
    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def model() -> LocusToGeneModel:
    """Creates an instance of the LocusToGene class."""
    return LocusToGeneModel(model=RandomForestClassifier())


class TestColocalisationFactory:
    """Test the ColocalisationFactory methods."""

    def test_get_max_coloc_per_credible_set(
        self: TestColocalisationFactory,
        mock_study_locus: StudyLocus,
        mock_study_index: StudyIndex,
        mock_colocalisation: Colocalisation,
    ) -> None:
        """Test the function that extracts the maximum log likelihood ratio for each pair of overlapping study-locus returns the right data type."""
        coloc_features = ColocalisationFactory._get_max_coloc_per_credible_set(
            mock_colocalisation,
            mock_study_locus,
            mock_study_index,
        )
        assert isinstance(
            coloc_features, L2GFeature
        ), "Unexpected type returned from _get_max_coloc_per_credible_set"

    def test_get_max_coloc_per_credible_set_semantic(
        self: TestColocalisationFactory,
        spark: SparkSession,
    ) -> None:
        """Test logic of the function that extracts the maximum log likelihood ratio for each pair of overlapping study-locus."""
        # Prepare mock datasets based on 2 associations
        credset = StudyLocus(
            _df=spark.createDataFrame(
                # 2 associations with a common variant in the locus
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",  # this is a GWAS
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead2",
                        "studyId": "study2",  # this is a eQTL study
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
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
                        "geneId": "gene1",
                        "traitFromSource": "trait2",
                        "projectId": "project2",
                    },
                ]
            ),
            _schema=StudyIndex.get_schema(),
        )
        coloc = Colocalisation(
            _df=spark.createDataFrame(
                [
                    {
                        "leftStudyLocusId": 1,
                        "rightStudyLocusId": 2,
                        "chromosome": "1",
                        "colocalisationMethod": "eCAVIAR",
                        "numberColocalisingVariants": 1,
                        "clpp": 0.81,  # 0.9*0.9
                        "log2h4h3": None,
                    }
                ],
                schema=Colocalisation.get_schema(),
            ),
            _schema=Colocalisation.get_schema(),
        )
        expected_coloc_features_df = spark.createDataFrame(
            [
                (1, "gene1", "eqtlColocClppMaximum", 0.81),
                (1, "gene1", "eqtlColocClppMaximumNeighborhood", -4.0),
            ],
            L2GFeature.get_schema(),
        )
        # Test
        coloc_features = ColocalisationFactory._get_max_coloc_per_credible_set(
            coloc,
            credset,
            studies,
        )
        assert coloc_features.df.collect() == expected_coloc_features_df.collect()


class TestStudyLocusFactory:
    """Test the StudyLocusFactory methods."""

    def test_get_tss_distance_features(
        self: TestStudyLocusFactory, mock_study_locus: StudyLocus, mock_v2g: V2G
    ) -> None:
        """Test the function that extracts the distance to the TSS."""
        tss_distance = StudyLocusFactory._get_tss_distance_features(
            mock_study_locus, mock_v2g
        )
        assert isinstance(
            tss_distance, L2GFeature
        ), "Unexpected model type returned from _get_tss_distance_features"

    def test_get_vep_features(
        self: TestStudyLocusFactory, mock_study_locus: StudyLocus, mock_v2g: V2G
    ) -> None:
        """Test the function that extracts the VEP features."""
        vep_features = StudyLocusFactory._get_vep_features(mock_study_locus, mock_v2g)
        assert isinstance(
            vep_features, L2GFeature
        ), "Unexpected model type returned from _get_vep_features"
