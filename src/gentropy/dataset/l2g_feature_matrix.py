"""Feature matrix of study locus pairs annotated with their functional genomics features."""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Type

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import convert_from_long_to_wide
from gentropy.dataset.dataset import Dataset
from gentropy.method.l2g.feature_factory import ColocalisationFactory, StudyLocusFactory

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.colocalisation import Colocalisation
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import StudyLocus
    from gentropy.dataset.v2g import V2G


@dataclass
class L2GFeatureMatrix(Dataset):
    """Dataset with features for Locus to Gene prediction.

    Attributes:
        features_list (list[str] | None): List of features to use. If None, all possible features are used.
    """

    features_list: list[str] | None = None

    def __post_init__(self: L2GFeatureMatrix) -> None:
        """Post-initialisation to set the features list. If not provided, all columns except the fixed ones are used."""
        fixed_cols = ["studyLocusId", "geneId", "goldStandardSet"]
        self.features_list = self.features_list or [
            col for col in self._df.columns if col not in fixed_cols
        ]
        self.validate_schema()

    @classmethod
    def generate_features(
        cls: Type[L2GFeatureMatrix],
        features_list: list[str],
        credible_set: StudyLocus,
        study_index: StudyIndex,
        variant_gene: V2G,
        colocalisation: Colocalisation,
    ) -> L2GFeatureMatrix:
        """Generate features from the gentropy datasets.

        Args:
            features_list (list[str]): List of features to generate
            credible_set (StudyLocus): Credible set dataset
            study_index (StudyIndex): Study index dataset
            variant_gene (V2G): Variant to gene dataset
            colocalisation (Colocalisation): Colocalisation dataset

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset

        Raises:
            ValueError: If the feature matrix is empty
        """
        if features_dfs := [
            # Extract features
            ColocalisationFactory._get_max_coloc_per_credible_set(
                colocalisation,
                credible_set,
                study_index,
            ).df,
            StudyLocusFactory._get_tss_distance_features(credible_set, variant_gene).df,
            StudyLocusFactory._get_vep_features(credible_set, variant_gene).df,
        ]:
            fm = reduce(
                lambda x, y: x.unionByName(y),
                features_dfs,
            )
        else:
            raise ValueError("No features found")

        # raise error if the feature matrix is empty
        return cls(
            _df=convert_from_long_to_wide(
                fm, ["studyLocusId", "geneId"], "featureName", "featureValue"
            ),
            _schema=cls.get_schema(),
            features_list=features_list,
        )

    @classmethod
    def get_schema(cls: type[L2GFeatureMatrix]) -> StructType:
        """Provides the schema for the L2gFeatureMatrix dataset.

        Returns:
            StructType: Schema for the L2gFeatureMatrix dataset
        """
        return parse_spark_schema("l2g_feature_matrix.json")

    def calculate_feature_missingness_rate(
        self: L2GFeatureMatrix,
    ) -> dict[str, float]:
        """Calculate the proportion of missing values in each feature.

        Returns:
            dict[str, float]: Dictionary of feature names and their missingness rate.

        Raises:
            ValueError: If no features are found.
        """
        total_count = self._df.count()
        if not self.features_list:
            raise ValueError("No features found")

        return {
            feature: (
                self._df.filter(
                    (self._df[feature].isNull()) | (self._df[feature] == 0)
                ).count()
                / total_count
            )
            for feature in self.features_list
        }

    def fill_na(
        self: L2GFeatureMatrix, value: float = 0.0, subset: list[str] | None = None
    ) -> L2GFeatureMatrix:
        """Fill missing values in a column with a given value.

        Args:
            value (float): Value to replace missing values with. Defaults to 0.0.
            subset (list[str] | None): Subset of columns to consider. Defaults to None.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        self.df = self._df.fillna(value, subset=subset)
        return self

    def select_features(
        self: L2GFeatureMatrix, features_list: list[str] | None
    ) -> L2GFeatureMatrix:
        """Select a subset of features from the feature matrix.

        Args:
            features_list (list[str] | None): List of features to select

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        features_list = features_list or self.features_list
        fixed_cols = ["studyLocusId", "geneId", "goldStandardSet"]
        self.df = self._df.select(fixed_cols + features_list)  # type: ignore
        return self

    def train_test_split(
        self: L2GFeatureMatrix, fraction: float
    ) -> tuple[L2GFeatureMatrix, L2GFeatureMatrix]:
        """Split the dataset into training and test sets.

        Args:
            fraction (float): Fraction of the dataset to use for training

        Returns:
            tuple[L2GFeatureMatrix, L2GFeatureMatrix]: Training and test datasets
        """
        train, test = self._df.randomSplit([fraction, 1 - fraction], seed=42)
        return (
            L2GFeatureMatrix(_df=train, _schema=L2GFeatureMatrix.get_schema()),
            L2GFeatureMatrix(_df=test, _schema=L2GFeatureMatrix.get_schema()),
        )
