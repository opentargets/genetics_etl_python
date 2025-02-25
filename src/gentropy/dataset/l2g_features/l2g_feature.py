"""L2G Feature Dataset with a collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class L2GFeature(Dataset, ABC):
    """Locus-to-gene feature dataset that serves as template to generate each of the features that inform about locus to gene assignments."""

    def __post_init__(
        self: L2GFeature,
        feature_dependency_type: Any = None,
        credible_set: StudyLocus | None = None,
    ) -> None:
        """Initializes a L2GFeature dataset. Any child class of L2GFeature must implement the `compute` method.

        Args:
            feature_dependency_type (Any): The dependency that the L2GFeature dataset depends on. Defaults to None.
            credible_set (StudyLocus | None): The credible set that the L2GFeature dataset is based on. Defaults to None.
        """
        super().__post_init__()
        self.feature_dependency_type = feature_dependency_type
        self.credible_set = credible_set

    @classmethod
    def get_schema(cls: type[L2GFeature]) -> StructType:
        """Provides the schema for the L2GFeature dataset.

        Returns:
            StructType: Schema for the L2GFeature dataset
        """
        return parse_spark_schema("l2g_feature.json")

    @classmethod
    @abstractmethod
    def compute(
        cls: type[L2GFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: Any,
    ) -> L2GFeature:
        """Computes the L2GFeature dataset.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (Any): The dependency that the L2GFeature class needs to compute the feature
        Returns:
            L2GFeature: a L2GFeature dataset

        Raises:
                NotImplementedError: This method must be implemented in the child classes
        """
        raise NotImplementedError("Must be implemented in the child classes")
