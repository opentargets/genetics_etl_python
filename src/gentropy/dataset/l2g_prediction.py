"""Dataset that contains the Locus to Gene predictions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f
from pyspark.ml.functions import vector_to_array

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.v2g import V2G
from gentropy.method.l2g.model import LocusToGeneModel

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class L2GPrediction(Dataset):
    """Dataset that contains the Locus to Gene predictions.

    It is the result of applying the L2G model on a feature matrix, which contains all
    the study/locus pairs and their functional annotations. The score column informs the
    confidence of the prediction that a gene is causal to an association.
    """

    @classmethod
    def get_schema(cls: type[L2GPrediction]) -> StructType:
        """Provides the schema for the L2GPrediction dataset.

        Returns:
            StructType: Schema for the L2GPrediction dataset
        """
        return parse_spark_schema("l2g_predictions.json")

    @classmethod
    def from_credible_set(
        cls: Type[L2GPrediction],
        model_path: str,
        features_list: list[str],
        credible_set: StudyLocus,
        study_index: StudyIndex,
        v2g: V2G,
        coloc: Colocalisation,
    ) -> tuple[L2GPrediction, L2GFeatureMatrix]:
        """Extract L2G predictions for a set of credible sets derived from GWAS.

        Args:
            model_path (str): Path to the fitted model
            features_list (list[str]): List of features to use for the model
            credible_set (StudyLocus): Credible set dataset
            study_index (StudyIndex): Study index dataset
            v2g (V2G): Variant to gene dataset
            coloc (Colocalisation): Colocalisation dataset

        Returns:
            tuple[L2GPrediction, L2GFeatureMatrix]: L2G dataset and feature matrix limited to GWAS study only.
        """
        fm = L2GFeatureMatrix.generate_features(
            features_list=features_list,
            credible_set=credible_set,
            study_index=study_index,
            variant_gene=v2g,
            colocalisation=coloc,
        ).fill_na()

        gwas_fm = L2GFeatureMatrix(
            _df=(
                fm.df.join(
                    credible_set.filter_by_study_type("gwas", study_index).df.select(
                        "studyLocusId"
                    ),
                    on="studyLocusId",
                )
            ),
            _schema=L2GFeatureMatrix.get_schema(),
        )
        return (
            L2GPrediction(
                # Load and apply fitted model
                _df=(
                    LocusToGeneModel.load_from_disk(
                        model_path,
                        features_list=features_list,
                    )
                    .predict(gwas_fm)
                    # the probability of the positive class is the second element inside the probability array
                    # - this is selected as the L2G probability
                    .select(
                        "studyLocusId",
                        "geneId",
                        vector_to_array(f.col("probability"))[1].alias("score"),
                    )
                ),
                _schema=cls.get_schema(),
            ),
            gwas_fm,
        )
