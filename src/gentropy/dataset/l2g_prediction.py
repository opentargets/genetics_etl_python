"""Dataset that contains the Locus to Gene predictions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.session import Session
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
        features_list: list[str],
        credible_set: StudyLocus,
        study_index: StudyIndex,
        v2g: V2G,
        coloc: Colocalisation,
        session: Session,
        model_path: str | None,
        hf_token: str | None = None,
        download_from_hub: bool = True,
    ) -> tuple[L2GPrediction, L2GFeatureMatrix]:
        """Extract L2G predictions for a set of credible sets derived from GWAS.

        Args:
            features_list (list[str]): List of features to use for the model
            credible_set (StudyLocus): Credible set dataset
            study_index (StudyIndex): Study index dataset
            v2g (V2G): Variant to gene dataset
            coloc (Colocalisation): Colocalisation dataset
            session (Session): Session object that contains the Spark session
            model_path (str | None): Path to the model file. It can be either in the filesystem or the name on the Hugging Face Hub (in the form of username/repo_name).
            hf_token (str | None): Hugging Face token to download the model from the Hub. Only required if the model is private.
            download_from_hub (bool): Whether to download the model from the Hugging Face Hub. Defaults to True.

        Returns:
            tuple[L2GPrediction, L2GFeatureMatrix]: L2G dataset and feature matrix limited to GWAS study only.
        """
        # Load the model
        if download_from_hub:
            # Model ID defaults to "opentargets/locus_to_gene" and it assumes the name of the classifier is "classifier.skops".
            model_id = model_path or "opentargets/locus_to_gene"
            l2g_model = LocusToGeneModel.load_from_hub(model_id, hf_token)
        elif model_path:
            l2g_model = LocusToGeneModel.load_from_disk(model_path)

        # Prepare data
        fm = L2GFeatureMatrix.generate_features(
            features_list=features_list,
            credible_set=credible_set,
            study_index=study_index,
            variant_gene=v2g,
            colocalisation=coloc,
        ).fill_na()

        gwas_fm = (
            L2GFeatureMatrix(
                _df=(
                    fm.df.join(
                        credible_set.filter_by_study_type(
                            "gwas", study_index
                        ).df.select("studyLocusId"),
                        on="studyLocusId",
                    )
                ),
                _schema=L2GFeatureMatrix.get_schema(),
                mode="predict",
            )
            .select_features(features_list)
            .persist()
        )
        return (
            l2g_model.predict(gwas_fm, session),
            gwas_fm,
        )
