"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
from sklearn.ensemble import GradientBoostingClassifier
from wandb import login as wandb_login

from gentropy.common.session import Session
from gentropy.common.utils import access_gcp_secret
from gentropy.config import LocusToGeneConfig
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.v2g import V2G
from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.method.l2g.trainer import LocusToGeneTrainer


class LocusToGeneStep:
    """Locus to gene step."""

    def __init__(
        self,
        session: Session,
        run_mode: str,
        predictions_path: str,
        credible_set_path: str,
        variant_gene_path: str,
        colocalisation_path: str,
        study_index_path: str,
        gold_standard_curation_path: str,
        gene_interactions_path: str,
        features_list: list[str],
        hyperparameters: dict[str, Any],
        download_from_hub: bool,
        model_path: str | None,
        feature_matrix_path: str | None = None,
        wandb_run_name: str | None = None,
        hf_hub_repo_id: str | None = LocusToGeneConfig().hf_hub_repo_id,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            run_mode (str): Run mode, either 'train' or 'predict'
            predictions_path (str): Path to save the predictions
            credible_set_path (str): Path to the credible set dataset
            variant_gene_path (str): Path to the variant to gene dataset
            colocalisation_path (str): Path to the colocalisation dataset
            study_index_path (str): Path to the study index dataset
            gold_standard_curation_path (str): Path to the gold standard curation dataset
            gene_interactions_path (str): Path to the gene interactions dataset
            features_list (list[str]): List of features to use for the model
            hyperparameters (dict[str, Any]): Hyperparameters for the model
            download_from_hub (bool): Whether to download the model from the Hugging Face Hub
            model_path (str | None): Path to the fitted model
            feature_matrix_path (str | None): Path to save the feature matrix. Defaults to None.
            wandb_run_name (str | None): Name of the wandb run. Defaults to None.
            hf_hub_repo_id (str | None): Hugging Face Hub repo id. Defaults to the one set in the step configuration.

        Raises:
            ValueError: If run_mode is not 'train' or 'predict'
        """
        if run_mode not in ["train", "predict"]:
            raise ValueError(
                f"run_mode must be one of 'train' or 'predict', got {run_mode}"
            )

        self.session = session
        self.run_mode = run_mode
        self.model_path = model_path
        self.predictions_path = predictions_path
        self.credible_set_path = credible_set_path
        self.variant_gene_path = variant_gene_path
        self.colocalisation_path = colocalisation_path
        self.study_index_path = study_index_path
        self.gold_standard_curation_path = gold_standard_curation_path
        self.gene_interactions_path = gene_interactions_path
        self.features_list = list(features_list)
        self.hyperparameters = dict(hyperparameters)
        self.feature_matrix_path = feature_matrix_path
        self.wandb_run_name = wandb_run_name
        self.hf_hub_repo_id = hf_hub_repo_id
        self.download_from_hub = download_from_hub

        # Load common inputs
        self.credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        self.studies = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )
        self.v2g = V2G.from_parquet(session, variant_gene_path)
        self.coloc = Colocalisation.from_parquet(
            session, colocalisation_path, recursiveFileLookup=True
        )

        if run_mode == "predict":
            self.run_predict()
        elif run_mode == "train":
            self.run_train()

    def run_predict(self) -> None:
        """Run the prediction step.

        Raises:
            ValueError: If predictions_path is not set.
        """
        if not self.predictions_path:
            raise ValueError("predictions_path must be set for predict mode.")
        predictions, feature_matrix = L2GPrediction.from_credible_set(
            self.features_list,
            self.credible_set,
            self.studies,
            self.v2g,
            self.coloc,
            self.session,
            model_path=self.model_path,
            hf_token=access_gcp_secret("hfhub-key", "open-targets-genetics-dev"),
            download_from_hub=self.download_from_hub,
        )
        if self.feature_matrix_path:
            feature_matrix.df.write.mode(self.session.write_mode).parquet(
                self.feature_matrix_path
            )
        predictions.df.write.mode(self.session.write_mode).parquet(
            self.predictions_path
        )
        self.session.logger.info(self.predictions_path)

    def run_train(self) -> None:
        """Run the training step.

        Raises:
            ValueError: If gold_standard_curation_path, gene_interactions_path, or wandb_run_name are not set.
        """
        if not (
            self.gold_standard_curation_path
            and self.gene_interactions_path
            and self.wandb_run_name
            and self.model_path
        ):
            raise ValueError(
                "gold_standard_curation_path, gene_interactions_path, and wandb_run_name, and a path to save the model must be set for train mode."
            )

        wandb_key = access_gcp_secret("wandb-key", "open-targets-genetics-dev")
        # Process gold standard and L2G features
        data = self._generate_feature_matrix().persist()

        # Instantiate classifier and train model
        l2g_model = LocusToGeneModel(
            model=GradientBoostingClassifier(random_state=42),
            hyperparameters=self.hyperparameters,
        )
        wandb_login(key=wandb_key)
        trained_model = LocusToGeneTrainer(model=l2g_model, feature_matrix=data).train(
            self.wandb_run_name
        )
        if trained_model.training_data and trained_model.model:
            trained_model.save(self.model_path)
            if self.hf_hub_repo_id:
                hf_hub_token = access_gcp_secret(
                    "hfhub-key", "open-targets-genetics-dev"
                )
                trained_model.export_to_hugging_face_hub(
                    # we upload the model in the filesystem
                    self.model_path.split("/")[-1],
                    hf_hub_token,
                    data=trained_model.training_data.df.drop(
                        "goldStandardSet", "geneId"
                    ).toPandas(),
                    repo_id=self.hf_hub_repo_id,
                    commit_message="chore: update model",
                )

    def _generate_feature_matrix(self) -> L2GFeatureMatrix:
        """Generate the feature matrix for training.

        Returns:
            L2GFeatureMatrix: Feature matrix with gold standards annotated with features.
        """
        gs_curation = self.session.spark.read.json(self.gold_standard_curation_path)
        interactions = self.session.spark.read.parquet(self.gene_interactions_path)
        study_locus_overlap = StudyLocus(
            _df=self.credible_set.df.join(
                f.broadcast(
                    gs_curation.select(
                        StudyLocus.assign_study_locus_id(
                            f.col("association_info.otg_id"),  # studyId
                            f.concat_ws(  # variantId
                                "_",
                                f.col("sentinel_variant.locus_GRCh38.chromosome"),
                                f.col("sentinel_variant.locus_GRCh38.position"),
                                f.col("sentinel_variant.alleles.reference"),
                                f.col("sentinel_variant.alleles.alternative"),
                            ),
                        ).alias("studyLocusId"),
                    )
                ),
                "studyLocusId",
                "inner",
            ),
            _schema=StudyLocus.get_schema(),
        ).find_overlaps(self.studies)

        gold_standards = L2GGoldStandard.from_otg_curation(
            gold_standard_curation=gs_curation,
            v2g=self.v2g,
            study_locus_overlap=study_locus_overlap,
            interactions=interactions,
        )

        fm = L2GFeatureMatrix.generate_features(
            features_list=self.features_list,
            credible_set=self.credible_set,
            study_index=self.studies,
            variant_gene=self.v2g,
            colocalisation=self.coloc,
        )

        return (
            L2GFeatureMatrix(
                _df=fm.df.join(
                    f.broadcast(
                        gold_standards.df.drop("variantId", "studyId", "sources")
                    ),
                    on=["studyLocusId", "geneId"],
                    how="inner",
                ),
                _schema=L2GFeatureMatrix.get_schema(),
            )
            .fill_na()
            .select_features(self.features_list)
        )
