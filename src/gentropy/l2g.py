"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from wandb.sdk.wandb_login import login as wandb_login

from gentropy.common.session import Session
from gentropy.common.spark_helpers import calculate_harmonic_sum
from gentropy.common.utils import access_gcp_secret
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_features.namespace import L2GFeatureName
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader
from gentropy.method.l2g.model import L2GModelWrapper
from gentropy.method.l2g.trainer import L2GTrainer

if TYPE_CHECKING:
    from collections.abc import Sequence


class RunMode(StrEnum):
    """Class representing available run modes for Locus To Gene step."""

    TRAIN = "train"
    PREDICT = "predict"


class LocusToGeneFeatureMatrixStep:
    """Annotate credible set with functional genomics features."""

    def __init__(
        self,
        session: Session,
        *,
        features_list: list[str],
        credible_set_path: str,
        variant_index_path: str | None = None,
        colocalisation_path: str | None = None,
        study_index_path: str | None = None,
        target_index_path: str | None = None,
        feature_matrix_path: str,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            features_list (list[str]): List of features to use for the model
            credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
            variant_index_path (str | None): Path to the variant index dataset
            colocalisation_path (str | None): Path to the colocalisation dataset
            study_index_path (str | None): Path to the study index dataset
            target_index_path (str | None): Path to the target index dataset
            feature_matrix_path (str): Path to the L2G feature matrix output dataset
        """
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        studies = (
            StudyIndex.from_parquet(session, study_index_path, recursiveFileLookup=True)
            if study_index_path
            else None
        )
        variant_index = (
            VariantIndex.from_parquet(session, variant_index_path)
            if variant_index_path
            else None
        )
        coloc = (
            Colocalisation.from_parquet(
                session, colocalisation_path, recursiveFileLookup=True
            )
            if colocalisation_path
            else None
        )
        target_index = (
            TargetIndex.from_parquet(
                session, target_index_path, recursiveFileLookup=True
            )
            if target_index_path
            else None
        )
        features_input_loader = L2GFeatureInputLoader(
            variant_index=variant_index,
            colocalisation=coloc,
            study_index=studies,
            study_locus=credible_set,
            target_index=target_index,
        )

        fm = credible_set.filter(f.col("studyType") == "gwas").build_feature_matrix(
            features_list, features_input_loader
        )
        fm._df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(feature_matrix_path)


class LocusToGeneTrainingFeatureMatrixStep:
    """Prepare Training Feature Matrix from gold standard."""

    def __init__(
        self,
        session: Session,
        *,
        credible_set_path: str,
        feature_matrix_path: str,
        gold_standard_path: str,
        training_feature_matrix_path: str,
    ) -> None:
        """Initialize the step and create a Feature matrix based on Gold standard for the L2G training.

        Args:
            session (Session):
            credible_set_path (str):
            feature_matrix_path (str):
            gold_standard_curation_path (str):
            training_feature_matrix_path (str):

        """
        credible_set = StudyLocus.from_parquet(
            session,
            credible_set_path,
            recursiveFileLookup=True,
        )
        feature_matrix = L2GFeatureMatrix(_df=session.load_data(feature_matrix_path))
        gold_standard = L2GGoldStandard.from_gold_standard(session, gold_standard_path)
        training_fm = gold_standard.build_feature_matrix(feature_matrix, credible_set)
        training_fm._df.write.parquet(training_feature_matrix_path)


class LocusToGeneTraining:
    """Train Locus To Gene model."""

    def __init__(self, hyperparameters: dict, training_feature_matrix_glob: str):
        l2g_model = L2GModelWrapper(hyperparameters=hyperparameters)
        # read feature matrix with polars
        import polars as pl

        if not training_feature_matrix_glob.endswith("*.parquet"):
            training_feature_matrix_glob = (
                training_feature_matrix_glob.removesuffix("/") + "/*.parquet"
            )

        dataset = pl.read_parquet(training_feature_matrix_glob)

    # def __init__(
    #     self,
    #     session: Session,
    #     *,
    #     run_mode: RunMode,
    #     hyperparameters: dict[str, Any],
    #     download_from_hub: bool,
    #     cross_validate: bool,
    #     wandb_run_name: str,
    #     model_path: str | None = None,
    #     features_list: Sequence[L2GFeatureName] | None,
    #     gold_standard_path: str | None = None,
    #     variant_index_path: str | None = None,
    #     gene_interactions_path: str | None = None,
    #     predictions_path: str | None = None,
    #     l2g_threshold: float | None = None,
    #     hf_hub_repo_id: str | None = None,
    #     hf_model_commit_message: str | None = "chore: update model",
    # ) -> None:
    #     """Initialise the step and run the logic based on mode.

    #     Args:
    #         session (Session): Session object that contains the Spark session
    #         run_mode (str): Run mode, either 'train' or 'predict'
    #         hyperparameters (dict[str, Any]): Hyperparameters for the model
    #         download_from_hub (bool): Whether to download the model from Hugging Face Hub
    #         cross_validate (bool): Whether to run cross validation (5-fold by default) to train the model.
    #         wandb_run_name (str): Name of the run to track model training in Weights and Biases
    #         credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
    #         feature_matrix_path (str): Path to the L2G feature matrix input dataset
    #         model_path (str | None): Path to the model. It can be either in the filesystem or the name on the Hugging Face Hub (in the form of username/repo_name).
    #         features_list (list[str] | None): List of features to use to train the model
    #         gold_standard_curation_path (str | None): Path to the gold standard curation file
    #         variant_index_path (str | None): Path to the variant index
    #         gene_interactions_path (str | None): Path to the gene interactions dataset
    #         predictions_path (str | None): Path to the L2G predictions output dataset
    #         l2g_threshold (float | None): An optional threshold for the L2G score to filter predictions. A threshold of 0.05 is recommended.
    #         hf_hub_repo_id (str | None): Hugging Face Hub repository ID. If provided, the model will be uploaded to Hugging Face.
    #         hf_model_commit_message (str | None): Commit message when we upload the model to the Hugging Face Hub

    #     Raises:
    #         ValueError: If run_mode is not 'train' or 'predict'
    #     """
    # self.session = session
    # self.run_mode = run_mode
    # self.predictions_path = predictions_path
    # self.hyperparameters = dict(hyperparameters)
    # self.wandb_run_name = wandb_run_name
    # self.cross_validate = cross_validate
    # self.hf_hub_repo_id = hf_hub_repo_id
    # self.download_from_hub = download_from_hub
    # self.hf_model_commit_message = hf_model_commit_message
    # self.l2g_threshold = l2g_threshold or 0.0
    # self.gold_standard_curation_path = gold_standard_path
    # self.gene_interactions_path = gene_interactions_path
    # self.variant_index_path = variant_index_path
    # self.model_path = (
    #     hf_hub_repo_id
    #     if not model_path and download_from_hub and hf_hub_repo_id
    #     else model_path
    # )

    # Load common inputs

    def run_train(
        self,
    ) -> None:
        """Run the training step.

        Raises:
            ValueError: If features list is not provided for model training.
        """
        # Initialize access to weights and biases
        # wandb_key = access_gcp_secret("wandb-key", "open-targets-genetics-dev")
        # wandb_login(key=wandb_key)

        # Run the training

        # Export the model
        if trained_model.training_data and trained_model.model and self.model_path:
            trained_model.save(self.model_path)
            if self.hf_hub_repo_id and self.hf_model_commit_message:
                hf_hub_token = access_gcp_secret(
                    "hfhub-key", "open-targets-genetics-dev"
                )
                trained_model.export_to_hugging_face_hub(
                    # we upload the model saved in the filesystem
                    self.model_path.split("/")[-1],
                    hf_hub_token,
                    data=trained_model.training_data._df.toPandas(),
                    repo_id=self.hf_hub_repo_id,
                    commit_message=self.hf_model_commit_message,
                )

    # def run_predict(self) -> None:
    #     """Run the prediction step.

    #     Raises:
    #         ValueError: If predictions_path is not provided for prediction mode
    #     """
    #     if not self.predictions_path:
    #         raise ValueError("predictions_path must be provided for prediction mode")
    #     predictions = L2GPrediction.from_credible_set(
    #         self.session,
    #         self.credible_set,
    #         self.feature_matrix,
    #         model_path=self.model_path,
    #         features_list=self.features_list,
    #         hf_token=access_gcp_secret("hfhub-key", "open-targets-genetics-dev"),
    #         download_from_hub=self.download_from_hub,
    #     )
    #     predictions.filter(f.col("score") >= self.l2g_threshold).add_features(
    #         self.feature_matrix,
    #     ).explain().df.coalesce(self.session.output_partitions).write.mode(
    #         self.session.write_mode
    #     ).parquet(self.predictions_path)
    #     self.session.logger.info("L2G predictions saved successfully.")


class LocusToGeneEvidenceStep:
    """Locus to gene evidence step."""

    def __init__(
        self,
        session: Session,
        locus_to_gene_predictions_path: str,
        credible_set_path: str,
        study_index_path: str,
        evidence_output_path: str,
        locus_to_gene_threshold: float,
    ) -> None:
        """Initialise the step and generate disease/target evidence.

        Args:
            session (Session): Session object that contains the Spark session
            locus_to_gene_predictions_path (str): Path to the L2G predictions dataset
            credible_set_path (str): Path to the credible set dataset
            study_index_path (str): Path to the study index dataset
            evidence_output_path (str): Path to the L2G evidence output dataset. The output format is ndjson gzipped.
            locus_to_gene_threshold (float, optional): Threshold to consider a gene as a target. Defaults to 0.05.
        """
        # Reading the predictions
        locus_to_gene_prediction = L2GPrediction.from_parquet(
            session, locus_to_gene_predictions_path
        )
        # Reading the credible set
        credible_sets = StudyLocus.from_parquet(session, credible_set_path)

        # Reading the study index
        study_index = StudyIndex.from_parquet(session, study_index_path)

        # Generate evidence and save file:
        (
            locus_to_gene_prediction.to_disease_target_evidence(
                credible_sets, study_index, locus_to_gene_threshold
            )
            .coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .option("compression", "gzip")
            .json(evidence_output_path)
        )


class LocusToGeneAssociationsStep:
    """Locus to gene associations step."""

    def __init__(
        self,
        session: Session,
        evidence_input_path: str,
        disease_index_path: str,
        direct_associations_output_path: str,
        indirect_associations_output_path: str,
    ) -> None:
        """Create direct and indirect association datasets.

        Args:
            session (Session): Session object that contains the Spark session
            evidence_input_path (str): Path to the L2G evidence input dataset
            disease_index_path (str): Path to disease index file
            direct_associations_output_path (str): Path to the direct associations output dataset
            indirect_associations_output_path (str): Path to the indirect associations output dataset
        """
        # Read in the disease index
        disease_index = session.spark.read.parquet(disease_index_path).select(
            f.col("id").alias("diseaseId"),
            f.explode("ancestors").alias("ancestorDiseaseId"),
        )

        # Read in the L2G evidence
        disease_target_evidence = session.spark.read.json(evidence_input_path).select(
            f.col("targetFromSourceId").alias("targetId"),
            f.col("diseaseFromSourceMappedId").alias("diseaseId"),
            f.col("resourceScore"),
        )

        # Generate direct assocations and save file
        (
            disease_target_evidence.groupBy("targetId", "diseaseId")
            .agg(f.collect_set("resourceScore").alias("scores"))
            .select(
                "targetId",
                "diseaseId",
                calculate_harmonic_sum(f.col("scores")).alias("harmonicSum"),
            )
            .write.mode(session.write_mode)
            .parquet(direct_associations_output_path)
        )

        # Generate indirect assocations and save file
        (
            disease_target_evidence.join(disease_index, on="diseaseId", how="inner")
            .groupBy("targetId", "ancestorDiseaseId")
            .agg(f.collect_set("resourceScore").alias("scores"))
            .select(
                "targetId",
                "ancestorDiseaseId",
                calculate_harmonic_sum(f.col("scores")).alias("harmonicSum"),
            )
            .write.mode(session.write_mode)
            .parquet(indirect_associations_output_path)
        )
