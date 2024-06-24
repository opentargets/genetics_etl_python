"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Any

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from wandb.data_types import Table
from wandb.sdk.wandb_init import init as wandb_init
from wandb.sdk.wandb_sweep import sweep as wandb_sweep
from wandb.sklearn import plot_classifier
from wandb.wandb_agent import agent as wandb_agent

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.model import LocusToGeneModel


@dataclass
class LocusToGeneTrainer:
    """Modelling of what is the most likely causal gene associated with a given locus."""

    model: LocusToGeneModel
    feature_matrix: L2GFeatureMatrix

    # Initialise vars
    features_list: list[str] | None = None
    target_labels: list[str] | None = None
    x_train: pd.DataFrame | None = None
    y_train: pd.Series | None = None
    x_test: pd.DataFrame | None = None
    y_test: pd.Series | None = None
    wandb_l2g_project_name: str = "gentropy-locus-to-gene"

    def fit(
        self: LocusToGeneTrainer,
    ) -> LocusToGeneModel:
        """Fit the pipeline to the feature matrix dataframe.

        Returns:
            LocusToGeneModel: Fitted model

        Raises:
            ValueError: Train data not set, nothing to fit.
        """
        if self.x_train is not None and self.y_train is not None:
            assert (
                not self.x_train.empty and not self.y_train.empty
            ), "Train data not set, nothing to fit."
            fitted_model = self.model.model.fit(X=self.x_train.values, y=self.y_train)
            self.model = LocusToGeneModel(
                model=fitted_model,
                hyperparameters=fitted_model.get_params(),
                training_data=self.feature_matrix,
            )
            return self.model
        raise ValueError("Train data not set, nothing to fit.")

    def log_to_wandb(
        self: LocusToGeneTrainer,
        wandb_run_name: str,
    ) -> None:
        """Log evaluation results and feature importance to W&B to compare between different L2G runs.

        Dashboard is available at https://wandb.ai/open-targets/gentropy-locus-to-gene?nw=nwuseropentargets
        Credentials to access W&B are available at the OT central login sheet.

        Args:
            wandb_run_name (str): Name of the W&B run
        """
        if (
            self.x_train is not None
            and self.x_test is not None
            and self.y_train is not None
            and self.y_test is not None
        ):
            assert (
                not self.x_train.empty and not self.y_train.empty
            ), "Train data not set, nothing to evaluate."
            fitted_classifier = self.model.model
            y_predicted = fitted_classifier.predict(self.x_test.values)
            y_probas = fitted_classifier.predict_proba(self.x_test.values)
            run = wandb_init(
                project=self.wandb_l2g_project_name,
                name=wandb_run_name,
                config=fitted_classifier.get_params(),
            )
            # Track classification plots
            plot_classifier(
                self.model.model,
                self.x_train.values,
                self.x_test.values,
                self.y_train,
                self.y_test,
                y_predicted,
                y_probas,
                labels=list(self.model.label_encoder.values()),
                model_name="L2G-classifier",
                feature_names=self.features_list,
                is_binary=True,
            )
            # Track evaluation metrics
            run.log(
                {
                    "areaUnderROC": roc_auc_score(
                        self.y_test, y_probas[:, 1], average="weighted"
                    )
                }
            )
            run.log({"accuracy": accuracy_score(self.y_test, y_predicted)})
            run.log(
                {
                    "weightedPrecision": precision_score(
                        self.y_test, y_predicted, average="weighted"
                    )
                }
            )
            run.log(
                {
                    "weightedRecall": recall_score(
                        self.y_test, y_predicted, average="weighted"
                    )
                }
            )
            run.log({"f1": f1_score(self.y_test, y_predicted, average="weighted")})
            # Track gold standards and their features
            run.log(
                {"featureMatrix": Table(dataframe=self.feature_matrix.df.toPandas())}
            )
            # Log feature missingness
            run.log(
                {
                    "missingnessRates": self.feature_matrix.calculate_feature_missingness_rate()
                }
            )

    def train(
        self: LocusToGeneTrainer,
        wandb_run_name: str,
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        Args:
            wandb_run_name (str): Name of the W&B run. Unless this is provided, the model will not be logged to W&B.

        Returns:
            LocusToGeneModel: Fitted model
        """
        data_df = self.feature_matrix.df.drop("geneId").toPandas()

        # Encode labels in `goldStandardSet` to a numeric value
        data_df["goldStandardSet"] = data_df["goldStandardSet"].map(
            self.model.label_encoder
        )

        # Convert all columns to numeric and split
        data_df = data_df.apply(pd.to_numeric)
        self.feature_cols = [
            col
            for col in data_df.columns
            if col not in ["studyLocusId", "goldStandardSet"]
        ]
        label_col = "goldStandardSet"
        X = data_df[self.feature_cols].copy()
        y = data_df[label_col].copy()
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Train
        model = self.fit()

        # Evaluate
        self.log_to_wandb(
            wandb_run_name=wandb_run_name,
        )

        return model

    def hyperparameter_tuning(
        self: LocusToGeneTrainer, wandb_run_name: str, parameter_grid: dict[str, Any]
    ) -> None:
        """Perform hyperparameter tuning on the model with W&B Sweeps. Metrics for every combination of hyperparameters will be logged to W&B for comparison.

        Args:
            wandb_run_name (str): Name of the W&B run
            parameter_grid (dict[str, Any]): Dictionary containing the hyperparameters to sweep over. The keys are the hyperparameter names, and the values are dictionaries containing the values to sweep over.
        """
        sweep_config = {
            "method": "grid",
            "metric": {"name": "roc", "goal": "maximize"},
            "parameters": parameter_grid,
        }
        sweep_id = wandb_sweep(sweep_config, project=self.wandb_l2g_project_name)

        wandb_agent(sweep_id, partial(self.train, wandb_run_name=wandb_run_name))
