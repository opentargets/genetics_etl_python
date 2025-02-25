"""Module defining classes for L2G data preprocessing before model fit."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix


@dataclass
class SplittedDataset:
    """Container for splitted dataset."""

    X: pl.DataFrame
    y: pl.DataFrame


@dataclass
class LabeledDataset:
    """Container for labeled dataset."""


class L2GDataPreprocessor:
    """Class to split and label Locus To Gene dataset before running model fit."""

    def __init__(self, dataset: pl.DataFrame):
        self.dataset = dataset

    def from_l2g_feature_matrix(fm: L2GFeatureMatrix) -> L2GDataPreprocessor:
        fm._df

    @classmethod
    def split() -> SplittedDataset:
        """Split the dataset."""

    @classmethod
    def label() -> LabeledDataset:
        """Label the dataset."""
