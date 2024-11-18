"""Step to generate colocalisation results."""

from __future__ import annotations

import logging
from enum import Enum
from functools import partial, reduce
from typing import Any, Type

from pyspark.sql.functions import col

from gentropy.common.session import Session
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation import Coloc, ColocalisationMethodInterface


class OverlapRunMode(Enum):
    """Modes to run the overlaps in colocalisation step."""

    ITERATIVE = "iterative"
    BATCH = "batch"


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    __coloc_methods__ = {
        method.METHOD_NAME.lower(): method
        for method in ColocalisationMethodInterface.__subclasses__()
    }

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        coloc_path: str,
        colocalisation_method: str,
        overlap_run_mode: OverlapRunMode,
        colocalisation_method_params: dict[str, Any] | None = None,
    ) -> None:
        """Run Colocalisation step.

        This step allows for running two colocalisation methods: ecaviar and coloc.

        Args:
            session (Session): Session object.
            credible_set_path (str): Input credible sets path.
            coloc_path (str): Output Colocalisation path.
            colocalisation_method (str): Colocalisation method.
            overlap_run_mode (OverlapRunMode): The way how to parallelize the tasks when finding overlaps.
            colocalisation_method_params (dict[str, Any] | None): Keyword arguments passed to the colocalise method of Colocalisation class. Defaults to None

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4. For coloc method only.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4. For coloc method only.
            priorc12 (float): Prior on variant being causal for both traits. Defaults to 1e-5. For coloc method only.
        """
        coloc_method = colocalisation_method.lower()
        coloc_class = self._get_colocalisation_class(coloc_method)
        credible_set = StudyLocus.from_parquet(
            self.session, credible_set_path, recusiveFileLookup=True
        )

        # Calculate overlaps
        match overlap_run_mode:
            case OverlapRunMode.ITERATIVE.value:
                overlaps = self.__run_overlaps_iteratively()
            case OverlapRunMode.BATCH.value:
                overlaps = self.__run_overlaps_in_batch(credible_set, coloc_method)

        coloc = coloc_class.colocalise
        if colocalisation_method_params:
            coloc = partial(coloc, **colocalisation_method_params)
        colocalisation_results = coloc(overlaps)
        # Load
        colocalisation_results.df.write.mode(session.write_mode).parquet(
            f"{coloc_path}/{colocalisation_method.lower()}"
        )

    def __run_overlaps_in_batch(
        self, credible_set: StudyLocus, coloc_method: str
    ) -> StudyLocusOverlap:
        """Run overlaps in batch mode."""
        if coloc_method == Coloc.METHOD_NAME.lower():
            credible_set = credible_set.filter(
                col("finemappingMethod").isin(
                    FinemappingMethod.SUSIE.value, FinemappingMethod.SUSIE_INF.value
                )
            )

        return credible_set.find_overlaps()

    def __run_overlaps_iteratively(
        self, credible_set: StudyLocus, coloc_method: str
    ) -> StudyLocusOverlap:
        """Run overlaps using iterative mode."""
        susie_methods = [
            FinemappingMethod.SUSIE.value,
            FinemappingMethod.SUSIE_INF.value,
        ]
        iterative_map = {
            "gwas_pics_vs_gwas_pics": {
                "left": [
                    col("studyType") == "gwas",
                    col("finemappingMethod") == FinemappingMethod.PICS.value,
                ],
                "right": [
                    col("studyType") == "gwas",
                    col("finemappingMethod") == FinemappingMethod.PICS.value,
                ],
                "methods": ["ECaviar"],
            },
            "gwas_pics_vs_gwas_susie": {
                "left": [
                    col("studyType") == "gwas",
                    col("finemappingMethod") == FinemappingMethod.PICS.value,
                ],
                "right": [
                    col("studyType") == "gwas",
                    col("finemappongMethod").isin(susie_methods),
                ],
                "methods": ["ECaviar"],
            },
            "gwas_susie_vs_gwas_susie": {
                "left": [
                    col("studyType") == "gwas",
                    col("finemappongMethod").isin(susie_methods),
                ],
                "right": [
                    col("studyType") == "gwas",
                    col("finemappongMethod").isin(susie_methods),
                ],
                "methods": ["ECaviar", "Coloc"],
            },
            "gwas_susie_vs_qtl_susie": {
                "left": [
                    col("studyType") == "gwas",
                    col("finemappingMethod").isin(susie_methods),
                ],
                "right": [
                    col("studyType") != "gwas",
                    col("finemappingMethod").isin(susie_methods),
                ],
                "methods": ["ECaviar", "Coloc"],
            },
            "gwas_pics_vs_qtl_susie": {
                "left": [
                    col("studyType") == "gwas",
                    col("finemappingMethod") == FinemappingMethod.PICS.value,
                ],
                "right": [
                    col("studyType") != "gwas",
                    col("finemappingMethod").isin(susie_methods),
                ],
                "methods": ["ECaviar"],
            },
        }

        results = []
        for name, combination in iterative_map.items():
            logging.info("Running overlaps on %s", name)
            if coloc_method in combination["methods"]:
                left = credible_set.filter(
                    reduce(lambda x, y: x & y, combination["left"])
                )
                right = credible_set.filter(
                    reduce(lambda x, y: x & y, combination["right"])
                )

        # Make a partial caller to ensure that colocalisation_method_params are added to the call only when dict is not empty

        return credible_set.find_overlaps()

    @classmethod
    def _get_colocalisation_class(
        cls, method: str
    ) -> Type[ColocalisationMethodInterface]:
        """Get colocalisation class.

        Args:
            method (str): Colocalisation method.

        Returns:
            Type[ColocalisationMethodInterface]: Class that implements the ColocalisationMethodInterface.

        Raises:
            ValueError: if method not available.

        Examples:
            >>> ColocalisationStep._get_colocalisation_class("ECaviar")
            <class 'gentropy.method.colocalisation.ECaviar'>
        """
        method = method.lower()
        if method not in cls.__coloc_methods__:
            raise ValueError(f"Colocalisation method {method} not available.")
        coloc_method = cls.__coloc_methods__[method]
        return coloc_method
