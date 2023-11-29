"""Step to run clump associations from summary statistics or study locus."""
from __future__ import annotations

from dataclasses import dataclass, field

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class ClumpStep:
    """Perform clumping of an association dataset to identify independent signals.

    Two types of clumping are supported and are applied based on the input dataset:
    - Clumping of summary statistics based on a window-based approach.
    - Clumping of study locus based on LD.

    Both approaches yield a StudyLocus dataset.

    Attributes:
        session (Session): Session object.
        input_path (str): Input path for the study locus or summary statistics files.
        study_index_path (str): Path to study index.
        ld_index_path (str): Path to LD index.
        locus_collect_distance (int | None): The distance to collect locus around semi-indices.
        clumped_study_locus_path (str): Output path for the clumped study locus dataset.
    """

    session: Session = MISSING
    input_path: str = MISSING
    clumped_study_locus_path: str = MISSING
    study_index_path: str | None = field(default=None)
    ld_index_path: str | None = field(default=None)

    locus_collect_distance: int | None = field(default=None)
    data: StudyLocus | SummaryStatistics = field(init=False)

    def __post_init__(self: ClumpStep) -> None:
        """Run the clumping step.

        Raises:
            ValueError: If study index and LD index paths are not provided for study locus.
        """
        input_cols = self.session.spark.read.parquet(self.input_path).columns
        if "studyLocusId" in input_cols:
            if self.study_index_path is None or self.ld_index_path is None:
                raise ValueError(
                    "Study index and LD index paths are required for clumping study locus."
                )
            self.data = StudyLocus.from_parquet(self.session, self.input_path)
            ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)
            study_index = StudyIndex.from_parquet(self.session, self.study_index_path)

            clumped_study_locus = self.data.annotate_ld(
                study_index=study_index, ld_index=ld_index
            ).clump()
        else:
            self.data = SummaryStatistics.from_parquet(self.session, self.input_path)
            clumped_study_locus = self.data.window_based_clumping(
                locus_collect_distance=self.locus_collect_distance
            )

        clumped_study_locus.df.write.mode(self.session.write_mode).parquet(
            self.clumped_study_locus_path
        )
