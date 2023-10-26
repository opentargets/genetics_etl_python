"""Step to run UKBiobank study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.ukbiobank.study_index import UKBiobankStudyIndex


@dataclass
class UKBiobankStep:
    """UKBiobank study table ingestion step.

    Attributes:
        ukbiobank_manifest (str): UKBiobank manifest of studies.
        ukbiobank_study_index_out (str): Output path for the UKBiobank study index dataset.
    """

    session: Session = Session()

    ukbiobank_manifest: str = MISSING
    ukbiobank_study_index_out: str = MISSING

    def run(self: UKBiobankStep) -> None:
        """Run UKBiobank study table ingestion step."""
        # Read in the UKBiobank manifest tsv file.
        df = self.session.spark.read.csv(
            self.ukbiobank_manifest, sep="\t", header=True, inferSchema=True
        )

        # Parse the study index data.
        ukbiobank_study_index = UKBiobankStudyIndex.from_source(df)

        # Write the output.
        ukbiobank_study_index.df.write.mode(self.session.write_mode).parquet(
            self.ukbiobank_study_index_out
        )
