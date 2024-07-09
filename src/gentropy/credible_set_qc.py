"""Step to run credible set quality control on finemapping output StudyLoci."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.susie_inf import SUSIE_inf


class CredibleSetQCStep:
    """Credible set quality control step for fine mapped StudyLoci."""

    def __init__(
        self,
        session: Session,
        credible_sets_path: str,
        study_index_path: str,
        ld_index_path: str,
        output_path: str,
        p_value_threshold: float = 1e-5,
        purity_min_r2: float = 0.01,
        ld_min_r2: float = 0.8,
    ) -> None:
        """Run credible set quality control step.

        Args:
            session (Session): Session object.
            credible_sets_path (str): Path to credible sets file.
            study_index_path (str): Path to study index file.
            ld_index_path (str): Path to LD index file.
            output_path (str): Path to write the output file.
            p_value_threshold (float): P-value threshold for credible set quality control. Default is 1e-5.
            purity_min_r2 (float): Minimum R2 for purity estimation. Default is 0.01.
            ld_min_r2 (float): Minimum R2 for LD estimation. Default is 0.8.
        """
        cred_sets = StudyLocus.from_parquet(session, credible_sets_path)
        study_index = StudyIndex.from_parquet(session, study_index_path)
        ld_index = LDIndex.from_parquet(session, ld_index_path)

        cred_sets_clean = SUSIE_inf.credible_set_qc(
            cred_sets,
            study_index,
            ld_index,
            p_value_threshold,
            purity_min_r2,
            ld_min_r2,
        )

        cred_sets_clean.df.write.mode(session.write_mode).parquet(output_path)
