"""Step to generate variant annotation dataset."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import functions as f

from gentropy.common.Liftover import LiftOverSpark
from gentropy.common.session import Session
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.v2g import V2G
from gentropy.dataset.variant_index import VariantIndex


class V2GStep:
    """Variant-to-gene (V2G) step.

    This step aims to generate a dataset that contains multiple pieces of evidence supporting the functional association of specific variants with genes. Some of the evidence types include:

    1. Chromatin interaction experiments, e.g. Promoter Capture Hi-C (PCHi-C).
    2. In silico functional predictions, e.g. Variant Effect Predictor (VEP) from Ensembl.
    3. Distance between the variant and each gene's canonical transcription start site (TSS).

    Attributes:
        session (Session): Session object.
        variant_index_path (str): Input variant index path.
        gene_index_path (str): Input gene index path.
        vep_consequences_path (str): Input VEP consequences path.
        liftover_chain_file_path (str): Path to GRCh37 to GRCh38 chain file.
        liftover_max_length_difference: Maximum length difference for liftover.
        max_distance (int): Maximum distance to consider.
        approved_biotypes (list[str]): List of approved biotypes.
        intervals (dict): Dictionary of interval sources.
        v2g_path (str): Output V2G path.
    """

    def __init__(
        self,
        session: Session,
        variant_index_path: str,
        gene_index_path: str,
        vep_consequences_path: str,
        liftover_chain_file_path: str,
        approved_biotypes: list[str],
        interval_sources: dict[str, str],
        v2g_path: str,
        max_distance: int = 500_000,
        liftover_max_length_difference: int = 100,
    ) -> None:
        """Run Variant-to-gene (V2G) step.

        Args:
            session (Session): Session object.
            variant_index_path (str): Input variant index path.
            gene_index_path (str): Input gene index path.
            vep_consequences_path (str): Input VEP consequences path.
            liftover_chain_file_path (str): Path to GRCh37 to GRCh38 chain file.
            approved_biotypes (list[str]): List of approved biotypes.
            interval_sources (dict[str, str]): Dictionary of interval sources.
            v2g_path (str): Output V2G path.
            max_distance (int): Maximum distance to consider.
            liftover_max_length_difference (int): Maximum length difference for liftover.
        """
        # Read
        gene_index = GeneIndex.from_parquet(session, gene_index_path)
        vi = VariantIndex.from_parquet(session, variant_index_path).persist()
        # Reading VEP consequence to score table and cast the score to the right type:
        vep_consequences = session.spark.read.csv(
            vep_consequences_path, sep="\t", header=True
        ).withColumn("score", f.col("score").cast("double"))

        # Transform
        lift = LiftOverSpark(
            # lift over variants to hg38
            liftover_chain_file_path,
            liftover_max_length_difference,
        )
        gene_index_filtered = gene_index.filter_by_biotypes(
            # Filter gene index by approved biotypes to define V2G gene universe
            list(approved_biotypes)
        )

        intervals = Intervals(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                # create interval instances by parsing each source
                [
                    Intervals.from_source(
                        session.spark, source_name, source_path, gene_index, lift
                    ).df
                    for source_name, source_path in interval_sources.items()
                ],
            ),
            _schema=Intervals.get_schema(),
        )
        v2g_datasets = [
            vi.get_distance_to_tss(gene_index_filtered, max_distance),
            vi.get_most_severe_transcript_consequence(
                vep_consequences, gene_index_filtered
            ),
            vi.get_plof_v2g(gene_index_filtered),
            intervals.v2g(vi),
        ]
        v2g = V2G(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [dataset.df for dataset in v2g_datasets],
            ).repartition("chromosome"),
            _schema=V2G.get_schema(),
        )

        # Load
        (
            v2g.df.write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(v2g_path)
        )
