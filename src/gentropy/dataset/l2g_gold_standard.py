"""L2G gold standard dataset."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from loguru import logger
from py4j.protocol import Py4JJavaError
from pyspark.sql import Window

from gentropy.common.schemas import (
    SchemaValidationError,
    parse_spark_schema,
)
from gentropy.common.session import Session
from gentropy.common.spark_helpers import get_record_with_maximum_value
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
    from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
    from gentropy.dataset.variant_index import VariantIndex


@dataclass
class L2GGoldStandard(Dataset):
    """L2G gold standard dataset."""

    INTERACTION_THRESHOLD = 0.7
    GS_POSITIVE_LABEL = "positive"
    GS_NEGATIVE_LABEL = "negative"

    @classmethod
    def from_gold_standard(
        cls: type[L2GGoldStandard], session: Session, gold_standard_path: str
    ) -> L2GGoldStandard:
        """Prepare the gold standard for training from default dataset.

        This method is simillar to `Dataset.from_parquet` but allows for reading from either
        - json
        - parquet

        Args:
            session (Session): Gentropy session object.
            gold_standard_path (str): Path to the gold standard curation file, must be either JSON or parquet.

        Returns:
            L2GGoldStandard: training dataset.

        Raises:
            ValueError: When gold standard path, is not provided, or when
                parsing OTG gold standard but missing interactions and variant index paths.
            SchemaValidationError: When gold standard is not L2GGoldStandard.

        """
        loader = partial(session.load_data, path=gold_standard_path)
        try:
            gold_standard = loader(format="parquet")
        except Py4JJavaError:
            gold_standard = loader(format="json")

        try:
            return L2GGoldStandard(_df=gold_standard)
        except SchemaValidationError as e:
            match e.errors:
                # NOTE: Specific case when user provides the old Genetics Portal gold standard,
                # We are not parsing it since, this requires access to the intervals, variant index
                # and credible sets, the user gets redirected to build the L2GGoldStandard another method first.
                case {
                    "missing_mandatory_columns": [
                        "studyLocusId",
                        "variantId",
                        "studyId",
                        "geneId",
                        "goldStandardSet",
                    ],
                    "unexpected_columns": [
                        "association_info",
                        "gold_standard_info",
                        "metadata",
                        "sentinel_variant",
                        "trait_info",
                    ],
                }:
                    logger.error(
                        "Prepare gold standard dataset with `L2GGoldStandard.from_otg_curation` first."
                    )
                    raise e

                case _:
                    raise e

    @classmethod
    def from_otg_curation(
        cls: type[L2GGoldStandard],
        otg_curation: DataFrame,
        credible_sets: StudyLocus,
        variant_index: VariantIndex,
        interactions: DataFrame,
    ) -> L2GGoldStandard:
        """Initialise L2GGoldStandard from source dataset.

        Args:
            otg_curation (DataFrame): Gold standard defined by OpenTargetsL2GGoldStandard datasource.
            credible_sets (StudyLocus): Dataset to build the overlaps to the loci from OpenTargetsL2GGoldStandard.
            variant_index (VariantIndex): Dataset to bring distance between a variant and a gene's footprint.
            interactions (DataFrame): Gene-gene interactions dataset to remove negative cases where the gene interacts with a positive gene

        Returns:
            L2GGoldStandard: L2G Gold Standard dataset
        """
        from gentropy.datasource.open_targets.l2g_gold_standard import (
            OpenTargetsL2GGoldStandard,
        )

        interactions_df = cls.process_gene_interactions(interactions)
        # There are schema mismatches, this would mean that we have

        curation_overlaps = StudyLocus(
            _df=credible_sets.df.join(
                otg_curation.select(
                    f.concat_ws(
                        "_",
                        f.col("sentinel_variant.locus_GRCh38.chromosome"),
                        f.col("sentinel_variant.locus_GRCh38.position"),
                        f.col("sentinel_variant.alleles.reference"),
                        f.col("sentinel_variant.alleles.alternative"),
                    ).alias("variantId"),
                    f.col("association_info.otg_id").alias("studyId"),
                ),
                on=[
                    "studyId",
                    "variantId",
                ],
                how="inner",
            ),
            _schema=StudyLocus.get_schema(),
        ).find_overlaps()

        return (
            OpenTargetsL2GGoldStandard.as_l2g_gold_standard(otg_curation, variant_index)
            .filter_unique_associations(curation_overlaps)
            .remove_false_negatives(interactions_df)
        )

    @classmethod
    def get_schema(cls: type[L2GGoldStandard]) -> StructType:
        """Provides the schema for the L2GGoldStandard dataset.

        Returns:
            StructType: Spark schema for the L2GGoldStandard dataset
        """
        return parse_spark_schema("l2g_gold_standard.json")

    @classmethod
    def process_gene_interactions(
        cls: type[L2GGoldStandard], interactions: DataFrame
    ) -> DataFrame:
        """Extract top scoring gene-gene interaction from the interactions dataset of the Platform.

        Args:
            interactions (DataFrame): Gene-gene interactions dataset from the Open Targets Platform

        Returns:
            DataFrame: Top scoring gene-gene interaction per pair of genes

        Examples:
            >>> interactions = spark.createDataFrame([("gene1", "gene2", 0.8), ("gene1", "gene2", 0.5), ("gene2", "gene3", 0.7)], ["targetA", "targetB", "scoring"])
            >>> L2GGoldStandard.process_gene_interactions(interactions).show()
            +-------+-------+-----+
            |geneIdA|geneIdB|score|
            +-------+-------+-----+
            |  gene1|  gene2|  0.8|
            |  gene2|  gene3|  0.7|
            +-------+-------+-----+
            <BLANKLINE>
        """
        return get_record_with_maximum_value(
            interactions,
            ["targetA", "targetB"],
            "scoring",
        ).selectExpr(
            "targetA as geneIdA",
            "targetB as geneIdB",
            "scoring as score",
        )

    def build_feature_matrix(
        self: L2GGoldStandard,
        full_feature_matrix: L2GFeatureMatrix,
        credible_set: StudyLocus,
    ) -> L2GFeatureMatrix:
        """Return a feature matrix for study loci in the gold standard.

        Args:
            full_feature_matrix (L2GFeatureMatrix): Feature matrix for all study loci to join on
            credible_set (StudyLocus): Full credible sets to annotate the feature matrix with variant and study IDs and perform the join

        Returns:
            L2GFeatureMatrix: Feature matrix for study loci in the gold standard
        """
        from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix

        return L2GFeatureMatrix(
            _df=full_feature_matrix._df.join(
                credible_set.df.select("studyLocusId", "variantId", "studyId"),
                "studyLocusId",
                "left",
            )
            .join(
                f.broadcast(self.df.drop("studyLocusId", "sources")),
                on=["studyId", "variantId", "geneId"],
                how="inner",
            )
            .filter(f.col("isProteinCoding") == 1)
            .drop("studyId", "variantId")
            .distinct(),
            with_gold_standard=True,
        ).fill_na()

    def filter_unique_associations(
        self: L2GGoldStandard,
        study_locus_overlap: StudyLocusOverlap,
    ) -> L2GGoldStandard:
        """Refines the gold standard to filter out loci that are not independent.

        Rules:
        - If two loci point to the same gene, one positive and one negative, and have overlapping variants, we keep the positive one.
        - If two loci point to the same gene, both positive or negative, and have overlapping variants, we drop one.
        - If two loci point to different genes, and have overlapping variants, we keep both.

        Args:
            study_locus_overlap (StudyLocusOverlap): A dataset detailing variants that overlap between StudyLocus.

        Returns:
            L2GGoldStandard: L2GGoldStandard updated to exclude false negatives and redundant positives.
        """
        squared_overlaps = study_locus_overlap._convert_to_square_matrix()
        unique_associations = (
            self.df.alias("left")
            # identify all the study loci that point to the same gene
            .withColumn(
                "sl_same_gene",
                f.collect_set("studyLocusId").over(Window.partitionBy("geneId")),
            )
            # identify all the study loci that have an overlapping variant
            .join(
                squared_overlaps.df.alias("right"),
                (f.col("left.studyLocusId") == f.col("right.leftStudyLocusId"))
                & (f.col("left.variantId") == f.col("right.tagVariantId")),
                "left",
            )
            .withColumn(
                "overlaps",
                f.when(f.col("right.tagVariantId").isNotNull(), f.lit(True)).otherwise(
                    f.lit(False)
                ),
            )
            # drop redundant rows: where the variantid overlaps and the gene is "explained" by more than one study locus
            .filter(~((f.size("sl_same_gene") > 1) & (f.col("overlaps") == 1)))
            .select(*self.df.columns)
        )
        return L2GGoldStandard(_df=unique_associations, _schema=self.get_schema())

    def remove_false_negatives(
        self: L2GGoldStandard,
        interactions_df: DataFrame,
    ) -> L2GGoldStandard:
        """Refines the gold standard to remove negative gold standard instances where the gene interacts with a positive gene.

        Args:
            interactions_df (DataFrame): Top scoring gene-gene interaction per pair of genes

        Returns:
            L2GGoldStandard: A refined set of locus-to-gene associations with increased reliability, having excluded loci that were likely false negatives due to gene-gene interaction confounding.
        """
        squared_interactions = interactions_df.unionByName(
            interactions_df.selectExpr(
                "geneIdB as geneIdA", "geneIdA as geneIdB", "score"
            )
        ).filter(f.col("score") > self.INTERACTION_THRESHOLD)
        df = (
            self.df.alias("left")
            .join(
                # bring gene partners
                squared_interactions.alias("right"),
                f.col("left.geneId") == f.col("right.geneIdA"),
                "left",
            )
            .withColumnRenamed("geneIdB", "interactorGeneId")
            .join(
                # bring gold standard status for gene partners
                self.df.selectExpr(
                    "geneId as interactorGeneId",
                    "goldStandardSet as interactorGeneIdGoldStandardSet",
                ),
                "interactorGeneId",
                "left",
            )
            # remove self-interactions
            .filter(
                (f.col("geneId") != f.col("interactorGeneId"))
                | (f.col("interactorGeneId").isNull())
            )
            # remove false negatives
            .filter(
                # drop rows where the GS gene is negative but the interactor is a GS positive
                ~(f.col("goldStandardSet") == "negative")
                & (f.col("interactorGeneIdGoldStandardSet") == "positive")
                |
                # keep rows where the gene does not interact
                (f.col("interactorGeneId").isNull())
            )
            .select(*self.df.columns)
            .distinct()
        )
        return L2GGoldStandard(_df=df, _schema=self.get_schema())
