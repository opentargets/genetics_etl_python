"""Study locus dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.types import FloatType

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    calculate_neglog_pvalue,
    order_array_of_structs_by_field,
)
from gentropy.common.utils import get_logsum, parse_region
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.clump import LDclumping

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.ld_index import LDIndex
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.summary_statistics import SummaryStatistics


class StudyLocusQualityCheck(Enum):
    """Study-Locus quality control options listing concerns on the quality of the association.

    Attributes:
        SUBSIGNIFICANT_FLAG (str): p-value below significance threshold
        NO_GENOMIC_LOCATION_FLAG (str): Incomplete genomic mapping
        COMPOSITE_FLAG (str): Composite association due to variant x variant interactions
        INCONSISTENCY_FLAG (str): Inconsistencies in the reported variants
        NON_MAPPED_VARIANT_FLAG (str): Variant not mapped to GnomAd
        PALINDROMIC_ALLELE_FLAG (str): Alleles are palindromic - cannot harmonize
        AMBIGUOUS_STUDY (str): Association with ambiguous study
        UNRESOLVED_LD (str): Variant not found in LD reference
        LD_CLUMPED (str): Explained by a more significant variant in high LD (clumped)
        NO_POPULATION (str): Study does not have population annotation to resolve LD
        NOT_QUALIFYING_LD_BLOCK (str): LD block does not contain variants at the required R^2 threshold
    """

    SUBSIGNIFICANT_FLAG = "Subsignificant p-value"
    NO_GENOMIC_LOCATION_FLAG = "Incomplete genomic mapping"
    COMPOSITE_FLAG = "Composite association"
    INCONSISTENCY_FLAG = "Variant inconsistency"
    NON_MAPPED_VARIANT_FLAG = "No mapping in GnomAd"
    PALINDROMIC_ALLELE_FLAG = "Palindrome alleles - cannot harmonize"
    AMBIGUOUS_STUDY = "Association with ambiguous study"
    UNRESOLVED_LD = "Variant not found in LD reference"
    LD_CLUMPED = "Explained by a more significant variant in high LD (clumped)"
    NO_POPULATION = "Study does not have population annotation to resolve LD"
    NOT_QUALIFYING_LD_BLOCK = (
        "LD block does not contain variants at the required R^2 threshold"
    )


class CredibleInterval(Enum):
    """Credible interval enum.

    Interval within which an unobserved parameter value falls with a particular probability.

    Attributes:
        IS95 (str): 95% credible interval
        IS99 (str): 99% credible interval
    """

    IS95 = "is95CredibleSet"
    IS99 = "is99CredibleSet"


@dataclass
class StudyLocus(Dataset):
    """Study-Locus dataset.

    This dataset captures associations between study/traits and a genetic loci as provided by finemapping methods.
    """

    @staticmethod
    def _overlapping_peaks(
        credset_to_overlap: DataFrame, intra_study_overlap: bool = False
    ) -> DataFrame:
        """Calculate overlapping signals (study-locus) between GWAS-GWAS and GWAS-Molecular trait.

        Args:
            credset_to_overlap (DataFrame): DataFrame containing at least `studyLocusId`, `studyType`, `chromosome` and `tagVariantId` columns.
            intra_study_overlap (bool): When True, finds intra-study overlaps for credible set deduplication. Default is False.

        Returns:
            DataFrame: containing `leftStudyLocusId`, `rightStudyLocusId` and `chromosome` columns.
        """
        # Reduce columns to the minimum to reduce the size of the dataframe
        credset_to_overlap = credset_to_overlap.select(
            "studyLocusId",
            "studyId",
            "studyType",
            "chromosome",
            "region",
            "tagVariantId",
        )
        # Define join condition - if intra_study_overlap is True, finds overlaps within the same study. Otherwise finds gwas vs everything overlaps for coloc.
        join_condition = (
            [
                f.col("left.studyId") == f.col("right.studyId"),
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                f.col("left.studyLocusId") > f.col("right.studyLocusId"),
                f.col("left.region") != f.col("right.region"),
            ]
            if intra_study_overlap
            else [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                (f.col("right.studyType") != "gwas")
                | (f.col("left.studyLocusId") > f.col("right.studyLocusId")),
                f.col("left.studyType") == f.lit("gwas"),
            ]
        )

        return (
            credset_to_overlap.alias("left")
            # Self join with complex condition.
            .join(
                credset_to_overlap.alias("right"),
                on=join_condition,
                how="inner",
            )
            .select(
                f.col("left.studyLocusId").alias("leftStudyLocusId"),
                f.col("right.studyLocusId").alias("rightStudyLocusId"),
                f.col("left.chromosome").alias("chromosome"),
            )
            .distinct()
            .repartition("chromosome")
            .persist()
        )

    @staticmethod
    def _align_overlapping_tags(
        loci_to_overlap: DataFrame, peak_overlaps: DataFrame
    ) -> StudyLocusOverlap:
        """Align overlapping tags in pairs of overlapping study-locus, keeping all tags in both loci.

        Args:
            loci_to_overlap (DataFrame): containing `studyLocusId`, `studyType`, `chromosome`, `tagVariantId`, `logBF` and `posteriorProbability` columns.
            peak_overlaps (DataFrame): containing `leftStudyLocusId`, `rightStudyLocusId` and `chromosome` columns.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        # Complete information about all tags in the left study-locus of the overlap
        stats_cols = [
            "logBF",
            "posteriorProbability",
            "beta",
            "pValueMantissa",
            "pValueExponent",
        ]
        overlapping_left = loci_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("leftStudyLocusId"),
            *[f.col(col).alias(f"left_{col}") for col in stats_cols],
        ).join(peak_overlaps, on=["chromosome", "leftStudyLocusId"], how="inner")

        # Complete information about all tags in the right study-locus of the overlap
        overlapping_right = loci_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("rightStudyLocusId"),
            *[f.col(col).alias(f"right_{col}") for col in stats_cols],
        ).join(peak_overlaps, on=["chromosome", "rightStudyLocusId"], how="inner")

        # Include information about all tag variants in both study-locus aligned by tag variant id
        overlaps = overlapping_left.join(
            overlapping_right,
            on=[
                "chromosome",
                "rightStudyLocusId",
                "leftStudyLocusId",
                "tagVariantId",
            ],
            how="outer",
        ).select(
            "leftStudyLocusId",
            "rightStudyLocusId",
            "chromosome",
            "tagVariantId",
            f.struct(
                *[f"left_{e}" for e in stats_cols] + [f"right_{e}" for e in stats_cols]
            ).alias("statistics"),
        )
        return StudyLocusOverlap(
            _df=overlaps,
            _schema=StudyLocusOverlap.get_schema(),
        )

    @staticmethod
    def update_quality_flag(
        qc: Column, flag_condition: Column, flag_text: StudyLocusQualityCheck
    ) -> Column:
        """Update the provided quality control list with a new flag if condition is met.

        Args:
            qc (Column): Array column with the current list of qc flags.
            flag_condition (Column): This is a column of booleans, signing which row should be flagged
            flag_text (StudyLocusQualityCheck): Text for the new quality control flag

        Returns:
            Column: Array column with the updated list of qc flags.
        """
        qc = f.when(qc.isNull(), f.array()).otherwise(qc)
        return f.when(
            flag_condition,
            f.array_union(qc, f.array(f.lit(flag_text.value))),
        ).otherwise(qc)

    @staticmethod
    def assign_study_locus_id(study_id_col: Column, variant_id_col: Column) -> Column:
        """Hashes a column with a variant ID and a study ID to extract a consistent studyLocusId.

        Args:
            study_id_col (Column): column name with a study ID
            variant_id_col (Column): column name with a variant ID

        Returns:
            Column: column with a study locus ID

        Examples:
            >>> df = spark.createDataFrame([("GCST000001", "1_1000_A_C"), ("GCST000002", "1_1000_A_C")]).toDF("studyId", "variantId")
            >>> df.withColumn("study_locus_id", StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId"))).show()
            +----------+----------+-------------------+
            |   studyId| variantId|     study_locus_id|
            +----------+----------+-------------------+
            |GCST000001|1_1000_A_C|1553357789130151995|
            |GCST000002|1_1000_A_C|-415050894682709184|
            +----------+----------+-------------------+
            <BLANKLINE>
        """
        variant_id_col = f.coalesce(variant_id_col, f.rand().cast("string"))
        return f.xxhash64(study_id_col, variant_id_col).alias("studyLocusId")

    @classmethod
    def calculate_credible_set_log10bf(cls: type[StudyLocus], logbfs: Column) -> Column:
        """Calculate Bayes factor for the entire credible set. The Bayes factor is calculated as the logsumexp of the logBF values of the variants in the locus.

        Args:
            logbfs (Column): Array column with the logBF values of the variants in the locus.

        Returns:
            Column: log10 Bayes factor for the entire credible set.

        Examples:
            >>> spark.createDataFrame([([0.2, 0.1, 0.05, 0.0],)]).toDF("logBF").select(f.round(StudyLocus.calculate_credible_set_log10bf(f.col("logBF")), 7).alias("credibleSetlog10BF")).show()
            +------------------+
            |credibleSetlog10BF|
            +------------------+
            |         1.4765565|
            +------------------+
            <BLANKLINE>
        """
        logsumexp_udf = f.udf(lambda x: get_logsum(x), FloatType())
        return logsumexp_udf(logbfs).cast("double").alias("credibleSetlog10BF")

    @classmethod
    def get_schema(cls: type[StudyLocus]) -> StructType:
        """Provides the schema for the StudyLocus dataset.

        Returns:
            StructType: schema for the StudyLocus dataset.
        """
        return parse_spark_schema("study_locus.json")

    def filter_by_study_type(
        self: StudyLocus, study_type: str, study_index: StudyIndex
    ) -> StudyLocus:
        """Creates a new StudyLocus dataset filtered by study type.

        Args:
            study_type (str): Study type to filter for. Can be one of `gwas`, `eqtl`, `pqtl`, `eqtl`.
            study_index (StudyIndex): Study index to resolve study types.

        Returns:
            StudyLocus: Filtered study-locus dataset.

        Raises:
            ValueError: If study type is not supported.
        """
        if study_type not in ["gwas", "eqtl", "pqtl", "sqtl"]:
            raise ValueError(
                f"Study type {study_type} not supported. Supported types are: gwas, eqtl, pqtl, sqtl."
            )
        new_df = (
            self.df.join(study_index.study_type_lut(), on="studyId", how="inner")
            .filter(f.col("studyType") == study_type)
            .drop("studyType")
        )
        return StudyLocus(
            _df=new_df,
            _schema=self._schema,
        )

    def filter_credible_set(
        self: StudyLocus,
        credible_interval: CredibleInterval,
    ) -> StudyLocus:
        """Filter study-locus tag variants based on given credible interval.

        Args:
            credible_interval (CredibleInterval): Credible interval to filter for.

        Returns:
            StudyLocus: Filtered study-locus dataset.
        """
        self.df = self._df.withColumn(
            "locus",
            f.filter(
                f.col("locus"),
                lambda tag: (tag[credible_interval.value]),
            ),
        )
        return self

    def find_overlaps(
        self: StudyLocus, study_index: StudyIndex, intra_study_overlap: bool = False
    ) -> StudyLocusOverlap:
        """Calculate overlapping study-locus.

        Find overlapping study-locus that share at least one tagging variant. All GWAS-GWAS and all GWAS-Molecular traits are computed with the Molecular traits always
        appearing on the right side.

        Args:
            study_index (StudyIndex): Study index to resolve study types.
            intra_study_overlap (bool): If True, finds intra-study overlaps for credible set deduplication. Default is False.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        loci_to_overlap = (
            self.df.join(study_index.study_type_lut(), on="studyId", how="inner")
            .withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                "region",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
            )
            .persist()
        )

        # overlapping study-locus
        peak_overlaps = self._overlapping_peaks(loci_to_overlap, intra_study_overlap)

        # study-locus overlap by aligning overlapping variants
        return self._align_overlapping_tags(loci_to_overlap, peak_overlaps)

    def unique_variants_in_locus(self: StudyLocus) -> DataFrame:
        """All unique variants collected in a `StudyLocus` dataframe.

        Returns:
            DataFrame: A dataframe containing `variantId` and `chromosome` columns.
        """
        return (
            self.df.withColumn(
                "variantId",
                # Joint array of variants in that studylocus. Locus can be null
                f.explode(
                    f.array_union(
                        f.array(f.col("variantId")),
                        f.coalesce(f.col("locus.variantId"), f.array()),
                    )
                ),
            )
            .select(
                "variantId", f.split(f.col("variantId"), "_")[0].alias("chromosome")
            )
            .distinct()
        )

    def neglog_pvalue(self: StudyLocus) -> Column:
        """Returns the negative log p-value.

        Returns:
            Column: Negative log p-value
        """
        return calculate_neglog_pvalue(
            self.df.pValueMantissa,
            self.df.pValueExponent,
        )

    def annotate_credible_sets(self: StudyLocus) -> StudyLocus:
        """Annotate study-locus dataset with credible set flags.

        Sorts the array in the `locus` column elements by their `posteriorProbability` values in descending order and adds
        `is95CredibleSet` and `is99CredibleSet` fields to the elements, indicating which are the tagging variants whose cumulative sum
        of their `posteriorProbability` values is below 0.95 and 0.99, respectively.

        Returns:
            StudyLocus: including annotation on `is95CredibleSet` and `is99CredibleSet`.

        Raises:
            ValueError: If `locus` column is not available.
        """
        if "locus" not in self.df.columns:
            raise ValueError("Locus column not available.")

        self.df = self.df.withColumn(
            # Sort credible set by posterior probability in descending order
            "locus",
            f.when(
                f.col("locus").isNotNull() & (f.size(f.col("locus")) > 0),
                order_array_of_structs_by_field("locus", "posteriorProbability"),
            ),
        ).withColumn(
            # Calculate array of cumulative sums of posterior probabilities to determine which variants are in the 95% and 99% credible sets
            # and zip the cumulative sums array with the credible set array to add the flags
            "locus",
            f.when(
                f.col("locus").isNotNull() & (f.size(f.col("locus")) > 0),
                f.zip_with(
                    f.col("locus"),
                    f.transform(
                        f.sequence(f.lit(1), f.size(f.col("locus"))),
                        lambda index: f.aggregate(
                            f.slice(
                                # By using `index - 1` we introduce a value of `0.0` in the cumulative sums array. to ensure that the last variant
                                # that exceeds the 0.95 threshold is included in the cumulative sum, as its probability is necessary to satisfy the threshold.
                                f.col("locus.posteriorProbability"),
                                1,
                                index - 1,
                            ),
                            f.lit(0.0),
                            lambda acc, el: acc + el,
                        ),
                    ),
                    lambda struct_e, acc: struct_e.withField(
                        CredibleInterval.IS95.value, (acc < 0.95) & acc.isNotNull()
                    ).withField(
                        CredibleInterval.IS99.value, (acc < 0.99) & acc.isNotNull()
                    ),
                ),
            ),
        )
        return self

    def annotate_locus_statistics(
        self: StudyLocus,
        summary_statistics: SummaryStatistics,
        collect_locus_distance: int,
    ) -> StudyLocus:
        """Annotates study locus with summary statistics in the specified distance around the position.

        Args:
            summary_statistics (SummaryStatistics): Summary statistics to be used for annotation.
            collect_locus_distance (int): distance from variant defining window for inclusion of variants in locus.

        Returns:
            StudyLocus: Study locus annotated with summary statistics in `locus` column. If no statistics are found, the `locus` column will be empty.
        """
        # The clumps will be used several times (persisting)
        self.df.persist()
        # Renaming columns:
        sumstats_renamed = summary_statistics.df.selectExpr(
            *[f"{col} as tag_{col}" for col in summary_statistics.df.columns]
        ).alias("sumstat")

        locus_df = (
            sumstats_renamed
            # Joining the two datasets together:
            .join(
                f.broadcast(
                    self.df.alias("clumped").select(
                        "position", "chromosome", "studyId", "studyLocusId"
                    )
                ),
                on=[
                    (f.col("sumstat.tag_studyId") == f.col("clumped.studyId"))
                    & (f.col("sumstat.tag_chromosome") == f.col("clumped.chromosome"))
                    & (
                        f.col("sumstat.tag_position")
                        >= (f.col("clumped.position") - collect_locus_distance)
                    )
                    & (
                        f.col("sumstat.tag_position")
                        <= (f.col("clumped.position") + collect_locus_distance)
                    )
                ],
                how="inner",
            )
            .withColumn(
                "locus",
                f.struct(
                    f.col("tag_variantId").alias("variantId"),
                    f.col("tag_beta").alias("beta"),
                    f.col("tag_pValueMantissa").alias("pValueMantissa"),
                    f.col("tag_pValueExponent").alias("pValueExponent"),
                    f.col("tag_standardError").alias("standardError"),
                ),
            )
            .groupBy("studyLocusId")
            .agg(
                f.collect_list(f.col("locus")).alias("locus"),
            )
        )

        self.df = self.df.drop("locus").join(
            locus_df,
            on="studyLocusId",
            how="left",
        )

        return self

    def annotate_ld(
        self: StudyLocus, study_index: StudyIndex, ld_index: LDIndex
    ) -> StudyLocus:
        """Annotate LD information to study-locus.

        Args:
            study_index (StudyIndex): Study index to resolve ancestries.
            ld_index (LDIndex): LD index to resolve LD information.

        Returns:
            StudyLocus: Study locus annotated with ld information from LD index.
        """
        from gentropy.method.ld import LDAnnotator

        return LDAnnotator.ld_annotate(self, study_index, ld_index)

    def clump(self: StudyLocus) -> StudyLocus:
        """Perform LD clumping of the studyLocus.

        Evaluates whether a lead variant is linked to a tag (with lowest p-value) in the same studyLocus dataset.

        Returns:
            StudyLocus: with empty credible sets for linked variants and QC flag.
        """
        self.df = (
            self.df.withColumn(
                "is_lead_linked",
                LDclumping._is_lead_linked(
                    self.df.studyId,
                    self.df.variantId,
                    self.df.pValueExponent,
                    self.df.pValueMantissa,
                    self.df.ldSet,
                ),
            )
            .withColumn(
                "ldSet",
                f.when(f.col("is_lead_linked"), f.array()).otherwise(f.col("ldSet")),
            )
            .withColumn(
                "qualityControls",
                StudyLocus.update_quality_flag(
                    f.col("qualityControls"),
                    f.col("is_lead_linked"),
                    StudyLocusQualityCheck.LD_CLUMPED,
                ),
            )
            .drop("is_lead_linked")
        )
        return self

    def exclude_region(
        self: StudyLocus, region: str, exclude_overlap: bool = False
    ) -> StudyLocus:
        """Exclude a region from the StudyLocus dataset.

        Args:
            region (str): region given in "chr##:#####-####" format
            exclude_overlap (bool): If True, excludes StudyLocus windows with any overlap with the region.

        Returns:
            StudyLocus: filtered StudyLocus object.
        """
        (chromosome, start_position, end_position) = parse_region(region)
        if exclude_overlap:
            filter_condition = ~(
                (f.col("chromosome") == chromosome)
                & (
                    (f.col("locusStart") <= end_position)
                    & (f.col("locusEnd") >= start_position)
                )
            )
        else:
            filter_condition = ~(
                (f.col("chromosome") == chromosome)
                & (
                    (f.col("position") >= start_position)
                    & (f.col("position") <= end_position)
                )
            )

        return StudyLocus(
            _df=self.df.filter(filter_condition),
            _schema=StudyLocus.get_schema(),
        )

    def _qc_no_population(self: StudyLocus) -> StudyLocus:
        """Flag associations where the study doesn't have population information to resolve LD.

        Returns:
            StudyLocus: Updated study locus.
        """
        # If the tested column is not present, return self unchanged:
        if "ldPopulationStructure" not in self.df.columns:
            return self

        self.df = self.df.withColumn(
            "qualityControls",
            self.update_quality_flag(
                f.col("qualityControls"),
                f.col("ldPopulationStructure").isNull(),
                StudyLocusQualityCheck.NO_POPULATION,
            ),
        )
        return self
