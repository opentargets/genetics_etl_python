"""Process SuSIE finemapping results from eQTL Catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.session import Session
from gentropy.common.utils import parse_pvalue
from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class EqtlCatalogueFinemapping:
    """SuSIE finemapping dataset for eQTL Catalogue.

    Credible sets from SuSIE are extracted and transformed into StudyLocus objects:
    - A study ID is defined as a triad between: the publication, the tissue, and the measured trait (e.g. Braineac2_substantia_nigra_ENSG00000248275)
    - Each row in the `credible_sets.tsv.gz` files is represented by molecular_trait_id/variant/rsid trios relevant for a given tissue. Each have their own finemapping statistics
    - log Bayes Factors are available for all variants in the `lbf_variable.txt` files
    """

    raw_credible_set_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("gene_id", StringType(), True),
            StructField("cs_id", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("rsid", StringType(), True),
            StructField("cs_size", IntegerType(), True),
            StructField("pip", DoubleType(), True),
            StructField("pvalue", DoubleType(), True),
            StructField("beta", DoubleType(), True),
            StructField("se", DoubleType(), True),
            StructField("z", DoubleType(), True),
            StructField("cs_min_r2", DoubleType(), True),
            StructField("region", StringType(), True),
        ]
    )
    raw_lbf_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("lbf_variable1", DoubleType(), True),
            StructField("lbf_variable2", DoubleType(), True),
            StructField("lbf_variable3", DoubleType(), True),
            StructField("lbf_variable4", DoubleType(), True),
            StructField("lbf_variable5", DoubleType(), True),
            StructField("lbf_variable6", DoubleType(), True),
            StructField("lbf_variable7", DoubleType(), True),
            StructField("lbf_variable8", DoubleType(), True),
            StructField("lbf_variable9", DoubleType(), True),
            StructField("lbf_variable10", DoubleType(), True),
        ]
    )

    @classmethod
    def _extract_credible_set_index(
        cls: type[EqtlCatalogueFinemapping], cs_id: Column
    ) -> Column:
        """Extract the credible set index from the cs_id.

        Args:
            cs_id (Column): column with the credible set id as defined in the eQTL Catalogue.

        Returns:
            Column: The credible set index.

        Examples:
            >>> spark.createDataFrame([("QTD000046_L1",)], ["cs_id"]).select(EqtlCatalogueFinemapping._extract_credible_set_index(f.col("cs_id"))).show()
            +----------------+
            |credibleSetIndex|
            +----------------+
            |               1|
            +----------------+
            <BLANKLINE>
        """
        return f.split(cs_id, "_L")[1].cast(IntegerType()).alias("credibleSetIndex")

    @classmethod
    def _extract_dataset_id_from_file_path(
        cls: type[EqtlCatalogueFinemapping], file_path: Column
    ) -> Column:
        """Extract the dataset_id from the file_path. The dataset_id follows the pattern QTD{6}.

        Args:
            file_path (Column): A column containing the file path.

        Returns:
            Column: The dataset_id.

        Examples:
            >>> spark.createDataFrame([("QTD000046.credible_sets.tsv",)], ["filename"]).select(EqtlCatalogueFinemapping._extract_dataset_id_from_file_path(f.col("filename"))).show()
            +----------+
            |dataset_id|
            +----------+
            | QTD000046|
            +----------+
            <BLANKLINE>
        """
        return f.regexp_extract(file_path, r"QTD\d{6}", 0).alias("dataset_id")

    @classmethod
    def parse_susie_results(
        cls: type[EqtlCatalogueFinemapping],
        credible_sets: DataFrame,
        lbf: DataFrame,
        studies_metadata: DataFrame,
    ) -> DataFrame:
        """Parse the SuSIE results into a DataFrame containing the finemapping statistics and metadata about the studies.

        Args:
            credible_sets (DataFrame): DataFrame containing raw statistics of all variants in the credible sets.
            lbf (DataFrame): DataFrame containing the raw log Bayes Factors for all variants.
            studies_metadata (DataFrame): DataFrame containing the study metadata.

        Returns:
            DataFrame: Processed SuSIE results to contain metadata about the studies and the finemapping statistics.
        """
        ss_ftp_path_template = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/sumstats"
        return (
            lbf.withColumn(
                "dataset_id",
                cls._extract_dataset_id_from_file_path(f.input_file_name()),
            )
            .join(
                (
                    credible_sets.withColumn(
                        "dataset_id",
                        cls._extract_dataset_id_from_file_path(f.input_file_name()),
                    )
                    .withColumn(
                        "credibleSetIndex",
                        cls._extract_credible_set_index(f.col("cs_id")),
                    )
                    .join(f.broadcast(studies_metadata), on="dataset_id")
                ),
                on=["molecular_trait_id", "region", "variant", "dataset_id"],
                how="inner",
            )
            .withColumn(
                "logBF",
                f.when(f.col("credibleSetIndex") == 1, f.col("lbf_variable1"))
                .when(f.col("credibleSetIndex") == 2, f.col("lbf_variable2"))
                .when(f.col("credibleSetIndex") == 3, f.col("lbf_variable3"))
                .when(f.col("credibleSetIndex") == 4, f.col("lbf_variable4"))
                .when(f.col("credibleSetIndex") == 5, f.col("lbf_variable5"))
                .when(f.col("credibleSetIndex") == 6, f.col("lbf_variable6"))
                .when(f.col("credibleSetIndex") == 7, f.col("lbf_variable7"))
                .when(f.col("credibleSetIndex") == 8, f.col("lbf_variable8"))
                .when(f.col("credibleSetIndex") == 9, f.col("lbf_variable9"))
                .when(f.col("credibleSetIndex") == 10, f.col("lbf_variable10")),
            )
            .select(
                f.regexp_replace(f.col("variant"), r"chr", "").alias("variantId"),
                f.col("region"),
                f.col("chromosome"),
                f.col("position"),
                f.col("pip").alias("posteriorProbability"),
                *parse_pvalue(f.col("pvalue")),
                f.col("sample_size").alias("nSamples"),
                f.col("beta"),
                f.col("se").alias("standardError"),
                f.col("credibleSetIndex"),
                f.col("logBF"),
                f.lit("SuSie").alias("finemappingMethod"),
                # Study metadata
                f.col("molecular_trait_id").alias("traitFromSource"),
                f.col("gene_id").alias("geneId"),
                f.col("dataset_id"),
                f.concat_ws(
                    "_",
                    f.col("study_label"),
                    f.col("sample_group"),
                    f.col("molecular_trait_id"),
                ).alias("studyId"),
                f.col("tissue_id").alias("biosampleFromSourceId"),
                EqtlCatalogueStudyIndex._identify_study_type(
                    f.col("quant_method")
                ).alias("studyType"),
                f.col("study_label").alias("projectId"),
                f.concat_ws(
                    "/",
                    f.lit(ss_ftp_path_template),
                    f.col("study_id"),
                    f.col("dataset_id"),
                ).alias("summarystatsLocation"),
                f.lit(True).alias("hasSumstats"),
                f.col("molecular_trait_id"),
            )
        )

    @classmethod
    def from_susie_results(
        cls: type[EqtlCatalogueFinemapping], processed_finemapping_df: DataFrame
    ) -> StudyLocus:
        """Create a StudyLocus object from the processed SuSIE results.

        Args:
            processed_finemapping_df (DataFrame): DataFrame containing the processed SuSIE results.

        Returns:
            StudyLocus: eQTL Catalogue credible sets.
        """
        lead_w = Window.partitionBy(
            "dataset_id", "molecular_trait_id", "region", "credibleSetIndex"
        )
        study_locus_cols = [
            field.name
            for field in StudyLocus.get_schema().fields
            if field.name in processed_finemapping_df.columns
        ] + ["locus"]
        return StudyLocus(
            _df=(
                processed_finemapping_df.withColumn(
                    "isLead",
                    f.row_number().over(lead_w.orderBy(f.desc("posteriorProbability")))
                    == f.lit(1),
                )
                .withColumn(
                    # Collecting all variants that constitute the credible set brings as many variants as the credible set size
                    "locus",
                    f.when(
                        f.col("isLead"),
                        f.collect_list(
                            f.struct(
                                "variantId",
                                "posteriorProbability",
                                "pValueMantissa",
                                "pValueExponent",
                                "logBF",
                                "beta",
                                "standardError",
                            )
                        ).over(lead_w),
                    ),
                )
                .filter(f.col("isLead"))
                .drop("isLead")
                .select(
                    *study_locus_cols,
                    StudyLocus.assign_study_locus_id(
                        f.col("studyId"), f.col("variantId")
                    ),
                    StudyLocus.calculate_credible_set_log10bf(
                        f.col("locus.logBF")
                    ).alias("credibleSetlog10BF"),
                )
            ),
            _schema=StudyLocus.get_schema(),
        ).annotate_credible_sets()

    @classmethod
    def read_credible_set_from_source(
        cls: type[EqtlCatalogueFinemapping],
        session: Session,
        credible_set_path: str | list[str],
    ) -> DataFrame:
        """Load raw credible sets from eQTL Catalogue.

        Args:
            session (Session): Spark session.
            credible_set_path (str | list[str]): Path to raw table(s) containing finemapping results for any variant belonging to a credible set.

        Returns:
            DataFrame: Credible sets DataFrame.
        """
        return session.spark.read.csv(
            credible_set_path,
            sep="\t",
            header=True,
            schema=cls.raw_credible_set_schema,
        )

    @classmethod
    def read_lbf_from_source(
        cls: type[EqtlCatalogueFinemapping],
        session: Session,
        lbf_path: str | list[str],
    ) -> DataFrame:
        """Load raw log Bayes Factors from eQTL Catalogue.

        Args:
            session (Session): Spark session.
            lbf_path (str | list[str]): Path to raw table(s) containing Log Bayes Factors for each variant.

        Returns:
            DataFrame: Log Bayes Factors DataFrame.
        """
        return session.spark.read.csv(
            lbf_path,
            sep="\t",
            header=True,
            schema=cls.raw_lbf_schema,
        )
