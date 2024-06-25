"""Summary statistics ingestion for UKB PPP (EUR)."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

from gentropy.common.spark_helpers import neglog_pvalue_to_mantissa_and_exponent
from gentropy.dataset.summary_statistics import SummaryStatistics


@dataclass
class UkbPppEurSummaryStats:
    """Summary statistics dataset for UKB PPP (EUR)."""

    @classmethod
    def from_source(
        cls: type[UkbPppEurSummaryStats],
        spark: SparkSession,
        raw_summary_stats_path: str,
        tmp_variant_annotation_path: str,
        chromosome: str,
    ) -> SummaryStatistics:
        """Ingest and harmonise all summary stats for UKB PPP (EUR) data.

        1. Rename chromosome 23 to X.
        2. Filter out low INFO rows.
        3. Filter out low frequency rows.
        4. Assign variant types.
        5. Create variant ID for joining the variant annotation dataset.
        6. Join with the Variant Annotation dataset.
        7. Drop bad quality variants.

        Args:
            spark (SparkSession): Spark session object.
            raw_summary_stats_path (str): Input raw summary stats path.
            tmp_variant_annotation_path (str): Input variant annotation dataset path.
            chromosome (str): Which chromosome to process.

        Returns:
            SummaryStatistics: Processed summary statistics dataset for a given chromosome.
        """
        # Read the precomputed variant annotation dataset.
        va_df = (
            spark
            .read
            .parquet(tmp_variant_annotation_path)
            .filter(f.col("vaChromosome") == ("X" if chromosome == "23" else chromosome))
            .persist()
        )

        # Read and process the summary stats dataset.
        df = (
            spark
            .read
            .parquet(raw_summary_stats_path)
            .filter(f.col("chromosome") == chromosome)
            # Harmonise, 1: Rename chromosome 23 to X.
            .withColumn(
                "chromosome",
                f.when(
                    f.col("chromosome") == "23", "X"
                ).otherwise(f.col("chromosome"))
            )
            # Harmonise, 2: Filter out low INFO rows.
            .filter(f.col("INFO") >= 0.8)
            # Harmonise, 3: Filter out low frequency rows.
            .withColumn(
                "MAF",
                f.when(f.col("A1FREQ") < 0.5, f.col("A1FREQ"))
                .otherwise(1 - f.col("A1FREQ"))
            )
            .filter(f.col("MAF") >= 0.0001)
            .drop("MAF")
            # Harmonise, 4: Assign variant types.
            .withColumn(
                "variant_type",
                f.when(
                    (f.length("ALLELE0") == 1) & (f.length("ALLELE1") == 1),
                    f.when(
                        ((f.col("ALLELE0") == "A") & (f.col("ALLELE1") == "T")) |
                        ((f.col("ALLELE0") == "T") & (f.col("ALLELE1") == "A")) |
                        ((f.col("ALLELE0") == "G") & (f.col("ALLELE1") == "C")) |
                        ((f.col("ALLELE0") == "C") & (f.col("ALLELE1") == "G")),
                        "snp_c"
                    )
                    .otherwise(
                        "snp_n"
                    )
                )
                .otherwise(
                    "indel"
                )
            )
            # Harmonise, 5: Create variant ID for joining the variant annotation dataset.
            .withColumn(
                "GENPOS",
                f.col("GENPOS").cast("integer")
            )
            .withColumn(
                "ukb_ppp_id",
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("GENPOS"),
                    f.col("ALLELE0"),
                    f.col("ALLELE1")
                )
            )
        )
        # Harmonise, 6: Join with the Variant Annotation dataset.
        df = (
            df
            .join(va_df, (df["chromosome"] == va_df["vaChromosome"]) & (df["ukb_ppp_id"] == va_df["ukb_ppp_id"]), "inner")
            .drop("vaChromosome", "ukb_ppp_id")
            .withColumn(
                "effectAlleleFrequencyFromSource",
                f.when(
                    f.col("direction") == "direct",
                    f.col("A1FREQ").cast("float")
                ).otherwise(1 - f.col("A1FREQ").cast("float"))
            )
            .withColumn(
                "beta",
                f.when(
                    f.col("direction") == "direct",
                    f.col("BETA").cast("double")
                ).otherwise(-f.col("BETA").cast("double"))
            )
        )
        df = (
            # Harmonise, 7: Drop bad quality variants.
            df
            .filter(
                ~ ((f.col("variant_type") == "snp_c") & (f.col("direction") == "flip"))
            )
        )

        # Prepare the fields according to schema.
        df = (
            df
            .select(
                f.col("studyId"),
                f.col("chromosome"),
                f.col("variantId"),
                f.col("beta"),
                f.col("GENPOS").cast(t.IntegerType()).alias("position"),
                # Parse p-value into mantissa and exponent.
                *neglog_pvalue_to_mantissa_and_exponent(f.col("LOG10P").cast(t.DoubleType())),
                # Add standard error and sample size information.
                f.col("SE").cast("double").alias("standardError"),
                f.col("N").cast("integer").alias("sampleSize"),
            )
            # Drop rows which don't have proper position or beta value.
            .filter(
                f.col("position").cast(t.IntegerType()).isNotNull()
                & (f.col("beta") != 0)
            )
        )

        # Create the summary statistics object.
        return SummaryStatistics(
            _df=df,
            _schema=SummaryStatistics.get_schema(),
        )
