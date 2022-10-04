"""Interval helper functions."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from src.etl.common.utils import get_gene_tss


def prepare_gene_interval_lut(gene_index: DataFrame) -> DataFrame:
    """Gene symbol lookup table.

    Pre-processess gene/target dataset to create lookup table of gene symbols, including
    obsoleted gene symbols.

    Args:
        gene_index (DataFrame): gene/target DataFrame

    Returns:
        DataFrame: Gene LUT for symbol mapping
    """
    # Prepare gene set:
    genes = (
        # Include TSS TODO
        gene_index.withColumn(
            "tss",
            get_gene_tss(
                f.col("genomicLocation.strand"),
                f.col("genomicLocation.start"),
                f.col("genomicLocation.end"),
            ),
        )
        # Consider also obsoleted symbols (explode)
        .withColumn(
            "symbols",
            f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label")),
        )
        .withColumn("symbols", f.explode("symbols"))
        .withColumnRenamed("id", "geneId")
        .withColumn("chromosome", f.col("genomicLocation.chromosome"))
    )
    return genes
