"""
Compute all vs all Bayesian colocalisation analysis for all Genetics Portal

This script calculates posterior probabilities of different causal variants
configurations under the assumption of a single causal variant for each trait.

Logic reproduced from: https://github.com/chr1swallace/coloc/blob/main/R/claudia.R
"""

import os
import hydra
from pyspark import SparkConf
from pyspark.sql import SparkSession
from omegaconf import DictConfig
from coloc_metadata import add_coloc_sumstats_info, add_moleculartrait_phenotype_genes
from coloc import colocalisation
from overlaps import find_all_vs_all_overlapping_signals


@hydra.main(config_path=os.getcwd(), config_name="config")
def main(cfg: DictConfig) -> None:
    """
    Run colocalisation analysis
    """

    sparkConf = (
        SparkConf()
        .set("spark.hadoop.fs.gs.requester.pays.mode", "AUTO")
        .set("spark.hadoop.fs.gs.requester.pays.project.id", cfg.project.id)
        .set("spark.sql.broadcastTimeout", "36000")
    )

    # establish spark connection
    spark = SparkSession.builder.config(conf=sparkConf).master("yarn").getOrCreate()

    # 1. Obtain overlapping signals in OT genetics portal
    overlapping_signals = find_all_vs_all_overlapping_signals(
        spark, cfg.coloc.credible_set
    )

    # 2. Perform colocalisation analysis
    coloc = colocalisation(
        overlapping_signals,
        cfg.coloc.priorc1,
        cfg.coloc.priorc2,
        cfg.coloc.priorc12,
    )

    # 3. Add molecular trait genes (metadata)
    coloc_with_genes = add_moleculartrait_phenotype_genes(
        spark, coloc, cfg.coloc.phenotype_id_gene
    )

    # 4. Add more info from sumstats (metadata)
    # colocWithAllMetadata = addColocSumstatsInfo(
    #     spark, coloc_with_genes, cfg.coloc.sumstats_filtered
    # )

    # Write output
    (coloc_with_genes.write.mode("overwrite").parquet(cfg.coloc.output))


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
