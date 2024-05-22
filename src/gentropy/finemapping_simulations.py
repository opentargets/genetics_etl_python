"""Step/stash of functions to run a simulations to benchmark finemapping."""

from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from scipy.stats import chi2

from gentropy.common.session import Session
from gentropy.susie_finemapper import SusieFineMapperStep


class FineMappingSimulations:
    """The module describes functions for running fine-mapping simulations and benchmarking."""

    @staticmethod
    def ProvideSummary(cred_sets: DataFrame, n_causal: int) -> dict[str, Any]:
        """Provides summary for the simulation results.

        Args:
            cred_sets (DataFrame): DataFrame containing the credible sets.
            n_causal (int): Number of causal SNPs.

        Returns:
            dict[str, Any]: Dictionary containing the summary.
        """
        return {
            "successful_runs": cred_sets["studyId"].nunique(),
            "number_of_cs": len(cred_sets["is_in_X"]),
            "expected_results": n_causal * cred_sets["studyId"].nunique(),
            "false_positives": (len(cred_sets["is_in_X"]) - sum(cred_sets["is_in_X"]))
            / len(cred_sets["is_in_X"]),
            "accuracy": sum(cred_sets["is_in_X"]) / len(cred_sets["is_in_X"]),
            "accuracy_lead": sum(cred_sets["is_in_lead"])
            / len(cred_sets["is_in_lead"]),
            "sensitivity": sum(cred_sets["is_in_X"])
            / (n_causal * cred_sets["studyId"].nunique()),
        }

    @staticmethod
    def SimulationLoop(
        n_iter: int,
        ld_index: DataFrame,
        n_causal: int,
        ld_matrix_for_sim: np.ndarray,
        session: Session,
        he2_reggen: float,
        sample_size: int,
        noise: bool = False,
        scale_noise: float = 1,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        prop_of_snps_to_noise: float = 0.1,
    ) -> DataFrame:
        """Run a simulation cycle.

        Args:
            n_iter (int): Number of iterations.
            ld_index (DataFrame): DataFrame containing the LD index.
            n_causal (int): Number of causal SNPs.
            ld_matrix_for_sim (np.ndarray): LD matrix.
            session (Session): Session object.
            he2_reggen (float): Heritability explained by the combined effect of the region and gene.
            sample_size (int): Sample size.
            noise (bool, optional): Add noise to the simulation. Defaults to False.
            scale_noise (float, optional): Scale of the noise. Defaults to 1.
            run_carma (bool, optional): Run CARMA. Defaults to False.
            run_sumstat_imputation (bool, optional): Run summary statistics imputation. Defaults to False.
            prop_of_snps_to_noise (float, optional): Proportion of SNPs to add noise to. Defaults to 0.1.

        Returns:
            DataFrame: DataFrame containing the credible sets.
        """
        pd.DataFrame.iteritems = pd.DataFrame.items

        ld_index_pd = ld_index.toPandas()
        counter = 1
        cred_sets = None
        iteration = 0
        column_list = [
            "credibleSetIndex",
            "studyLocusId",
            "studyId",
            "region",
            "exploded_locus",
            "variantId",
            "chromosome",
            "position",
            "credibleSetlog10BF",
            "purityMeanR2",
            "purityMinR2",
            "zScore",
            "pValueMantissa",
            "pValueExponent",
            "is_in_X",
            "is_in_lead",
        ]
        for iteration in range(n_iter):
            x_cycle = FineMappingSimulations.SimSumStatFromLD(
                n_causal=n_causal,
                he2_reggen=he2_reggen,
                n=sample_size,
                U=ld_matrix_for_sim,
                noise=noise,
                scale_noise=scale_noise,
            )

            if sum(x_cycle["P"] <= 5e-8) > 0:
                df = pd.DataFrame(
                    {"z": x_cycle["Z"], "variantId": ld_index_pd["variantId"]}
                )
                schema = StructType(
                    [
                        StructField("z", DoubleType(), True),
                        StructField("variantId", StringType(), True),
                    ]
                )
                df_spark = session.spark.createDataFrame(df, schema=schema)

                j = ""
                for ii in ld_index_pd["variantId"][x_cycle["indexes"]].tolist():
                    j = j + str(ii) + ","

                CS_sim = SusieFineMapperStep.susie_finemapper_from_prepared_dataframes(
                    GWAS_df=df_spark,
                    ld_index=ld_index,
                    gnomad_ld=ld_matrix_for_sim,
                    L=10,
                    session=session,
                    studyId="sim" + str(iteration),
                    region=j,
                    susie_est_tausq=False,
                    run_carma=run_carma,
                    run_sumstat_imputation=run_sumstat_imputation,
                    carma_time_limit=600,
                    imputed_r2_threshold=0.9,
                    ld_score_threshold=5,
                    sum_pips=0.99,
                    primary_signal_pval_threshold=1e-2,
                    secondary_signal_pval_threshold=1e-2,
                    purity_mean_r2_threshold=0,
                    purity_min_r2_threshold=0,
                    cs_lbf_thr=2,
                )
                cred_set = CS_sim["study_locus"].df

                X = ld_index_pd["variantId"][x_cycle["indexes"]].tolist()

                cred_set = cred_set.withColumn("exploded_locus", col("locus.variantId"))
                # Create a condition for each element in X
                conditions = [array_contains(col("exploded_locus"), x) for x in X]
                # Combine the conditions using the | operator
                combined_condition = conditions[0]
                for condition in conditions[1:]:
                    combined_condition = combined_condition | condition
                # Create a new column that is True if any condition is True and False otherwise
                cred_set = cred_set.withColumn("is_in_X", combined_condition)

                cred_set = cred_set.withColumn(
                    "is_in_lead", when(col("variantId").isin(X), 1).otherwise(0)
                )

                cred_set = cred_set.toPandas()
                cred_set = cred_set[column_list]

                if counter == 1:
                    cred_sets = cred_set
                else:
                    # cred_sets = cred_sets.unionByName(cred_set)
                    cred_sets = pd.concat([cred_sets, cred_set], axis=0)
                    # cred_sets=cred_sets.merge(cred_set)
                counter = counter + 1

        return cred_sets

    @staticmethod
    def SimSumStatFromLD(
        n_causal: int,
        he2_reggen: float,
        U: np.ndarray,
        n: int,
        noise: bool = False,
        scale_noise: float = 1,
    ) -> dict[str, Any]:
        """Simulates summary statistics (vector of Z-scores) using numbr of causla SNPs and LD matrix as input.

        Args:
            n_causal (int): number of causal snps.
            he2_reggen (float): Heritability explained by the combined effect of the region and gene.
            U (np.ndarray): LD.
            n (int): Sample size.
            noise (bool, optional): Add noise to the simulation. Defaults to False.
            scale_noise (float, optional): Scale of the noise. Defaults to 1.

        Returns:
            dict[str, Any]: Dictionary containing the simulated summary statistics.
        """
        # Calculate the total number of SNPs in analysis
        M = U.shape[0]

        # Calculate heritability explained by one causal SNP
        Tau = n * he2_reggen / n_causal

        # Simulate the causal status of SNPs
        indexes = np.random.choice(np.arange(M), size=n_causal, replace=False)
        cc = np.repeat(0, M)
        cc[indexes] = 1

        # Simulate joint z-statistics
        jz = np.zeros(M)
        x = np.random.normal(loc=0, scale=1, size=n_causal)
        jz[cc == 1] = x * np.sqrt(Tau)

        # Simulate GWAS z-statistics
        muz = U @ jz
        GWASz = np.random.multivariate_normal(mean=muz, cov=U, size=1)

        GWASz = GWASz.flatten()

        if noise:
            # M1 = int(M * prop_of_snps_to_noise)
            # indexes_causal = indexes[
            #    np.random.choice(np.arange(len(indexes)), size=1, replace=False)
            # ]
            # indexes_noise = np.random.choice(np.arange(M), size=M1, replace=False)
            # combined = np.concatenate((indexes_causal, indexes_noise))
            # unique_elements = np.unique(combined)
            # GWASz[unique_elements] = GWASz[unique_elements] + np.random.normal(
            #    loc=0, scale=scale_noise, size=len(unique_elements)
            # )
            indexes_causal = indexes[
                np.random.choice(np.arange(len(indexes)), size=1, replace=False)
            ]
            x_tmp = U[indexes_causal, :]
            x_tmp = np.abs(x_tmp)
            x_tmp = x_tmp.flatten()
            x_tmp[indexes_causal] = 0
            ind_tmp = np.where(x_tmp > 0.5)
            ind_tmp = ind_tmp[0]
            if len(ind_tmp) >= 2:
                indexes_noise = ind_tmp[
                    np.random.choice(np.arange(len(ind_tmp)), size=2, replace=False)
                ]
            else:
                indexes_noise = np.random.choice(M, size=2, replace=False)
            GWASz[indexes_noise] = GWASz[indexes_noise] + np.random.normal(
                loc=0, scale=scale_noise, size=len(indexes_noise)
            )

        GWASp = chi2.sf(GWASz**2, df=1)  # convert Z to Pval

        return {
            "Z": GWASz.flatten(),
            "P": GWASp.flatten(),
            "Tau": Tau,
            "indexes": indexes,
        }
