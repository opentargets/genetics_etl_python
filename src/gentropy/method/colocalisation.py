"""Utilities to perform colocalisation analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import numpy as np
import pyspark.ml.functions as fml
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
from pyspark.sql.types import DoubleType

from gentropy.common.utils import get_logsum
from gentropy.dataset.colocalisation import Colocalisation

if TYPE_CHECKING:
    from typing import Any

    from numpy.typing import NDArray
    from pyspark.sql import Column

    from gentropy.dataset.study_locus_overlap import StudyLocusOverlap


def get_tag_variant_source(statistics: Column) -> Column:
    """Get the source of the tag variant for a locus-overlap row.

    Args:
        statistics (Column): statistics column

    Returns:
        Column: source of the tag variant

    Examples:
        >>> data = [('a', 'b'),(None, 'b'),('a', None),]
        >>> (
        ...     spark.createDataFrame(data, ['a', 'b'])
        ...     .select(
        ...         'a', 'b',
        ...         get_tag_variant_source(
        ...             f.struct(
        ...                 f.col('a').alias('left_posteriorProbability'),
        ...                 f.col('b').alias('right_posteriorProbability'),
        ...             )
        ...         ).alias('source')
        ...     )
        ...     .show()
        ... )
        +----+----+------+
        |   a|   b|source|
        +----+----+------+
        |   a|   b|  both|
        |NULL|   b| right|
        |   a|NULL|  left|
        +----+----+------+
        <BLANKLINE>
    """
    return (
        # Both posterior probabilities are not null:
        f.when(
            statistics.left_posteriorProbability.isNotNull()
            & statistics.right_posteriorProbability.isNotNull(),
            f.lit("both"),
        )
        # Only the left posterior probability is not null:
        .when(statistics.left_posteriorProbability.isNotNull(), f.lit("left"))
        # It must be right only:
        .otherwise(f.lit("right"))
    )


class ColocalisationMethodInterface(Protocol):
    """Colocalisation method interface."""

    METHOD_NAME: str
    METHOD_METRIC: str

    @classmethod
    def colocalise(
        cls, overlapping_signals: StudyLocusOverlap, **kwargs: Any
    ) -> Colocalisation:
        """Method to generate the colocalisation.

        Args:
            overlapping_signals (StudyLocusOverlap): Overlapping study loci.
            **kwargs (Any): Additional keyword arguments to the colocalise method.


        Returns:
            Colocalisation: loci colocalisation

        Raises:
            NotImplementedError: Implement in derivative classes.
        """
        raise NotImplementedError("Implement in derivative classes.")


class ECaviar(ColocalisationMethodInterface):
    """ECaviar-based colocalisation analysis.

    It extends [CAVIAR](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5142122/#bib18) framework to explicitly estimate the posterior probability that the same variant is causal in 2 studies while accounting for the uncertainty of LD. eCAVIAR computes the colocalization posterior probability (**CLPP**) by utilizing the marginal posterior probabilities. This framework allows for **multiple variants to be causal** in a single locus.
    """

    METHOD_NAME: str = "eCAVIAR"
    METHOD_METRIC: str = "clpp"

    @staticmethod
    def _get_clpp(left_pp: Column, right_pp: Column) -> Column:
        """Calculate the colocalisation posterior probability (CLPP).

        If the fact that the same variant is found causal for two studies are independent events,
        CLPP is defined as the product of posterior porbabilities that a variant is causal in both studies.

        Args:
            left_pp (Column): left posterior probability
            right_pp (Column): right posterior probability

        Returns:
            Column: CLPP

        Examples:
            >>> d = [{"left_pp": 0.5, "right_pp": 0.5}, {"left_pp": 0.25, "right_pp": 0.75}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("clpp", ECaviar._get_clpp(f.col("left_pp"), f.col("right_pp"))).show()
            +-------+--------+------+
            |left_pp|right_pp|  clpp|
            +-------+--------+------+
            |    0.5|     0.5|  0.25|
            |   0.25|    0.75|0.1875|
            +-------+--------+------+
            <BLANKLINE>

        """
        return left_pp * right_pp

    @classmethod
    def colocalise(
        cls: type[ECaviar],
        overlapping_signals: StudyLocusOverlap,
        **kwargs: Any,
    ) -> Colocalisation:
        """Calculate bayesian colocalisation based on overlapping signals.

        Args:
            overlapping_signals (StudyLocusOverlap): overlapping signals.
            **kwargs (Any): Additional parameters passed to the colocalise method.

        Returns:
            Colocalisation: colocalisation results based on eCAVIAR.
        """
        return Colocalisation(
            _df=(
                overlapping_signals.df.withColumns(
                    {
                        "clpp": ECaviar._get_clpp(
                            f.col("statistics.left_posteriorProbability"),
                            f.col("statistics.right_posteriorProbability"),
                        ),
                        "tagVariantSource": get_tag_variant_source(f.col("statistics")),
                    }
                )
                .groupBy(
                    "leftStudyLocusId",
                    "rightStudyLocusId",
                    "rightStudyType",
                    "chromosome",
                )
                .agg(
                    # Count the number of tag variants that can be found in both loci:
                    f.size(
                        f.filter(
                            f.collect_list(f.col("tagVariantSource")),
                            lambda x: x == "both",
                        )
                    )
                    .cast(t.LongType())
                    .alias("numberColocalisingVariants"),
                    f.sum(f.col("clpp")).alias("clpp"),
                )
                .withColumn("colocalisationMethod", f.lit(cls.METHOD_NAME))
                .join(
                    overlapping_signals.calculate_beta_ratio(),
                    on=["leftStudyLocusId", "rightStudyLocusId", "chromosome"],
                    how="left",
                )
            ),
            _schema=Colocalisation.get_schema(),
        )


class Coloc(ColocalisationMethodInterface):
    """Calculate bayesian colocalisation based on overlapping signals from credible sets.

    Based on the [R COLOC package](https://github.com/chr1swallace/coloc/blob/main/R/claudia.R), which uses the Bayes factors from the credible set to estimate the posterior probability of colocalisation. This method makes the simplifying assumption that **only one single causal variant** exists for any given trait in any genomic region.

    | Hypothesis    | Description                                                           |
    | ------------- | --------------------------------------------------------------------- |
    | H<sub>0</sub> | no association with either trait in the region                        |
    | H<sub>1</sub> | association with trait 1 only                                         |
    | H<sub>2</sub> | association with trait 2 only                                         |
    | H<sub>3</sub> | both traits are associated, but have different single causal variants |
    | H<sub>4</sub> | both traits are associated and share the same single causal variant   |

    !!! warning "Bayes factors required"

        Coloc requires the availability of Bayes factors (BF) for each variant in the credible set (`logBF` column).

    Attributes:
        PSEUDOCOUNT (float): Pseudocount to avoid log(0). Defaults to 1e-10.
        OVERLAP_SIZE_CUTOFF (int): Minimum number of overlapping variants bfore filtering. Defaults to 5.
        POSTERIOR_CUTOFF (float): Minimum overlapping Posterior probability cutoff for small overlaps. Defaults to 0.5.
    """

    METHOD_NAME: str = "COLOC"
    METHOD_METRIC: str = "h4"
    PSEUDOCOUNT: float = 1e-10
    OVERLAP_SIZE_CUTOFF: int = 5
    POSTERIOR_CUTOFF: float = 0.1

    @staticmethod
    def _get_posteriors(all_bfs: NDArray[np.float64]) -> DenseVector:
        """Calculate posterior probabilities for each hypothesis.

        Args:
            all_bfs (NDArray[np.float64]): h0-h4 bayes factors

        Returns:
            DenseVector: Posterior

        Example:
            >>> l = np.array([0.2, 0.1, 0.05, 0])
            >>> Coloc._get_posteriors(l)
            DenseVector([0.279, 0.2524, 0.2401, 0.2284])
        """
        diff = all_bfs - get_logsum(all_bfs)
        bfs_posteriors = np.exp(diff)
        return Vectors.dense(bfs_posteriors)

    @classmethod
    def colocalise(
        cls: type[Coloc],
        overlapping_signals: StudyLocusOverlap,
        **kwargs: float,
    ) -> Colocalisation:
        """Calculate bayesian colocalisation based on overlapping signals.

        Args:
            overlapping_signals (StudyLocusOverlap): overlapping peaks
            **kwargs (float): Additional parameters passed to the colocalise method.

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4.
            priorc12 (float): Prior on variant being causal for traits 1 and 2. Defaults to 1e-5.

        Returns:
            Colocalisation: Colocalisation results

        Raises:
            TypeError: When passed incorrect prior argument types.
        """
        # Ensure priors are always present, even if not passed
        priorc1 = kwargs.get("priorc1") or 1e-4
        priorc2 = kwargs.get("priorc2") or 1e-4
        priorc12 = kwargs.get("priorc12") or 1e-5
        priors = [priorc1, priorc2, priorc12]
        if any(not isinstance(prior, float) for prior in priors):
            raise TypeError(
                "Passed incorrect type(s) for prior parameters. got %s",
                {type(p): p for p in priors},
            )

        # register udfs
        logsum = f.udf(get_logsum, DoubleType())
        posteriors = f.udf(Coloc._get_posteriors, VectorUDT())
        return Colocalisation(
            _df=(
                overlapping_signals.df.withColumn(
                    "tagVariantSource", get_tag_variant_source(f.col("statistics"))
                )
                .select("*", "statistics.*")
                # Before summing log_BF columns nulls need to be filled with 0:
                .fillna(
                    0,
                    subset=[
                        "left_logBF",
                        "right_logBF",
                        "left_posteriorProbability",
                        "right_posteriorProbability",
                    ],
                )
                # Sum of log_BFs for each pair of signals
                .withColumn(
                    "sum_log_bf",
                    f.col("left_logBF") + f.col("right_logBF"),
                )
                # Group by overlapping peak and generating dense vectors of log_BF:
                .groupBy(
                    "chromosome",
                    "leftStudyLocusId",
                    "rightStudyLocusId",
                    "rightStudyType",
                )
                .agg(
                    f.size(
                        f.filter(
                            f.collect_list(f.col("tagVariantSource")),
                            lambda x: x == "both",
                        )
                    )
                    .cast(t.LongType())
                    .alias("numberColocalisingVariants"),
                    fml.array_to_vector(f.collect_list(f.col("left_logBF"))).alias(
                        "left_logBF"
                    ),
                    fml.array_to_vector(f.collect_list(f.col("right_logBF"))).alias(
                        "right_logBF"
                    ),
                    fml.array_to_vector(
                        f.collect_list(f.col("left_posteriorProbability"))
                    ).alias("left_posteriorProbability"),
                    fml.array_to_vector(
                        f.collect_list(f.col("right_posteriorProbability"))
                    ).alias("right_posteriorProbability"),
                    fml.array_to_vector(f.collect_list(f.col("sum_log_bf"))).alias(
                        "sum_log_bf"
                    ),
                    f.collect_list(f.col("tagVariantSource")).alias(
                        "tagVariantSourceList"
                    ),
                )
                .withColumn("logsum1", logsum(f.col("left_logBF")))
                .withColumn("logsum2", logsum(f.col("right_logBF")))
                .withColumn("logsum12", logsum(f.col("sum_log_bf")))
                .drop("left_logBF", "right_logBF", "sum_log_bf")
                # Add priors
                # priorc1 Prior on variant being causal for trait 1
                .withColumn("priorc1", f.lit(priorc1))
                # priorc2 Prior on variant being causal for trait 2
                .withColumn("priorc2", f.lit(priorc2))
                # priorc12 Prior on variant being causal for traits 1 and 2
                .withColumn("priorc12", f.lit(priorc12))
                # h0-h2
                .withColumn("lH0bf", f.lit(0))
                .withColumn("lH1bf", f.log(f.col("priorc1")) + f.col("logsum1"))
                .withColumn("lH2bf", f.log(f.col("priorc2")) + f.col("logsum2"))
                # h3
                .withColumn("sumlogsum", f.col("logsum1") + f.col("logsum2"))
                .withColumn("max", f.greatest("sumlogsum", "logsum12"))
                .withColumn(
                    "anySnpBothSidesHigh",
                    f.aggregate(
                        f.transform(
                            f.arrays_zip(
                                fml.vector_to_array(f.col("left_posteriorProbability")),
                                fml.vector_to_array(
                                    f.col("right_posteriorProbability")
                                ),
                                f.col("tagVariantSourceList"),
                            ),
                            # row["0"] = left PP, row["1"] = right PP, row["tagVariantSourceList"]
                            lambda row: f.when(
                                (row["tagVariantSourceList"] == "both")
                                & (row["0"] > Coloc.POSTERIOR_CUTOFF)
                                & (row["1"] > Coloc.POSTERIOR_CUTOFF),
                                1.0,
                            ).otherwise(0.0),
                        ),
                        f.lit(0.0),
                        lambda acc, x: acc + x,
                    )
                    > 0,  # True if sum of these 1.0's > 0
                )
                .filter(
                    (f.col("numberColocalisingVariants") > Coloc.OVERLAP_SIZE_CUTOFF)
                    | (f.col("anySnpBothSidesHigh"))
                )
                .withColumn(
                    "logdiff",
                    f.when(
                        (f.col("sumlogsum") == f.col("logsum12")),
                        Coloc.PSEUDOCOUNT,
                    ).otherwise(
                        f.col("max")
                        + f.log(
                            f.exp(f.col("sumlogsum") - f.col("max"))
                            - f.exp(f.col("logsum12") - f.col("max"))
                        )
                    ),
                )
                .withColumn(
                    "lH3bf",
                    f.log(f.col("priorc1"))
                    + f.log(f.col("priorc2"))
                    + f.col("logdiff"),
                )
                .drop("right_logsum", "left_logsum", "sumlogsum", "max", "logdiff")
                # h4
                .withColumn("lH4bf", f.log(f.col("priorc12")) + f.col("logsum12"))
                # cleaning
                .drop(
                    "priorc1", "priorc2", "priorc12", "logsum1", "logsum2", "logsum12"
                )
                # posteriors
                .withColumn(
                    "allBF",
                    fml.array_to_vector(
                        f.array(
                            f.col("lH0bf"),
                            f.col("lH1bf"),
                            f.col("lH2bf"),
                            f.col("lH3bf"),
                            f.col("lH4bf"),
                        )
                    ),
                )
                .withColumn(
                    "posteriors", fml.vector_to_array(posteriors(f.col("allBF")))
                )
                .withColumn("h0", f.col("posteriors").getItem(0))
                .withColumn("h1", f.col("posteriors").getItem(1))
                .withColumn("h2", f.col("posteriors").getItem(2))
                .withColumn("h3", f.col("posteriors").getItem(3))
                .withColumn("h4", f.col("posteriors").getItem(4))
                # clean up
                .drop(
                    "posteriors",
                    "allBF",
                    "lH0bf",
                    "lH1bf",
                    "lH2bf",
                    "lH3bf",
                    "lH4bf",
                    "left_posteriorProbability",
                    "right_posteriorProbability",
                    "tagVariantSourceList",
                    "anySnpBothSidesHigh",
                )
                .withColumn("colocalisationMethod", f.lit(cls.METHOD_NAME))
                .join(
                    overlapping_signals.calculate_beta_ratio(),
                    on=["leftStudyLocusId", "rightStudyLocusId", "chromosome"],
                    how="left",
                )
            ),
            _schema=Colocalisation.get_schema(),
        )
