"""Dataset definition for variant annotation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    get_record_with_maximum_value,
    normalise_column,
    rename_all_columns,
    safe_array_union,
)
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


@dataclass
class VariantIndex(Dataset):
    """Dataset for representing variants and methods applied on them."""

    def __post_init__(self: VariantIndex) -> None:
        """Forcing the presence of empty arrays even if the schema allows missing values.

        To bring in annotations from other sources, we use the `array_union()` function. However it assumes
        both columns have arrays (not just the array schema!). If one of the array is null, the union
        is nullified. This needs to be avoided.
        """
        # Calling dataset's post init to validate schema:
        super().__post_init__()

        # Composing a list of expressions to replace nulls with empty arrays if the schema assumes:
        array_columns = {
            column.name: f.when(f.col(column.name).isNull(), f.array()).otherwise(
                f.col(column.name)
            )
            for column in self.df.schema
            if "ArrayType" in column.dataType.__str__()
        }

        # Not returning, but changing the data:
        self.df = self.df.withColumns(array_columns)

    @classmethod
    def get_schema(cls: type[VariantIndex]) -> StructType:
        """Provides the schema for the variant index dataset.

        Returns:
            StructType: Schema for the VariantIndex dataset
        """
        return parse_spark_schema("variant_index.json")

    @staticmethod
    def hash_long_variant_ids(
        variant_id: Column, chromosome: Column, position: Column, threshold: int = 100
    ) -> Column:
        """Hash long variant identifiers.

        Args:
            variant_id (Column): Column containing variant identifiers.
            chromosome (Column): Chromosome column.
            position (Column): position column.
            threshold (int): Above this limit, a hash will be generated.

        Returns:
            Column: Hashed variant identifiers for long variants.

        Examples:
            >>> (
            ...    spark.createDataFrame([('v_short', 'x', 23),('v_looooooong', '23', 23), ('no_chrom', None, None), (None, None, None)], ['variantId', 'chromosome', 'position'])
            ...    .select('variantId', VariantIndex.hash_long_variant_ids(f.col('variantId'), f.col('chromosome'), f.col('position'), 10).alias('hashedVariantId'))
            ...    .show(truncate=False)
            ... )
            +------------+--------------------------------------------+
            |variantId   |hashedVariantId                             |
            +------------+--------------------------------------------+
            |v_short     |v_short                                     |
            |v_looooooong|OTVAR_23_23_3749d019d645894770c364992ae70a05|
            |no_chrom    |OTVAR_41acfcd7d4fd523b33600b504914ef25      |
            |null        |null                                        |
            +------------+--------------------------------------------+
            <BLANKLINE>
        """
        return (
            # If either the position or the chromosome is missing, we hash the identifier:
            f.when(
                chromosome.isNull() | position.isNull(),
                f.concat(
                    f.lit("OTVAR_"),
                    f.md5(variant_id).cast(t.StringType()),
                ),
            )
            # If chromosome and position are given, but alleles are too long, create hash:
            .when(
                f.length(variant_id) > threshold,
                f.concat_ws(
                    "_",
                    f.lit("OTVAR"),
                    chromosome,
                    position,
                    f.md5(variant_id).cast(t.StringType()),
                ),
            )
            # Missing and regular variant identifiers are left unchanged:
            .otherwise(variant_id)
        )

    def add_annotation(
        self: VariantIndex, annotation_source: VariantIndex
    ) -> VariantIndex:
        """Import annotation from an other variant index dataset.

        At this point the annotation can be extended with extra cross-references,
        in-silico predictions and allele frequencies.

        Args:
            annotation_source (VariantIndex): Annotation to add to the dataset

        Returns:
            VariantIndex: VariantIndex dataset with the annotation added
        """
        # Prefix for renaming columns:
        prefix = "annotation_"

        # Generate select expressions that to merge and import columns from annotation:
        select_expressions = []

        # Collect columns by iterating over the variant index schema:
        for field in VariantIndex.get_schema():
            column = field.name

            # If an annotation column can be found in both datasets:
            if (column in self.df.columns) and (column in annotation_source.df.columns):
                # Arrays are merged:
                if "ArrayType" in field.dataType.__str__():
                    select_expressions.append(
                        safe_array_union(
                            f.col(column), f.col(f"{prefix}{column}")
                        ).alias(column)
                    )
                # Non-array columns are coalesced:
                else:
                    select_expressions.append(
                        f.coalesce(f.col(column), f.col(f"{prefix}{column}")).alias(
                            column
                        )
                    )
            # If the column is only found in the annotation dataset rename it:
            elif column in annotation_source.df.columns:
                select_expressions.append(f.col(f"{prefix}{column}").alias(column))
            # If the column is only found in the main dataset:
            elif column in self.df.columns:
                select_expressions.append(f.col(column))
            # VariantIndex columns not found in either dataset are ignored.

        # Join the annotation to the dataset:
        return VariantIndex(
            _df=(
                f.broadcast(self.df)
                .join(
                    rename_all_columns(annotation_source.df, prefix),
                    on=[f.col("variantId") == f.col(f"{prefix}variantId")],
                    how="left",
                )
                .select(*select_expressions)
            ),
            _schema=self.schema,
        )

    def max_maf(self: VariantIndex) -> Column:
        """Maximum minor allele frequency accross all populations assuming all variants biallelic.

        Returns:
            Column: Maximum minor allele frequency accross all populations.

        Raises:
            ValueError: Allele frequencies are not present in the dataset.
        """
        if "alleleFrequencies" not in self.df.columns:
            raise ValueError("Allele frequencies are not present in the dataset.")

        return f.array_max(
            f.transform(
                self.df.alleleFrequencies,
                lambda af: f.when(
                    af.alleleFrequency > 0.5, 1 - af.alleleFrequency
                ).otherwise(af.alleleFrequency),
            )
        )

    def filter_by_variant(self: VariantIndex, df: DataFrame) -> VariantIndex:
        """Filter variant annotation dataset by a variant dataframe.

        Args:
            df (DataFrame): A dataframe of variants

        Returns:
            VariantIndex: A filtered variant annotation dataset
        """
        join_columns = ["variantId", "chromosome"]

        assert all(
            col in df.columns for col in join_columns
        ), "The variant dataframe must contain the columns 'variantId' and 'chromosome'."

        return VariantIndex(
            _df=self._df.join(
                f.broadcast(df.select(*join_columns).distinct()),
                on=join_columns,
                how="inner",
            ),
            _schema=self.schema,
        )

    def get_transcript_consequence_df(
        self: VariantIndex, gene_index: GeneIndex | None = None
    ) -> DataFrame:
        """Dataframe of exploded transcript consequences.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            gene_index (GeneIndex | None): A gene index. Defaults to None.

        Returns:
            DataFrame: A dataframe exploded by transcript consequences with the columns variantId, chromosome, transcriptConsequence
        """
        # exploding the array removes records without VEP annotation
        transript_consequences = self.df.withColumn(
            "transcriptConsequence", f.explode("transcriptConsequences")
        ).select(
            "variantId",
            "chromosome",
            "position",
            "transcriptConsequence",
            f.col("transcriptConsequence.targetId").alias("geneId"),
        )
        if gene_index:
            transript_consequences = transript_consequences.join(
                f.broadcast(gene_index.df),
                on=["chromosome", "geneId"],
            )
        return transript_consequences

    def get_distance_to_tss(
        self: VariantIndex,
        gene_index: GeneIndex,
        max_distance: int = 500_000,
    ) -> V2G:
        """Extracts variant to gene assignments for variants falling within a window of a gene's TSS.

        Args:
            gene_index (GeneIndex): A gene index to filter by.
            max_distance (int): The maximum distance from the TSS to consider. Defaults to 500_000.

        Returns:
            V2G: variant to gene assignments with their distance to the TSS
        """
        return V2G(
            _df=(
                self.df.alias("variant")
                .join(
                    f.broadcast(gene_index.locations_lut()).alias("gene"),
                    on=[
                        f.col("variant.chromosome") == f.col("gene.chromosome"),
                        f.abs(f.col("variant.position") - f.col("gene.tss"))
                        <= max_distance,
                    ],
                    how="inner",
                )
                .withColumn(
                    "distance", f.abs(f.col("variant.position") - f.col("gene.tss"))
                )
                .withColumn(
                    "inverse_distance",
                    max_distance - f.col("distance"),
                )
                .transform(lambda df: normalise_column(df, "inverse_distance", "score"))
                .select(
                    "variantId",
                    f.col("variant.chromosome").alias("chromosome"),
                    "distance",
                    "geneId",
                    "score",
                    f.lit("distance").alias("datatypeId"),
                    f.lit("canonical_tss").alias("datasourceId"),
                )
            ),
            _schema=V2G.get_schema(),
        )

    def get_plof_v2g(self: VariantIndex, gene_index: GeneIndex) -> V2G:
        """Creates a dataset with variant to gene assignments with a flag indicating if the variant is predicted to be a loss-of-function variant by the LOFTEE algorithm.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            gene_index (GeneIndex): A gene index to filter by.

        Returns:
            V2G: variant to gene assignments from the LOFTEE algorithm
        """
        return V2G(
            _df=(
                self.get_transcript_consequence_df(gene_index)
                .filter(f.col("transcriptConsequence.lofteePrediction").isNotNull())
                .withColumn(
                    "isHighQualityPlof",
                    f.when(
                        f.col("transcriptConsequence.lofteePrediction") == "HC", True
                    ).when(
                        f.col("transcriptConsequence.lofteePrediction") == "LC", False
                    ),
                )
                .withColumn(
                    "score",
                    f.when(f.col("isHighQualityPlof"), 1.0).when(
                        ~f.col("isHighQualityPlof"), 0
                    ),
                )
                .select(
                    "variantId",
                    "chromosome",
                    "geneId",
                    "isHighQualityPlof",
                    f.col("score"),
                    f.lit("vep").alias("datatypeId"),
                    f.lit("loftee").alias("datasourceId"),
                )
            ),
            _schema=V2G.get_schema(),
        )

    def get_most_severe_transcript_consequence(
        self: VariantIndex,
        vep_consequences: DataFrame,
        gene_index: GeneIndex,
    ) -> V2G:
        """Creates a dataset with variant to gene assignments based on VEP's predicted consequence of the transcript.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            vep_consequences (DataFrame): A dataframe of VEP consequences
            gene_index (GeneIndex): A gene index to filter by. Defaults to None.

        Returns:
            V2G: High and medium severity variant to gene assignments
        """
        return V2G(
            _df=self.get_transcript_consequence_df(gene_index)
            .select(
                "variantId",
                "chromosome",
                f.col("transcriptConsequence.targetId").alias("geneId"),
                f.explode(
                    "transcriptConsequence.variantFunctionalConsequenceIds"
                ).alias("variantFunctionalConsequenceId"),
                f.lit("vep").alias("datatypeId"),
                f.lit("variantConsequence").alias("datasourceId"),
            )
            .join(
                f.broadcast(vep_consequences),
                on="variantFunctionalConsequenceId",
                how="inner",
            )
            .drop("label")
            .filter(f.col("score") != 0)
            # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
            .transform(
                lambda df: get_record_with_maximum_value(
                    df, ["variantId", "geneId"], "score"
                )
            ),
            _schema=V2G.get_schema(),
        )
