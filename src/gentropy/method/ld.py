"""Performing linkage disequilibrium (LD) operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.dataset.ld_index import LDIndex
    from gentropy.dataset.study_index import StudyIndex


class LDAnnotator:
    """Class to annotate linkage disequilibrium (LD) operations from GnomAD."""

    @staticmethod
    def _calculate_weighted_r_overall(ld_set: Column) -> Column:
        """Aggregation of weighted R information using ancestry proportions.

        Args:
            ld_set (Column): LD set

        Returns:
            Column: LD set with added 'r2Overall' field
        """
        return f.transform(
            ld_set,
            lambda x: f.struct(
                x["tagVariantId"].alias("tagVariantId"),
                # r2Overall is the accumulated sum of each r2 relative to the population size
                f.aggregate(
                    x["rValues"],
                    f.lit(0.0),
                    lambda acc, y: acc
                    + f.coalesce(
                        f.pow(y["r"], 2) * y["relativeSampleSize"], f.lit(0.0)
                    ),  # we use coalesce to avoid problems when r/relativeSampleSize is null
                ).alias("r2Overall"),
            ),
        )

    @staticmethod
    def _add_population_size(ld_set: Column, study_populations: Column) -> Column:
        """Add population size to each rValues entry in the ldSet.

        Args:
            ld_set (Column): LD set
            study_populations (Column): Study populations

        Returns:
            Column: LD set with added 'relativeSampleSize' field
        """
        # Create a population to relativeSampleSize map from the struct
        populations_map = f.map_from_arrays(
            study_populations["ldPopulation"],
            study_populations["relativeSampleSize"],
        )
        return f.transform(
            ld_set,
            lambda x: f.struct(
                x["tagVariantId"].alias("tagVariantId"),
                f.transform(
                    x["rValues"],
                    lambda y: f.struct(
                        y["population"].alias("population"),
                        y["r"].alias("r"),
                        populations_map[y["population"]].alias("relativeSampleSize"),
                    ),
                ).alias("rValues"),
            ),
        )

    @staticmethod
    def _qc_unresolved_ld(ld_set: Column, quality_controls: Column) -> Column:
        """Flag associations with unresolved LD.

        Args:
            ld_set (Column): LD set
            quality_controls (Column): Quality controls

        Returns:
            Column: Quality controls with added 'UNRESOLVED_LD' field
        """
        return StudyLocus.update_quality_flag(
            quality_controls,
            ld_set.isNull(),
            StudyLocusQualityCheck.UNRESOLVED_LD,
        )

    @staticmethod
    def _rescue_lead_variant(ld_set: Column, variant_id: Column) -> Column:
        """Rescue lead variant.

        In cases in which no LD information is available but a lead variant is available, we include the lead as the only variant in the ldSet.

        Args:
            ld_set (Column): LD set
            variant_id (Column): Variant ID

        Returns:
            Column: LD set with added 'tagVariantId' field
        """
        return f.when(
            ((ld_set.isNull() | (f.size(ld_set) == 0)) & variant_id.isNotNull()),
            f.array(
                f.struct(
                    variant_id.alias("tagVariantId"),
                    f.lit(1).alias("r2Overall"),
                )
            ),
        ).otherwise(ld_set)

    @classmethod
    def ld_annotate(
        cls: type[LDAnnotator],
        associations: StudyLocus,
        studies: StudyIndex,
        ld_index: LDIndex,
        r2_threshold: float = 0.5,
    ) -> StudyLocus:
        """Annotate linkage disequilibrium (LD) information to a set of studyLocus.

        This function:
            1. Annotates study locus with population structure information from the study index
            2. Joins the LD index to the StudyLocus
            3. Adds the population size of the study to each rValues entry in the ldSet
            4. Calculates the overall R weighted by the ancestry proportions in every given study.
            5. Flags associations with variants that are not found in the LD reference
            6. Rescues lead variant when no LD information is available but lead variant is available

        !!! note
            Because the LD index has a pre-set threshold of R2 = 0.5, this is the minimum threshold for the LD information to be included in the ldSet.

        Args:
            associations (StudyLocus): Dataset to be LD annotated
            studies (StudyIndex): Dataset with study information
            ld_index (LDIndex): Dataset with LD information for every variant present in LD matrix
            r2_threshold (float): R2 threshold to filter the LD set on. Default is 0.5.

        Returns:
            StudyLocus: including additional column with LD information.
        """
        return StudyLocus(
            _df=(
                associations.df
                # Drop ldSet column if already available
                .select(*[col for col in associations.df.columns if col != "ldSet"])
                # Annotate study locus with population structure from study index
                .join(
                    studies.df.select("studyId", "ldPopulationStructure"),
                    on="studyId",
                    how="left",
                )
                # Bring LD information from LD Index
                .join(
                    ld_index.df,
                    on=["variantId", "chromosome"],
                    how="left",
                )
                # Add population size to each rValues entry in the ldSet if population structure available:
                .withColumn(
                    "ldSet",
                    f.when(
                        f.col("ldPopulationStructure").isNotNull(),
                        cls._add_population_size(
                            f.col("ldSet"), f.col("ldPopulationStructure")
                        ),
                    ),
                )
                # Aggregate weighted R information using ancestry proportions
                .withColumn(
                    "ldSet",
                    f.when(
                        f.col("ldPopulationStructure").isNotNull(),
                        cls._calculate_weighted_r_overall(f.col("ldSet")),
                    ),
                )
                .drop("ldPopulationStructure")
                # Filter the LD set by the R2 threshold and set to null if no LD information passes the threshold
                .withColumn(
                    "ldSet",
                    StudyLocus.filter_ld_set(f.col("ldSet"), r2_threshold),
                )
                .withColumn("ldSet", f.when(f.size("ldSet") > 0, f.col("ldSet")))
                # QC: Flag associations with variants that are not found in the LD reference
                .withColumn(
                    "qualityControls",
                    cls._qc_unresolved_ld(f.col("ldSet"), f.col("qualityControls")),
                )
                # Add lead variant to empty ldSet when no LD information is available but lead variant is available
                .withColumn(
                    "ldSet",
                    cls._rescue_lead_variant(f.col("ldSet"), f.col("variantId")),
                )
            ),
            _schema=StudyLocus.get_schema(),
        )._qc_no_population()
