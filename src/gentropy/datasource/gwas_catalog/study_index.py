"""Study Index for GWAS Catalog data source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark import SparkFiles

from gentropy.common.session import Session
from gentropy.common.spark_helpers import column2camel_case
from gentropy.common.utils import parse_efos
from gentropy.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def read_curation_table(
    curation_path: str | None, session: Session
) -> DataFrame | None:
    """Read curation table if path or URL is given.

    Curation itself is fully optional everything should work without it.

    Args:
        curation_path (str | None): Optionally given path the curation tsv.
        session (Session): session object

    Returns:
        DataFrame | None: if curation was provided,
    """
    # If no curation path provided, we are returning none:
    if curation_path is None:
        return None
    # Read curation from the web:
    elif curation_path.startswith("http"):
        # Registering file:
        session.spark.sparkContext.addFile(curation_path)

        # Reading file:
        curation_df = session.spark.read.csv(
            SparkFiles.get(curation_path.split("/")[-1]), sep="\t", header=True
        )
    # Read curation from file:
    else:
        curation_df = session.spark.read.csv(curation_path, sep="\t", header=True)
    return curation_df.select(
        "studyId",
        "studyType",
        f.when(f.col("analysisFlag").isNotNull(), f.split(f.col("analysisFlag"), r"\|"))
        .otherwise(f.array())
        .alias("analysisFlags"),
        f.when(
            f.col("qualityControl").isNotNull(), f.split(f.col("qualityControl"), r"\|")
        )
        .otherwise(f.array())
        .alias("qualityControls"),
        f.col("isCurated").cast(t.BooleanType()),
    )


@dataclass
class StudyIndexGWASCatalogParser:
    """GWAS Catalog study index parser.

    The following information is harmonised from the GWAS Catalog:

    - All publication related information retained.
    - Mapped measured and background traits parsed.
    - Flagged if harmonized summary statistics datasets available.
    - If available, the ftp path to these files presented.
    - Ancestries from the discovery and replication stages are structured with sample counts.
    - Case/control counts extracted.
    - The number of samples with European ancestry extracted.

    """

    @staticmethod
    def _parse_discovery_samples(discovery_samples: Column) -> Column:
        """Parse discovery sample sizes from GWAS Catalog.

        This is a curated field. From publication sometimes it is not clear how the samples were split
        across the reported ancestries. In such cases we are assuming the ancestries were evenly presented
        and the total sample size is split:

        ["European, African", 100] -> ["European, 50], ["African", 50]

        Args:
            discovery_samples (Column): Raw discovery sample sizes

        Returns:
            Column: Parsed and de-duplicated list of discovery ancestries with sample size.

        Examples:
            >>> data = [('s1', "European", 10), ('s1', "African", 10), ('s2', "European, African, Asian", 100), ('s2', "European", 50)]
            >>> df = (
            ...    spark.createDataFrame(data, ['studyId', 'ancestry', 'sampleSize'])
            ...    .groupBy('studyId')
            ...    .agg(
            ...        f.collect_set(
            ...            f.struct('ancestry', 'sampleSize')
            ...        ).alias('discoverySampleSize')
            ...    )
            ...    .orderBy('studyId')
            ...    .withColumn('discoverySampleSize', StudyIndexGWASCatalogParser._parse_discovery_samples(f.col('discoverySampleSize')))
            ...    .select('discoverySampleSize')
            ...    .show(truncate=False)
            ... )
            +--------------------------------------------+
            |discoverySampleSize                         |
            +--------------------------------------------+
            |[{African, 10}, {European, 10}]             |
            |[{European, 83}, {African, 33}, {Asian, 33}]|
            +--------------------------------------------+
            <BLANKLINE>
        """
        # To initialize return objects for aggregate functions, schema has to be defined:
        schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("ancestry", t.StringType(), True),
                    t.StructField("sampleSize", t.IntegerType(), True),
                ]
            )
        )

        # Splitting comma separated ancestries:
        exploded_ancestries = f.transform(
            discovery_samples,
            lambda sample: f.split(sample.ancestry, r",\s(?![^()]*\))"),
        )

        # Initialize discoverySample object from unique list of ancestries:
        unique_ancestries = f.transform(
            f.aggregate(
                exploded_ancestries,
                f.array().cast(t.ArrayType(t.StringType())),
                lambda x, y: f.array_union(x, y),
                f.array_distinct,
            ),
            lambda ancestry: f.struct(
                ancestry.alias("ancestry"),
                f.lit(0).alias("sampleSize"),
            ),
        )

        # Computing sample sizes for ancestries when splitting is needed:
        resolved_sample_count = f.transform(
            f.arrays_zip(
                f.transform(exploded_ancestries, lambda pop: f.size(pop)).alias(
                    "pop_size"
                ),
                f.transform(discovery_samples, lambda pop: pop.sampleSize).alias(
                    "pop_count"
                ),
            ),
            lambda pop: (pop.pop_count / pop.pop_size).cast(t.IntegerType()),
        )

        # Flattening out ancestries with sample sizes:
        parsed_sample_size = f.aggregate(
            f.transform(
                f.arrays_zip(
                    exploded_ancestries.alias("ancestries"),
                    resolved_sample_count.alias("sample_count"),
                ),
                StudyIndexGWASCatalogParser._merge_ancestries_and_counts,
            ),
            f.array().cast(schema),
            lambda x, y: f.array_union(x, y),
        )

        # Normalize ancestries:
        return f.aggregate(
            parsed_sample_size,
            unique_ancestries,
            StudyIndexGWASCatalogParser._normalize_ancestries,
        )

    @staticmethod
    def _normalize_ancestries(merged: Column, ancestry: Column) -> Column:
        """Normalize ancestries from a list of structs.

        As some ancestry label might be repeated with different sample counts,
        these counts need to be collected.

        Args:
            merged (Column): Resulting list of struct with unique ancestries.
            ancestry (Column): One ancestry object coming from raw.

        Returns:
            Column: Unique list of ancestries with the sample counts.
        """
        # Iterating over the list of unique ancestries and adding the sample size if label matches:
        return f.transform(
            merged,
            lambda a: f.when(
                a.ancestry == ancestry.ancestry,
                f.struct(
                    a.ancestry.alias("ancestry"),
                    (a.sampleSize + ancestry.sampleSize)
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                ),
            ).otherwise(a),
        )

    @staticmethod
    def _merge_ancestries_and_counts(ancestry_group: Column) -> Column:
        """Merge ancestries with sample sizes.

        After splitting ancestry annotations, all resulting ancestries needs to be assigned
        with the proper sample size.

        Args:
            ancestry_group (Column): Each element is a struct with `sample_count` (int) and `ancestries` (list)

        Returns:
            Column: a list of structs with `ancestry` and `sampleSize` fields.

        Examples:
            >>> data = [(12, ['African', 'European']),(12, ['African'])]
            >>> (
            ...     spark.createDataFrame(data, ['sample_count', 'ancestries'])
            ...     .select(StudyIndexGWASCatalogParser._merge_ancestries_and_counts(f.struct('sample_count', 'ancestries')).alias('test'))
            ...     .show(truncate=False)
            ... )
            +-------------------------------+
            |test                           |
            +-------------------------------+
            |[{African, 12}, {European, 12}]|
            |[{African, 12}]                |
            +-------------------------------+
            <BLANKLINE>
        """
        # Extract sample size for the ancestry group:
        count = ancestry_group.sample_count

        # We need to loop through the ancestries:
        return f.transform(
            ancestry_group.ancestries,
            lambda ancestry: f.struct(
                ancestry.alias("ancestry"),
                count.alias("sampleSize"),
            ),
        )

    @staticmethod
    def parse_cohorts(raw_cohort: Column) -> Column:
        """Return a list of unique cohort labels from pipe separated list if provided.

        Args:
            raw_cohort (Column): Cohort list column, where labels are separated by `|` sign.

        Returns:
            Column: an array colun with string elements.

        Examples:
        >>> data = [('BioME|CaPS|Estonia|FHS|UKB|GERA|GERA|GERA',),(None,),]
        >>> spark.createDataFrame(data, ['cohorts']).select(StudyIndexGWASCatalogParser.parse_cohorts(f.col('cohorts')).alias('parsedCohorts')).show(truncate=False)
        +--------------------------------------+
        |parsedCohorts                         |
        +--------------------------------------+
        |[BioME, CaPS, Estonia, FHS, UKB, GERA]|
        |null                                  |
        +--------------------------------------+
        <BLANKLINE>
        """
        return f.when(
            (raw_cohort.isNotNull()) & (raw_cohort != ""),
            f.array_distinct(f.split(raw_cohort, r"\|")),
        )

    @classmethod
    def _parse_study_table(
        cls: type[StudyIndexGWASCatalogParser], catalog_studies: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Harmonise GWASCatalog study table with `StudyIndex` schema.

        Args:
            catalog_studies (DataFrame): GWAS Catalog study table

        Returns:
            StudyIndexGWASCatalog: Parsed and annotated GWAS Catalog study table.
        """
        return StudyIndexGWASCatalog(
            _df=catalog_studies.select(
                f.coalesce(
                    f.col("STUDY ACCESSION"), f.monotonically_increasing_id()
                ).alias("studyId"),
                f.lit("GCST").alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.col("PUBMED ID").alias("pubmedId"),
                f.col("FIRST AUTHOR").alias("publicationFirstAuthor"),
                f.col("DATE").alias("publicationDate"),
                f.col("JOURNAL").alias("publicationJournal"),
                f.col("STUDY").alias("publicationTitle"),
                f.coalesce(f.col("DISEASE/TRAIT"), f.lit("Unreported")).alias(
                    "traitFromSource"
                ),
                f.col("INITIAL SAMPLE SIZE").alias("initialSampleSize"),
                parse_efos(f.col("MAPPED_TRAIT_URI")).alias("traitFromSourceMappedIds"),
                parse_efos(f.col("MAPPED BACKGROUND TRAIT URI")).alias(
                    "backgroundTraitFromSourceMappedIds"
                ),
                cls.parse_cohorts(f.col("COHORT")).alias("cohorts"),
            ),
            _schema=StudyIndexGWASCatalog.get_schema(),
        )

    @classmethod
    def from_source(
        cls: type[StudyIndexGWASCatalogParser],
        catalog_studies: DataFrame,
        ancestry_file: DataFrame,
        sumstats_lut: DataFrame,
    ) -> StudyIndexGWASCatalog:
        """Ingests study level metadata from the GWAS Catalog.

        Args:
            catalog_studies (DataFrame): GWAS Catalog raw study table
            ancestry_file (DataFrame): GWAS Catalog ancestry table.
            sumstats_lut (DataFrame): GWAS Catalog summary statistics list.

        Returns:
            StudyIndexGWASCatalog: Parsed and annotated GWAS Catalog study table.
        """
        # Read GWAS Catalogue raw data
        return (
            cls._parse_study_table(catalog_studies)
            .annotate_ancestries(ancestry_file)
            .annotate_sumstats_info(sumstats_lut)
            .annotate_discovery_sample_sizes()
        )


@dataclass
class StudyIndexGWASCatalog(StudyIndex):
    """Study index dataset from GWAS Catalog.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    def update_study_id(
        self: StudyIndexGWASCatalog, study_annotation: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Update studyId with a dataframe containing study.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId`, `traitFromSource`, `traitFromSourceMappedIds` and key column `studyId`.

        Returns:
            StudyIndexGWASCatalog: Updated study table.
        """
        self.df = (
            self._df.join(
                study_annotation.select(
                    *[
                        f.col(c).alias(f"updated{c}")
                        if c not in ["studyId", "updatedStudyId"]
                        else f.col(c)
                        for c in study_annotation.columns
                    ]
                ),
                on="studyId",
                how="left",
            )
            .withColumn(
                "studyId",
                f.coalesce(f.col("updatedStudyId"), f.col("studyId")),
            )
            .withColumn(
                "traitFromSource",
                f.coalesce(f.col("updatedtraitFromSource"), f.col("traitFromSource")),
            )
            .withColumn(
                "traitFromSourceMappedIds",
                f.coalesce(
                    f.col("updatedtraitFromSourceMappedIds"),
                    f.col("traitFromSourceMappedIds"),
                ),
            )
            .select(self._df.columns)
        )

        return self

    def annotate_from_study_curation(
        self: StudyIndexGWASCatalog, curation_table: DataFrame | None
    ) -> StudyIndexGWASCatalog:
        """Annotating study index with curation.

        Args:
            curation_table (DataFrame | None): Curated GWAS Catalog studies with summary statistics

        Returns:
            StudyIndexGWASCatalog: Updated study index
        """
        # Providing curation table is optional. However once this method is called, the quality and studyFlag columns are added.
        if curation_table is None:
            return self

        columns = self.df.columns

        # Adding prefix to columns in the curation table:
        curation_table = curation_table.select(
            *[
                f.col(column).alias(f"curation_{column}")
                if column != "studyId"
                else f.col(column)
                for column in curation_table.columns
            ]
        )

        # Create expression how to update/create quality controls dataset:
        qualityControls_expression = (
            f.col("curation_qualityControls")
            if "qualityControls" not in columns
            else f.when(
                f.col("curation_qualityControls").isNotNull(),
                f.array_union(
                    f.col("qualityControls"), f.array(f.col("curation_qualityControls"))
                ),
            ).otherwise(f.col("qualityControls"))
        )

        # Create expression how to update/create analysis flag:
        analysis_expression = (
            f.col("curation_analysisFlags")
            if "analysisFlags" not in columns
            else f.when(
                f.col("curation_analysisFlags").isNotNull(),
                f.array_union(
                    f.col("analysisFlags"), f.array(f.col("curation_analysisFlags"))
                ),
            ).otherwise(f.col("analysisFlags"))
        )

        # Updating columns list. We might or might not list columns twice, but that doesn't matter, unique set will generated:
        columns = list(set(columns + ["qualityControls", "analysisFlags"]))

        # Based on the curation table, columns needs to be updated:
        curated_df = (
            self.df.join(curation_table, on="studyId", how="left")
            # Updating study type:
            .withColumn(
                "studyType", f.coalesce(f.col("curation_studyType"), f.col("studyType"))
            )
            # Updating quality controls:
            .withColumn("qualityControls", qualityControls_expression)
            # Updating study annotation flags:
            .withColumn("analysisFlags", analysis_expression)
            # Dropping columns coming from the curation table:
            .select(*columns)
        )
        return StudyIndexGWASCatalog(
            _df=curated_df, _schema=StudyIndexGWASCatalog.get_schema()
        )

    def extract_studies_for_curation(
        self: StudyIndexGWASCatalog, curation: DataFrame | None
    ) -> DataFrame:
        """Extract studies for curation.

        Args:
            curation (DataFrame | None): Dataframe with curation.

        Returns:
            DataFrame: Updated curation table. New studies are have the `isCurated` False.
        """
        # If no curation table provided, assume all studies needs curation:
        if curation is None:
            return (
                self.df
                # Curation only applyed on studies with summary statistics:
                .filter(f.col("hasSumstats"))
                # Adding columns expected in the curation table - array columns aready flattened:
                .withColumn("studyType", f.lit(None).cast(t.StringType()))
                .withColumn("analysisFlag", f.lit(None).cast(t.StringType()))
                .withColumn("qualityControl", f.lit(None).cast(t.StringType()))
                .withColumn("isCurated", f.lit(False).cast(t.StringType()))
            )

        # Adding prefix to columns in the curation table:
        curation = curation.select(
            *[
                f.col(column).alias(f"curation_{column}")
                if column != "studyId"
                else f.col(column)
                for column in curation.columns
            ]
        )

        return (
            self.df
            # Curation only applyed on studies with summary statistics:
            .filter(f.col("hasSumstats"))
            .join(curation, on="studyId", how="left")
            .select(
                "studyId",
                # Propagate existing curation - array columns are being flattened:
                f.col("curation_studyType").alias("studyType"),
                f.array_join(f.col("curation_analysisFlags"), "|").alias(
                    "analysisFlag"
                ),
                f.array_join(f.col("curation_qualityControls"), "|").alias(
                    "qualityControl"
                ),
                # This boolean flag needs to be casted to string, because saving to tsv would fail otherwise:
                f.coalesce(f.col("curation_isCurated"), f.lit(False))
                .cast(t.StringType())
                .alias("isCurated"),
                # The following columns are propagated to make curation easier:
                "pubmedId",
                "publicationTitle",
                "traitFromSource",
            )
        )

    def annotate_ancestries(
        self: StudyIndexGWASCatalog, ancestry_lut: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Extracting sample sizes and ancestry information.

        This function parses the ancestry data. Also get counts for the europeans in the same
        discovery stage.

        Args:
            ancestry_lut (DataFrame): Ancestry table as downloaded from the GWAS Catalog

        Returns:
            StudyIndexGWASCatalog: Slimmed and cleaned version of the ancestry annotation.
        """
        from gentropy.datasource.gwas_catalog.study_index import (
            StudyIndexGWASCatalogParser as GWASCatalogStudyIndexParser,
        )

        ancestry = (
            ancestry_lut
            # Convert column headers to camelcase:
            .transform(
                lambda df: df.select(
                    *[f.expr(column2camel_case(x)) for x in df.columns]
                )
            ).withColumnRenamed(
                "studyAccession", "studyId"
            )  # studyId has not been split yet
        )

        # Get a high resolution dataset on experimental stage:
        ancestry_stages = (
            ancestry.groupBy("studyId")
            .pivot("stage")
            .agg(
                f.collect_set(
                    f.struct(
                        f.col("broadAncestralCategory").alias("ancestry"),
                        f.col("numberOfIndividuals")
                        .cast(t.IntegerType())
                        .alias("sampleSize"),
                    )
                )
            )
            .withColumn(
                "discoverySamples",
                GWASCatalogStudyIndexParser._parse_discovery_samples(f.col("initial")),
            )
            .withColumnRenamed("replication", "replicationSamples")
            # Mapping discovery stage ancestries to LD reference:
            .withColumn(
                "ldPopulationStructure",
                self.aggregate_and_map_ancestries(f.col("discoverySamples")),
            )
            .drop("initial")
            .persist()
        )

        # Generate information on the ancestry composition of the discovery stage, and calculate
        # the proportion of the Europeans:
        europeans_deconvoluted = (
            ancestry
            # Focus on discovery stage:
            .filter(f.col("stage") == "initial")
            # Sorting ancestries if European:
            .withColumn(
                "ancestryFlag",
                # Excluding finnish:
                f.when(
                    f.col("initialSampleDescription").contains("Finnish"),
                    f.lit("other"),
                )
                # Excluding Icelandic population:
                .when(
                    f.col("initialSampleDescription").contains("Icelandic"),
                    f.lit("other"),
                )
                # Including European ancestry:
                .when(f.col("broadAncestralCategory") == "European", f.lit("european"))
                # Exclude all other population:
                .otherwise("other"),
            )
            # Grouping by study accession and initial sample description:
            .groupBy("studyId")
            .pivot("ancestryFlag")
            .agg(
                # Summarizing sample sizes for all ancestries:
                f.sum(f.col("numberOfIndividuals"))
            )
            # Do arithmetics to make sure we have the right proportion of european in the set:
            .withColumn(
                "initialSampleCountEuropean",
                f.when(f.col("european").isNull(), f.lit(0)).otherwise(
                    f.col("european")
                ),
            )
            .withColumn(
                "initialSampleCountOther",
                f.when(f.col("other").isNull(), f.lit(0)).otherwise(f.col("other")),
            )
            .withColumn(
                "initialSampleCount",
                f.col("initialSampleCountEuropean") + f.col("other"),
            )
            .drop(
                "european",
                "other",
                "initialSampleCount",
                "initialSampleCountEuropean",
                "initialSampleCountOther",
            )
        )

        parsed_ancestry_lut = ancestry_stages.join(
            europeans_deconvoluted, on="studyId", how="outer"
        ).select(
            "studyId", "discoverySamples", "ldPopulationStructure", "replicationSamples"
        )
        self.df = self.df.join(parsed_ancestry_lut, on="studyId", how="left")
        return self

    def annotate_sumstats_info(
        self: StudyIndexGWASCatalog, sumstats_lut: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Annotate summary stat locations.

        Args:
            sumstats_lut (DataFrame): listing GWAS Catalog summary stats paths

        Returns:
            StudyIndexGWASCatalog: including `summarystatsLocation` and `hasSumstats` columns

        Raises:
            ValueError: if the sumstats_lut table doesn't have the right columns
        """
        gwas_sumstats_base_uri = (
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/"
        )

        if "_c0" not in sumstats_lut.columns:
            raise ValueError(
                f'Sumstats look-up table needs to have `_c0` column. However it has: {",".join(sumstats_lut.columns)}'
            )

        parsed_sumstats_lut = sumstats_lut.withColumn(
            "summarystatsLocation",
            f.concat(
                f.lit(gwas_sumstats_base_uri),
                f.regexp_replace(f.col("_c0"), r"^\.\/", ""),
            ),
        ).select(
            self._parse_gwas_catalog_study_id("summarystatsLocation").alias("studyId"),
            "summarystatsLocation",
            f.lit(True).alias("hasSumstats"),
        )
        self.df = (
            self.df.drop("hasSumstats")
            .join(parsed_sumstats_lut, on="studyId", how="left")
            .withColumn("hasSumstats", f.coalesce(f.col("hasSumstats"), f.lit(False)))
        )
        return self

    def annotate_discovery_sample_sizes(
        self: StudyIndexGWASCatalog,
    ) -> StudyIndexGWASCatalog:
        """Extract the sample size of the discovery stage of the study as annotated in the GWAS Catalog.

        For some studies that measure quantitative traits, nCases and nControls can't be extracted. Therefore, we assume these are 0.

        Returns:
            StudyIndexGWASCatalog: object with columns `nCases`, `nControls`, and `nSamples` per `studyId` correctly extracted.
        """
        sample_size_lut = (
            self.df.select(
                "studyId",
                f.explode_outer(f.split(f.col("initialSampleSize"), r",\s+")).alias(
                    "samples"
                ),
            )
            # Extracting the sample size from the string:
            .withColumn(
                "sampleSize",
                f.regexp_extract(
                    f.regexp_replace(f.col("samples"), ",", ""), r"[0-9,]+", 0
                ).cast(t.IntegerType()),
            )
            .select(
                "studyId",
                "sampleSize",
                f.when(f.col("samples").contains("cases"), f.col("sampleSize"))
                .otherwise(f.lit(0))
                .alias("nCases"),
                f.when(f.col("samples").contains("controls"), f.col("sampleSize"))
                .otherwise(f.lit(0))
                .alias("nControls"),
            )
            # Aggregating sample sizes for all ancestries:
            .groupBy("studyId")  # studyId has not been split yet
            .agg(
                f.sum("nCases").cast("integer").alias("nCases"),
                f.sum("nControls").cast("integer").alias("nControls"),
                f.sum("sampleSize").cast("integer").alias("nSamples"),
            )
        )
        self.df = self.df.join(sample_size_lut, on="studyId", how="left")
        return self

    def apply_inclusion_list(
        self: StudyIndexGWASCatalog, inclusion_list: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Restricting GWAS Catalog studies based on a list of accepted study identifiers.

        Args:
            inclusion_list (DataFrame): List of accepted GWAS Catalog study identifiers

        Returns:
            StudyIndexGWASCatalog: Filtered dataset.
        """
        return StudyIndexGWASCatalog(
            _df=self.df.join(inclusion_list, on="studyId", how="inner"),
            _schema=StudyIndexGWASCatalog.get_schema(),
        )

    @staticmethod
    def _parse_gwas_catalog_study_id(sumstats_path_column: str) -> Column:
        """Extract GWAS Catalog study accession from the summary statistics path.

        Args:
            sumstats_path_column (str): column *name* for the summary statistics path

        Returns:
            Column: GWAS Catalog study accession.

        Examples:
            >>> data = [
            ... ('./GCST90086001-GCST90087000/GCST90086758/harmonised/35078996-GCST90086758-EFO_0007937.h.tsv.gz',),
            ...    ('gs://open-targets-gwas-summary-stats/harmonised/GCST000568.parquet/',),
            ...    (None,)
            ... ]
            >>> spark.createDataFrame(data, ['testColumn']).select(StudyIndexGWASCatalog._parse_gwas_catalog_study_id('testColumn').alias('accessions')).collect()
            [Row(accessions='GCST90086758'), Row(accessions='GCST000568'), Row(accessions=None)]
        """
        accesions = f.expr(rf"regexp_extract_all({sumstats_path_column}, '(GCST\\d+)')")
        return accesions[f.size(accesions) - 1]
