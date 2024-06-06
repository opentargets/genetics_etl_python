"""Study Index for eQTL Catalogue data source."""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column


class EqtlCatalogueStudyIndex:
    """Study index dataset from eQTL Catalogue.

    We extract study level metadata from eQTL Catalogue's fine mapping results. All available studies can be found [here](https://www.ebi.ac.uk/eqtl/Studies/).

    One study from the eQTL Catalogue clusters together all the molecular QTLs (mQTLs) that were found:

        - in the same publication (e.g. Alasoo_2018)
        - in the same cell type or tissue (e.g. monocytes)
        - and for the same measured molecular trait (e.g. ENSG00000141510)

    """

    raw_studies_metadata_schema: StructType = StructType(
        [
            StructField("study_id", StringType(), True),
            StructField("dataset_id", StringType(), True),
            StructField("study_label", StringType(), True),
            StructField("sample_group", StringType(), True),
            StructField("tissue_id", StringType(), True),
            StructField("tissue_label", StringType(), True),
            StructField("condition_label", StringType(), True),
            StructField("sample_size", IntegerType(), True),
            StructField("quant_method", StringType(), True),
        ]
    )
    raw_studies_metadata_path = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/4c8ca340e3eb2878073b290785cb8ff1a4c788f8/data_tables/dataset_metadata_upcoming.tsv"

    @classmethod
    def _identify_study_type(
        cls: type[EqtlCatalogueStudyIndex], quantification_method_col: Column
    ) -> Column:
        """Identify the study type based on the method to quantify the trait.

        Args:
            quantification_method_col (Column): column with the label of the method to quantify the trait. Available methods are [here](https://www.ebi.ac.uk/eqtl/Methods/)

        Returns:
            Column: The study type.

        Examples:
            >>> df = spark.createDataFrame([("ge",), ("exon",), ("tx",)], ["quant_method"])
            >>> df.withColumn("study_type", EqtlCatalogueStudyIndex._identify_study_type(f.col("quant_method"))).show()
            +------------+----------+
            |quant_method|study_type|
            +------------+----------+
            |          ge|      eqtl|
            |        exon|      eqtl|
            |          tx|      eqtl|
            +------------+----------+
            <BLANKLINE>
        """
        method_to_study_type_mapping = {
            "ge": "eqtl",
            "exon": "eqtl",
            "tx": "eqtl",
            "microarray": "eqtl",
            "leafcutter": "sqtl",
            "aptamer": "pqtl",
            "txrev": "tuqtl",
        }
        map_expr = f.create_map(
            *[f.lit(x) for x in chain(*method_to_study_type_mapping.items())]
        )
        return map_expr.getItem(quantification_method_col)

    @classmethod
    def get_studies_of_interest(
        cls: type[EqtlCatalogueStudyIndex],
        studies_metadata: DataFrame,
    ) -> list[str]:
        """Filter studies of interest from the raw studies metadata.

        Args:
            studies_metadata (DataFrame): raw studies metadata filtered with studies of interest.

        Returns:
            list[str]: QTD IDs defining the studies of interest for ingestion.
        """
        return (
            studies_metadata.select("dataset_id")
            .distinct()
            .toPandas()["dataset_id"]
            .tolist()
        )

    @classmethod
    def from_susie_results(
        cls: type[EqtlCatalogueStudyIndex],
        processed_finemapping_df: DataFrame,
    ) -> StudyIndex:
        """Ingest study level metadata from eQTL Catalogue.

        Args:
            processed_finemapping_df (DataFrame): processed fine mapping results with study metadata.

        Returns:
            StudyIndex: eQTL Catalogue study index dataset derived from the selected SuSIE results.
        """
        study_index_cols = [
            field.name
            for field in StudyIndex.get_schema().fields
            if field.name in processed_finemapping_df.columns
        ]
        return StudyIndex(
            _df=processed_finemapping_df.select(study_index_cols).distinct(),
            _schema=StudyIndex.get_schema(),
        )

    @classmethod
    def read_studies_from_source(
        cls: type[EqtlCatalogueStudyIndex],
        session: Session,
        mqtl_quantification_methods_blacklist: list[str],
    ) -> DataFrame:
        """Read raw studies metadata from eQTL Catalogue.

        Args:
            session (Session): Spark session.
            mqtl_quantification_methods_blacklist (list[str]): Molecular trait quantification methods that we don't want to ingest. Available options in https://github.com/eQTL-Catalogue/eQTL-Catalogue-resources/blob/master/data_tables/dataset_metadata.tsv

        Returns:
            DataFrame: raw studies metadata.
        """
        pd.DataFrame.iteritems = pd.DataFrame.items
        return session.spark.createDataFrame(
            pd.read_csv(cls.raw_studies_metadata_path, sep="\t"),
            schema=cls.raw_studies_metadata_schema,
        ).filter(~(f.col("quant_method").isin(mqtl_quantification_methods_blacklist)))
