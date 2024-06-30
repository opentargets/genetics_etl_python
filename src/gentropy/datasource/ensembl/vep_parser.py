"""Generating variant index based on Esembl's Variant Effect Predictor output."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from itertools import chain
from typing import TYPE_CHECKING, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.assets import data
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    enforce_schema,
    order_array_of_structs_by_field,
)
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class VariantEffectPredictorParser:
    """Collection of methods to parse VEP output in json format."""

    # Schema description of the dbXref object:
    DBXREF_SCHEMA = t.ArrayType(
        t.StructType(
            [
                t.StructField("id", t.StringType(), True),
                t.StructField("source", t.StringType(), True),
            ]
        )
    )

    # Schema description of the in silico predictor object:
    IN_SILICO_PREDICTOR_SCHEMA = t.StructType(
        [
            t.StructField("method", t.StringType(), True),
            t.StructField("assessment", t.StringType(), True),
            t.StructField("score", t.FloatType(), True),
            t.StructField("assessmentFlag", t.StringType(), True),
            t.StructField("targetId", t.StringType(), True),
        ]
    )

    # Schema for the allele frequency column:
    ALLELE_FREQUENCY_SCHEMA = t.ArrayType(
        t.StructType(
            [
                t.StructField("populationName", t.StringType(), True),
                t.StructField("alleleFrequency", t.DoubleType(), True),
            ]
        ),
        False,
    )

    @staticmethod
    def get_schema() -> t.StructType:
        """Return the schema of the VEP output.

        Returns:
            t.StructType: VEP output schema.

        Examples:
            >>> type(VariantEffectPredictorParser.get_schema())
            <class 'pyspark.sql.types.StructType'>
        """
        return parse_spark_schema("vep_json_output.json")

    @classmethod
    def extract_variant_index_from_vep(
        cls: type[VariantEffectPredictorParser],
        spark: SparkSession,
        vep_output_path: str | list[str],
        **kwargs: bool | float | int | str | None,
    ) -> VariantIndex:
        """Extract variant index from VEP output.

        Args:
            spark (SparkSession): Spark session.
            vep_output_path (str | list[str]): Path to the VEP output.
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.json.

        Returns:
            VariantIndex: Variant index dataset.

        Raises:
            ValueError: Failed reading file.
            ValueError: The dataset is empty.
        """
        # To speed things up and simplify the json structure, read data following an expected schema:
        vep_schema = cls.get_schema()

        try:
            vep_data = spark.read.json(vep_output_path, schema=vep_schema, **kwargs)
        except ValueError as e:
            raise ValueError(f"Failed reading file: {vep_output_path}.") from e

        if vep_data.isEmpty():
            raise ValueError(f"The dataset is empty: {vep_output_path}")

        # Convert to VariantAnnotation dataset:
        return VariantIndex(
            _df=VariantEffectPredictorParser.process_vep_output(vep_data),
            _schema=VariantIndex.get_schema(),
        )

    @staticmethod
    def _extract_ensembl_xrefs(colocated_variants: Column) -> Column:
        """Extract rs identifiers and build cross reference to Ensembl's variant page.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of dbXrefs for rs identifiers.
        """
        return VariantEffectPredictorParser._generate_dbxrefs(
            VariantEffectPredictorParser._colocated_variants_to_rsids(
                colocated_variants
            ),
            "ensembl_variation",
        )

    @enforce_schema(DBXREF_SCHEMA)
    @staticmethod
    def _generate_dbxrefs(ids: Column, source: str) -> Column:
        """Convert a list of variant identifiers to dbXrefs given the source label.

        Identifiers are cast to strings, then Null values are filtered out of the id list.

        Args:
            ids (Column): List of variant identifiers.
            source (str): Source label for the dbXrefs.

        Returns:
            Column: List of dbXrefs.

        Examples:
            >>> (
            ...     spark.createDataFrame([('rs12',),(None,)], ['test_id'])
            ...     .select(VariantEffectPredictorParser._generate_dbxrefs(f.array(f.col('test_id')), "ensemblVariation").alias('col'))
            ...     .show(truncate=False)
            ... )
            +--------------------------+
            |col                       |
            +--------------------------+
            |[{rs12, ensemblVariation}]|
            |[]                        |
            +--------------------------+
            <BLANKLINE>
            >>> (
            ...     spark.createDataFrame([('rs12',),(None,)], ['test_id'])
            ...     .select(VariantEffectPredictorParser._generate_dbxrefs(f.array(f.col('test_id')), "ensemblVariation").alias('col'))
            ...     .first().col[0].asDict()
            ... )
            {'id': 'rs12', 'source': 'ensemblVariation'}
        """
        ids = f.filter(ids, lambda id: id.isNotNull())
        xref_column = f.transform(
            ids,
            lambda id: f.struct(
                id.cast(t.StringType()).alias("id"), f.lit(source).alias("source")
            ),
        )

        return f.when(xref_column.isNull(), f.array()).otherwise(xref_column)

    @staticmethod
    def _colocated_variants_to_rsids(colocated_variants: Column) -> Column:
        """Extract rs identifiers from the colocated variants VEP field.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of rs identifiers.

        Examples:
            >>> data = [('s1', 'rs1'),('s1', 'rs2'),('s1', 'rs3'),('s2', None),]
            >>> (
            ...    spark.createDataFrame(data, ['s','v'])
            ...    .groupBy('s')
            ...    .agg(f.collect_list(f.struct(f.col('v').alias('id'))).alias('cv'))
            ...    .select(VariantEffectPredictorParser._colocated_variants_to_rsids(f.col('cv')).alias('rsIds'),)
            ...    .show(truncate=False)
            ... )
            +---------------+
            |rsIds          |
            +---------------+
            |[rs1, rs2, rs3]|
            |[null]         |
            +---------------+
            <BLANKLINE>
        """
        return f.when(
            colocated_variants.isNotNull(),
            f.transform(
                colocated_variants, lambda variant: variant.getItem("id").alias("id")
            ),
        ).alias("rsIds")

    @staticmethod
    def _extract_omim_xrefs(colocated_variants: Column) -> Column:
        """Build xdbRefs for OMIM identifiers.

        OMIM identifiers are extracted from the colocated variants field, casted to strings and formatted as dbXrefs.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of dbXrefs for OMIM identifiers.

        Examples:
            >>> data = [('234234.32', 'rs1', 's1',),(None, 'rs1', 's1',),]
            >>> (
            ...    spark.createDataFrame(data, ['id', 'rsid', 's'])
            ...    .groupBy('s')
            ...    .agg(f.collect_list(f.struct(f.struct(f.array(f.col('id')).alias('OMIM')).alias('var_synonyms'),f.col('rsid').alias('id'),),).alias('cv'),).select(VariantEffectPredictorParser._extract_omim_xrefs(f.col('cv')).alias('dbXrefs'))
            ...    .show(truncate=False)
            ... )
            +-------------------+
            |dbXrefs            |
            +-------------------+
            |[{234234#32, omim}]|
            +-------------------+
            <BLANKLINE>
        """
        variants_w_omim_ref = f.filter(
            colocated_variants,
            lambda variant: variant.getItem("var_synonyms").getItem("OMIM").isNotNull(),
        )

        omim_var_ids = f.transform(
            variants_w_omim_ref,
            lambda var: f.transform(
                var.getItem("var_synonyms").getItem("OMIM"),
                lambda var: f.regexp_replace(var.cast(t.StringType()), r"\.", r"#"),
            ),
        )

        return VariantEffectPredictorParser._generate_dbxrefs(
            f.flatten(omim_var_ids), "omim"
        )

    @staticmethod
    def _extract_clinvar_xrefs(colocated_variants: Column) -> Column:
        """Build xdbRefs for VCV ClinVar identifiers.

        ClinVar identifiers are extracted from the colocated variants field.
        VCV-identifiers are filtered out to generate cross-references.

        Args:
            colocated_variants (Column): Colocated variants field from VEP output.

        Returns:
            Column: List of dbXrefs for VCV ClinVar identifiers.

        Examples:
            >>> data = [('VCV2323,RCV3423', 'rs1', 's1',),(None, 'rs1', 's1',),]
            >>> (
            ...    spark.createDataFrame(data, ['id', 'rsid', 's'])
            ...    .groupBy('s')
            ...    .agg(f.collect_list(f.struct(f.struct(f.split(f.col('id'),',').alias('ClinVar')).alias('var_synonyms'),f.col('rsid').alias('id'),),).alias('cv'),).select(VariantEffectPredictorParser._extract_clinvar_xrefs(f.col('cv')).alias('dbXrefs'))
            ...    .show(truncate=False)
            ... )
            +--------------------+
            |dbXrefs             |
            +--------------------+
            |[{VCV2323, clinvar}]|
            +--------------------+
            <BLANKLINE>
        """
        variants_w_clinvar_ref = f.filter(
            colocated_variants,
            lambda variant: variant.getItem("var_synonyms")
            .getItem("ClinVar")
            .isNotNull(),
        )

        clin_var_ids = f.transform(
            variants_w_clinvar_ref,
            lambda var: f.filter(
                var.getItem("var_synonyms").getItem("ClinVar"),
                lambda x: x.startswith("VCV"),
            ),
        )

        return VariantEffectPredictorParser._generate_dbxrefs(
            f.flatten(clin_var_ids), "clinvar"
        )

    @staticmethod
    def _get_most_severe_transcript(
        transcript_column_name: str, score_field_name: str
    ) -> Column:
        """Return transcript with the highest in silico predicted score.

        This method assumes the higher the score, the more severe the consequence of the variant is.
        Hence, by selecting the transcript with the highest score, we are selecting the most severe consequence.

        Args:
            transcript_column_name (str): Name of the column containing the list of transcripts.
            score_field_name (str): Name of the field containing the severity score.

        Returns:
            Column: Most severe transcript.

        Examples:
            >>> data = [("v1", 0.2, 'transcript1'),("v1", None, 'transcript2'),("v1", 0.6, 'transcript3'),("v1", 0.4, 'transcript4'),]
            >>> (
            ...    spark.createDataFrame(data, ['v', 'score', 'transcriptId'])
            ...    .groupBy('v')
            ...    .agg(f.collect_list(f.struct(f.col('score'),f.col('transcriptId'))).alias('transcripts'))
            ...    .select(VariantEffectPredictorParser._get_most_severe_transcript('transcripts', 'score').alias('most_severe_transcript'))
            ...    .show(truncate=False)
            ... )
            +----------------------+
            |most_severe_transcript|
            +----------------------+
            |{0.6, transcript3}    |
            +----------------------+
            <BLANKLINE>
        """
        assert isinstance(
            transcript_column_name, str
        ), "transcript_column_name must be a string and not a column."
        # Order transcripts by severity score:
        ordered_transcripts = order_array_of_structs_by_field(
            transcript_column_name, score_field_name
        )

        # Drop transcripts with no severity score and return the most severe one:
        return f.filter(
            ordered_transcripts,
            lambda transcript: transcript.getItem(score_field_name).isNotNull(),
        )[0]

    @enforce_schema(IN_SILICO_PREDICTOR_SCHEMA)
    @staticmethod
    def _get_max_alpha_missense(transcripts: Column) -> Column:
        """Return the most severe alpha missense prediction from all transcripts.

        This function assumes one variant can fall onto only one gene with alpha-sense prediction available on the canonical transcript.

        Args:
            transcripts (Column): List of transcripts.

        Returns:
            Column: Most severe alpha missense prediction.

        Examples:
        >>> data = [('v1', 0.4, 'assessment 1'), ('v1', None, None), ('v1', None, None),('v2', None, None),]
        >>> (
        ...     spark.createDataFrame(data, ['v', 'a', 'b'])
        ...    .groupBy('v')
        ...    .agg(f.collect_list(f.struct(f.struct(
        ...        f.col('a').alias('am_pathogenicity'),
        ...        f.col('b').alias('am_class')).alias('alphamissense'),
        ...        f.lit('gene1').alias('gene_id'))).alias('transcripts')
        ...    )
        ...    .select(VariantEffectPredictorParser._get_max_alpha_missense(f.col('transcripts')).alias('am'))
        ...    .show(truncate=False)
        ... )
        +----------------------------------------------------+
        |am                                                  |
        +----------------------------------------------------+
        |{max alpha missense, assessment 1, 0.4, null, gene1}|
        |{max alpha missense, null, null, null, gene1}       |
        +----------------------------------------------------+
        <BLANKLINE>
        """
        return f.transform(
            # Extract transcripts with alpha missense values:
            f.filter(
                transcripts,
                lambda transcript: transcript.getItem("alphamissense").isNotNull(),
            ),
            # Extract alpha missense values:
            lambda transcript: f.struct(
                transcript.getItem("alphamissense")
                .getItem("am_pathogenicity")
                .cast(t.FloatType())
                .alias("score"),
                transcript.getItem("alphamissense")
                .getItem("am_class")
                .alias("assessment"),
                f.lit("max alpha missense").alias("method"),
                transcript.getItem("gene_id").alias("targetId"),
            ),
        )[0]

    @enforce_schema(IN_SILICO_PREDICTOR_SCHEMA)
    @staticmethod
    def _vep_in_silico_prediction_extractor(
        transcript_column_name: str,
        method_name: str,
        score_column_name: str | None = None,
        assessment_column_name: str | None = None,
        assessment_flag_column_name: str | None = None,
    ) -> Column:
        """Extract in silico prediction from VEP output.

        Args:
            transcript_column_name (str): Name of the column containing the list of transcripts.
            method_name (str): Name of the in silico predictor.
            score_column_name (str | None): Name of the column containing the score.
            assessment_column_name (str | None): Name of the column containing the assessment.
            assessment_flag_column_name (str | None): Name of the column containing the assessment flag.

        Returns:
            Column: In silico predictor.
        """
        # Get highest score:
        most_severe_transcript: Column = (
            # Getting the most severe transcript:
            VariantEffectPredictorParser._get_most_severe_transcript(
                transcript_column_name, score_column_name
            )
            if score_column_name is not None
            # If we don't have score, just pick one of the transcript where assessment is available:
            else f.filter(
                f.col(transcript_column_name),
                lambda transcript: transcript.getItem(
                    assessment_column_name
                ).isNotNull(),
            )
        )

        return f.when(
            most_severe_transcript.isNotNull(),
            f.struct(
                # Adding method name:
                f.lit(method_name).cast(t.StringType()).alias("method"),
                # Adding assessment:
                f.lit(None).cast(t.StringType()).alias("assessment")
                if assessment_column_name is None
                else most_severe_transcript.getField(assessment_column_name).alias(
                    "assessment"
                ),
                # Adding score:
                f.lit(None).cast(t.FloatType()).alias("score")
                if score_column_name is None
                else most_severe_transcript.getField(score_column_name)
                .cast(t.FloatType())
                .alias("score"),
                # Adding assessment flag:
                f.lit(None).cast(t.StringType()).alias("assessmentFlag")
                if assessment_flag_column_name is None
                else most_severe_transcript.getField(assessment_flag_column_name)
                .cast(t.FloatType())
                .alias("assessmentFlag"),
                # Adding target id if present:
                most_severe_transcript.getItem("gene_id").alias("targetId"),
            ),
        )

    @staticmethod
    def _parser_amino_acid_change(amino_acids: Column, protein_end: Column) -> Column:
        """Convert VEP amino acid change information to one letter code aa substitution code.

        The logic assumes that the amino acid change is given in the format "from/to" and the protein end is given also.
        If any of the information is missing, the amino acid change will be set to None.

        Args:
            amino_acids (Column): Amino acid change information.
            protein_end (Column): Protein end information.

        Returns:
            Column: Amino acid change in one letter code.

        Examples:
            >>> data = [('A/B', 1),('A/B', None),(None, 1),]
            >>> (
            ...    spark.createDataFrame(data, ['amino_acids', 'protein_end'])
            ...    .select(VariantEffectPredictorParser._parser_amino_acid_change(f.col('amino_acids'), f.col('protein_end')).alias('amino_acid_change'))
            ...    .show()
            ... )
            +-----------------+
            |amino_acid_change|
            +-----------------+
            |              A1B|
            |             null|
            |             null|
            +-----------------+
            <BLANKLINE>
        """
        return f.when(
            amino_acids.isNotNull() & protein_end.isNotNull(),
            f.concat(
                f.split(amino_acids, r"\/")[0],
                protein_end,
                f.split(amino_acids, r"\/")[1],
            ),
        ).otherwise(f.lit(None))

    @staticmethod
    def _collect_uniprot_accessions(trembl: Column, swissprot: Column) -> Column:
        """Flatten arrays containing Uniprot accessions.

        Args:
            trembl (Column): List of TrEMBL protein accessions.
            swissprot (Column): List of SwissProt protein accessions.

        Returns:
            Column: List of unique Uniprot accessions extracted from swissprot and trembl arrays, splitted from version numbers.

        Examples:
        >>> data = [('v1', ["sp_1"], ["tr_1"]), ('v1', None, None), ('v1', None, ["tr_2"]),]
        >>> (
        ...    spark.createDataFrame(data, ['v', 'sp', 'tr'])
        ...    .select(VariantEffectPredictorParser._collect_uniprot_accessions(f.col('sp'), f.col('tr')).alias('proteinIds'))
        ...    .show()
        ... )
        +------------+
        |  proteinIds|
        +------------+
        |[sp_1, tr_1]|
        |          []|
        |      [tr_2]|
        +------------+
        <BLANKLINE>
        """
        # Dropping Null values and flattening the arrays:
        return f.filter(
            f.array_distinct(
                f.transform(
                    f.flatten(
                        f.filter(
                            f.array(trembl, swissprot),
                            lambda x: x.isNotNull(),
                        )
                    ),
                    lambda x: f.split(x, r"\.")[0],
                )
            ),
            lambda x: x.isNotNull(),
        )

    @staticmethod
    def _consequence_to_sequence_ontology(
        col: Column, so_dict: Dict[str, str]
    ) -> Column:
        """Convert VEP consequence terms to sequence ontology identifiers.

        Missing consequence label will be converted to None, unmapped consequences will be mapped as None.

        Args:
            col (Column): Column containing VEP consequence terms.
            so_dict (Dict[str, str]): Dictionary mapping VEP consequence terms to sequence ontology identifiers.

        Returns:
            Column: Column containing sequence ontology identifiers.

        Examples:
            >>> data = [('consequence_1',),('unmapped_consequence',),(None,)]
            >>> m = {'consequence_1': 'SO:000000'}
            >>> (
            ...    spark.createDataFrame(data, ['label'])
            ...    .select('label',VariantEffectPredictorParser._consequence_to_sequence_ontology(f.col('label'),m).alias('id'))
            ...    .show()
            ... )
            +--------------------+---------+
            |               label|       id|
            +--------------------+---------+
            |       consequence_1|SO:000000|
            |unmapped_consequence|     null|
            |                null|     null|
            +--------------------+---------+
            <BLANKLINE>
        """
        map_expr = f.create_map(*[f.lit(x) for x in chain(*so_dict.items())])

        return map_expr[col].alias("ancestry")

    @staticmethod
    def _parse_variant_location_id(vep_input_field: Column) -> List[Column]:
        r"""Parse variant identifier, chromosome, position, reference allele and alternate allele from VEP input field.

        Args:
            vep_input_field (Column): Column containing variant vcf string used as VEP input.

        Returns:
            List[Column]: List of columns containing chromosome, position, reference allele and alternate allele.
        """
        variant_fields = f.split(vep_input_field, r"\t")
        return [
            f.concat_ws(
                "_",
                f.array(
                    variant_fields[0],
                    variant_fields[1],
                    variant_fields[3],
                    variant_fields[4],
                ),
            ).alias("variantId"),
            variant_fields[0].cast(t.StringType()).alias("chromosome"),
            variant_fields[1].cast(t.IntegerType()).alias("position"),
            variant_fields[3].cast(t.StringType()).alias("referenceAllele"),
            variant_fields[4].cast(t.StringType()).alias("alternateAllele"),
        ]

    @classmethod
    def process_vep_output(cls, vep_output: DataFrame) -> DataFrame:
        """Process and format a VEP output in JSON format.

        Args:
            vep_output (DataFrame): raw VEP output, read as spark DataFrame.

        Returns:
           DataFrame: processed data in the right shape.
        """
        # Reading consequence to sequence ontology map:
        sequence_ontology_map = json.loads(
            pkg_resources.read_text(data, "so_mappings.json", encoding="utf-8")
        )
        # Processing VEP output:
        return (
            vep_output
            # Dropping non-canonical transcript consequences:  # TODO: parametrize this.
            .withColumn(
                "transcript_consequences",
                f.filter(
                    f.col("transcript_consequences"),
                    lambda consequence: consequence.getItem("canonical") == 1,
                ),
            )
            .select(
                # Parse id and chr:pos:alt:ref:
                *cls._parse_variant_location_id(f.col("input")),
                # Extracting corss_references from colocated variants:
                cls._extract_ensembl_xrefs(f.col("colocated_variants")).alias(
                    "ensembl_xrefs"
                ),
                cls._extract_omim_xrefs(f.col("colocated_variants")).alias(
                    "omim_xrefs"
                ),
                cls._extract_clinvar_xrefs(f.col("colocated_variants")).alias(
                    "clinvar_xrefs"
                ),
                # Extracting in silico predictors
                f.when(
                    # The following in-silico predictors are only available for variants with transcript consequences:
                    f.col("transcript_consequences").isNotNull(),
                    f.filter(
                        f.array(
                            # Extract CADD scores:
                            cls._vep_in_silico_prediction_extractor(
                                transcript_column_name="transcript_consequences",
                                method_name="phred scaled CADD",
                                score_column_name="cadd_phred",
                            ),
                            # Extract polyphen scores:
                            cls._vep_in_silico_prediction_extractor(
                                transcript_column_name="transcript_consequences",
                                method_name="polyphen",
                                score_column_name="polyphen_score",
                                assessment_column_name="polyphen_prediction",
                            ),
                            # Extract sift scores:
                            cls._vep_in_silico_prediction_extractor(
                                transcript_column_name="transcript_consequences",
                                method_name="sift",
                                score_column_name="sift_score",
                                assessment_column_name="sift_prediction",
                            ),
                            # Extract loftee scores:
                            cls._vep_in_silico_prediction_extractor(
                                method_name="loftee",
                                transcript_column_name="transcript_consequences",
                                score_column_name="lof",
                                assessment_column_name="lof",
                                assessment_flag_column_name="lof_filter",
                            ),
                            # Extract max alpha missense:
                            cls._get_max_alpha_missense(
                                f.col("transcript_consequences")
                            ),
                        ),
                        lambda predictor: predictor.isNotNull(),
                    ),
                )
                .otherwise(
                    # Extract CADD scores from intergenic object:
                    f.array(
                        cls._vep_in_silico_prediction_extractor(
                            transcript_column_name="intergenic_consequences",
                            method_name="phred scaled CADD",
                            score_column_name="cadd_phred",
                        ),
                    )
                )
                .alias("inSilicoPredictors"),
                # Convert consequence to SO:
                cls._consequence_to_sequence_ontology(
                    f.col("most_severe_consequence"), sequence_ontology_map
                ).alias("mostSevereConsequenceId"),
                # Collect transcript consequence:
                f.when(
                    f.col("transcript_consequences").isNotNull(),
                    f.transform(
                        f.col("transcript_consequences"),
                        lambda transcript: f.struct(
                            # Convert consequence terms to SO identifier:
                            f.transform(
                                transcript.consequence_terms,
                                lambda y: cls._consequence_to_sequence_ontology(
                                    y, sequence_ontology_map
                                ),
                            ).alias("variantFunctionalConsequenceIds"),
                            # Format amino acid change:
                            cls._parser_amino_acid_change(
                                transcript.amino_acids, transcript.protein_end
                            ).alias("aminoAcidChange"),
                            # Extract and clean uniprot identifiers:
                            cls._collect_uniprot_accessions(
                                transcript.swissprot,
                                transcript.trembl,
                            ).alias("uniprotAccessions"),
                            # Add canonical flag:
                            f.when(transcript.canonical == 1, f.lit(True))
                            .otherwise(f.lit(False))
                            .alias("isEnsemblCanonical"),
                            # Extract other fields as is:
                            transcript.codons.alias("codons"),
                            transcript.distance.alias("distance"),
                            transcript.gene_id.alias("targetId"),
                            transcript.impact.alias("impact"),
                            transcript.transcript_id.alias("transcriptId"),
                            transcript.lof.cast(t.StringType()).alias(
                                "lofteePrediction"
                            ),
                            transcript.lof.cast(t.FloatType()).alias("siftPrediction"),
                            transcript.lof.cast(t.FloatType()).alias(
                                "polyphenPrediction"
                            ),
                        ),
                    ),
                ).alias("transcriptConsequences"),
                # Extracting rsids:
                cls._colocated_variants_to_rsids(f.col("colocated_variants")).alias(
                    "rsIds"
                ),
                # Adding empty array for allele frequencies - now this piece of data is not coming form the VEP data:
                f.array().cast(cls.ALLELE_FREQUENCY_SCHEMA).alias("alleleFrequencies"),
            )
            # Adding protvar xref for missense variants:  # TODO: making and extendable list of consequences
            .withColumn(
                "protvar_xrefs",
                f.when(
                    f.size(
                        f.filter(
                            f.col("transcriptConsequences"),
                            lambda x: f.array_contains(
                                x.variantFunctionalConsequenceIds, "SO_0001583"
                            ),
                        )
                    )
                    > 0,
                    cls._generate_dbxrefs(f.array(f.col("variantId")), "protvar"),
                ),
            )
            .withColumn(
                "dbXrefs",
                f.flatten(
                    f.filter(
                        f.array(
                            f.col("ensembl_xrefs"),
                            f.col("omim_xrefs"),
                            f.col("clinvar_xrefs"),
                            f.col("protvar_xrefs"),
                        ),
                        lambda x: x.isNotNull(),
                    )
                ),
            )
            # Dropping intermediate xref columns:
            .drop(*["ensembl_xrefs", "omim_xrefs", "clinvar_xrefs", "protvar_xrefs"])
        )
