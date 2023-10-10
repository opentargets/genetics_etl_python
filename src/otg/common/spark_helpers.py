"""Common utilities in Spark that can be used across the project."""
from __future__ import annotations

import re
import sys
from typing import TYPE_CHECKING, Iterable, Optional

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql.types import FloatType
from scipy.stats import norm

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, WindowSpec


def _convert_from_wide_to_long(
    df: DataFrame,
    id_vars: Iterable[str],
    var_name: str,
    value_name: str,
    value_vars: Optional[Iterable[str]] = None,
) -> DataFrame:
    """Converts a dataframe from wide to long format.

    Args:
        df (DataFrame): Dataframe to melt
        id_vars (Iterable[str]): List of fixed columns to keep
        var_name (str): Name of the column containing the variable names
        value_name (str): Name of the column containing the values
        value_vars (Optional[Iterable[str]]): List of columns to melt. Defaults to None.

    Returns:
        DataFrame: Melted dataframe

    Examples:
    >>> df = spark.createDataFrame([("a", 1, 2)], ["id", "feature_1", "feature_2"])
    >>> _convert_from_wide_to_long(df, ["id"], "feature", "value").show()
    +---+---------+-----+
    | id|  feature|value|
    +---+---------+-----+
    |  a|feature_1|  1.0|
    |  a|feature_2|  2.0|
    +---+---------+-----+
    <BLANKLINE>
    """
    if not value_vars:
        value_vars = [c for c in df.columns if c not in id_vars]
    _vars_and_vals = f.array(
        *(
            f.struct(
                f.lit(c).alias(var_name), f.col(c).cast(FloatType()).alias(value_name)
            )
            for c in value_vars
        )
    )

    # Add to the DataFrame and explode to convert into rows
    _tmp = df.withColumn("_vars_and_vals", f.explode(_vars_and_vals))

    cols = list(id_vars) + [
        f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)


def _convert_from_long_to_wide(
    df: DataFrame, id_vars: list[str], var_name: str, value_name: str
) -> DataFrame:
    """Converts a dataframe from long to wide format using Spark pivot built-in function.

    Args:
        df (DataFrame): Dataframe to pivot
        id_vars (list[str]): List of fixed columns to keep
        var_name (str): Name of the column to pivot on
        value_name (str): Name of the column containing the values

    Returns:
        DataFrame: Pivoted dataframe

    Examples:
    >>> df = spark.createDataFrame([("a", "feature_1", 1), ("a", "feature_2", 2)], ["id", "featureName", "featureValue"])
    >>> _convert_from_long_to_wide(df, ["id"], "featureName", "featureValue").show()
    +---+---------+---------+
    | id|feature_1|feature_2|
    +---+---------+---------+
    |  a|        1|        2|
    +---+---------+---------+
    <BLANKLINE>
    """
    return df.groupBy(id_vars).pivot(var_name).agg(f.first(value_name))


def pvalue_to_zscore(pval_col: Column) -> Column:
    """Convert p-value column to z-score column.

    Args:
        pval_col (Column): pvalues to be casted to floats.

    Returns:
        Column: p-values transformed to z-scores

    Examples:
        >>> d = [{"id": "t1", "pval": "1"}, {"id": "t2", "pval": "0.9"}, {"id": "t3", "pval": "0.05"}, {"id": "t4", "pval": "1e-300"}, {"id": "t5", "pval": "1e-1000"}, {"id": "t6", "pval": "NA"}]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("zscore", pvalue_to_zscore(f.col("pval"))).show()
        +---+-------+----------+
        | id|   pval|    zscore|
        +---+-------+----------+
        | t1|      1|       0.0|
        | t2|    0.9|0.12566137|
        | t3|   0.05|  1.959964|
        | t4| 1e-300| 37.537838|
        | t5|1e-1000| 37.537838|
        | t6|     NA|      null|
        +---+-------+----------+
        <BLANKLINE>

    """
    pvalue_float = pval_col.cast(t.FloatType())
    pvalue_nozero = f.when(pvalue_float == 0, sys.float_info.min).otherwise(
        pvalue_float
    )
    return f.udf(
        lambda pv: float(abs(norm.ppf((float(pv)) / 2))) if pv else None,
        t.FloatType(),
    )(pvalue_nozero)


def nullify_empty_array(column: Column) -> Column:
    """Returns null when a Spark Column has an array of size 0, otherwise return the array.

    Args:
        column (Column): The Spark Column to be processed.

    Returns:
        Column: Nullified column when the array is empty.

    Examples:
    >>> df = spark.createDataFrame([[], [1, 2, 3]], "array<int>")
    >>> df.withColumn("new", nullify_empty_array(df.value)).show()
    +---------+---------+
    |    value|      new|
    +---------+---------+
    |       []|     null|
    |[1, 2, 3]|[1, 2, 3]|
    +---------+---------+
    <BLANKLINE>
    """
    return f.when(f.size(column) != 0, column)


def get_top_ranked_in_window(df: DataFrame, w: WindowSpec) -> DataFrame:
    """Returns the record with the top rank within each group of the window."""
    return (
        df.withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number")
    )


def get_record_with_minimum_value(
    df: DataFrame,
    grouping_col: Column | str | list[Column | str],
    sorting_col: str,
) -> DataFrame:
    """Returns the record with the minimum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str): The column name(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the minimum value of the sorting column within each group of the grouping column.
    """
    w = Window.partitionBy(grouping_col).orderBy(sorting_col)
    return get_top_ranked_in_window(df, w)


def get_record_with_maximum_value(
    df: DataFrame,
    grouping_col: Column | str | list[Column | str],
    sorting_col: str,
) -> DataFrame:
    """Returns the record with the maximum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str): The column name(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the maximum value of the sorting column within each group of the grouping column.
    """
    w = Window.partitionBy(grouping_col).orderBy(f.col(sorting_col).desc())
    return get_top_ranked_in_window(df, w)


def normalise_column(
    df: DataFrame, input_col_name: str, output_col_name: str
) -> DataFrame:
    """Normalises a numerical column to a value between 0 and 1.

    Args:
        df (DataFrame): The DataFrame to be processed.
        input_col_name (str): The name of the column to be normalised.
        output_col_name (str): The name of the column to store the normalised values.

    Returns:
        DataFrame: The DataFrame with the normalised column.

    Examples:
    >>> df = spark.createDataFrame([5, 50, 1000], "int")
    >>> df.transform(lambda df: normalise_column(df, "value", "norm_value")).show()
    +-----+----------+
    |value|norm_value|
    +-----+----------+
    |    5|       0.0|
    |   50|      0.05|
    | 1000|       1.0|
    +-----+----------+
    <BLANKLINE>
    """
    vec_assembler = VectorAssembler(
        inputCols=[input_col_name], outputCol="feature_vector"
    )
    scaler = MinMaxScaler(inputCol="feature_vector", outputCol="norm_vector")
    unvector_score = f.round(vector_to_array(f.col("norm_vector"))[0], 2).alias(
        output_col_name
    )
    pipeline = Pipeline(stages=[vec_assembler, scaler])
    return (
        pipeline.fit(df)
        .transform(df)
        .select("*", unvector_score)
        .drop("feature_vector", "norm_vector")
    )


def calculate_neglog_pvalue(
    p_value_mantissa: Column, p_value_exponent: Column
) -> Column:
    """Compute the negative log p-value.

    Args:
        p_value_mantissa (Column): P-value mantissa
        p_value_exponent (Column): P-value exponent

    Returns:
        Column: Negative log p-value

    Examples:
        >>> d = [(1, 1), (5, -2), (1, -1000)]
        >>> df = spark.createDataFrame(d).toDF("p_value_mantissa", "p_value_exponent")
        >>> df.withColumn("neg_log_p", calculate_neglog_pvalue(f.col("p_value_mantissa"), f.col("p_value_exponent"))).show()
        +----------------+----------------+------------------+
        |p_value_mantissa|p_value_exponent|         neg_log_p|
        +----------------+----------------+------------------+
        |               1|               1|              -1.0|
        |               5|              -2|1.3010299956639813|
        |               1|           -1000|            1000.0|
        +----------------+----------------+------------------+
        <BLANKLINE>
    """
    return -1 * (f.log10(p_value_mantissa) + p_value_exponent)


def string2camelcase(col_name: str) -> str:
    """Converting a string to camelcase.

    Args:
        col_name (str): a random string

    Returns:
        str: Camel cased string

    Examples:
        >>> string2camelcase("hello_world")
        'helloWorld'
        >>> string2camelcase("hello world")
        'helloWorld'
    """
    # Removing a bunch of unwanted characters from the column names:
    col_name_normalised = re.sub(r"[\/\(\)\-]+", " ", col_name)

    first, *rest = re.split("[ _-]", col_name_normalised)
    return "".join([first.lower(), *map(str.capitalize, rest)])


def column2camel_case(col_name: str) -> str:
    """A helper function to convert column names to camel cases.

    Args:
        col_name (str): a single column name

    Returns:
        str: spark expression to select and rename the column

    Examples:
        >>> column2camel_case("hello_world")
        '`hello_world` as helloWorld'
    """
    return f"`{col_name}` as {string2camelcase(col_name)}"


def order_array_of_structs_by_field(column_name: str, field_name: str) -> Column:
    """Sort a column of array of structs by a field in descending order, nulls last."""
    return f.expr(
        f"""
        array_sort(
        {column_name},
        (left, right) -> case
                        when left.{field_name} is null then 1
                        when right.{field_name} is null then -1
                        when left.{field_name} < right.{field_name} then 1
                        when left.{field_name} > right.{field_name} then -1
                        else 0
                end)
        """
    )


def pivot_df(
    df: DataFrame,
    pivot_col: str,
    value_col: str,
    grouping_cols: list,
) -> DataFrame:
    """Pivot a dataframe.

    Args:
        df (DataFrame): Dataframe to pivot
        pivot_col (str): Column to pivot on
        value_col (str): Column to pivot
        grouping_cols (list): Columns to group by

    Returns:
        DataFrame: Pivoted dataframe
    """
    pivot_values = df.select(pivot_col).distinct().rdd.flatMap(lambda x: x).collect()
    return (
        df.groupBy(grouping_cols)
        .pivot(pivot_col)
        .agg({value_col: "first"})
        .select(
            grouping_cols
            + [
                f.when(f.col(x).isNull(), None)
                .otherwise(f.col(x))
                .alias(f"{x}_{value_col}")
                for x in pivot_values
            ],
        )
    )
