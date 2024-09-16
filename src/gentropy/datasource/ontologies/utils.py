"""Utility functions for Biosample ontology processing."""
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.sql.functions import col, explode_outer, collect_set, collect_list, array_distinct, regexp_replace, udf, coalesce, first
from pyspark.sql.window import Window
from functools import reduce
from gentropy.dataset.biosample_index import BiosampleIndex

def extract_ontology_from_json(
    ontology_json : str,
    spark : SparkSession
) -> BiosampleIndex:
    """
    Extracts the ontology information from a JSON file. Currently only supports Uberon and Cell Ontology.

    Args:
        ontology_json (str): Path to the JSON file containing the ontology information.
        spark (SparkSession): Spark session.

    Returns:
        BiosampleIndex: Parsed and annotated biosample index table.
    """

    def json_graph_traversal(df, node_col, link_col, traversal_type="ancestors"):
        """
        Traverse a graph represented in a DataFrame to find all ancestors or descendants.
        """
        # Collect graph data as a map
        graph_map = df.select(node_col, link_col).rdd.collectAsMap()
        broadcasted_graph = spark.sparkContext.broadcast(graph_map)

        def get_relationships(node):
            relationships = set()
            stack = [node]
            while stack:
                current = stack.pop()
                if current in broadcasted_graph.value:
                    current_links = broadcasted_graph.value[current]
                    stack.extend(current_links)
                    relationships.update(current_links)
            return list(relationships)

        # Choose column name based on traversal type
        result_col = "ancestors" if traversal_type == "ancestors" else "descendants"

        # Register the UDF based on traversal type
        relationship_udf = udf(get_relationships, ArrayType(StringType()))

        # Apply the UDF to create the result column
        return df.withColumn(result_col, relationship_udf(col(node_col)))

    # Load the JSON file
    df = spark.read.json(ontology_json, multiLine=True)

    # Exploding the 'graphs' array to make individual records easier to access
    df_graphs = df.select(explode_outer("graphs").alias("graph"))

    # Exploding the 'nodes' array within each graph
    df_nodes = df_graphs.select(
        col("graph.id").alias("graph_id"),
        explode_outer("graph.nodes").alias("node"))

    # Exploding the 'edges' array within each graph for relationship data
    df_edges = df_graphs.select(
        col("graph.id").alias("graph_id"),
        explode_outer("graph.edges").alias("edge")
    ).select(
        col("edge.sub").alias("subject"),
        col("edge.pred").alias("predicate"),
        col("edge.obj").alias("object")
    )
    df_edges = df_edges.withColumn("subject", regexp_replace(col("subject"), "http://purl.obolibrary.org/obo/", ""))
    df_edges = df_edges.withColumn("object", regexp_replace(col("object"), "http://purl.obolibrary.org/obo/", ""))

    # Extract the relevant information from the nodes
    transformed_df = df_nodes.select(
    regexp_replace(col("node.id"), "http://purl.obolibrary.org/obo/", "").alias("biosampleId"),
    col("node.lbl").alias("biosampleName"),
    col("node.meta.definition.val").alias("description"),
    collect_set(col("node.meta.xrefs.val")).over(Window.partitionBy("node.id")).getItem(0).alias("dbXrefs"),
    # col("node.meta.deprecated").alias("deprecated"),
    collect_set(col("node.meta.synonyms.val")).over(Window.partitionBy("node.id")).getItem(0).alias("synonyms"))
    
    
    # Extract the relationships from the edges
    # Prepare relationship-specific DataFrames
    df_parents = df_edges.filter(col("predicate") == "is_a").select("subject", "object").withColumnRenamed("object", "parent")
    df_children = df_edges.filter(col("predicate") == "is_a").select("object", "subject").withColumnRenamed("subject", "child")

    # Aggregate relationships back to nodes
    df_parents_grouped = df_parents.groupBy("subject").agg(array_distinct(collect_list("parent")).alias("parents"))
    df_children_grouped = df_children.groupBy("object").agg(array_distinct(collect_list("child")).alias("children"))

    # Get all ancestors
    df_with_ancestors = json_graph_traversal(df_parents_grouped, "subject", "parents", "ancestors")
    # Get all descendants
    df_with_descendants = json_graph_traversal(df_children_grouped, "object", "children", "descendants")

    # Join the ancestor and descendant DataFrames
    df_with_relationships = df_with_ancestors.join(df_with_descendants, df_with_ancestors.subject == df_with_descendants.object, "full_outer").withColumn("biosampleId", coalesce(df_with_ancestors.subject, df_with_descendants.object)).drop("subject", "object")

    # Join the original DataFrame with the relationship DataFrame
    final_df = transformed_df.join(df_with_relationships, ['biosampleId'], "left")
    
    return final_df

def merge_biosample_indices(
         biosample_indices: list[BiosampleIndex], 
    ) -> BiosampleIndex:
    """Merge a list of biosample indexes into a single biosample index.
    Where there are conflicts, in single values - the first value is taken. In list values, the union of all values is taken.

    Args:
        biosample_indexes (BiosampleIndex): Biosample indexes to merge.

    Returns:
        BiosampleIndex: Merged biosample index.
    """
    
    def merge_lists(lists):
        """Merge a list of lists into a single list."""
        return list(set([item for sublist in lists if sublist is not None for item in sublist]))
    
    # Make a spark udf (user defined function) to merge lists
    merge_lists_udf = udf(merge_lists, ArrayType(StringType()))

    # Merge the DataFrames
    merged_df = reduce(DataFrame.unionAll, biosample_indices)
    
    # Define dictionary of columns and corresponding aggregation functions
    # Currently this will take the first value for single values and merge lists for list values
    agg_funcs = {}
    for column in merged_df.columns:
        if column != 'biosampleId':
            if 'list' in column:  # Assuming column names that have 'list' need list merging
                agg_funcs[column] = merge_lists_udf(collect_list(column)).alias(column)
            else:
                agg_funcs[column] = first(column, ignorenulls=True).alias(column)

    # Group by biosampleId and aggregate the columns
    merged_df = merged_df.groupBy('biosampleId').agg(agg_funcs)

    return merged_df