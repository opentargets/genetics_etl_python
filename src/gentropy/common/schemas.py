"""Methods for handling schemas."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from collections import defaultdict

from pyspark.sql.types import ArrayType, StructType

from gentropy.assets import schemas


class SchemaValidationError(Exception):
    """This exception is raised when a schema validation fails."""

    def __init__(
        self: SchemaValidationError, message: str, errors: defaultdict[str, list[str]]
    ) -> None:
        """Initialize the SchemaValidationError.

        Args:
            message (str): The message to be displayed.
            errors (defaultdict[str, list[str]]): The collection of observed discrepancies
        """
        super().__init__(message)
        self.message = message  # Explicitly set the message attribute
        self.errors = errors

    def __str__(self: SchemaValidationError) -> str:
        """Return a string representation of the exception.

        Returns:
            str: The string representation of the exception.
        """
        stringified_errors = "\n  ".join(
            [f'{k}: {",".join(v)}' for k, v in self.errors.items()]
        )
        return f"{self.message}\nErrors:\n  {stringified_errors}"


def parse_spark_schema(schema_json: str) -> StructType:
    """Parse Spark schema from JSON.

    Args:
        schema_json (str): JSON filename containing spark schema in the schemas package

    Returns:
        StructType: Spark schema
    """
    core_schema = json.loads(
        pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
    )
    return StructType.fromJson(core_schema)


def compare_array_schemas(
    observed_schema: ArrayType,
    expected_schema: ArrayType,
    parent_field_name: str | None = None,
    schema_issues: defaultdict[str, list[str]] | None = None,
) -> defaultdict[str, list[str]]:
    """Compare two array schemas.

    The comparison is done recursively, so nested structs are also compared.

    Args:
        observed_schema (ArrayType): The observed schema.
        expected_schema (ArrayType): The expected schema.
        parent_field_name (str | None): The parent field name. Defaults to None.
        schema_issues (defaultdict[str, list[str]] | None): The schema issues. Defaults to None.

    Returns:
        defaultdict[str, list[str]]: The schema issues.
    """
    # Create default values if not provided:
    if schema_issues is None:
        schema_issues = defaultdict(list)

    if parent_field_name is None:
        parent_field_name = ""

    observed_type = observed_schema.elementType.typeName()
    expected_type = expected_schema.elementType.typeName()

    # If element types are not matching, no further tests are needed:
    if observed_type != expected_type:
        schema_issues["columns_with_non_matching_type"].append(
            f'For column "{parent_field_name}[]" found {observed_type} instead of {expected_type}'
        )

    # If element type is a struct, resolve nesting:
    elif (observed_type == "struct") and (expected_type == "struct"):
        schema_issues = compare_struct_schemas(
            observed_schema.elementType,
            expected_schema.elementType,
            f"{parent_field_name}[].",
            schema_issues,
        )

    # If element type is an array, resolve nesting:
    elif (observed_type == "array") and (expected_type == "array"):
        schema_issues = compare_array_schemas(
            observed_schema.elementType,
            expected_schema.elementType,
            parent_field_name,
            schema_issues,
        )

    return schema_issues


def compare_struct_schemas(
    observed_schema: StructType,
    expected_schema: StructType,
    parent_field_name: str | None = None,
    schema_issues: defaultdict[str, list[str]] | None = None,
) -> defaultdict[str, list[str]]:
    """Compare two struct schemas.

    The comparison is done recursively, so nested structs are also compared.

    Checking logic:
    1. Checking for duplicated columns in the observed schema.
    2. Checking for missing mandatory columns in the observed schema.
    3. Now we know that all mandatory columns are present, we can iterate over the observed schema and compare the types.
    4. Flagging unexpected columns in the observed schema.
    5. Flagging columns with non-matching types.
    6. If a column is a struct -> call compare_struct_schemas
    7. If a column is an array -> call compare_array_schemas
    8. Return dictionary with issues.

    Args:
        observed_schema (StructType): The observed schema.
        expected_schema (StructType): The expected schema.
        parent_field_name (str | None): The parent field name. Defaults to None.
        schema_issues (defaultdict[str, list[str]] | None): The schema issues. Defaults to None.

    Returns:
        defaultdict[str, list[str]]: The schema issues.
    """
    # Create default values if not provided:
    if schema_issues is None:
        schema_issues = defaultdict(list)

    if parent_field_name is None:
        parent_field_name = ""

    # Flagging duplicated columns if present:
    if duplicated_columns := list(
        {
            f"{parent_field_name}{field.name}"
            for field in observed_schema
            if list(observed_schema).count(field) > 1
        }
    ):
        schema_issues["duplicated_columns"] += duplicated_columns

    # Testing mandatory fields:
    required_fields = [x.name for x in expected_schema if not x.nullable]
    if missing_required_fields := [
        f"{parent_field_name}{req}"
        for req in required_fields
        if not any(field.name == req for field in observed_schema)
    ]:
        schema_issues["missing_mandatory_columns"] += missing_required_fields

    # Converting schema to dictionaries for easier comparison:
    expected_schema_dict = {field.name: field for field in expected_schema}

    # Testing optional fields and types:
    for field in observed_schema:
        field_name = field.name
        # Testing observed field name, if name is not matched, no further tests are needed:
        if field_name not in expected_schema_dict:
            schema_issues["unexpected_columns"].append(
                f"{parent_field_name}{field_name}"
            )
            continue

        # When we made sure the field is in both schemas, extracting field type information:
        observed_type = field.dataType
        observed_type_name = field.dataType.typeName()

        expected_type = expected_schema_dict[field_name].dataType
        expected_type_name = expected_schema_dict[field_name].dataType.typeName()

        # Flagging non-matching types if types don't match, jumping to next field:
        if observed_type_name != expected_type_name:
            schema_issues["columns_with_non_matching_type"].append(
                f'For column "{parent_field_name}{field_name}" found {observed_type_name} instead of {expected_type_name}'
            )
            continue

        # If column is a struct, resolve nesting:
        if observed_type_name == "struct":
            schema_issues = compare_struct_schemas(
                observed_type,
                expected_type,
                f"{parent_field_name}{field_name}.",
                schema_issues,
            )
        # If column is an array, resolve nesting:
        elif observed_type_name == "array":
            schema_issues = compare_array_schemas(
                observed_type,
                expected_type,
                f"{parent_field_name}{field_name}[]",
                schema_issues,
            )

        # Extract metadata:
        expected_metadata = expected_schema_dict[field_name].metadata

        # Update observed metadata with expected metadata:
        field.metadata = expected_metadata

    return schema_issues
