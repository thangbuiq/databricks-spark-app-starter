"""Module for writing DataFrames to Delta tables with insert overwrite functionality."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t

logger = logging.getLogger(__name__)


def _escape_sql_double_quoted(value: str) -> str:
    """Escape double quotes so a comment value can't break out of a double-quoted SQL literal."""
    return value.replace('"', '\\"')


def create_table_with_comments(
    fqtn: str,
    table_comment: str,
    column_comments: dict[str, str] | None,
    force_schema: t.StructType,
    partition_by: list[str] | None = None,
) -> None:
    """
    Create table with schema and comments including partition columns.
    """
    partition_by = partition_by or []
    column_comments = column_comments or {}
    spark = SparkSession.getActiveSession()
    column_definitions = []
    partition_columns = []

    for field in force_schema.fields:
        comment_clause = ""
        if field.name in column_comments:
            comment_clause = f' COMMENT "{_escape_sql_double_quoted(column_comments[field.name])}"'

        column_def = f"{field.name} {field.dataType.simpleString().upper()}{comment_clause}"

        if field.name in partition_by:
            partition_columns.append(column_def)
        else:
            column_definitions.append(column_def)

    columns_sql = ",\n  ".join(column_definitions)
    create_table_sql_stm = f"CREATE TABLE IF NOT EXISTS {fqtn} (\n  {columns_sql}\n)"

    if partition_columns:
        partitioned_by_sql = ",\n  ".join(partition_columns)
        create_table_sql_stm += f" PARTITIONED BY (\n  {partitioned_by_sql}\n)"

    if table_comment:
        create_table_sql_stm += f'\nCOMMENT "{_escape_sql_double_quoted(table_comment)}"'

    logger.info("Creating table with SQL: \n" + create_table_sql_stm)
    spark.sql(create_table_sql_stm)
    logger.info(f"Table {fqtn} created with schema and comments.")


def insert_overwrite(
    fqtn: str,
    spark_df: DataFrame,
    force_schema: t.StructType,
    table_comment: str = "",
    column_comments: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
):
    """
    Insert overwrite into a Delta table. Create table if it does not exist.

    Args:
        fqtn (str): Fully qualified table name.
        spark_df (DataFrame): DataFrame to write.
        force_schema (StructType): Schema of the table.
        table_comment (str): Comment for the table.
        column_comments (dict, optional): Comments for the columns.
        partition_by (list, optional): List of partition columns.
    """
    column_comments = column_comments or {}
    partition_by = partition_by or []
    spark = SparkSession.getActiveSession()
    is_table_exists: bool = spark.catalog.tableExists(fqtn)

    for field in force_schema.fields:
        if field.name in spark_df.columns:
            spark_df = spark_df.withColumn(field.name, spark_df[field.name].cast(field.dataType))

    if not is_table_exists:
        logger.info("Table does not exist. Creating table...")
        create_table_with_comments(fqtn, table_comment, column_comments, force_schema, partition_by)
    else:
        logger.info("Table already exists. Skipping creation.")

    existing_df = spark.sql(f"SELECT * FROM {fqtn}")
    spark_df = spark_df.select(*existing_df.schema.names)
    dynamic_partition_writer = spark_df.write.option("partitionOverwriteMode", "dynamic")
    dynamic_partition_writer.mode("overwrite").insertInto(fqtn)
    logger.info(f"Sink process completed for table {fqtn}.")
    post_sink_hook(fqtn, table_comment, column_comments, partition_by)


def post_sink_hook(
    fqtn: str,
    table_comment: str = "",
    column_comments: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
) -> None:
    """
    Update table and column comments after data insertion.
    Note: Partition column comments cannot be altered after table creation.

    Args:
        fqtn (str): Fully qualified table name.
        table_comment (str): Comment for the table.
        column_comments (dict, optional): Comments for the columns.
        partition_by (list, optional): List of partition columns.
    """
    column_comments = column_comments or {}
    partition_by = partition_by or []
    spark = SparkSession.getActiveSession()
    logger.info(f"Post sink operations completed for table {fqtn}.")

    if table_comment:
        spark.sql(f'ALTER TABLE {fqtn} SET TBLPROPERTIES ("comment"="{_escape_sql_double_quoted(table_comment)}")')

    if column_comments:
        for column, comment in column_comments.items():
            if column not in partition_by:
                try:
                    spark.sql(
                        f'ALTER TABLE {fqtn} ALTER COLUMN {column} COMMENT "{_escape_sql_double_quoted(comment)}"'
                    )
                except Exception:
                    logger.exception(f"Could not set comment for column {column}")

    logger.info(f"Updated table {fqtn} with comments and properties.")
