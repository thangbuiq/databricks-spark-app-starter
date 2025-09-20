import logging

from pyspark.sql import SparkSession

from databricks_spark_app.io.writer import insert_overwrite

logger = logging.getLogger(__name__)


def pipeline():
    spark = SparkSession.getActiveSession()

    df = spark.sql("""
        SELECT
            'Hello, Databricks!' AS message,
            CAST(`params.run_date` AS STRING) AS part_date
    """)
    df.show(truncate=False)
    spark.sql("CREATE DATABASE IF NOT EXISTS temp_db")
    insert_overwrite(
        fqtn="temp_db.hello_table",
        spark_df=df,
        force_schema=df.schema,
        table_comment="Sample table for insert overwrite demonstration",
        column_comments={"message": "A greeting message", "part_date": "Partition date"},
        partition_by=["part_date"],
    )
    logger.info("Sample job completed successfully.")
