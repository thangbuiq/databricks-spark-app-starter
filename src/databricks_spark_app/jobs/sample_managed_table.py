import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t

from databricks_spark_app.io.dataframe import ManagedDataFrame

logger = logging.getLogger(__name__)


class SampleHelloTable(ManagedDataFrame):
    table_comment = "Sample table for insert overwrite demonstration"
    column_comments = {
        "message": "A greeting message",
        "part_date": "Partition date",
    }
    table_schema = t.StructType(
        [
            t.StructField("message", t.StringType(), nullable=False),
            t.StructField("part_date", t.StringType(), nullable=False),
        ]
    )

    def process(self) -> DataFrame:
        spark = SparkSession.getActiveSession()
        df = spark.sql("""
            SELECT
                'Hello, Databricks! From ManagedDataFrame' AS message,
                CAST(CURRENT_DATE() AS STRING) AS part_date
        """)
        return df


def pipeline():
    hello_table = SampleHelloTable()
    hello_table.insert_overwrite(fqtn="temp_db.hello_table", partition_by=["part_date"])
    logger.info("Sample job completed successfully.")
