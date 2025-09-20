import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def pipeline():
    spark = SparkSession.getActiveSession()
    df = spark.sql("SELECT 'Hello, Databricks!' AS message")
    df.show(truncate=False)
    logger.info("Sample job completed successfully.")
