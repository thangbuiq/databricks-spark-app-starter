"""Main execution pipeline for Databricks Spark application."""

from __future__ import annotations

import argparse
import importlib
import traceback

from databricks.connect import DatabricksSession
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from databricks_spark_app.utils import (
    get_databricks_settings,
    get_logger,
    is_databricks_runtime,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_name", required=True, help="The job module to run defined in jobs/")
    parser.add_argument("--host", default=None, required=False, help="Databricks workspace URL")
    parser.add_argument(
        "--token",
        default=None,
        required=False,
        help="Databricks personal access token (PAT)",
    )
    args = parser.parse_args()
    logger = get_logger()
    try:
        if is_databricks_runtime():
            logger.info("Detected running in Databricks environment.")
            databricks_spark_session = DatabricksSession.builder.getOrCreate()
        else:
            settings = get_databricks_settings(args.host, args.token)
            databricks_spark_session: SparkSession = (
                DatabricksSession.builder.host(settings.databricks_host)
                .token(settings.databricks_token)
                .serverless(True)
                .getOrCreate()
            )

        try:
            logger.info(f"Initialized Spark {databricks_spark_session.version} session.")
            logger.info(f"Running job: {args.job_name}")
        except Exception:
            raise PySparkException("Failed to establish Databricks Spark session")

        job_name: str = args.job_name
        job_module = importlib.import_module(f"databricks_spark_app.jobs.{job_name}")
        job_module.pipeline()

    except Exception as exception:
        logger.error(traceback.format_exc())
        raise exception


if __name__ == "__main__":
    main()
