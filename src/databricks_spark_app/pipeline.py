"""Main execution pipeline for Databricks Spark application."""

import argparse
import importlib
import traceback

from databricks.connect import DatabricksSession
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from databricks_spark_app.config import DatabricksAdditionalParams
from databricks_spark_app.utils import get_databricks_settings, get_logger, is_databricks_runtime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_name", required=True, help="The job module to run defined in jobs/")
    parser.add_argument("--host", default=None, required=False, help="Databricks workspace URL")
    parser.add_argument("--token", default=None, required=False, help="Databricks token (PAT)")
    for field_name, field_configs in DatabricksAdditionalParams.model_fields.items():
        parser.add_argument(
            f"--{field_name}", default=field_configs.default, required=False, help=f"Job parameter: {field_name}"
        )
    args = parser.parse_args()
    logger = get_logger()

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
        for field_name in DatabricksAdditionalParams.model_fields.keys():
            logger.info(f"Setting job parameter {field_name} from args: {vars(args).get(field_name)}")
            param_value = vars(args).get(field_name)
            databricks_spark_session.sql(f"DECLARE OR REPLACE `params.{field_name}` = '{param_value}'")
        job_name = args.job_name
        logger.info(f"Initialized Spark {databricks_spark_session.version} session.")
        logger.info(f"Running job: {job_name}")
        job_module = importlib.import_module(f"databricks_spark_app.jobs.{job_name}")
        job_module.pipeline()
    except Exception as job_exception:
        logger.error(traceback.format_exc())
        raise PySparkException(f"Job {job_name} failed: {job_exception}") from job_exception
    finally:
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
