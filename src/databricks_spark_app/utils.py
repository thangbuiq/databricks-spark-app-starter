"""Utility functions for Databricks Spark App."""

from __future__ import annotations

import logging
import os

from databricks_spark_app.config import DatabricksSettings


def is_databricks_runtime():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_databricks_settings(databricks_host=None, databricks_token=None):
    if databricks_host and databricks_token:
        databricks_settings = DatabricksSettings(databricks_host, databricks_token)
    elif os.path.exists(".env"):
        databricks_settings = DatabricksSettings()
    elif os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        databricks_settings = DatabricksSettings(
            databricks_host=os.environ["DATABRICKS_HOST"],
            databricks_token=os.environ["DATABRICKS_TOKEN"],
        )
    else:
        raise ValueError("Please provide DATABRICKS_HOST and DATABRICKS_TOKEN.")

    return databricks_settings


def get_logger(log_level=logging.INFO):
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(lineno)d - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info("Logging is set up.")
    return logger
