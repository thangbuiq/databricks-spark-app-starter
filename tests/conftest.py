from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def mock_spark(monkeypatch):
    """Stand in for the active SparkSession.

    databricks-connect's pyspark shim raises RuntimeError for any master other than a
    Databricks Connect remote session, so a real local SparkSession can't be created here.
    """
    spark = MagicMock(name="SparkSession")
    monkeypatch.setattr(SparkSession, "getActiveSession", classmethod(lambda cls: spark))
    return spark
