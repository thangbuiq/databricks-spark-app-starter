from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import types as t

from databricks_spark_app.io.dataframe import ManagedDataFrame


class _NoSchemaDataFrame(ManagedDataFrame):
    table_comment = "x"
    column_comments = {}

    def process(self):
        return MagicMock()


class _ValidDataFrame(ManagedDataFrame):
    table_comment = "a table"
    column_comments = {"id": "the id"}
    table_schema = t.StructType([t.StructField("id", t.IntegerType())])

    def process(self):
        return MagicMock(name="dataframe")


def test_missing_table_schema_raises_clear_error(mock_spark):
    with pytest.raises(ValueError, match="table_schema must be defined"):
        _NoSchemaDataFrame()


def test_insert_overwrite_delegates_to_writer(mock_spark):
    managed_df = _ValidDataFrame()
    with patch("databricks_spark_app.io.dataframe.insert_overwrite") as mocked_insert_overwrite:
        managed_df.insert_overwrite(fqtn="cat.schema.tbl", partition_by=None)
    mocked_insert_overwrite.assert_called_once()
    _, kwargs = mocked_insert_overwrite.call_args
    assert kwargs["fqtn"] == "cat.schema.tbl"
    assert kwargs["partition_by"] == []
    assert kwargs["table_comment"] == "a table"
    assert kwargs["column_comments"] == {"id": "the id"}
