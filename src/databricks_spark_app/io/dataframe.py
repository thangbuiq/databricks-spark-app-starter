from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from databricks_spark_app.io.writer import insert_overwrite

logger = logging.getLogger(__name__)


class ManagedDataFrame(ABC):
    """Base class for managed table operations."""

    table_comment: str
    column_comments: dict[str, str]
    table_schema: StructType | None = None

    def __init__(self):
        self.spark: SparkSession = SparkSession.getActiveSession()
        if not self.table_schema:
            raise ValueError("table_schema must be defined.")

    @abstractmethod
    def process(self, *args, **kwargs) -> DataFrame:
        """
        Process data and return a Spark DataFrame.
        """
        raise NotImplementedError("Subclasses must implement the 'process' method.")

    def insert_overwrite(self, fqtn: str, partition_by: list[str] | None = None) -> None:
        """
        Write processed DataFrame to the specified table.
        """
        df = self.process()
        insert_overwrite(
            spark_df=df,
            force_schema=self.table_schema,
            table_comment=self.table_comment,
            column_comments=self.column_comments,
            fqtn=fqtn,
            partition_by=partition_by or [],
        )
        logger.info(f"Insert overwrite process completed for table {fqtn}.")
