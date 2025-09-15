"""Configuration for Databricks Spark App."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class DatabricksSettings(BaseSettings):
    databricks_host: str
    databricks_token: str

    class Config:
        env_file = ".env"

    def __init__(self, **data):
        super().__init__(**data)
        self.validate_settings()

    def validate_settings(self):
        if not self.databricks_host or not self.databricks_token:
            raise ValueError("Environment variables for Databricks are not set properly.")
