"""Module to deploy a Python wheel to Databricks and create a job to run it."""

import argparse
import logging
import os
import subprocess
import sys
import traceback
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog, compute, jobs

from databricks_spark_app.config import DatabricksAdditionalParams, DatabricksSettings
from databricks_spark_app.utils import get_logger, is_databricks_runtime

logger = logging.getLogger(__name__)


class DatabricksDeployer:
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        catalog_name: str = "workspace",
        schema_name: str = "default",
        wheels_dir: str = "python_wheels",
    ):
        self.workspace_client: WorkspaceClient = workspace_client
        self.catalog_name: str = catalog_name
        self.schema_name: str = schema_name
        self.wheels_dir: str = wheels_dir
        self.datetime_str = datetime.now().strftime("%Y%m%d")

    def _build_wheel(self) -> str:
        """Builds the Python wheel using uv."""
        try:
            if not os.path.exists("pyproject.toml"):
                logger.error("pyproject.toml not found in the current directory.")
                sys.exit(1)
            if os.path.exists("dist"):
                subprocess.run(["rm", "-rf", "dist"], check=True)
            subprocess.run(["uv", "build", "--verbose", "--wheel"], check=True)
            whl_files = [f for f in os.listdir("dist") if f.endswith(".whl")]
            if not whl_files:
                raise FileNotFoundError("No wheel file found in the dist directory.")
            whl_path = os.path.join("dist", whl_files[0])
            logger.info(f"Built wheel at {whl_path}")
            return whl_path
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build wheel: {e}")
            sys.exit(1)

    def _upload_file(self, local_path: str, job_name: str) -> str:
        """Uploads a file to Databricks workspace."""
        base_filename = os.path.basename(local_path)
        catalog_name = self.catalog_name
        schema_name = self.schema_name
        wheels_dir = self.wheels_dir
        unity_catalog_path = f"/Volumes/{catalog_name}/{schema_name}/{wheels_dir}/{job_name}/{self.datetime_str}"
        file_path = f"{unity_catalog_path}/{base_filename}"
        logger.info(f"Uploading file from {local_path} to {file_path}")
        try:
            volumes_list = list(self.workspace_client.volumes.list(catalog_name, schema_name))
            if not any(v.name == wheels_dir for v in volumes_list):
                self.workspace_client.volumes.create(
                    catalog_name=self.catalog_name,
                    schema_name=self.schema_name,
                    name=self.wheels_dir,
                    volume_type=catalog.VolumeType.MANAGED,
                )
            self.workspace_client.files.create_directory(unity_catalog_path)  # treat as mkdir -p
            with open(local_path, "rb") as file:
                self.workspace_client.files.upload(
                    file_path=file_path,
                    contents=file.read(),
                    overwrite=True,
                )
            logger.info(f"Successfully uploaded to {file_path}")
            return file_path
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {local_path}")
        except Exception as e:
            raise Exception(f"Failed to upload file: {e}")

    def _create_wheel_job(
        self,
        job_name: str,
        whl_workspace_path: str,
        package_name: str,
        entry_point: str,
    ) -> int:
        """Creates a job that runs a Python wheel task. Returns job_id."""
        environment_key = "spark_app_environment"
        job_params = [jobs.JobParameterDefinition(name="job_name", default=job_name)]
        additional_params = DatabricksAdditionalParams()
        for field_name, value in additional_params.model_dump().items():
            job_params.append(jobs.JobParameterDefinition(name=field_name, default=value))

        job = self.workspace_client.jobs.create(
            name=f"spark_app_job_{job_name}_{self.datetime_str}",
            email_notifications=jobs.JobEmailNotifications(no_alert_for_skipped_runs=False),
            webhook_notifications=jobs.WebhookNotifications(),
            timeout_seconds=0,
            max_concurrent_runs=1,
            tasks=[
                jobs.Task(
                    task_key=job_name,
                    run_if=jobs.RunIf.ALL_SUCCESS,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name=package_name,
                        entry_point=entry_point,
                    ),
                    timeout_seconds=0,
                    email_notifications=jobs.JobEmailNotifications(),
                    webhook_notifications=jobs.WebhookNotifications(),
                    environment_key=environment_key,
                )
            ],
            queue=jobs.QueueSettings(enabled=True),
            parameters=job_params,
            environments=[
                jobs.JobEnvironment(
                    environment_key=environment_key,
                    spec=compute.Environment(
                        environment_version="4",
                        dependencies=[whl_workspace_path],
                    ),
                ),
            ],
            performance_target=jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED,
        )
        logger.info(f"Created job with ID: {job.job_id}")
        return job.job_id

    def run_workflow(
        self,
        job_name: str,
        package_name: str,
        entry_point: str,
    ) -> int | None:
        """Runs the deployment workflow: builds wheel, uploads it, and creates a job."""
        try:
            local_whl_path = self._build_wheel()
            uploaded_path = self._upload_file(local_whl_path, job_name)
            if not uploaded_path:
                logger.error("Upload failed. Aborting job creation.")
                return None
            job_id = self._create_wheel_job(
                job_name=job_name,
                whl_workspace_path=uploaded_path,
                package_name=package_name,
                entry_point=entry_point,
            )
            logger.info(f"Please check the job in Databricks UI. Job ID: {job_id}")
            return job_id
        except Exception as e:
            logger.error(f"Deployment workflow failed: {e}")
            logger.error(traceback.format_exc())
            return None


def main():
    logger = get_logger()
    parser = argparse.ArgumentParser(description="Deploy wheel job to Databricks.")
    parser.add_argument("--job_name", required=True, help="Name of the Databricks job.")
    args = parser.parse_args()

    if is_databricks_runtime():
        logger.error("Deployment script should not be run in a Databricks environment.")
        sys.exit(1)

    settings = DatabricksSettings()
    client = WorkspaceClient(
        host=settings.databricks_host,
        token=settings.databricks_token,
    )
    deployer = DatabricksDeployer(workspace_client=client)
    deployer.run_workflow(
        job_name=args.job_name,
        package_name="databricks_spark_app",
        entry_point="spark_app",
    )


if __name__ == "__main__":
    main()
