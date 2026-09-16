# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A starter kit for building PySpark applications that run locally via Databricks Connect and deploy to Databricks Jobs as a Python Wheel Task. There is no cluster-side code path during local dev — every job runs against a real (serverless) Databricks Spark session over Databricks Connect.

## Commands

```bash
make install   # uv sync --all-groups --all-extras
make format    # ruff check --select I,F --fix . && ruff format .
make whl       # format, then uv build --verbose --wheel
make test      # uv run pytest
```

Run a job locally (requires `.env` with `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, copied from `.env.example`):

```bash
spark_app --job_name <job_module_name>
# e.g. spark_app --job_name sample_simple_job --run_date "2025-09-01"
```

Deploy a job to Databricks Jobs via Databricks Asset Bundles (builds the wheel, uploads to a UC volume, creates/updates the job; scheduling itself is done in the Databricks UI under Schedules & Triggers). Requires the standalone Databricks CLI and `DATABRICKS_HOST`/`DATABRICKS_TOKEN` exported for the target workspace — there's no host hardcoded in `databricks.yml`, so re-export those two vars when switching between `uat` and `prod`:

```bash
databricks bundle deploy -t uat --var="job_name=<job_module_name>"   # or -t prod
databricks bundle run spark_app_job -t uat                           # trigger a run
```

Unit tests live in `tests/` (pytest, `make test`) and cover only pure/mockable logic in `io/writer.py` and `io/dataframe.py` — `databricks-connect`'s `pyspark` shim raises `RuntimeError` for any non-Connect master, so there is no local `SparkSession` and no way to test the actual Unity Catalog table-creation/insert-overwrite paths locally; those tests mock `SparkSession.getActiveSession()` (see `tests/conftest.py`) rather than run real Spark. There is no integration-test suite against a live workspace. Lint/format is `ruff` (line-length 120, rules `E`, `W`, `F`, `I001`, `RUF022`, `B`, `UP`), also runnable via `.pre-commit-config.yaml`. CI (`.github/workflows/build.yaml`) runs a `lint` job (`ruff check` + `ruff format --check`), a `test` job (`pytest`), and the original `uv build --wheel` job on push/PR to `main`.

## Architecture

- **`jobs/`** is the only directory app authors normally touch. Each file is a standalone job module exposing a `pipeline()` function as its entry point; the filename (without `.py`) is the `--job_name` used both locally and when deployed. Treat each job file as independent/self-contained — there's no shared job registry.
- **`pipeline.py`** (`spark_app` CLI entry point) is the local/Databricks-runtime dispatcher: it detects whether it's running inside a Databricks runtime (`DATABRICKS_RUNTIME_VERSION` env var) vs. locally, obtains a `SparkSession` accordingly (serverless Databricks Connect locally, `DatabricksSession.builder.getOrCreate()` in-runtime), declares any `DatabricksAdditionalParams` as Spark SQL variables, dynamically imports `databricks_spark_app.jobs.<job_name>`, and calls its `pipeline()`.
- **`databricks.yml`** + **`resources/jobs/spark_app_job.yml`** define a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) that replaces the old imperative `deploy.py`/`DatabricksDeployer` (removed). `databricks bundle deploy -t <uat|prod>` builds the wheel (`uv build --wheel`, declared as a bundle artifact), uploads it to a Unity Catalog volume path (`/Volumes/<catalog>/<schema>/python_wheels`, per-target `catalog`/`schema` variables), and creates/updates a single Databricks Job (`spark_app_job`) running it as a `PythonWheelTask` (entry point `spark_app`, package `databricks_spark_app`). Which job module it runs is set by the `job_name` bundle variable (`--var="job_name=<job_module_name>"`), matching the job's `job_name` parameter consumed by `pipeline.py`. Auth comes from `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars, not a hardcoded host — there is no local/Databricks-runtime distinction here since bundle deploys always run outside a Databricks runtime, from the developer's machine or CI.
- **`config.py`** defines `DatabricksSettings` (host/token, loaded from `.env` via pydantic-settings) and `DatabricksAdditionalParams` — the place to add new job parameters for local runs. Any field added there automatically becomes a `--<field_name>` CLI arg in `pipeline.py`, and is exposed inside job SQL as a Spark SQL variable via `` `params.<field_name>` `` (backtick, not quote). This is the *only* way to pass parameters into a job, because Spark Connect sessions don't support mutating Spark configs directly. Note this is a separate, parallel definition from the bundle's `variables`/`parameters` in `databricks.yml`/`resources/jobs/spark_app_job.yml` — adding a field here does not automatically add it to the deployed job; mirror it in both places.
- **`io/writer.py`** — functional API: `insert_overwrite(fqtn, spark_df, force_schema, ...)` casts columns to `force_schema`, creates the table with column/table comments and partitioning if it doesn't exist, then does a dynamic-partition overwrite insert, followed by `post_sink_hook` to (re)apply table/column comments (partition column comments can't be altered post-creation).
- **`io/dataframe.py`** — class-based API: `ManagedDataFrame` is an ABC that pairs a `process() -> DataFrame` implementation with `table_schema`/`table_comment`/`column_comments` class attributes, then calls the same `insert_overwrite` under the hood via its own `.insert_overwrite(fqtn, partition_by)` method. Prefer this for jobs where schema/comments should live next to the transformation logic; use the plain `insert_overwrite` function for simple one-off jobs.
- **`utils.py`** — `is_databricks_runtime()`, `get_databricks_settings()` (resolves host/token from args → `.env` → env vars, in that order), and `get_logger()`.

## Conventions specific to this repo

- If you rename the `src/databricks_spark_app` package directory, update every reference to `databricks_spark_app` across the repo (pyproject.toml entry points, hatch build target, imports) — there's no dynamic package-name resolution.
- New job files go directly in `src/databricks_spark_app/jobs/`; the module is imported dynamically by filename, so no `__init__.py` registration is needed there.
- New job-level parameters belong in `DatabricksAdditionalParams` (`config.py`), not as ad hoc argparse flags in `pipeline.py`.
