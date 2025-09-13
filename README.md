# databricks-spark-app-starter

Starter kit for building and deploying Apache Spark applications on Databricks with production-ready structure, best practices, and CI/CD integration.

This project uses [uv](https://docs.astral.sh/uv/) for fast Python package management and includes sample Spark jobs for data processing.

## Quick Start

1. Install dependencies:
   ```bash
   make dev-install  # Install all dependencies including dev tools
   ```

2. Format and lint code:
   ```bash
   make format  # Format code with ruff (includes isort and formatting)
   make lint    # Run linting checks
   ```

3. Run sample jobs:
   ```bash
   # Simple ETL job with sample data
   uv run python -m src.databricks_spark_app.simple_etl
   
   # Word count job (requires input files)
   uv run python -m src.databricks_spark_app.wordcount
   ```

## Project Structure

```
├── src/databricks_spark_app/       # Main package directory
│   ├── __init__.py                 # Package initialization
│   ├── wordcount.py                # Word count Spark job
│   └── simple_etl.py               # Simple ETL Spark job
├── pyproject.toml                  # Project configuration and dependencies
├── uv.lock                         # Locked dependencies for reproducibility
└── Makefile                        # Common development tasks
```

## Dependencies

- **pyspark**: Apache Spark Python API
- **databricks-connect**: Databricks Connect for local development
- **ruff**: Fast Python linter and formatter (dev dependency)

## Development

Available make commands:
- `make help` - Show available commands
- `make install` - Install production dependencies
- `make dev-install` - Install development dependencies  
- `make format` - Format code with ruff
- `make lint` - Run linting checks
- `make clean` - Clean up build artifacts and cache

## Sample Jobs

### Simple ETL Job
Demonstrates basic Extract, Transform, Load operations with sample data:
- Creates sample customer data
- Applies data transformations (cleaning, categorization)
- Shows before/after data states

### Word Count Job
Classic Spark word count example:
- Reads text files from input path
- Counts word frequencies
- Saves results to output path
- Demonstrates basic DataFrame operations
