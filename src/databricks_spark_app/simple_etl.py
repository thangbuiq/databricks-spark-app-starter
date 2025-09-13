"""
Simple ETL Job - Data transformation example for Databricks.

This job demonstrates basic ETL operations: Extract, Transform, Load.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper, when
from pyspark.sql.types import StringType, StructField, StructType


def create_spark_session(app_name: str = "SimpleETLJob") -> SparkSession:
    """Create and configure Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def extract_data(spark: SparkSession, source_path: str):
    """Extract data from source."""
    return spark.read.option("header", "true").csv(source_path)


def transform_data(df):
    """
    Transform the data with basic cleaning and processing.

    Args:
        df: Input DataFrame

    Returns:
        Transformed DataFrame
    """
    return (
        df.filter(col("name").isNotNull())
        .withColumn("name_upper", upper(col("name")))
        .withColumn("name_clean", regexp_replace(col("name_upper"), "[^A-Z0-9\\s]", ""))
        .withColumn(
            "age_category",
            when(col("age").cast("int") < 18, "Minor")
            .when(col("age").cast("int") < 65, "Adult")
            .otherwise("Senior"),
        )
    )


def load_data(df, output_path: str) -> None:
    """Load transformed data to destination."""
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data loaded successfully to: {output_path}")


def create_sample_data(spark: SparkSession):
    """Create sample data for testing."""
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )

    sample_data = [
        ("John Doe", "25", "New York"),
        ("Jane Smith", "30", "San Francisco"),
        ("Bob Johnson", "45", "Chicago"),
        ("Alice Brown", "22", "Los Angeles"),
        ("Charlie Wilson", "67", "Miami"),
    ]

    return spark.createDataFrame(sample_data, schema)


def simple_etl_job(
    spark: SparkSession, input_path: str = None, output_path: str = "/tmp/output/etl"
):
    """
    Execute the ETL pipeline.

    Args:
        spark: SparkSession instance
        input_path: Path to input data (optional, will use sample data if None)
        output_path: Path to save processed data
    """
    # Extract
    if input_path:
        df = extract_data(spark, input_path)
    else:
        print("Using sample data for demonstration")
        df = create_sample_data(spark)

    print("Original data:")
    df.show()

    # Transform
    transformed_df = transform_data(df)

    print("Transformed data:")
    transformed_df.show()

    # Load
    load_data(transformed_df, output_path)

    return transformed_df


def main():
    """Main execution function."""
    spark = create_spark_session("SimpleETLJob")

    try:
        # Run ETL job with sample data
        simple_etl_job(spark)

        print("ETL job completed successfully!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
