"""
Word Count Spark Job - A simple ETL example for Databricks.

This job reads text data, performs word counting, and saves the results.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, trim


def create_spark_session(app_name: str = "WordCountJob") -> SparkSession:
    """Create and configure Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def word_count_job(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Perform word count on input text data.

    Args:
        spark: SparkSession instance
        input_path: Path to input text files
        output_path: Path to save word count results
    """
    # Read text files
    df = spark.read.text(input_path)

    # Split lines into words and count them
    words_df = (
        df.select(explode(split(col("value"), " ")).alias("word"))
        .filter(col("word") != "")
        .select(lower(trim(col("word"))).alias("word"))
        .groupBy("word")
        .count()
        .orderBy(col("count").desc())
    )

    # Save results
    words_df.write.mode("overwrite").parquet(output_path)

    print(f"Word count completed. Results saved to: {output_path}")
    return words_df


def main():
    """Main execution function."""
    spark = create_spark_session("WordCountJob")

    try:
        # Example usage - in a real scenario these would be parameters
        input_path = "/tmp/input/*.txt"
        output_path = "/tmp/output/wordcount"

        result_df = word_count_job(spark, input_path, output_path)

        # Show sample results
        print("Top 10 words:")
        result_df.show(10)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
