from pyspark.sql import types as t

from databricks_spark_app.io import writer


def test_escape_sql_double_quoted():
    assert writer._escape_sql_double_quoted('hello "world"') == 'hello \\"world\\"'
    assert writer._escape_sql_double_quoted("no quotes") == "no quotes"


def test_create_table_with_comments_escapes_quotes(mock_spark):
    schema = t.StructType([t.StructField("id", t.IntegerType())])
    writer.create_table_with_comments(
        fqtn="cat.schema.tbl",
        table_comment='a "quoted" comment',
        column_comments={"id": 'the "id" column'},
        force_schema=schema,
    )
    sql = mock_spark.sql.call_args[0][0]
    assert 'COMMENT "the \\"id\\" column"' in sql
    assert 'COMMENT "a \\"quoted\\" comment"' in sql


def test_create_table_with_comments_partitioning(mock_spark):
    schema = t.StructType([t.StructField("id", t.IntegerType()), t.StructField("dt", t.StringType())])
    writer.create_table_with_comments(
        fqtn="cat.schema.tbl",
        table_comment="",
        column_comments=None,
        force_schema=schema,
        partition_by=["dt"],
    )
    sql = mock_spark.sql.call_args[0][0]
    assert "PARTITIONED BY" in sql
    assert "dt STRING" in sql


def test_create_table_with_comments_defaults_do_not_leak_between_calls(mock_spark):
    """Regression guard for the fixed mutable-default-argument bug."""
    schema = t.StructType([t.StructField("id", t.IntegerType())])
    writer.create_table_with_comments(fqtn="cat.schema.a", table_comment="", column_comments=None, force_schema=schema)
    writer.create_table_with_comments(fqtn="cat.schema.b", table_comment="", column_comments=None, force_schema=schema)
    first_sql, second_sql = (call.args[0] for call in mock_spark.sql.call_args_list)
    assert "PARTITIONED BY" not in first_sql
    assert "PARTITIONED BY" not in second_sql


def test_post_sink_hook_escapes_table_comment(mock_spark):
    writer.post_sink_hook(fqtn="cat.schema.tbl", table_comment='a "quoted" comment')
    sql = mock_spark.sql.call_args[0][0]
    assert 'comment"="a \\"quoted\\" comment"' in sql


def test_post_sink_hook_continues_after_failed_column_comment(mock_spark, caplog):
    mock_spark.sql.side_effect = [None, Exception("boom"), None]
    with caplog.at_level("ERROR"):
        writer.post_sink_hook(
            fqtn="cat.schema.tbl",
            table_comment="a table",
            column_comments={"bad_col": "x", "good_col": "y"},
        )
    # table comment + 2 column comment attempts, despite the first raising.
    assert mock_spark.sql.call_count == 3
    assert "Could not set comment for column bad_col" in caplog.text
