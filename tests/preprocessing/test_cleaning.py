from chispa import assert_df_equality
from pyspark.sql import SparkSession

from example_ci_cd_databricks.preprocessing.cleaning import format_names


def test_format_names(spark: SparkSession):
    schema = ", ".join(
        [
            "country string",
            "first_name string",
            "last_name string",
            "age int",
            "email string",
        ]
    )

    input_df = spark.createDataFrame(
        [
            ["US", "John", "Doe", 30, "john.doe@gmail.com"],
            ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
        ],
        schema
    )

    actual_df = format_names(input_df)

    expected_df = spark.createDataFrame(
        [
            ["US", "john", "doe", 30, "john.doe@gmail.com"],
            ["US", "jane", "smith", 40, "jane.smith@gmail.com"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
        ],
        schema
    )

    assert_df_equality(actual_df, expected_df)
