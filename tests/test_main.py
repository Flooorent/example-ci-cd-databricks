from chispa import assert_df_equality
from pyspark.sql import SparkSession

from example_ci_cd_databricks.main import main


SCHEMA = ", ".join(
    [
        "country string",
        "first_name string",
        "last_name string",
        "age int",
        "email string",
        "status string",
    ]
)


def set_up_init(spark: SparkSession, table_path: str):
    (
        spark
            .createDataFrame(
                [
                    ["US", "John", "Doe", 30, "john.doe@gmail.com", "Added"],
                    ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com", "Added"],
                    ["FR", "laura", "simon", 50, "laura.simon@gmail.com", "Added"],
                ],
                schema=SCHEMA,
            )
            .write
            .format("delta")
            .save(table_path)
    )


def set_up_merge(spark: SparkSession, table_path: str):
    (
        spark
            .createDataFrame(
                [
                    ["US", "John", "Doe", 31, "john.doe@gmail.com", "Updated"],
                    ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com", "Removed"],
                    ["IT", "marco", "simone", 70, "marco.simone@gmail.com", "Added"],
                ],
                schema=SCHEMA
            )
            .write
            .format("delta")
            .save(table_path)
    )


def test_init(spark: SparkSession, temp_dir: str):
    input_path = f"{temp_dir}/main/input_data"
    output_path = f"{temp_dir}/main/output_table"

    args = {
        "input_path": input_path,
        "output_path": output_path,
        "action": "init",
    }

    set_up_init(spark, input_path)
    main(spark, args)

    actual_df = spark.read.format("delta").load(output_path)
    actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{output_path}`").select("partitionColumns").head()[0]

    expected_df = spark.createDataFrame(
        [
            ["US", "john", "doe", 30, "john.doe@gmail.com", "Added"],
            ["US", "jane", "smith", 40, "jane.smith@gmail.com", "Added"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com", "Added"],
        ],
        schema=SCHEMA,
    )

    expected_partitioning_columns = {"country"}

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    assert set(actual_partition_columns) == set(expected_partitioning_columns)


def test_merge(spark: SparkSession, temp_dir: str):
    input_path = f"{temp_dir}/main/input_data_2"
    output_path = f"{temp_dir}/main/output_table"

    args = {
        "input_path": input_path,
        "output_path": output_path,
        "action": "merge",
    }

    set_up_merge(spark, input_path)
    main(spark, args)

    actual_df = spark.read.format("delta").load(output_path)
    actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{output_path}`").select("partitionColumns").head()[0]

    expected_df = spark.createDataFrame(
        [
            ["US", "john", "doe", 31, "john.doe@gmail.com", "Updated"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com", "Added"],
            ["IT", "marco", "simone", 70, "marco.simone@gmail.com", "Added"],
        ],
        schema=SCHEMA,
    )

    expected_partitioning_columns = {"country"}

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    assert set(actual_partition_columns) == set(expected_partitioning_columns)
