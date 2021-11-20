from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pytest

from example_ci_cd_databricks.utils.data import init_table, merge_updates


class TestInitTable:
    def test_should_partition(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_partition"

        schema = ", ".join(
            [
                "country string",
                "first_name string",
                "last_name string",
                "age int",
                "email string",
            ]
        )

        input_data = [
            ["US", "John", "Doe", 30, "john.doe@gmail.com"],
            ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
        ]

        input_df = spark.createDataFrame(input_data, schema)

        partitioning_columns = ["country"]
        init_table(table_path, input_df, partitioning_columns)

        actual_df = spark.read.format("delta").load(table_path)
        expected_df = spark.createDataFrame(input_data, schema=schema)

        assert_df_equality(actual_df, expected_df, ignore_row_order=True)

        actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("partitionColumns").head()[0]
        assert set(actual_partition_columns) == set(partitioning_columns)

    def test_should_not_partition(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_not_partition"

        schema = ", ".join(
            [
                "country string",
                "first_name string",
                "last_name string",
                "age int",
                "email string",
            ]
        )

        input_data = [
            ["US", "John", "Doe", 30, "john.doe@gmail.com"],
            ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
        ]

        input_df = spark.createDataFrame(input_data, schema)
        init_table(table_path, input_df)

        actual_df = spark.read.format("delta").load(table_path)
        expected_df = spark.createDataFrame(input_data, schema=schema)

        assert_df_equality(actual_df, expected_df, ignore_row_order=True)

        actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("partitionColumns").head()[0]
        assert not actual_partition_columns, f"There should be no partition columns, instead got {actual_partition_columns}"

    def test_should_not_partition_on_empty_partitioning_columns_list(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_not_partition_on_empty_partitioning_columns_list"

        schema = ", ".join(
            [
                "country string",
                "first_name string",
                "last_name string",
                "age int",
                "email string",
            ]
        )

        input_data = [
            ["US", "John", "Doe", 30, "john.doe@gmail.com"],
            ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
        ]

        input_df = spark.createDataFrame(input_data, schema)
        init_table(table_path, input_df)

        actual_df = spark.read.format("delta").load(table_path)
        expected_df = spark.createDataFrame(input_data, schema=schema)

        assert_df_equality(actual_df, expected_df, ignore_row_order=True)

        actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("partitionColumns").head()[0]
        assert not actual_partition_columns, f"There should be no partition columns, instead got {actual_partition_columns}"

    def test_should_not_overwrite_by_default(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_not_overwrite_by_default"

        schema = ", ".join(
            [
                "country string",
                "first_name string",
                "last_name string",
                "age int",
                "email string",
            ]
        )

        input_data = [
            ["US", "John", "Doe", 30, "john.doe@gmail.com"],
            ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com"],
            ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
        ]

        input_df = spark.createDataFrame(input_data, schema)

        partitioning_columns = ["country"]
        init_table(table_path, input_df, partitioning_columns)

        with pytest.raises(AnalysisException):
            init_table(table_path, input_df, partitioning_columns)

    def test_should_overwrite_if_specified(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_overwrite_if_specified"

        schema = ", ".join(
            [
                "country string",
                "first_name string",
                "last_name string",
                "age int",
                "email string",
            ]
        )

        df_1 = spark.createDataFrame(
            [
                ["US", "John", "Doe", 30, "john.doe@gmail.com"],
                ["US", "JANE", "SMITH", 40, "jane.smith@gmail.com"],
                ["FR", "laura", "simon", 50, "laura.simon@gmail.com"],
            ],
            schema,
        )

        df_2 = spark.createDataFrame(
            [
                ["ES", "Albert", "Dorio", 60, "akbert.dorio@gmail.com"],
                ["IT", "Marco", "Simone", 70, "marco.simone@gmail.com"],
            ],
            schema,
        )

        partitioning_columns = ["country"]
        init_table(table_path, df_1, partitioning_columns)
        init_table(table_path, df_2, partitioning_columns, overwrite=True)

        actual_df = spark.read.format("delta").load(table_path)

        assert_df_equality(actual_df, df_2, ignore_row_order=True)

    def test_should_overwrite_and_partition_if_specified(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_overwrite_and_partition_if_specified"

        df_1 = spark.createDataFrame(
            [
                ["john", 10, "US"],
                ["jean", 20, "FR"],
            ],
            schema="name string, age int, country string"
        )

        df_2 = spark.createDataFrame(
            [
                ["laura", 11, "IT"],
                ["lena", 22, "ES"],
            ],
            schema="name string, age int, country string"
        )

        init_table(table_path, df_1)

        partitioning_columns = ["country"]
        init_table(table_path, df_2, partitioning_columns, overwrite=True)

        actual_df = spark.read.format("delta").load(table_path)
        actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("partitionColumns").head()[0]

        assert_df_equality(actual_df, df_2, ignore_row_order=True)
        assert set(actual_partition_columns) == set(partitioning_columns)

    def test_should_overwrite_and_change_schema_if_specified(self, spark: SparkSession, temp_dir: str):
        table_path = f"{temp_dir}/utils/data/TestInitTable/test_should_overwrite_and_change_schema_if_specified"

        df_1 = spark.createDataFrame(
            [
                ["john", 10, "US"],
                ["jean", 20, "FR"],
            ],
            schema="name string, age int, country string"
        )

        df_2 = spark.createDataFrame(
            [
                ["laura", 11, "IT"],
                ["lena", 22, "ES"],
            ],
            schema="name string, age int, country string"
        )

        partitioning_columns_1 = ["name"]
        init_table(table_path, df_1, partitioning_columns_1)

        partitioning_columns_2 = ["country"]
        init_table(table_path, df_2, partitioning_columns_2, overwrite=True)

        actual_df = spark.read.format("delta").load(table_path)
        actual_partition_columns = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("partitionColumns").head()[0]

        assert_df_equality(actual_df, df_2, ignore_row_order=True)
        assert set(actual_partition_columns) == set(partitioning_columns_2)
