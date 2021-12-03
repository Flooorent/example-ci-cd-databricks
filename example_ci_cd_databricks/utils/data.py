from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def init_table(path: str, df: DataFrame, partitioning_columns: list = None, overwrite: bool = False):
    """Init Delta table.

    :param path: table's path
    :param df: Dataframe used to initialize the table
    :param partitioning_columns: list of columns to use to partition the table. None by default.
    :param overwrite: True if we should overwrite the table, False otherwise
    """
    df_writer = df.write.format("delta")

    if partitioning_columns:
        df_writer = df_writer.partitionBy(*partitioning_columns)

    if overwrite:
        df_writer = df_writer.mode("overwrite").option("overwriteSchema", "true")

    df_writer.save(path)


def merge_updates(table_path: str, updates: DataFrame):
    """Merge new updates based on the email address.

    :param table_path: Delta table's path where data is stored
    :param updates: updates dataframe
    """
    spark = SparkSession.builder.getOrCreate()

    (
        DeltaTable.forPath(spark, table_path)
        .alias("t")
        .merge(updates.alias("s"), "s.email = t.email")
        .whenMatchedDelete("s.status = 'Removed'")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
