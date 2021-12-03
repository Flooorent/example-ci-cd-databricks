from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower


def format_names(df: DataFrame) -> DataFrame:
    """Have consistent first and last names.

    :param df: input dataframe, with at least columns "first_name" and "last_name"
    :return: input dataframe with formatted names
    """
    return df.withColumn("first_name", lower(col("first_name"))).withColumn("last_name", lower(col("last_name")))
