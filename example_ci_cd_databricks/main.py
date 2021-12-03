import argparse
import sys

from pyspark.sql import SparkSession

from example_ci_cd_databricks.preprocessing.cleaning import format_names
from example_ci_cd_databricks.utils.data import init_table, merge_updates


def entry_point():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input-path", type=str, help="Path where updates are appended")
    parser.add_argument("--output-path", type=str, help="Path where up-to-date people data are stored")
    parser.add_argument(
        "--action",
        type=str,
        default="merge",
        help="Choose between an init and a merge. Default is merge",
        choices=["init", "merge"],
    )
    parser.add_argument("--scope", type=str, help="Databricks secret scope")
    parser.add_argument("--storage-uri", type=str, help="Databricks secret key for the Azure storage account URI")
    parser.add_argument("--storage-key", type=str, help="Databricks secret key for the Azure storage account key")

    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()

    # import DBUtils inside function entry_point and not at the top of the file to prevent error
    # ModuleNotFoundError: No module named 'pyspark.dbutils' (since this is specific to Databricks and not open source)
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)

    adls_storage_uri = dbutils.secrets.get(scope=args.scope, key=args.storage_uri)
    adls_storage_key = dbutils.secrets.get(scope=args.scope, key=args.storage_key)

    spark.conf.set(adls_storage_uri, adls_storage_key)

    main(spark, vars(args))


def main(spark: SparkSession, args: dict):
    new_data = spark.read.format("delta").load(args["input_path"])
    cleaned_data = format_names(new_data)

    if args["action"] == "init":
        print("Initializing the output table...")
        partitioning_columns = ["country"]
        init_table(args["output_path"], cleaned_data, partitioning_columns, overwrite=True)
    else:
        print("Merging updates...")
        merge_updates(args["output_path"], cleaned_data)

    print("New data ingested.")


if __name__ == "__main__":
    sys.exit(entry_point())
