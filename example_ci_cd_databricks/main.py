import argparse
import logging
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

    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()

    main(spark, vars(args))


def main(spark: SparkSession, args: dict):
    new_data = spark.read.format("delta").load(args["input_path"])
    cleaned_data = format_names(new_data)

    if args["action"] == "init":
        logging.info("Initializing the output table...")
        partitioning_columns = ["country"]
        init_table(args["output_path"], cleaned_data, partitioning_columns)
    else:
        logging.info("Merging updates...")
        merge_updates(args["output_path"], cleaned_data)

    logging.info("New data ingested.")


if __name__ == "__main__":
    sys.exit(entry_point())
