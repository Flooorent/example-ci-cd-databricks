import shutil

from _pytest.tmpdir import TempdirFactory
from delta import configure_spark_with_delta_pip
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmpdir_factory: TempdirFactory) -> SparkSession:
    """Configure and create the Spark session for all tests.

    A temporary directory will be created and used to store all Delta tables.

    :param tmpdir_factory: pytest's TempdirFactory fixture,
    see https://docs.pytest.org/en/6.2.x/tmpdir.html#the-tmpdir-factory-fixture
    :return: the Spark session for all unit tests
    """
    warehouse_dir = str(tmpdir_factory.mktemp("warehouse"))
    print(f"Warehouse dir: {warehouse_dir}")

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("unit-tests")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # use yield instead of return to allow for cleanup
    # see https://docs.pytest.org/en/6.2.x/fixture.html#teardown-cleanup-aka-fixture-finalization
    yield configure_spark_with_delta_pip(builder).getOrCreate()
    shutil.rmtree(warehouse_dir)


@pytest.fixture(scope="session")
def temp_dir(spark: SparkSession) -> str:
    """Utility function to retrieve the temporary directory used to store all Delta tables.

    :param spark: the Spark session used for all unit tests
    :return: the temporary directory's path
    """
    return spark.conf.get("spark.sql.warehouse.dir")
