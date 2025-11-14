"""
This script initializes a Spark session configured to use Delta Lake and Hive support.

Modules:
    delta: Provides the `configure_spark_with_delta_pip` function to configure Spark with Delta Lake.
    pyspark.sql: Provides the `SparkSession` class to create a Spark session.

Constants:
    WORKSPACE_PATH (str): The base path for the workspace.
    DATABASE_PATH (str): The path for the database directory within the workspace.

Variables:
    builder: A SparkSession builder configured with necessary options for Delta Lake and Hive support.
    spark: The initialized Spark session with Delta Lake and Hive support enabled.
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

WORKSPACE_PATH = "/DIGTX-BIG-ROCKS-DATAENGINEERING-OPTIMUS"

DATABASE_PATH = f"{WORKSPACE_PATH}/.database"

IMPORT_PATH = f"{WORKSPACE_PATH}/.data/Tables"

builder = (
    SparkSession.builder.config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{DATABASE_PATH}'")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.warehouse.dir", f"{DATABASE_PATH}/spark-warehouse")
)

spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()

for schema_name in os.listdir(IMPORT_PATH):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    print(f"Importing tables for {schema_name} schema ---->")
    for table_name in os.listdir(f"{IMPORT_PATH}/{schema_name}"):
        print(f"Importing {table_name} table ---->")
        table_df: DataFrame = spark.read.parquet(f"{IMPORT_PATH}/{schema_name}/{table_name}")
        table_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name=f"{schema_name}.{table_name}")
        print(f"Importing {table_name} success.")
print("All tables imported successfully.")
