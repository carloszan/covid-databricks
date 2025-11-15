# %%
# Retrieve the values
landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_path")


# %%
from covid.bronze import ingest_raw_files

ingest_raw_files(spark, landing_path, bronze_path)
