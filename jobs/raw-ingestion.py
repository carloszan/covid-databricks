# %%
dbutils.widgets.text("input_path", "", "Input Path")
dbutils.widgets.text("output_path", "", "Output Path")

# %%
# Retrieve the values
landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_table_path")


# %%
from covid.bronze import ingest_raw_files

ingest_raw_files(spark, landing_path, bronze_path)
