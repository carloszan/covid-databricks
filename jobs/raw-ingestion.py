# %%
dbutils.widgets.text("landing_path", "/Volumes/workspace/bronze/landing/HIST_PAINEL_COVIDBR_2020_Parte1_05set2025.csv", "Input Path")
dbutils.widgets.text("bronze_path", "/Volumes/workspace/bronze/covid/", "Output Path")

# %%
# Retrieve the values
landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_path")


# %%
from covid.bronze import ingest_raw_files

ingest_raw_files(spark, landing_path, bronze_path)
