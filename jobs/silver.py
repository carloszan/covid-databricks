# %%
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
# %%
dbutils.widgets.text("bronze_path", "/Volumes/workspace/bronze/covid/", "Input Path")
dbutils.widgets.text("silver_path", "/Volumes/workspace/silver/covid/", "Output Path")

# %%
# Retrieve the values
bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")

# %%
from covid.common import read_delta

df = read_delta(spark, bronze_path)

# %%
from covid.silver import add_date_features, drop_columns, rename_columns

logging.info("Dropping columns")
df = drop_columns(df)
logging.info("Renaming columns")
df = rename_columns(df)
logging.info("Adding new columns")
df = add_date_features(df)

# %%o
from covid.silver import apply_smooth

logging.info("Smoothing series")
df = apply_smooth(df)


# %%
from covid.common import write_delta

logging.info("Writing to delta table")
write_delta(df, silver_path, "overwrite")
