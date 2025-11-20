# %%
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
# %%
dbutils.widgets.text("silver_path", "/Volumes/workspace/silver/covid/", "Output Path")
dbutils.widgets.text("gold_path", "/Volumes/workspace/gold/covid/", "Output Path")

# %%
# Retrieve the values
silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

# %%
from covid.gold import read_delta

df = read_delta(spark, silver_path)

# %%
from covid.gold import cases_per_region

cases_per_region_df = cases_per_region(df)

# %%
from covid.common import write_delta

write_delta(df, gold_path, "overwrite")
