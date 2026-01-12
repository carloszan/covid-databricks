from pyspark.sql import functions as F


def cases_per_state(df):
    cases_per_state_df = df.groupBy("coduf", "estado").agg(F.sum("casos_novos_suavizado").alias("total_casos")).orderBy("coduf", "estado")

    return cases_per_state_df
