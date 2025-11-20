from pyspark.sql import functions as F


def cases_per_region(df):
    cases_per_region_df = (
        df.select(F.col("casos_novos_suavizado").cast("int"), "regiao")
        .groupBy("regiao")
        .agg(F.sum("casos_novos_suavizado").alias("total_casos"))
        .orderBy("regiao")
    )
    return cases_per_region_df
