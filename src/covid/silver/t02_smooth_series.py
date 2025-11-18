from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_smooth(df):
    z_scored_df = apply_z_score(df)
    smothed_df = apply_rolling_mean(z_scored_df)
    new_smothed_df = recalculate_casos_acumulados(smothed_df)

    return new_smothed_df


def apply_z_score(df, limite_comum=1):
    # Window per estado and municipio
    w = Window.partitionBy("estado", "municipio")

    df_stats = (
        df.withColumn("mean", F.mean("casos_novos").over(w))
        .withColumn("std", F.stddev("casos_novos").over(w))
        .withColumn("z_score", (F.col("casos_novos") - F.col("mean")) / F.col("std"))
    )

    # Keep only rows inside threshold
    df_filtered = df_stats.filter(F.abs(F.col("z_score")) < limite_comum)

    return df_filtered.drop("mean", "std")


def apply_rolling_mean(df, window_size=7):
    partition_cols = ["municipio", "estado"]

    df = df.orderBy("data")

    w = Window.partitionBy(*partition_cols).orderBy("data").rowsBetween(-window_size, -1)

    df = df.withColumn("rolling_mean", F.mean("casos_novos").over(w))

    df = df.withColumn("casos_novos_suavizado", F.when(F.col("rolling_mean").isNull(), F.col("casos_novos")).otherwise(F.col("rolling_mean")))

    return df.drop("rolling_mean")


def recalculate_casos_acumulados(df):
    w = Window.partitionBy("estado", "municipio").orderBy("data").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return df.withColumn("casos_acumulados_suavizado", F.sum("casos_novos_suavizado").over(w))
