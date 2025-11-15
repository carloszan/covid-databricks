def read_csv(spark, path, delimiter=";"):
    return spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", delimiter).load(path)


def write_delta(df, path, mode="append"):
    df.write.format("delta").mode(mode).save(path)
