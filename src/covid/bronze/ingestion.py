from covid.common import read_csv, write_delta


def ingest_raw_files(spark, landing_path, bronze_table_path):
    df = read_csv(spark, landing_path)

    write_delta(df, bronze_table_path, mode="overwrite")
