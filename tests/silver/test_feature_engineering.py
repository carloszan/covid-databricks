import pytest
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from covid.silver import drop_columns, rename_columns


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame with test data"""
    schema = StructType(
        [
            StructField("Recuperadosnovos", IntegerType(), True),
            StructField("emAcompanhamentoNovos", IntegerType(), True),
            StructField("Casosnovos", IntegerType(), True),
            StructField("emAcompanhamento", IntegerType(), True),
            StructField("Country", StringType(), True),
        ]
    )

    data = [(10, 5, 100, 50, "CountryA"), (15, 8, 150, 75, "CountryB"), (20, 12, 200, 100, "CountryC")]

    return spark_session.createDataFrame(data, schema)


def test_drop_columns_removes_specified_columns(sample_dataframe):
    """Test that the function removes the specified columns"""
    result_df = drop_columns(sample_dataframe)

    # Verify specified columns are removed
    assert "Recuperadosnovos" not in result_df.columns
    assert "emAcompanhamentoNovos" not in result_df.columns

    # Verify other columns remain
    assert "Casosnovos" in result_df.columns
    assert "emAcompanhamento" in result_df.columns
    assert "Country" in result_df.columns


def test_drop_columns_remaining_columns_count(sample_dataframe):
    """Test that the correct number of columns remain"""
    original_column_count = len(sample_dataframe.columns)
    result_df = drop_columns(sample_dataframe)
    result_column_count = len(result_df.columns)

    # Should have 2 fewer columns (the ones we dropped)
    assert result_column_count == original_column_count - 2


def test_rename_columns_correctly_renames_columns(spark_session):
    """Test that the function correctly renames specified columns"""
    # Create DataFrame with original column names
    schema = StructType(
        [
            StructField("regiao", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("municipio", StringType(), True),
            StructField("coduf", IntegerType(), True),
            StructField("codmun", IntegerType(), True),
            StructField("codRegiaoSaude", IntegerType(), True),
            StructField("nomeRegiaoSaude", StringType(), True),
            StructField("data", StringType(), True),
            StructField("semanaEpi", IntegerType(), True),
            StructField("populacaoTCU2019", IntegerType(), True),
            StructField("casosNovos", IntegerType(), True),
            StructField("casosAcumulado", IntegerType(), True),
            StructField("obitosAcumulado", IntegerType(), True),
            StructField("obitosNovos", IntegerType(), True),
            StructField("interior/metropolitana", StringType(), True),
        ]
    )

    data = [("Norte", "AC", "Rio Branco", 12, 120040, 1, "Regional 1", "2023-01-01", 1, 100000, 10, 100, 5, 1, "Interior")]

    df = spark_session.createDataFrame(data, schema)

    # Apply the function
    result_df = rename_columns(df)

    # Verify columns that should be renamed
    expected_renamed_columns = [
        "cod_regiao_saude",
        "nome_regiao_saude",
        "semana_epi",
        "populacao_tcu_2019",
        "casos_novos",
        "casos_acumulados",
        "obitos_acumulados",
        "obitos_novos",
        "interior_metropolitana",
    ]

    # Verify columns that should remain the same
    expected_unchanged_columns = ["regiao", "estado", "municipio", "coduf", "codmun", "data"]

    # Check all renamed columns exist
    for col_name in expected_renamed_columns:
        assert col_name in result_df.columns

    # Check unchanged columns still exist
    for col_name in expected_unchanged_columns:
        assert col_name in result_df.columns

    # Verify original camelCase columns are gone
    original_camelcase_columns = [
        "codRegiaoSaude",
        "nomeRegiaoSaude",
        "semanaEpi",
        "populacaoTCU2019",
        "casosNovos",
        "casosAcumulado",
        "obitosAcumulado",
        "obitosNovos",
        "interior/metropolitana",
    ]

    for col_name in original_camelcase_columns:
        assert col_name not in result_df.columns


def test_rename_columns_preserves_data_and_structure(spark_session):
    """Test that the function preserves data integrity and returns proper DataFrame"""
    # Create test DataFrame with sample data
    schema = StructType(
        [
            StructField("codRegiaoSaude", IntegerType(), True),
            StructField("nomeRegiaoSaude", StringType(), True),
            StructField("semanaEpi", IntegerType(), True),
            StructField("populacaoTCU2019", IntegerType(), True),
            StructField("casosNovos", IntegerType(), True),
            StructField("casosAcumulado", IntegerType(), True),
            StructField("obitosNovos", IntegerType(), True),
            StructField("interior/metropolitana", StringType(), True),
        ]
    )

    data = [
        (1, "Regional Norte", 10, 500000, 25, 300, 2, "Metropolitana"),
        (2, "Regional Sul", 10, 750000, 45, 500, 4, "Interior"),
        (3, "Regional Leste", 10, 300000, 15, 150, 1, "Metropolitana"),
    ]

    original_df = spark_session.createDataFrame(data, schema)

    # Apply the function
    result_df = rename_columns(original_df)

    # Test 1: Verify return type is Spark DataFrame
    assert isinstance(result_df, SparkDataFrame)

    # Test 2: Verify row count remains the same
    assert original_df.count() == result_df.count()

    # Test 3: Verify data integrity by comparing values
    original_data = original_df.collect()
    result_data = result_df.collect()

    # Check that data values are preserved (just column names changed)
    for i in range(len(original_data)):
        original_row = original_data[i]
        result_row = result_data[i]

        # Verify specific column mappings preserve data
        assert original_row["codRegiaoSaude"] == result_row["cod_regiao_saude"]
        assert original_row["nomeRegiaoSaude"] == result_row["nome_regiao_saude"]
        assert original_row["semanaEpi"] == result_row["semana_epi"]
        assert original_row["populacaoTCU2019"] == result_row["populacao_tcu_2019"]
        assert original_row["casosNovos"] == result_row["casos_novos"]
        assert original_row["casosAcumulado"] == result_row["casos_acumulados"]
        assert original_row["obitosNovos"] == result_row["obitos_novos"]
        assert original_row["interior/metropolitana"] == result_row["interior_metropolitana"]

    # Test 4: Verify schema has correct number of columns
    assert len(original_df.columns) == len(result_df.columns)
