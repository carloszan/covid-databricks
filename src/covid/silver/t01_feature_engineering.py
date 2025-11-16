import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame


def drop_columns(df: SparkDataFrame) -> SparkDataFrame:
    columns = ["Recuperadosnovos", "emAcompanhamentoNovos"]

    return df.drop(*columns)


def rename_columns(df: SparkDataFrame) -> SparkDataFrame:
    columns = {
        "regiao": "regiao",
        "estado": "estado",
        "municipio": "municipio",
        "coduf": "coduf",
        "codmun": "codmun",
        "codRegiaoSaude": "cod_regiao_saude",
        "nomeRegiaoSaude": "nome_regiao_saude",
        "data": "data",
        "semanaEpi": "semana_epi",
        "populacaoTCU2019": "populacao_tcu_2019",
        "casosNovos": "casos_novos",
        "casosAcumulado": "casos_acumulados",
        "obitosAcumulado": "obitos_acumulados",
        "obitosNovos": "obitos_novos",
        "interior/metropolitana": "interior_metropolitana",
    }

    return df.withColumnsRenamed(columns)


def add_date_features(df):
    # Dicionários para tradução
    meses_portugues = {
        "January": "Janeiro",
        "February": "Fevereiro",
        "March": "Março",
        "April": "Abril",
        "May": "Maio",
        "June": "Junho",
        "July": "Julho",
        "August": "Agosto",
        "September": "Setembro",
        "October": "Outubro",
        "November": "Novembro",
        "December": "Dezembro",
    }

    dias_portugues = {
        "Sunday": "Domingo",
        "Monday": "Segunda-feira",
        "Tuesday": "Terça-feira",
        "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira",
        "Friday": "Sexta-feira",
        "Saturday": "Sábado",
    }

    # Criar expressões CASE WHEN para traduções
    mes_traduzido_expr = F.expr("CASE mes " + " ".join([f"WHEN '{ing}' THEN '{port}'" for ing, port in meses_portugues.items()]) + " END")

    dia_traduzido_expr = F.expr("CASE dia_semana " + " ".join([f"WHEN '{ing}' THEN '{port}'" for ing, port in dias_portugues.items()]) + " END")

    # Expressão para estação do ano
    estacao_expr = F.expr("""
        CASE 
            WHEN (MONTH(data) = 12 AND DAY(data) >= 21) OR 
                 (MONTH(data) < 3) OR 
                 (MONTH(data) = 3 AND DAY(data) < 21) THEN 'Verão'
            WHEN (MONTH(data) = 3 AND DAY(data) >= 21) OR 
                 (MONTH(data) BETWEEN 4 AND 5) OR 
                 (MONTH(data) = 6 AND DAY(data) < 21) THEN 'Outono'
            WHEN (MONTH(data) = 6 AND DAY(data) >= 21) OR 
                 (MONTH(data) BETWEEN 7 AND 8) OR 
                 (MONTH(data) = 9 AND DAY(data) < 21) THEN 'Inverno'
            ELSE 'Primavera'
        END
    """)

    result_df = (
        df.withColumn("ano", F.year("data"))
        .withColumn("mes", F.date_format("data", "MMMM"))
        .withColumn("mes_numerico", F.month("data"))
        .withColumn("mes_traduzido", mes_traduzido_expr)
        .withColumn("dia_semana", F.date_format("data", "EEEE"))
        .withColumn("dia_semana_traduzido", dia_traduzido_expr)
        .withColumn("dia_semana_numerico", F.dayofweek("data") - 1)
        .withColumn("estacao", estacao_expr)
    )

    return result_df
