# Databricks notebook source
# MAGIC %md
# MAGIC ## Instalar biblioteca do BigQuery

# COMMAND ----------

# Biblioteca já instalada no ambiente — descomente se rodar em novo ambiente
%pip install google-cloud-bigquery google-cloud-bigquery-storage pyarrow pandas-gbq
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json
from google.cloud import bigquery
from google.oauth2 import service_account

chave_json = dbutils.secrets.get(scope="gcp-pipeline-meteo", key="gcs-service-account-key")
SERVICE_ACCOUNT_KEY = json.loads(chave_json)

credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_KEY)
client_bq = bigquery.Client(project="pipeline-meteorologico", credentials=credentials)

PROJECT_ID  = "pipeline-meteorologico"
DATASET_ID  = "pipeline_meteo"

print(f"✓ Conectado ao BigQuery: {PROJECT_ID}.{DATASET_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de exportação Spark → BigQuery
# MAGIC WRITE_TRUNCATE: sobrescreve a tabela no BigQuery a cada execução — comportamento correto para a Gold, que já é uma visão agregada reconstruída.

# COMMAND ----------

def exportar_para_bigquery(nome_tabela_spark: str, nome_tabela_bq: str):
    """
    Lê uma tabela Delta do Unity Catalog e exporta para o BigQuery.
    """
    df = spark.table(nome_tabela_spark)
    df_pandas = df.toPandas()

    destino = f"{PROJECT_ID}.{DATASET_ID}.{nome_tabela_bq}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # sobrescreve
        autodetect=True
    )

    job = client_bq.load_table_from_dataframe(df_pandas, destino, job_config=job_config)
    job.result()  # aguarda conclusão

    tabela = client_bq.get_table(destino)
    print(f"✓ {destino}: {tabela.num_rows} linhas exportadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportar as 3 tabelas Gold

# COMMAND ----------

tabelas = [
    ("pipeline_meteo.gold_resumo_cidade",           "gold_resumo_cidade"),
    ("pipeline_meteo.gold_resumo_regiao",           "gold_resumo_regiao"),
    ("pipeline_meteo.gold_ranking_temperatura",     "gold_ranking_temperatura"),
]

for tabela_spark, tabela_bq in tabelas:
    try:
        exportar_para_bigquery(tabela_spark, tabela_bq)
    except Exception as e:
        print(f"✗ Erro ao exportar {tabela_spark}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar via SQL no BigQuery

# COMMAND ----------

queries = [
    ("Resumo por cidade",  f"SELECT cidade, temp_media_c, dia_com_chuva FROM `{PROJECT_ID}.{DATASET_ID}.gold_resumo_cidade` ORDER BY temp_media_c DESC"),
    ("Resumo por região",  f"SELECT regiao, temp_media_c, total_cidades FROM `{PROJECT_ID}.{DATASET_ID}.gold_resumo_regiao` ORDER BY temp_media_c DESC"),
    ("Ranking temperatura",f"SELECT rank_temp, cidade, temperatura_c FROM `{PROJECT_ID}.{DATASET_ID}.gold_ranking_temperatura` ORDER BY rank_temp"),
]

for titulo, query in queries:
    print(f"\n=== {titulo} ===")
    resultado = client_bq.query(query).to_dataframe()
    print(resultado.to_string(index=False))