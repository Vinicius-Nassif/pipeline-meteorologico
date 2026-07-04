# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze 
# MAGIC ## Lê o JSON do GCS, salva como tabela Delta sem transformação 

# COMMAND ----------

# Biblioteca já instalada no ambiente — descomente se rodar em novo ambiente
# %pip install google-cloud-storage
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup 

# COMMAND ----------

import json
from google.cloud import storage
from google.oauth2 import service_account

chave_json = dbutils.secrets.get(scope="gcp-pipeline-meteo", key="gcs-service-account-key")
SERVICE_ACCOUNT_KEY = json.loads(chave_json)

credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_KEY)
client = storage.Client(project="pipeline-meteorologico", credentials=credentials)

BUCKET_NAME = "pipeline-meteo-landing-nassif"
bucket = client.bucket(BUCKET_NAME)

print(f"✓ Conectado ao bucket: {bucket.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler JSON bruto do GCS

# COMMAND ----------

from datetime import date

hoje = date.today()
particao = f"ano={hoje.year}/mes={hoje.month:02d}/dia={hoje.day:02d}"
nome_arquivo = f"clima_{hoje.isoformat()}.json"
caminho_raw = f"raw/clima/{particao}/{nome_arquivo}"

blob = bucket.blob(caminho_raw)
conteudo = blob.download_as_text()
dados_raw = json.loads(conteudo)

print(f"✓ Arquivo lido: {caminho_raw}")
print(f"  Data de coleta : {dados_raw['data_coleta']}")
print(f"  Total cidades  : {dados_raw['total_cidades']}")
print(f"  Registros      : {len(dados_raw['registros'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Converter para Spark DataFrame (camada Bronze)
# MAGIC
# MAGIC Por que colunas com _ no prefixo? Convenção para colunas de metadados de pipeline — facilita identificar o que é dado de negócio vs dado de controle. Você vai ver isso bastante em projetos de engenharia de dados.

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime, timezone

# Converter registros para DataFrame
df_bronze = spark.createDataFrame(pd.DataFrame(dados_raw["registros"]))

# Adicionar metadados de ingestão — padrão Bronze
df_bronze = df_bronze.withColumn("_particao_data", F.lit(hoje.isoformat())) \
                     .withColumn("_arquivo_origem", F.lit(caminho_raw)) \
                     .withColumn("_ingestao_utc", F.lit(datetime.now(timezone.utc).isoformat()))

print(f"✓ Schema Bronze:")
df_bronze.printSchema()
df_bronze.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza do dia atual (idempotência)

# COMMAND ----------

# Garante idempotência: rodar múltiplas vezes no mesmo dia não duplica dados
spark.sql("CREATE DATABASE IF NOT EXISTS pipeline_meteo")

hoje_str = hoje.isoformat()
spark.sql(f"""
    DELETE FROM pipeline_meteo.bronze_clima
    WHERE _particao_data = '{hoje_str}'
""")
print(f"✓ Registros de {hoje_str} removidos (se existiam)")

count = spark.table("pipeline_meteo.bronze_clima").count() if spark.catalog.tableExists("pipeline_meteo.bronze_clima") else 0
print(f"Total de registros após limpeza: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar como tabela Delta
# MAGIC
# MAGIC mode("append"): cada execução adiciona os dados do dia, sem sobrescrever execuções anteriores — comportamento correto para um pipeline incremental.

# COMMAND ----------

# Criar database se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS pipeline_meteo")

# Salvar como tabela Delta gerenciada pelo Unity Catalog
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("_particao_data") \
    .saveAsTable("pipeline_meteo.bronze_clima")

print("✓ Camada Bronze salva como tabela: pipeline_meteo.bronze_clima")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar a tabela Delta

# COMMAND ----------

df_validacao = spark.table("pipeline_meteo.bronze_clima")

print(f"Total de registros na Bronze: {df_validacao.count()}")
df_validacao.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE DATABASE pipeline_meteo")

spark.table("pipeline_meteo.bronze_clima") \
    .groupBy("_particao_data") \
    .count() \
    .orderBy("_particao_data") \
    .show()

# COMMAND ----------

# spark.sql("""
#     DELETE FROM pipeline_meteo.bronze_clima
#     WHERE _particao_data = '2026-07-04'
# """)
# print("✓ Duplicatas removidas")