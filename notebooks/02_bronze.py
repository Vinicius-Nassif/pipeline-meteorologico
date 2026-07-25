# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze 
# MAGIC ## Lê o JSON do GCS, salva como tabela Delta sem transformação 

# COMMAND ----------

# Biblioteca já instalada no ambiente — descomente se rodar em novo ambiente
%pip install google-cloud-storage
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup 

# COMMAND ----------

import json
import pandas as pd
from datetime import datetime, timezone, date, timedelta
from google.cloud import storage
from google.oauth2 import service_account
from pyspark.sql import functions as F

chave_json = dbutils.secrets.get(scope="gcp-pipeline-meteo", key="gcs-service-account-key")
SERVICE_ACCOUNT_KEY = json.loads(chave_json)

credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_KEY)
client = storage.Client(project="pipeline-meteorologico", credentials=credentials)

BUCKET_NAME = "pipeline-meteo-landing-nassif"
bucket = client.bucket(BUCKET_NAME)

print(f"✓ Conectado ao bucket: {bucket.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler JSON bruto do GCS (dia anterior)

# COMMAND ----------

ontem = date.today() - timedelta(days=1)
ontem_str = ontem.isoformat()

particao = f"ano={ontem.year}/mes={ontem.month:02d}/dia={ontem.day:02d}"
caminho_raw = f"raw/clima/{particao}/clima_{ontem_str}.json"

blob = bucket.blob(caminho_raw)
conteudo = blob.download_as_text()
dados_raw = json.loads(conteudo)

print(f"✓ Arquivo lido    : {caminho_raw}")
print(f"  Data de coleta  : {dados_raw['data_coleta']}")
print(f"  Total cidades   : {dados_raw['total_cidades']}")
print(f"  Total registros : {dados_raw.get('total_registros', len(dados_raw['registros']))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Converter para Spark DataFrame (camada Bronze)
# MAGIC
# MAGIC Por que colunas com _ no prefixo? Convenção para colunas de metadados de pipeline — facilita identificar o que é dado de negócio vs dado de controle. Você vai ver isso bastante em projetos de engenharia de dados.

# COMMAND ----------

df_bronze = spark.createDataFrame(pd.DataFrame(dados_raw["registros"]))

df_bronze = df_bronze \
    .withColumn("_particao_data", F.lit(ontem_str)) \
    .withColumn("_arquivo_origem", F.lit(caminho_raw)) \
    .withColumn("_ingestao_utc", F.lit(datetime.now(timezone.utc).isoformat()))

print("✓ Schema Bronze:")
df_bronze.printSchema()
print(f"Total de registros no DataFrame: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza do dia atual (idempotência)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS pipeline_meteo")

spark.sql(f"""
    DELETE FROM pipeline_meteo.bronze_clima
    WHERE _particao_data = '{ontem_str}'
""")
print(f"✓ Registros de {ontem_str} removidos (se existiam)")

count = spark.table("pipeline_meteo.bronze_clima").count() \
    if spark.catalog.tableExists("pipeline_meteo.bronze_clima") else 0
print(f"Total de registros após limpeza: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar como tabela Delta
# MAGIC
# MAGIC mode("append"): cada execução adiciona os dados do dia, sem sobrescrever execuções anteriores — comportamento correto para um pipeline incremental.

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("_particao_data") \
    .saveAsTable("pipeline_meteo.bronze_clima")

print("✓ Camada Bronze salva: pipeline_meteo.bronze_clima")

# COMMAND ----------

# MAGIC %skip
# MAGIC # Reprocessar Bronze para 2026-06-15
# MAGIC data_str = "2026-07-04"
# MAGIC data_obj = date(2026, 6, 15)
# MAGIC particao = f"ano={data_obj.year}/mes={data_obj.month:02d}/dia={data_obj.day:02d}"
# MAGIC caminho_raw = f"raw/clima/{particao}/clima_{data_str}.json"
# MAGIC
# MAGIC blob = bucket.blob(caminho_raw)
# MAGIC conteudo = blob.download_as_text()
# MAGIC dados_raw = json.loads(conteudo)
# MAGIC
# MAGIC df_reprocess = spark.createDataFrame(pd.DataFrame(dados_raw["registros"]))
# MAGIC df_reprocess = df_reprocess.withColumn("_particao_data", F.lit(data_str)) \
# MAGIC                            .withColumn("_arquivo_origem", F.lit(caminho_raw)) \
# MAGIC                            .withColumn("_ingestao_utc", F.lit(datetime.now(timezone.utc).isoformat()))
# MAGIC
# MAGIC spark.sql(f"DELETE FROM pipeline_meteo.bronze_clima WHERE _particao_data = '{data_str}'")
# MAGIC print(f"✓ Registros antigos de {data_str} removidos")
# MAGIC
# MAGIC df_reprocess.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("append") \
# MAGIC     .partitionBy("_particao_data") \
# MAGIC     .saveAsTable("pipeline_meteo.bronze_clima")
# MAGIC
# MAGIC print(f"✓ {data_str} reprocessado na Bronze: {df_reprocess.count()} registros")
# MAGIC
# MAGIC # Validar
# MAGIC spark.table("pipeline_meteo.bronze_clima") \
# MAGIC     .groupBy("_particao_data").count() \
# MAGIC     .orderBy("_particao_data").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar a tabela Delta

# COMMAND ----------

print(f"Total de registros na Bronze: {spark.table('pipeline_meteo.bronze_clima').count()}")

spark.table("pipeline_meteo.bronze_clima") \
    .groupBy("_particao_data") \
    .count() \
    .orderBy(F.desc("_particao_data")) \
    .show(5)

# COMMAND ----------

# spark.sql("""
#     DELETE FROM pipeline_meteo.bronze_clima
#     WHERE _particao_data = '2026-07-04'
# """)
# print("✓ Duplicatas removidas")