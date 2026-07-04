# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver
# MAGIC ## Limpa, tipa colunas, remove nulos, adiciona metadados
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

spark.sql("USE DATABASE pipeline_meteo")
print("✓ Database selecionado: pipeline_meteo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler a Bronze e inspecionar

# COMMAND ----------

from datetime import date

hoje = str(date.today())

df_bronze = spark.table("pipeline_meteo.bronze_clima") \
    .filter(f"_particao_data = '{hoje}'")

print(f"Registros da Bronze para {hoje}: {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar transformações Silver
# MAGIC
# MAGIC O que fizemos aqui:
# MAGIC
# MAGIC - Tipagem explícita de todas as colunas numéricas e de tempo
# MAGIC - Extração de data_referencia e hora_referencia para facilitar filtros
# MAGIC - Enriquecimento com coluna regiao — dado de negócio derivado do estado
# MAGIC - Filtros de qualidade: remove registros sem temperatura ou cidade
# MAGIC - Drop de colunas de controle interno (não fazem sentido na Silver)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

df_silver = df_bronze \
    .withColumn("temperatura_c",    F.col("temperatura_c").cast(DoubleType())) \
    .withColumn("umidade_pct",      F.col("umidade_pct").cast(IntegerType())) \
    .withColumn("precipitacao_mm",  F.col("precipitacao_mm").cast(DoubleType())) \
    .withColumn("vento_kmh",        F.col("vento_kmh").cast(DoubleType())) \
    .withColumn("latitude",         F.col("latitude").cast(DoubleType())) \
    .withColumn("longitude",        F.col("longitude").cast(DoubleType())) \
    .withColumn("timestamp_coleta", F.to_timestamp("timestamp_coleta")) \
    .withColumn("timestamp_dados",  F.to_timestamp("timestamp_dados")) \
    .withColumn("data_referencia",  F.to_date("timestamp_dados")) \
    .withColumn("hora_referencia",  F.hour("timestamp_dados")) \
    .withColumn("regiao", F.when(F.col("estado").isin("SP","RJ","MG","ES","PR","SC","RS"), "Sul Sudeste")
                           .when(F.col("estado").isin("DF","GO","MT","MS"), "Centro Oeste")
                           .when(F.col("estado").isin("AM","PA","RR","AP","AC","RO","TO"), "Norte")
                           .otherwise("Nordeste")) \
    .filter(F.col("temperatura_c").isNotNull()) \
    .filter(F.col("cidade").isNotNull()) \
    .drop("_arquivo_origem", "_ingestao_utc")

print("✓ Schema Silver:")
df_silver.printSchema()
df_silver.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza do dia atual (dempotência)

# COMMAND ----------

spark.sql(f"""
    DELETE FROM pipeline_meteo.silver_clima
    WHERE _particao_data = '{hoje}'
""")
print(f"✓ Silver: registros de {hoje} removidos (se existiam)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar como tabela Silver

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_particao_data") \
    .saveAsTable("pipeline_meteo.silver_clima")

print("✓ Camada Silver salva como tabela: pipeline_meteo.silver_clima")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar

# COMMAND ----------

df_validacao = spark.table("pipeline_meteo.silver_clima")

print(f"Total de registros na Silver: {df_validacao.count()}")
print("\nDistribuição por região:")
df_validacao.groupBy("regiao").count().orderBy("regiao").show()
print("\nRegistros por data:")
df_validacao.groupBy("_particao_data").count().orderBy("_particao_data").show()
df_validacao.show(truncate=False)

# COMMAND ----------

# spark.sql("DELETE FROM pipeline_meteo.silver_clima WHERE _particao_data = '2026-06-15'")
# spark.sql("DELETE FROM pipeline_meteo.silver_clima WHERE _particao_data = '2026-07-04'")
# print("✓ Silver limpa")