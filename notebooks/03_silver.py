# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver
# MAGIC ## Limpa, tipa colunas, remove nulos, adiciona metadados
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

spark.sql("USE DATABASE pipeline_meteo")

ontem = date.today() - timedelta(days=1)
ontem_str = ontem.isoformat()

print(f"✓ Database selecionado: pipeline_meteo")
print(f"✓ Data de processamento: {ontem_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler a Bronze e inspecionar (dia anterior)

# COMMAND ----------

df_bronze = spark.table("pipeline_meteo.bronze_clima") \
    .filter(f"_particao_data = '{ontem_str}'")

print(f"Registros da Bronze para {ontem_str}: {df_bronze.count()}")
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
print(f"Total de registros: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza do dia (idempotência)

# COMMAND ----------

spark.sql(f"""
    DELETE FROM pipeline_meteo.silver_clima
    WHERE _particao_data = '{ontem_str}'
""")
print(f"✓ Silver: registros de {ontem_str} removidos (se existiam)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar como tabela Silver

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .partitionBy("_particao_data") \
    .saveAsTable("pipeline_meteo.silver_clima")

print("✓ Camada Silver salva: pipeline_meteo.silver_clima")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar

# COMMAND ----------

print(f"Total de registros na Silver: {spark.table('pipeline_meteo.silver_clima').count()}")

print("\nÚltimos 5 dias:")
spark.table("pipeline_meteo.silver_clima") \
    .groupBy("_particao_data") \
    .count() \
    .orderBy(F.desc("_particao_data")) \
    .show(5)

print("\nDistribuição por região:")
spark.table("pipeline_meteo.silver_clima") \
    .filter(f"_particao_data = '{ontem_str}'") \
    .groupBy("regiao") \
    .count() \
    .orderBy("regiao") \
    .show()

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("DELETE FROM pipeline_meteo.silver_clima WHERE _particao_data = '2026-07-04'")
# MAGIC print("✓ Registros de 2026-07-04 removidos da Silver")
# MAGIC
# MAGIC spark.table("pipeline_meteo.silver_clima") \
# MAGIC     .groupBy("_particao_data").count() \
# MAGIC     .orderBy("_particao_data").show()

# COMMAND ----------

# spark.sql("DELETE FROM pipeline_meteo.silver_clima WHERE _particao_data = '2026-06-15'")
# spark.sql("DELETE FROM pipeline_meteo.silver_clima WHERE _particao_data = '2026-07-04'")
# print("✓ Silver limpa")