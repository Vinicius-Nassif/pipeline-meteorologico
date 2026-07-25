# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold
# MAGIC ## Agrega - Média por cidade , dias com chuvas, anomolias de temperatura 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE DATABASE pipeline_meteo")

ontem = date.today() - timedelta(days=1)
ontem_str = ontem.isoformat()

print(f"✓ Database selecionado: pipeline_meteo")
print(f"✓ Data de processamento: {ontem_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler a Silver (dia anterior)

# COMMAND ----------

df_silver = spark.table("pipeline_meteo.silver_clima") \
    .filter(f"_particao_data = '{ontem_str}'")

print(f"Registros da Silver para {ontem_str}: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação 1: Agregação por cidade

# COMMAND ----------

df_gold_cidade = df_silver \
    .groupBy("cidade", "estado", "regiao", "data_referencia") \
    .agg(
        F.round(F.avg("temperatura_c"), 1).alias("temp_media_c"),
        F.round(F.max("temperatura_c"), 1).alias("temp_max_c"),
        F.round(F.min("temperatura_c"), 1).alias("temp_min_c"),
        F.round(F.avg("umidade_pct"), 1).alias("umidade_media_pct"),
        F.round(F.sum("precipitacao_mm"), 1).alias("precipitacao_total_mm"),
        F.round(F.avg("vento_kmh"), 1).alias("vento_medio_kmh"),
        F.count("*").alias("total_leituras")
    ) \
    .withColumn("dia_com_chuva", F.col("precipitacao_total_mm") > 0)

print(f"✓ Agregação por cidade: {df_gold_cidade.count()} registros")
df_gold_cidade.orderBy("regiao", "cidade").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação 2: Agregação por região

# COMMAND ----------

df_gold_regiao = df_silver \
    .groupBy("regiao", "data_referencia") \
    .agg(
        F.round(F.avg("temperatura_c"), 1).alias("temp_media_c"),
        F.round(F.max("temperatura_c"), 1).alias("temp_max_c"),
        F.round(F.min("temperatura_c"), 1).alias("temp_min_c"),
        F.round(F.avg("umidade_pct"), 1).alias("umidade_media_pct"),
        F.round(F.sum("precipitacao_mm"), 1).alias("precipitacao_total_mm"),
        F.countDistinct("cidade").alias("total_cidades")
    )

print(f"✓ Agregação por região: {df_gold_regiao.count()} registros")
df_gold_regiao.orderBy("regiao").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação 3: ranking de temperatura

# COMMAND ----------

window_rank = Window.partitionBy("data_referencia").orderBy(F.desc("temp_max_c"))

df_gold_ranking = df_silver \
    .groupBy("cidade", "estado", "regiao", "data_referencia") \
    .agg(
        F.round(F.max("temperatura_c"), 1).alias("temp_max_c"),
        F.round(F.avg("temperatura_c"), 1).alias("temp_media_c"),
        F.round(F.min("temperatura_c"), 1).alias("temp_min_c"),
    ) \
    .withColumn("rank_temp", F.rank().over(window_rank))

print(f"✓ Ranking: {df_gold_ranking.count()} registros (esperado: 27)")
df_gold_ranking.orderBy("rank_temp").show(27, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar as 3 tabelas Gold

# COMMAND ----------

df_gold_cidade.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pipeline_meteo.gold_resumo_cidade")

df_gold_regiao.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pipeline_meteo.gold_resumo_regiao")

df_gold_ranking.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pipeline_meteo.gold_ranking_temperatura")

print("✓ Tabelas Gold salvas:")
print("  - pipeline_meteo.gold_resumo_cidade")
print("  - pipeline_meteo.gold_resumo_regiao")
print("  - pipeline_meteo.gold_ranking_temperatura")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar todas as tabelas do projeto

# COMMAND ----------

print("=== TABELAS DO PROJETO ===\n")

tabelas = [
    ("Bronze",          "pipeline_meteo.bronze_clima"),
    ("Silver",          "pipeline_meteo.silver_clima"),
    ("Gold - Cidade",   "pipeline_meteo.gold_resumo_cidade"),
    ("Gold - Região",   "pipeline_meteo.gold_resumo_regiao"),
    ("Gold - Ranking",  "pipeline_meteo.gold_ranking_temperatura"),
]

for nome, tabela in tabelas:
    count = spark.table(tabela).count()
    print(f"  [{nome}] {tabela}: {count} registros")

# COMMAND ----------

# spark.sql("USE DATABASE pipeline_meteo")

# # Verificar Silver
# print("=== SILVER ===")
# spark.table("pipeline_meteo.silver_clima") \
#     .groupBy("_particao_data") \
#     .count() \
#     .orderBy("_particao_data") \
#     .show()

# # Verificar Gold
# print("=== GOLD resumo_cidade ===")
# spark.table("pipeline_meteo.gold_resumo_cidade") \
#     .groupBy("data_referencia") \
#     .count() \
#     .orderBy("data_referencia") \
#     .show()