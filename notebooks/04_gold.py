# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold
# MAGIC ## Agrega - Média por cidade , dias com chuvas, anomolias de temperatura 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

spark.sql("USE DATABASE pipeline_meteo")
print("✓ Database selecionado: pipeline_meteo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler a Silver

# COMMAND ----------

from datetime import date

hoje = str(date.today())

df_silver = spark.table("pipeline_meteo.silver_clima") \
    .filter(f"_particao_data = '{hoje}'")

print(f"Registros da Silver para {hoje}: {df_silver.count()}")
df_silver.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação 1: resumo diário por cidade

# COMMAND ----------

from pyspark.sql import functions as F

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

print("✓ Agregação por cidade:")
df_gold_cidade.orderBy("regiao", "cidade").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação 2: resumo diário por região

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

print("✓ Agregação por região:")
df_gold_regiao.orderBy("regiao").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação 3: ranking de temperatura do dia

# COMMAND ----------

from pyspark.sql.window import Window

window_rank = Window.partitionBy("data_referencia").orderBy(F.desc("temperatura_c"))

df_gold_ranking = df_silver \
    .withColumn("rank_temp", F.rank().over(window_rank)) \
    .select("rank_temp", "cidade", "estado", "regiao",
            "temperatura_c", "umidade_pct", "precipitacao_mm", "data_referencia")

print("✓ Ranking de temperatura do dia:")
df_gold_ranking.orderBy("rank_temp").show(truncate=False)

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