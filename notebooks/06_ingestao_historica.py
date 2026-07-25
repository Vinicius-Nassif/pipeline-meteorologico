# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 - Instalar biblioteca

# COMMAND ----------

# MAGIC %pip install google-cloud-storage
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 - Setup

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
# MAGIC ## 03 - Definir cidades

# COMMAND ----------

CIDADES = [
    # Norte
    {"nome": "Manaus",           "estado": "AM", "latitude": -3.10,  "longitude": -60.02},
    {"nome": "Belém",            "estado": "PA", "latitude": -1.46,  "longitude": -48.50},
    {"nome": "Porto Velho",      "estado": "RO", "latitude": -8.76,  "longitude": -63.90},
    {"nome": "Rio Branco",       "estado": "AC", "latitude": -9.97,  "longitude": -67.81},
    {"nome": "Macapá",           "estado": "AP", "latitude":  0.03,  "longitude": -51.07},
    {"nome": "Boa Vista",        "estado": "RR", "latitude":  2.82,  "longitude": -60.67},
    {"nome": "Palmas",           "estado": "TO", "latitude": -10.18, "longitude": -48.33},
    # Nordeste
    {"nome": "São Luis",         "estado": "MA", "latitude": -2.53,  "longitude": -44.30},
    {"nome": "Teresina",         "estado": "PI", "latitude": -5.09,  "longitude": -42.80},
    {"nome": "Fortaleza",        "estado": "CE", "latitude": -3.72,  "longitude": -38.54},
    {"nome": "Natal",            "estado": "RN", "latitude": -5.79,  "longitude": -35.21},
    {"nome": "João Pessoa",      "estado": "PB", "latitude": -7.12,  "longitude": -34.86},
    {"nome": "Recife",           "estado": "PE", "latitude": -8.05,  "longitude": -34.88},
    {"nome": "Maceió",           "estado": "AL", "latitude": -9.67,  "longitude": -35.74},
    {"nome": "Aracaju",          "estado": "SE", "latitude": -10.91, "longitude": -37.07},
    {"nome": "Salvador",         "estado": "BA", "latitude": -12.97, "longitude": -38.50},
    # Centro-Oeste
    {"nome": "Brasilia",         "estado": "DF", "latitude": -15.78, "longitude": -47.93},
    {"nome": "Goiânia",          "estado": "GO", "latitude": -16.69, "longitude": -49.25},
    {"nome": "Campo Grande",     "estado": "MS", "latitude": -20.44, "longitude": -54.65},
    {"nome": "Cuiabá",           "estado": "MT", "latitude": -15.60, "longitude": -56.10},
    # Sudeste
    {"nome": "São Paulo",        "estado": "SP", "latitude": -23.55, "longitude": -46.63},
    {"nome": "Rio de Janeiro",   "estado": "RJ", "latitude": -22.91, "longitude": -43.17},
    {"nome": "Belo Horizonte",   "estado": "MG", "latitude": -19.92, "longitude": -43.94},
    {"nome": "Vitória",          "estado": "ES", "latitude": -20.32, "longitude": -40.34},
    # Sul
    {"nome": "Curitiba",         "estado": "PR", "latitude": -25.43, "longitude": -49.27},
    {"nome": "Florianópolis",    "estado": "SC", "latitude": -27.59, "longitude": -48.55},
    {"nome": "Porto Alegre",     "estado": "RS", "latitude": -30.03, "longitude": -51.23},
]

print(f"Total de cidades: {len(CIDADES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 - Função de coleta horária por cidade e data

# COMMAND ----------

import requests
from datetime import datetime, timezone

def coletar_dados_historicos(cidade: dict, data_inicio: str, data_fim: str) -> list:
    """
    Coleta dados horários de uma cidade para um intervalo de datas.
    Retorna lista de registros, um por hora.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": cidade["latitude"],
        "longitude": cidade["longitude"],
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "America/Sao_Paulo",
        "start_date": data_inicio,
        "end_date": data_fim
    }

    response = requests.get(url, params=params, timeout=45)
    response.raise_for_status()
    dados = response.json()

    registros = []
    horas = dados["hourly"]["time"]

    for i, hora in enumerate(horas):
        registros.append({
            "cidade": cidade["nome"],
            "estado": cidade["estado"],
            "latitude": cidade["latitude"],
            "longitude": cidade["longitude"],
            "timestamp_coleta": datetime.now(timezone.utc).isoformat(),
            "timestamp_dados": hora,
            "temperatura_c": dados["hourly"]["temperature_2m"][i],
            "umidade_pct": dados["hourly"]["relative_humidity_2m"][i],
            "precipitacao_mm": dados["hourly"]["precipitation"][i],
            "vento_kmh": dados["hourly"]["wind_speed_10m"][i],
        })

    return registros

# Teste rápido com uma cidade e um dia
from datetime import date, timedelta
ontem = (date.today() - timedelta(days=1)).isoformat()
teste = coletar_dados_historicos(CIDADES[0], ontem, ontem)
print(f"✓ {CIDADES[0]['nome']}: {len(teste)} leituras horárias")
print(teste[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 - Definir período de backfill

# COMMAND ----------

from datetime import date, timedelta

DIAS_BACKFILL = 30
data_fim = date.today() - timedelta(days=1)       # ontem
data_inicio = data_fim - timedelta(days=DIAS_BACKFILL - 1)

print(f"Período de backfill:")
print(f"  Início : {data_inicio.isoformat()}")
print(f"  Fim    : {data_fim.isoformat()}")
print(f"  Total  : {DIAS_BACKFILL} dias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 - Executar backfill por dia e salvar no GCS

# COMMAND ----------

import time
import json
from datetime import timedelta

data_atual = data_inicio
dias_processados = 0
dias_pulados = 0

while data_atual <= data_fim:
    data_str = data_atual.isoformat()
    particao = f"ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}"
    caminho = f"raw/clima/{particao}/clima_{data_str}.json"

    # Verificar se o arquivo já existe no GCS (evita reprocessar)
    blob = bucket.blob(caminho)
    if blob.exists():
        print(f"⏭ {data_str} já existe — pulando")
        dias_pulados += 1
        data_atual += timedelta(days=1)
        continue

    # Coletar dados de todas as cidades para essa data
    registros_dia = []
    erros = 0

    for cidade in CIDADES:
        try:
            registros = coletar_dados_historicos(cidade, data_str, data_str)
            registros_dia.extend(registros)
        except Exception as e:
            print(f"  ✗ Erro em {cidade['nome']}: {e}")
            erros += 1
        time.sleep(0.3)

    # Salvar no GCS
    payload = {
        "data_coleta": data_str,
        "total_cidades": len(CIDADES) - erros,
        "total_registros": len(registros_dia),
        "registros": registros_dia
    }

    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

    dias_processados += 1
    print(f"✓ {data_str}: {len(registros_dia)} registros salvos ({len(CIDADES) - erros}/27 cidades)")

    data_atual += timedelta(days=1)
    time.sleep(1)  # pausa entre dias para não sobrecarregar a API

print(f"\n=== Backfill concluído ===")
print(f"  Dias processados : {dias_processados}")
print(f"  Dias pulados     : {dias_pulados}")
print(f"  Total de arquivos: {dias_processados + dias_pulados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07 - Validar arquivos no GCS

# COMMAND ----------

blobs = list(client.list_blobs(BUCKET_NAME, prefix="raw/clima/"))
print(f"Total de arquivos no GCS: {len(blobs)}")
for b in sorted(blobs, key=lambda x: x.name):
    print(f"  {b.name} ({b.size:,} bytes)")

# COMMAND ----------

# # Reprocessar apenas 2026-06-14 (forçando sobrescrita)
# data_str = "2026-06-14"
# data_obj = date(2026, 6, 14)
# particao = f"ano={data_obj.year}/mes={data_obj.month:02d}/dia={data_obj.day:02d}"
# caminho = f"raw/clima/{particao}/clima_{data_str}.json"

# registros_dia = []
# erros = 0

# for cidade in CIDADES:
#     try:
#         registros = coletar_dados_historicos(cidade, data_str, data_str)
#         registros_dia.extend(registros)
#         print(f"✓ {cidade['nome']}: {len(registros)} leituras")
#     except Exception as e:
#         print(f"✗ Erro em {cidade['nome']}: {e}")
#         erros += 1
#     time.sleep(0.5)

# payload = {
#     "data_coleta": data_str,
#     "total_cidades": len(CIDADES) - erros,
#     "total_registros": len(registros_dia),
#     "registros": registros_dia
# }

# blob = bucket.blob(caminho)
# blob.upload_from_string(
#     json.dumps(payload, ensure_ascii=False, indent=2),
#     content_type="application/json"
# )

# print(f"\n✓ {data_str} reprocessado: {len(registros_dia)} registros ({len(CIDADES) - erros}/27 cidades)")

# COMMAND ----------

# Reprocessar 2026-06-15 com dados horários (forçando sobrescrita no GCS)
from datetime import date
import time, json

data_str = "2026-07-04"
data_obj = date(2026, 6, 15)
particao = f"ano={data_obj.year}/mes={data_obj.month:02d}/dia={data_obj.day:02d}"
caminho = f"raw/clima/{particao}/clima_{data_str}.json"

registros_dia = []
erros = 0

for cidade in CIDADES:
    try:
        registros = coletar_dados_historicos(cidade, data_str, data_str)
        registros_dia.extend(registros)
        print(f"✓ {cidade['nome']}: {len(registros)} leituras")
    except Exception as e:
        print(f"✗ Erro em {cidade['nome']}: {e}")
        erros += 1
    time.sleep(0.5)

payload = {
    "data_coleta": data_str,
    "total_cidades": len(CIDADES) - erros,
    "total_registros": len(registros_dia),
    "registros": registros_dia
}

blob = bucket.blob(caminho)
blob.upload_from_string(
    json.dumps(payload, ensure_ascii=False, indent=2),
    content_type="application/json"
)

print(f"\n✓ {data_str} reprocessado: {len(registros_dia)} registros ({len(CIDADES) - erros}/27 cidades)")