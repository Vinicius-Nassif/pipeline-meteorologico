# Databricks notebook source
# MAGIC %md
# MAGIC ## Atualizando o notebook para usar Secrets

# COMMAND ----------

# Biblioteca já instalada no ambiente — descomente se rodar em novo ambiente
%pip install google-cloud-storage
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup com Secrets

# COMMAND ----------

import json
import requests
import time
from datetime import datetime, timezone, date, timedelta
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
# MAGIC # Passo 0 - Teste da conectivadade 
# MAGIC ## 0.1 Requisição simples de teste

# COMMAND ----------

import requests

# Coordenadas de São Paulo (exemplo)
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": -23.55,
    "longitude": -46.63,
    "current": "temperature_2m,relative_humidity_2m,precipitation",
    "timezone": "America/Sao_Paulo"
}

response = requests.get(url, params=params)
print(f"Status code: {response.status_code}")
print(response.json())

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 1 - Definir as cidades do projeto
# MAGIC ## 1.1 Configuração das cidades

# COMMAND ----------

CIDADES = [
    # Norte
    {"nome": "Manaus",         "estado": "AM", "latitude": -3.10,  "longitude": -60.02},
    {"nome": "Belém",          "estado": "PA", "latitude": -1.46,  "longitude": -48.50},
    {"nome": "Porto Velho",    "estado": "RO", "latitude": -8.76,  "longitude": -63.90},
    {"nome": "Rio Branco",     "estado": "AC", "latitude": -9.97,  "longitude": -67.81},
    {"nome": "Macapá",         "estado": "AP", "latitude":  0.03,  "longitude": -51.07},
    {"nome": "Boa Vista",      "estado": "RR", "latitude":  2.82,  "longitude": -60.67},
    {"nome": "Palmas",         "estado": "TO", "latitude": -10.18, "longitude": -48.33},
    # Nordeste
    {"nome": "São Luis",       "estado": "MA", "latitude": -2.53,  "longitude": -44.30},
    {"nome": "Teresina",       "estado": "PI", "latitude": -5.09,  "longitude": -42.80},
    {"nome": "Fortaleza",      "estado": "CE", "latitude": -3.72,  "longitude": -38.54},
    {"nome": "Natal",          "estado": "RN", "latitude": -5.79,  "longitude": -35.21},
    {"nome": "João Pessoa",    "estado": "PB", "latitude": -7.12,  "longitude": -34.86},
    {"nome": "Recife",         "estado": "PE", "latitude": -8.05,  "longitude": -34.88},
    {"nome": "Maceió",         "estado": "AL", "latitude": -9.67,  "longitude": -35.74},
    {"nome": "Aracaju",        "estado": "SE", "latitude": -10.91, "longitude": -37.07},
    {"nome": "Salvador",       "estado": "BA", "latitude": -12.97, "longitude": -38.50},
    # Centro-Oeste
    {"nome": "Brasilia",       "estado": "DF", "latitude": -15.78, "longitude": -47.93},
    {"nome": "Goiânia",        "estado": "GO", "latitude": -16.69, "longitude": -49.25},
    {"nome": "Campo Grande",   "estado": "MS", "latitude": -20.44, "longitude": -54.65},
    {"nome": "Cuiabá",         "estado": "MT", "latitude": -15.60, "longitude": -56.10},
    # Sudeste
    {"nome": "São Paulo",      "estado": "SP", "latitude": -23.55, "longitude": -46.63},
    {"nome": "Rio de Janeiro", "estado": "RJ", "latitude": -22.91, "longitude": -43.17},
    {"nome": "Belo Horizonte", "estado": "MG", "latitude": -19.92, "longitude": -43.94},
    {"nome": "Vitória",        "estado": "ES", "latitude": -20.32, "longitude": -40.34},
    # Sul
    {"nome": "Curitiba",       "estado": "PR", "latitude": -25.43, "longitude": -49.27},
    {"nome": "Florianópolis",  "estado": "SC", "latitude": -27.59, "longitude": -48.55},
    {"nome": "Porto Alegre",   "estado": "RS", "latitude": -30.03, "longitude": -51.23},
]

print(f"✓ {len(CIDADES)} cidades configuradas")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Passo 2 - Função de coleta para uma cidade
# MAGIC ## 2.1 Função de coleta horária

# COMMAND ----------

def coletar_dados_horarios(cidade: dict, data_str: str) -> list:
    """
    Coleta dados horários de uma cidade para uma data específica.
    Retorna lista de 24 registros (um por hora).
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": cidade["latitude"],
        "longitude": cidade["longitude"],
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "America/Sao_Paulo",
        "start_date": data_str,
        "end_date": data_str
    }

    response = requests.get(url, params=params, timeout=45)
    response.raise_for_status()
    dados = response.json()

    registros = []
    for i, hora in enumerate(dados["hourly"]["time"]):
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

# Teste rápido
ontem = (date.today() - timedelta(days=1)).isoformat()
teste = coletar_dados_horarios(CIDADES[0], ontem)
print(f"✓ {CIDADES[0]['nome']}: {len(teste)} leituras horárias")
print(f"  Primeira leitura: {teste[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 3 - Coletar dados de todas as cidades
# MAGIC ## 3.1 - Definir data de coleta (dia anterior)

# COMMAND ----------

ontem = date.today() - timedelta(days=1)
data_str = ontem.isoformat()

particao = f"ano={ontem.year}/mes={ontem.month:02d}/dia={ontem.day:02d}"
nome_arquivo = f"clima_{data_str}.json"
caminho_completo = f"raw/clima/{particao}/{nome_arquivo}"

print(f"✓ Data de coleta : {data_str} (dia anterior)")
print(f"✓ Caminho no GCS : gs://{BUCKET_NAME}/{caminho_completo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coletar dados de todas as cidades

# COMMAND ----------

dados_coletados = []
erros = 0

for cidade in CIDADES:
    try:
        registros = coletar_dados_horarios(cidade, data_str)
        dados_coletados.extend(registros)
        print(f"✓ {cidade['nome']}: {len(registros)} leituras horárias")
    except Exception as e:
        print(f"✗ Erro em {cidade['nome']}: {e}")
        erros += 1
    time.sleep(0.3)

print(f"\nTotal coletado: {len(dados_coletados)} registros ({len(CIDADES) - erros}/27 cidades)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 4 - Definir o path de particionamento no GCS
# MAGIC ## 4.1 Montar o path e o nome do arquivo
# MAGIC
# MAGIC Hive-Style = Padrão ano=YYYY/mes=MM/dia=DD/
# MAGIC Tanto Spark quanto BigQuery sabem utilizar essa estrutura 

# COMMAND ----------

# from datetime import date

# hoje = date.today()
# particao = f"ano={hoje.year}/mes={hoje.month:02d}/dia={hoje.day:02d}"
# nome_arquivo = f"clima_{hoje.isoformat()}.json"

# caminho_completo = f"raw/clima/{particao}/{nome_arquivo}"
# print(f"Caminho no GCS: gs://{BUCKET_NAME}/{caminho_completo}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 5 - Salvar no GCS
# MAGIC ## 5.1  Upload do JSON

# COMMAND ----------

payload = {
    "data_coleta": data_str,
    "total_cidades": len(CIDADES) - erros,
    "total_registros": len(dados_coletados),
    "registros": dados_coletados
}

blob = bucket.blob(caminho_completo)
blob.upload_from_string(
    json.dumps(payload, ensure_ascii=False, indent=2),
    content_type="application/json"
)

print(f"✓ Arquivo salvo em: gs://{BUCKET_NAME}/{caminho_completo}")
print(f"  Registros       : {len(dados_coletados)}")
print(f"  Cidades         : {len(CIDADES) - erros}/27")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 6 - Validar o que foi salvo
# MAGIC ## 6.1 Listar arquivos no prefixo `raw/`

# COMMAND ----------

blobs = list(client.list_blobs(BUCKET_NAME, prefix="raw/clima/"))
print(f"Total de arquivos no GCS: {len(blobs)}")

for b in sorted(blobs, key=lambda x: x.name)[-5:]:
    print(f"  {b.name} ({b.size:,} bytes)")