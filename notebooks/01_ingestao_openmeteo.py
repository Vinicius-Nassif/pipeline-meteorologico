# Databricks notebook source
# MAGIC %md
# MAGIC ## Atualiando o notebook para usar Secrets

# COMMAND ----------

# Biblioteca já instalada no ambiente — descomente se rodar em novo ambiente
%pip install google-cloud-storage
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup com Secrets

# COMMAND ----------

import json
from google.cloud import storage
from google.oauth2 import service_account

# Lê a chave do Secret Scope — nunca aparece em texto puro no notebook
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
    {"nome": "Manaus",        "estado": "AM", "latitude": -3.10,  "longitude": -60.02},
    {"nome": "Belém",         "estado": "PA", "latitude": -1.46,  "longitude": -48.50},
    {"nome": "Porto Velho",   "estado": "RO", "latitude": -8.76,  "longitude": -63.90},
    {"nome": "Rio Branco",    "estado": "AC", "latitude": -9.97,  "longitude": -67.81},
    {"nome": "Macapá",        "estado": "AP", "latitude":  0.03,  "longitude": -51.07},
    {"nome": "Boa Vista",     "estado": "RR", "latitude":  2.82,  "longitude": -60.67},
    {"nome": "Palmas",        "estado": "TO", "latitude": -10.18, "longitude": -48.33},

    # Nordeste
    {"nome": "São Luis",      "estado": "MA", "latitude": -2.53,  "longitude": -44.30},
    {"nome": "Teresina",      "estado": "PI", "latitude": -5.09,  "longitude": -42.80},
    {"nome": "Fortaleza",     "estado": "CE", "latitude": -3.72,  "longitude": -38.54},
    {"nome": "Natal",         "estado": "RN", "latitude": -5.79,  "longitude": -35.21},
    {"nome": "João Pessoa",   "estado": "PB", "latitude": -7.12,  "longitude": -34.86},
    {"nome": "Recife",        "estado": "PE", "latitude": -8.05,  "longitude": -34.88},
    {"nome": "Maceió",        "estado": "AL", "latitude": -9.67,  "longitude": -35.74},
    {"nome": "Aracaju",       "estado": "SE", "latitude": -10.91, "longitude": -37.07},
    {"nome": "Salvador",      "estado": "BA", "latitude": -12.97, "longitude": -38.50},

    # Centro-Oeste
    {"nome": "Brasilia",      "estado": "DF", "latitude": -15.78, "longitude": -47.93},
    {"nome": "Goiânia",       "estado": "GO", "latitude": -16.69, "longitude": -49.25},
    {"nome": "Campo Grande",  "estado": "MS", "latitude": -20.44, "longitude": -54.65},
    {"nome": "Cuiabá",        "estado": "MT", "latitude": -15.60, "longitude": -56.10},

    # Sudeste
    {"nome": "São Paulo",     "estado": "SP", "latitude": -23.55, "longitude": -46.63},
    {"nome": "Rio de Janeiro","estado": "RJ", "latitude": -22.91, "longitude": -43.17},
    {"nome": "Belo Horizonte","estado": "MG", "latitude": -19.92, "longitude": -43.94},
    {"nome": "Vitória",       "estado": "ES", "latitude": -20.32, "longitude": -40.34},

    # Sul
    {"nome": "Curitiba",      "estado": "PR", "latitude": -25.43, "longitude": -49.27},
    {"nome": "Florianópolis", "estado": "SC", "latitude": -27.59, "longitude": -48.55},
    {"nome": "Porto Alegre",  "estado": "RS", "latitude": -30.03, "longitude": -51.23},
]

for c in CIDADES:
    print(f"{c['nome']} ({c['estado']}): lat={c['latitude']}, lon={c['longitude']}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Passo 2 - Função de coleta para uma cidade
# MAGIC ## 2.1 Função coletar_dados_clima

# COMMAND ----------

from datetime import datetime, timezone

def coletar_dados_clima(cidade: dict) -> dict:
    """
    Coleta dados climáticos atuais de uma cidade via OpenMeteo API.
    Retorna um dicionário estruturado, pronto para serialização em JSON.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": cidade["latitude"],
        "longitude": cidade["longitude"],
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "America/Sao_Paulo"
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # lança erro se status != 200
    dados = response.json()

    return {
        "cidade": cidade["nome"],
        "estado": cidade["estado"],
        "latitude": cidade["latitude"],
        "longitude": cidade["longitude"],
        "timestamp_coleta": datetime.now(timezone.utc).isoformat(),
        "timestamp_dados": dados["current"]["time"],
        "temperatura_c": dados["current"]["temperature_2m"],
        "umidade_pct": dados["current"]["relative_humidity_2m"],
        "precipitacao_mm": dados["current"]["precipitation"],
        "vento_kmh": dados["current"]["wind_speed_10m"],
    }

# Teste com uma cidade
teste = coletar_dados_clima(CIDADES[0])
print(teste)

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 3 - Coletar dados de todas as cidades
# MAGIC ## 3.1 - loop de coleta

# COMMAND ----------

import time

dados_coletados = []

for cidade in CIDADES:
    try:
        dados = coletar_dados_clima(cidade)
        dados_coletados.append(dados)
        print(f"✓ {cidade['nome']}: {dados['temperatura_c']}°C")
    except Exception as e:
        print(f"✗ Erro ao coletar {cidade['nome']}: {e}")
    time.sleep(0.5)  # boa prática: não martelar a API

print(f"\nTotal coletado: {len(dados_coletados)}/{len(CIDADES)} cidades")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 4 - Definir o path de particionamento no GCS
# MAGIC ## 4.1 Montar o path e o nome do arquivo
# MAGIC
# MAGIC Hive-Style = Padrão ano=YYYY/mes=MM/dia=DD/
# MAGIC Tanto Spark quanto BigQuery sabem utilizar essa estrutura 

# COMMAND ----------

from datetime import date

hoje = date.today()
particao = f"ano={hoje.year}/mes={hoje.month:02d}/dia={hoje.day:02d}"
nome_arquivo = f"clima_{hoje.isoformat()}.json"

caminho_completo = f"raw/clima/{particao}/{nome_arquivo}"
print(f"Caminho no GCS: gs://{BUCKET_NAME}/{caminho_completo}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 5 - Salvar no GCS
# MAGIC ## 5.1  Upload do JSON

# COMMAND ----------

import json

payload = {
    "data_coleta": hoje.isoformat(),
    "total_cidades": len(dados_coletados),
    "registros": dados_coletados
}

blob = bucket.blob(caminho_completo)
blob.upload_from_string(
    json.dumps(payload, ensure_ascii=False, indent=2),
    content_type="application/json"
)

print(f"✓ Arquivo salvo em: gs://{BUCKET_NAME}/{caminho_completo}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Passo 6 - Validar o que foi salvo
# MAGIC ## 6.1 Listar arquivos no prefixo `raw/`

# COMMAND ----------

blobs = client.list_blobs(BUCKET_NAME, prefix="raw/clima/")
for b in blobs:
    print(f"{b.name}  ({b.size} bytes)")