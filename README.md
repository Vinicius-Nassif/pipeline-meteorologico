# Pipeline Meteorológico - Brasil 🌡️

Pipeline de dados das 27 capitais brasileiras construído com GCP e Databricks.

## Arquitetura
OpenMeteo API → GCP Cloud Storage → Databricks (Bronze → Silver → Gold) → BigQuery → Looker Studio
## Stack

| Camada | Tecnologia |
|---|---|
| Linguagem | Python + PySpark |
| Storage | GCP Cloud Storage |
| Processamento | Databricks (Delta Lake) |
| Data Warehouse | BigQuery |
| Visualização | Looker Studio |
| Orquestração | Databricks Workflows |

## Estrutura do projeto
pipeline-meteorologico/
├── notebooks/
│   ├── 01_ingestao_openmeteo.py   # Coleta dados da API OpenMeteo
│   ├── 02_bronze.py               # Ingestão raw → Delta (Bronze)
│   ├── 03_silver.py               # Limpeza e tipagem (Silver)
│   ├── 04_gold.py                 # Agregações (Gold)
│   └── 05_exportacao_bigquery.py  # Exportação para BigQuery
├── requirements.txt
└── README.md
## Dados coletados

- **Fonte**: [OpenMeteo API](https://open-meteo.com/) (gratuita, sem autenticação)
- **Frequência**: diária (agendado via Databricks Workflows)
- **Cidades**: 27 capitais brasileiras
- **Métricas**: temperatura, umidade, precipitação, velocidade do vento

## Padrão Medallion

- **Bronze**: dados brutos em Delta Lake, particionados por data
- **Silver**: dados limpos, tipados e enriquecidos com região geográfica
- **Gold**: agregações prontas para consumo (por cidade, por região, ranking)

## Dashboard

Dashboard público no Looker Studio com atualização diária:
- Temperatura média por capital
- Comparativo por região (temperatura vs umidade)
- Ranking das capitais mais quentes do dia

## Como executar

1. Configure as credenciais GCP via Databricks Secrets
2. Execute os notebooks na ordem (01 → 05)
3. Ou acione o job `pipeline-meteorologico-diario` no Databricks Workflows

## Autor

Desenvolvido por Nassif como projeto de aprendizado em Engenharia de Dados.
