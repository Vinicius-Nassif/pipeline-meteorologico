from datetime import date, timedelta, datetime, timezone
from typing import List, Dict


def get_ontem() -> date:
    """Retorna a data de ontem."""
    return date.today() - timedelta(days=1)


def build_gcs_path(data: date) -> str:
    """
    Constrói o path de particionamento Hive-style no GCS.
    Exemplo: raw/clima/ano=2026/mes=07/dia=24/clima_2026-07-24.json
    """
    particao = f"ano={data.year}/mes={data.month:02d}/dia={data.day:02d}"
    nome_arquivo = f"clima_{data.isoformat()}.json"
    return f"raw/clima/{particao}/{nome_arquivo}"


def build_payload(data_str: str, registros: list) -> dict:
    """
    Constrói o payload JSON para salvar no GCS.
    """
    cidades = list({r["cidade"] for r in registros})
    return {
        "data_coleta": data_str,
        "total_cidades": len(cidades),
        "total_registros": len(registros),
        "registros": registros
    }


def validar_temperatura(temperatura: float) -> bool:
    """
    Valida se a temperatura está dentro do range válido para o Brasil.
    Range: -10°C a 50°C
    """
    return -10.0 <= temperatura <= 50.0


def validar_registro(registro: dict) -> bool:
    """
    Valida se um registro de dados climáticos está completo e válido.
    """
    campos_obrigatorios = [
        "cidade", "estado", "latitude", "longitude",
        "timestamp_coleta", "timestamp_dados",
        "temperatura_c", "umidade_pct", "precipitacao_mm", "vento_kmh"
    ]
    for campo in campos_obrigatorios:
        if campo not in registro or registro[campo] is None:
            return False

    if not validar_temperatura(registro["temperatura_c"]):
        return False

    if not (0 <= registro["umidade_pct"] <= 100):
        return False

    if registro["precipitacao_mm"] < 0:
        return False

    return True


def get_cidades() -> List[Dict]:
    """Retorna a lista das 27 capitais brasileiras."""
    return [
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


def parse_resposta_api(cidade: dict, dados: dict) -> List[Dict]:
    """
    Transforma a resposta da API OpenMeteo em lista de registros.
    """
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
