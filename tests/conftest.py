import pytest


@pytest.fixture
def cidade_exemplo():
    return {
        "nome": "São Paulo",
        "estado": "SP",
        "latitude": -23.55,
        "longitude": -46.63
    }


@pytest.fixture
def resposta_api_mock():
    """Simula a resposta da API OpenMeteo para 24 horas."""
    horas = [f"2026-07-24T{h:02d}:00" for h in range(24)]
    return {
        "hourly": {
            "time": horas,
            "temperature_2m": [20.0 + h * 0.5 for h in range(24)],
            "relative_humidity_2m": [80 - h for h in range(24)],
            "precipitation": [0.0] * 23 + [1.5],
            "wind_speed_10m": [10.0] * 24
        }
    }


@pytest.fixture
def registro_valido():
    return {
        "cidade": "São Paulo",
        "estado": "SP",
        "latitude": -23.55,
        "longitude": -46.63,
        "timestamp_coleta": "2026-07-24T10:00:00+00:00",
        "timestamp_dados": "2026-07-24T00:00",
        "temperatura_c": 22.5,
        "umidade_pct": 80,
        "precipitacao_mm": 0.0,
        "vento_kmh": 10.0
    }
