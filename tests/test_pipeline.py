import pytest
from datetime import date
from unittest.mock import patch, MagicMock
from src.pipeline import (
    get_ontem,
    build_gcs_path,
    build_payload,
    validar_temperatura,
    validar_registro,
    get_cidades,
    parse_resposta_api,
)


# ── Testes de utilitários ──────────────────────────────────────────────────────

class TestGetOntem:
    def test_retorna_date(self):
        resultado = get_ontem()
        assert isinstance(resultado, date)

    def test_e_anterior_a_hoje(self):
        assert get_ontem() < date.today()


class TestBuildGcsPath:
    def test_formato_correto(self):
        data = date(2026, 7, 24)
        path = build_gcs_path(data)
        assert path == "raw/clima/ano=2026/mes=07/dia=24/clima_2026-07-24.json"

    def test_mes_com_zero(self):
        data = date(2026, 1, 5)
        path = build_gcs_path(data)
        assert "mes=01" in path
        assert "dia=05" in path

    def test_contem_prefixo_raw(self):
        data = date(2026, 7, 24)
        assert build_gcs_path(data).startswith("raw/clima/")

    def test_contem_extensao_json(self):
        data = date(2026, 7, 24)
        assert build_gcs_path(data).endswith(".json")


# ── Testes de validação ────────────────────────────────────────────────────────

class TestValidarTemperatura:
    def test_temperatura_valida(self):
        assert validar_temperatura(25.0) is True

    def test_temperatura_minima_valida(self):
        assert validar_temperatura(-10.0) is True

    def test_temperatura_maxima_valida(self):
        assert validar_temperatura(50.0) is True

    def test_temperatura_abaixo_do_minimo(self):
        assert validar_temperatura(-10.1) is False

    def test_temperatura_acima_do_maximo(self):
        assert validar_temperatura(50.1) is False

    def test_temperatura_zero(self):
        assert validar_temperatura(0.0) is True


class TestValidarRegistro:
    def test_registro_valido(self, registro_valido):
        assert validar_registro(registro_valido) is True

    def test_registro_sem_cidade(self, registro_valido):
        registro_valido["cidade"] = None
        assert validar_registro(registro_valido) is False

    def test_registro_temperatura_invalida(self, registro_valido):
        registro_valido["temperatura_c"] = 99.9
        assert validar_registro(registro_valido) is False

    def test_registro_umidade_invalida(self, registro_valido):
        registro_valido["umidade_pct"] = 101
        assert validar_registro(registro_valido) is False

    def test_registro_precipitacao_negativa(self, registro_valido):
        registro_valido["precipitacao_mm"] = -1.0
        assert validar_registro(registro_valido) is False

    def test_registro_campo_faltando(self, registro_valido):
        del registro_valido["vento_kmh"]
        assert validar_registro(registro_valido) is False


# ── Testes de cidades ──────────────────────────────────────────────────────────

class TestGetCidades:
    def test_total_27_cidades(self):
        assert len(get_cidades()) == 27

    def test_todos_tem_campos_obrigatorios(self):
        for cidade in get_cidades():
            assert "nome" in cidade
            assert "estado" in cidade
            assert "latitude" in cidade
            assert "longitude" in cidade

    def test_sem_estados_duplicados(self):
        estados = [c["estado"] for c in get_cidades()]
        assert len(estados) == len(set(estados)), "Estados duplicados encontrados"

    def test_latitudes_validas_brasil(self):
        for cidade in get_cidades():
            assert -35 <= cidade["latitude"] <= 6

    def test_longitudes_validas_brasil(self):
        for cidade in get_cidades():
            assert -75 <= cidade["longitude"] <= -32


# ── Testes de parsing da API ───────────────────────────────────────────────────

class TestParseRespostaApi:
    def test_retorna_24_registros(self, cidade_exemplo, resposta_api_mock):
        registros = parse_resposta_api(cidade_exemplo, resposta_api_mock)
        assert len(registros) == 24

    def test_campos_obrigatorios_presentes(self, cidade_exemplo, resposta_api_mock):
        registros = parse_resposta_api(cidade_exemplo, resposta_api_mock)
        campos = ["cidade", "estado", "latitude", "longitude",
                  "timestamp_coleta", "timestamp_dados",
                  "temperatura_c", "umidade_pct", "precipitacao_mm", "vento_kmh"]
        for campo in campos:
            assert campo in registros[0], f"Campo '{campo}' ausente"

    def test_cidade_correta(self, cidade_exemplo, resposta_api_mock):
        registros = parse_resposta_api(cidade_exemplo, resposta_api_mock)
        assert all(r["cidade"] == "São Paulo" for r in registros)

    def test_sem_timestamps_duplicados(self, cidade_exemplo, resposta_api_mock):
        registros = parse_resposta_api(cidade_exemplo, resposta_api_mock)
        timestamps = [r["timestamp_dados"] for r in registros]
        assert len(timestamps) == len(set(timestamps))


# ── Testes de payload ──────────────────────────────────────────────────────────

class TestBuildPayload:
    def test_estrutura_correta(self, registro_valido):
        payload = build_payload("2026-07-24", [registro_valido])
        assert "data_coleta" in payload
        assert "total_cidades" in payload
        assert "total_registros" in payload
        assert "registros" in payload

    def test_total_registros_correto(self, registro_valido):
        payload = build_payload("2026-07-24", [registro_valido] * 10)
        assert payload["total_registros"] == 10

    def test_data_coleta_correta(self, registro_valido):
        payload = build_payload("2026-07-24", [registro_valido])
        assert payload["data_coleta"] == "2026-07-24"
