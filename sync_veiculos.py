import requests
import time
import psycopg2
from datetime import datetime, timedelta
import os
from psycopg2.extras import execute_batch

BASE_URL = "https://api.hinova.com.br/api/sga/v2"

TOKEN_BASE = os.getenv("TOKEN_BASE")
USUARIO = os.getenv("USUARIO_API")
SENHA = os.getenv("SENHA_API")
CODIGO_COOPERATIVA = os.getenv("CODIGO_COOPERATIVA")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LIMIT_PER_PAGE = 1000

def autenticar():
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN_BASE}"
    }

    payload = {
        "usuario": USUARIO.strip(),
        "senha": SENHA.strip()
    }

    resp = requests.post(
        f"{BASE_URL}/usuario/autenticar",
        json=payload,
        headers=headers,
        timeout=30
    )

    resp.raise_for_status()
    data = resp.json()

    token = data.get("token_usuario")
    if not token:
        raise ValueError("Falha na autenticação")

    print("Autenticado com sucesso")
    return token


def listar_por_periodo(token, data_inicio, data_fim):

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    veiculos = []
    offset = 0

    while True:
        payload = {
            "inicio_paginacao": offset,
            "quantidade_por_pagina": LIMIT_PER_PAGE,
            "data_contrato": data_inicio,
            "data_contrato_final": data_fim,
            "codigo_cooperativa": CODIGO_COOPERATIVA
        }

        resp = requests.post(
            f"{BASE_URL}/listar/veiculo",
            json=payload,
            headers=headers,
            timeout=120
        )

        resp.raise_for_status()

        data = resp.json()
        lista = data.get("veiculos", [])

        if not lista:
            break

        veiculos.extend(lista)

        if len(lista) < LIMIT_PER_PAGE:
            break

        offset += LIMIT_PER_PAGE
        time.sleep(1)

    print(f"{data_inicio} até {data_fim} → {len(veiculos)} registros")
    return veiculos


def listar_tudo(token):

    inicio = datetime(2025, 1, 1)
    hoje = datetime.now()

    todos = []

    while inicio <= hoje:

        fim_mes = (inicio.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        fim = min(fim_mes, hoje)

        todos.extend(
            listar_por_periodo(
                token,
                inicio.strftime("%Y-%m-%d"),
                fim.strftime("%Y-%m-%d")
            )
        )

        inicio = fim + timedelta(days=1)

    return todos


def salvar_no_postgres(veiculos):

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS veiculos_valle (
            codigo_veiculo INT PRIMARY KEY,
            placa TEXT,
            modelo TEXT,
            marca TEXT,
            nome_associado TEXT,
            data_contrato DATE,
            codigo_cooperativa INT,
            codigo_situacao INT,
            codigo_associado INT,
            valor_fipe NUMERIC(10,2),
            ano_modelo INT,
            tipo TEXT,
            nome_voluntario TEXT,
            codigo_voluntario INT
        );
    """)

    insert_sql = """
        INSERT INTO veiculos_valle VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (codigo_veiculo) DO UPDATE SET
            placa = EXCLUDED.placa,
            modelo = EXCLUDED.modelo,
            marca = EXCLUDED.marca,
            nome_associado = EXCLUDED.nome_associado,
            data_contrato = EXCLUDED.data_contrato,
            codigo_cooperativa = EXCLUDED.codigo_cooperativa,
            codigo_situacao = EXCLUDED.codigo_situacao,
            codigo_associado = EXCLUDED.codigo_associado,
            valor_fipe = EXCLUDED.valor_fipe,
            ano_modelo = EXCLUDED.ano_modelo,
            tipo = EXCLUDED.tipo,
            nome_voluntario = EXCLUDED.nome_voluntario,
            codigo_voluntario = EXCLUDED.codigo_voluntario;
    """

    dados = []

    for v in veiculos:
        data_contrato = v.get("data_contrato")
        if data_contrato:
            data_contrato = datetime.strptime(data_contrato[:10], "%Y-%m-%d").date()

        dados.append((
            v.get("codigo_veiculo"),
            v.get("placa"),
            v.get("modelo"),
            v.get("marca"),
            v.get("nome_associado"),
            data_contrato,
            v.get("codigo_cooperativa"),
            v.get("codigo_situacao"),
            v.get("codigo_associado"),
            v.get("valor_fipe"),
            v.get("ano_modelo"),
            v.get("tipo"),
            v.get("nome_voluntario"),
            v.get("codigo_voluntario")
        ))

    execute_batch(cur, insert_sql, dados, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

    print("Carga finalizada com sucesso")


if __name__ == "__main__":
    token = autenticar()
    veiculos = listar_tudo(token)
    salvar_no_postgres(veiculos)
