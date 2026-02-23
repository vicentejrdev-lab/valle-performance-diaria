import requests
import time
import psycopg2
import datetime
from psycopg2.extras import execute_batch
from requests.exceptions import RequestException, JSONDecodeError

# ================= CONFIGURAÇÕES =================

BASE_URL = "https://api.hinova.com.br/api/sga/v2"

TOKEN_BASE = "63a016edf6e94187ca280bbee808d25c4bdd7b5ab27b371a081de12413fbfb7bdea75658f1e518480e2fb0ca33488d8bc711e02a80617d933603142469b1c5ee5f6a732c4d0014537caee7c87bcf16c359144a7a1f8ab6bf3805a8292289bbb4"
USUARIO = "VICENTEJR"
SENHA = "pa7663CG#"

CODIGO_COOPERATIVA = "48"
DATA_CONTRATO_INICIO = "2025-01-01"
DATA_CONTRATO_FIM = "2026-12-31"
LIMIT_PER_PAGE = 1000
VOLUNTARIO = "VOLUNTARIO"

DB_HOST = "opal4.opalstack.com"
DB_PORT = "5432"
DB_NAME = "ancore_db"
DB_USER = "ancore_user"
DB_PASSWORD = "urOcaq9XI7Y1CCS"

# ================= SITUAÇÕES =================

SITUACOES = [
    {"codigo": "1", "descricao": "ATIVO"},
    {"codigo": "2", "descricao": "INATIVO"},
    {"codigo": "3", "descricao": "PENDENTE"},
    {"codigo": "4", "descricao": "INADIMPLENTE"},
    {"codigo": "5", "descricao": "NEGADO"},
    {"codigo": "6", "descricao": "SUSPENSO"},
    {"codigo": "7", "descricao": "SUSPENSO PENDENCIA"},
    {"codigo": "8", "descricao": "PENDENCIA"},
    {"codigo": "9", "descricao": "SUSPENSO ADIMPLENTE"},
    {"codigo": "10", "descricao": "ATIVO PENDENTE"},
    {"codigo": "11", "descricao": "INATIVO PENDENTE"},
    {"codigo": "12", "descricao": "INADIMPLENTE PENDENTE"},
    {"codigo": "14", "descricao": "EXCLUSÃO PENDENTE"},
    {"codigo": "16", "descricao": "INADIMPLENTE - EM CANCELAMENTO"},
    {"codigo": "17", "descricao": "INATIVO - EM CANCELAMENTO"},
    {"codigo": "18", "descricao": "INADIMPLENTE PENDENTE - EM CANCELAMENTO"},
    {"codigo": "19", "descricao": "INADIMPLENTE L. FECHAMENTO"},
    {"codigo": "20", "descricao": "INADIMPLENTE PENDENTE L. FECHAMENTO"},
    {"codigo": "21", "descricao": "SUSPENSO TERCEIRIZADA"},
    {"codigo": "22", "descricao": "EM CANCELAMENTO"}
]

# ================= AUTENTICAÇÃO =================

def autenticar():
    url = BASE_URL + "/usuario/autenticar"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN_BASE}"
    }

    payload = {
        "usuario": USUARIO,
        "senha": SENHA
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        token_usuario = resp.json().get("token_usuario")
        print("✅ Autenticado")
        return token_usuario
    except Exception as e:
        print("❌ Falha na autenticação:", e)
        return None

# ================= LISTAR VEÍCULOS =================

def listar_veiculos_por_situacao(token_usuario, codigo_situacao, descricao):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_usuario}"
    }

    veiculos_totais = []
    offset = 0

    while True:
        payload = {
            "codigo_situacao": codigo_situacao,
            "inicio_paginacao": offset,
            "quantidade_por_pagina": LIMIT_PER_PAGE,
            "data_contrato": DATA_CONTRATO_INICIO,
            "data_contrato_final": DATA_CONTRATO_FIM,
            "codigo_cooperativa": CODIGO_COOPERATIVA,
            "nome_voluntario": VOLUNTARIO
        }

        try:
            resp = requests.post(BASE_URL + "/listar/veiculo", json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print("Erro ao buscar:", e)
            break

        veiculos = data.get("veiculos", [])
        if not veiculos:
            break

        for v in veiculos:
            v["descricao_situacao"] = descricao

        veiculos_totais.extend(veiculos)

        print(f"{descricao}: +{len(veiculos)} registros (total {len(veiculos_totais)})")

        if len(veiculos) < LIMIT_PER_PAGE:
            break

        offset += LIMIT_PER_PAGE
        time.sleep(1)

    return veiculos_totais

# ================= SALVAR NO POSTGRES =================

def salvar_no_postgres(veiculos):
    print("🔗 Conectando ao PostgreSQL...")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # cria tabela se não existir
    cur.execute("""
    CREATE TABLE IF NOT EXISTS veiculos_valle (
        codigo_veiculo INT,
        placa TEXT,
        modelo TEXT,
        marca TEXT,
        nome_associado TEXT,
        data_contrato DATE,
        codigo_cooperativa INT,
        codigo_situacao INT,
        codigo_associado INT,
        valor_fipe NUMERIC (10,2),
        ano_modelo INT,
        tipo TEXT,
        nome_voluntario TEXT,
        codigo_voluntario INT
    );
    """)

    # limpa dados (sem quebrar views)
    print("🧹 Limpando tabela...")
    cur.execute("TRUNCATE TABLE veiculos_valle;")

    insert_sql = """
    INSERT INTO veiculos_valle VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    dados = []
    for v in veiculos:
        data_contrato = v.get("data_contrato")
        if data_contrato:
            data_contrato = datetime.datetime.strptime(data_contrato[:10], "%Y-%m-%d").date()

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

    print(f"📥 Inserindo {len(dados)} registros...")
    execute_batch(cur, insert_sql, dados, page_size=1000)

    cur.close()
    conn.close()
    print("✅ Carga finalizada!")

# ================= MAIN =================

if __name__ == "__main__":
    token = autenticar()

    if token:
        todos = []
        for s in SITUACOES:
            todos.extend(listar_veiculos_por_situacao(token, s["codigo"], s["descricao"]))

        salvar_no_postgres(todos)
