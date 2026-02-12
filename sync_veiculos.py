import requests
import time
import psycopg2
import datetime
import os
from psycopg2.extras import execute_batch

# ================= CONFIGURAÇÕES =================

BASE_URL = "https://api.hinova.com.br/api/sga/v2"

TOKEN_BASE = os.getenv("TOKEN_BASE")
USUARIO = os.getenv("USUARIO_API")
SENHA = os.getenv("SENHA_API")

CODIGO_COOPERATIVA = os.getenv("CODIGO_COOPERATIVA")
DATA_CONTRATO_INICIO = os.getenv("DATA_CONTRATO_INICIO")
DATA_CONTRATO_FIM = os.getenv("DATA_CONTRATO_FIM")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LIMIT_PER_PAGE = 1000
VOLUNTARIO = "VOLUNTARIO"

# ===== LIMPAR ESPAÇOS =====
if TOKEN_BASE: TOKEN_BASE = TOKEN_BASE.strip()
if USUARIO: USUARIO = USUARIO.strip()
if SENHA: SENHA = SENHA.strip()

# ===== DEBUG =====
print("🔎 DEBUG VARIÁVEIS:")
print("TOKEN_BASE:", "OK" if TOKEN_BASE else "None")
print("USUARIO:", "OK" if USUARIO else "None")
print("DB_HOST:", "***" if DB_HOST else "None")

# ===== VALIDAÇÃO =====
variaveis = {
    "TOKEN_BASE": TOKEN_BASE,
    "USUARIO_API": USUARIO,
    "SENHA_API": SENHA,
    "CODIGO_COOPERATIVA": CODIGO_COOPERATIVA,
    "DATA_CONTRATO_INICIO": DATA_CONTRATO_INICIO,
    "DATA_CONTRATO_FIM": DATA_CONTRATO_FIM,
    "DB_HOST": DB_HOST,
    "DB_PORT": DB_PORT,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD
}

faltando = [k for k, v in variaveis.items() if not v]

if faltando:
    raise ValueError(f"❌ Variáveis ausentes: {faltando}")

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

    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()

    token_usuario = resp.json().get("token_usuario")

    if not token_usuario:
        raise ValueError("❌ token_usuario não retornado pela API.")

    print("✅ Autenticado")
    return token_usuario.strip()

# ================= LISTAR VEÍCULOS =================

def listar_veiculos(token_usuario):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_usuario}"
    }

    veiculos_totais = []
    offset = 0

    while True:
        payload = {
            "inicio_paginacao": offset,
            "quantidade_por_pagina": LIMIT_PER_PAGE,
            "data_contrato": DATA_CONTRATO_INICIO,
            "data_contrato_final": DATA_CONTRATO_FIM,
            "codigo_cooperativa": CODIGO_COOPERATIVA,
            "nome_voluntario": VOLUNTARIO
        }

        resp = requests.post(
            BASE_URL + "/listar/veiculo",
            json=payload,
            headers=headers,
            timeout=60
        )

        resp.raise_for_status()

        data = resp.json()
        veiculos = data.get("veiculos", [])

        if not veiculos:
            break

        veiculos_totais.extend(veiculos)

        if len(veiculos) < LIMIT_PER_PAGE:
            break

        offset += LIMIT_PER_PAGE
        time.sleep(1)

    print(f"📥 Total coletado: {len(veiculos_totais)}")
    return veiculos_totais

# ================= SALVAR =================

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

    cur.execute("TRUNCATE TABLE veiculos_valle;")

    insert_sql = """
    INSERT INTO veiculos_valle VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    dados = []

    for v in veiculos:
        data_contrato = v.get("data_contrato")

        if data_contrato:
            data_contrato = datetime.datetime.strptime(
                data_contrato[:10],
                "%Y-%m-%d"
            ).date()

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

    print("✅ Carga finalizada")

# ================= MAIN =================

if __name__ == "__main__":
    token = autenticar()
    veiculos = listar_veiculos(token)
    salvar_no_postgres(veiculos)
