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

LIMIT_PER_PAGE = 1000

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Limpar espaços invisíveis
for var in ["TOKEN_BASE", "USUARIO", "SENHA"]:
    if globals()[var]:
        globals()[var] = globals()[var].strip()

# Validação
faltando = [
    nome for nome, valor in {
        "TOKEN_BASE": TOKEN_BASE,
        "USUARIO_API": USUARIO,
        "SENHA_API": SENHA,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD
    }.items() if not valor
]

if faltando:
    raise ValueError(f"Variáveis de ambiente ausentes: {faltando}")

# ================= AUTENTICAÇÃO =================

def autenticar():
    url = BASE_URL + "/usuario/autenticar"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN_BASE}"
    }

    payloa
