import os
import requests
import time
import psycopg2
import sys
from datetime import datetime
from psycopg2.extras import execute_batch

# --- Configurações via Variáveis de Ambiente ---
BASE_URL = "https://api.hinova.com.br/api/sga/v2"

# Pegando os dados do GitHub Secrets
TOKEN_BASE = os.getenv("HINOVA_TOKEN_BASE")
USUARIO = os.getenv("HINOVA_USUARIO")
SENHA = os.getenv("HINOVA_SENHA")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --- DEBUG DE SEGURANÇA (Para você ver se o GitHub está lendo os Secrets) ---
print("--- CHECKLIST DE CREDENCIAIS ---")
print(f"TOKEN_BASE carregado: {'SIM' if TOKEN_BASE else 'NÃO (ERRO NO SECRET)'}")
print(f"USUARIO carregado: {USUARIO if USUARIO else 'NÃO (ERRO NO SECRET)'}")
print(f"SENHA carregada: {'SIM' if SENHA else 'NÃO (ERRO NO SECRET)'}")
print("--------------------------------")

if not all([TOKEN_BASE, USUARIO, SENHA]):
    print("ERRO CRÍTICO: Variáveis de ambiente faltando. Verifique os Secrets no GitHub.")
    sys.exit(1)

CODIGO_COOPERATIVA = "48"
DATA_CONTRATO_INICIO = "2025-01-01"
DATA_CONTRATO_FIM = datetime.now().strftime("%Y-%m-%d")
LIMIT_PER_PAGE = 1000
VOLUNTARIO = "VOLUNTARIO"

SITUACOES = [
    {"codigo": "1", "descricao": "ATIVO"}, {"codigo": "2", "descricao": "INATIVO"},
    {"codigo": "3", "descricao": "PENDENTE"}, {"codigo": "4", "descricao": "INADIMPLENTE"},
    {"codigo": "5", "descricao": "NEGADO"}, {"codigo": "6", "descricao": "SUSPENSO"},
    {"codigo": "7", "descricao": "SUSPENSO PENDENCIA"}, {"codigo": "8", "descricao": "PENDENCIA"},
    {"codigo": "9", "descricao": "SUSPENSO ADIMPLENTE"}, {"codigo": "10", "descricao": "ATIVO PENDENTE"},
    {"codigo": "11", "descricao": "INATIVO PENDENTE"}, {"codigo": "12", "descricao": "INADIMPLENTE PENDENTE"},
    {"codigo": "14", "descricao": "EXCLUSÃO PENDENTE"}, {"codigo": "16", "descricao": "INADIMPLENTE - EM CANCELAMENTO"},
    {"codigo": "17", "descricao": "INATIVO - EM CANCELAMENTO"}, {"codigo": "18", "descricao": "INADIMPLENTE PENDENTE - EM CANCELAMENTO"},
    {"codigo": "19", "descricao": "INADIMPLENTE L. FECHAMENTO"}, {"codigo": "20", "descricao": "INADIMPLENTE PENDENTE L. FECHAMENTO"},
    {"codigo": "21", "descricao": "SUSPENSO TERCEIRIZADA"}, {"codigo": "22", "descricao": "EM CANCELAMENTO"}
]

def autenticar():
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN_BASE}"
    }
    payload = {
        "usuario": USUARIO,
        "senha": SENHA
    }

    try:
        resp = requests.post(f"{BASE_URL}/usuario/autenticar", json=payload, headers=headers, timeout=30)
        
        if resp.status_code == 401:
            print(f"ERRO 401: Credenciais inválidas. Payload enviado: {{'usuario': '{USUARIO}', 'senha': '***'}}")
            print(f"Resposta da API: {resp.text}")
        
        resp.raise_for_status()
        token = resp.json().get("token_usuario")
        if not token:
            raise ValueError("Falha na autenticação: token_usuario não retornado.")
        
        print("Autenticado com sucesso")
        return token
    except Exception as e:
        print(f"Falha na conexão/autenticação: {e}")
        raise

def listar_veiculos_por_situacao(token_usuario, codigo_situacao, descricao):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token_usuario}"}
    todos = []
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
        resp = requests.post(f"{BASE_URL}/listar/veiculo", json=payload, headers=headers, timeout=120)
        resp.raise_for_status()
        veiculos = resp.json().get("veiculos", [])
        if not veiculos: break
        for v in veiculos: v["descricao_situacao"] = descricao
        todos.extend(veiculos)
        print(f"{descricao} | Offset {offset} | Total parcial {len(todos)}")
        if len(veiculos) < LIMIT_PER_PAGE: break
        offset += LIMIT_PER_PAGE
        time.sleep(1)
    return todos

def salvar_no_postgres(veiculos):
    if not veiculos:
        print("Nenhum veículo para salvar.")
        return
    
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS veiculos_valle (
            codigo_veiculo INT PRIMARY KEY, placa TEXT, modelo TEXT, marca TEXT,
            nome_associado TEXT, data_contrato DATE, codigo_cooperativa INT,
            codigo_situacao INT, codigo_associado INT, valor_fipe NUMERIC(10,2),
            ano_modelo INT, tipo TEXT, nome_voluntario TEXT, codigo_voluntario INT
        );
    """)
    insert_sql = """
        INSERT INTO veiculos_valle VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (codigo_veiculo) DO UPDATE SET
            placa = EXCLUDED.placa, modelo = EXCLUDED.modelo, marca = EXCLUDED.marca,
            nome_associado = EXCLUDED.nome_associado, data_contrato = EXCLUDED.data_contrato,
            codigo_cooperativa = EXCLUDED.codigo_cooperativa, codigo_situacao = EXCLUDED.codigo_situacao,
            codigo_associado = EXCLUDED.codigo_associado, valor_fipe = EXCLUDED.valor_fipe,
            ano_modelo = EXCLUDED.ano_modelo, tipo = EXCLUDED.tipo,
            nome_voluntario = EXCLUDED.nome_voluntario, codigo_voluntario = EXCLUDED.codigo_voluntario;
    """
    dados = []
    for v in veiculos:
        data_c = v.get("data_contrato")
        if data_c:
            try:
                data_c = datetime.strptime(data_c[:10], "%Y-%m-%d").date()
            except:
                data_c = None
        
        dados.append((
            v.get("codigo_veiculo"), v.get("placa"), v.get("modelo"), v.get("marca"),
            v.get("nome_associado"), data_c, v.get("codigo_cooperativa"),
            v.get("codigo_situacao"), v.get("codigo_associado"), v.get("valor_fipe"),
            v.get("ano_modelo"), v.get("tipo"), v.get("nome_voluntario"), v.get("codigo_voluntario")
        ))
    execute_batch(cur, insert_sql, dados, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Carga de {len(veiculos)} veículos finalizada com sucesso")

if __name__ == "__main__":
    try:
        token = autenticar()
        veiculos_geral = []
        for situacao in SITUACOES:
            veiculos_geral.extend(listar_veiculos_por_situacao(token, situacao["codigo"], situacao["descricao"]))
        salvar_no_postgres(veiculos_geral)
    except Exception as e:
        print(f"Erro na execução principal: {e}")
        sys.exit(1)
