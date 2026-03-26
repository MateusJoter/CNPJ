import os
import sqlite3
import pandas as pd
from tqdm import tqdm

def obter_conexao(DB_PATH):
    return sqlite3.connect(DB_PATH)

def carregar_dataframe_sqlite(csv_file, DB_PATH):
    """Transforma o CSV de entrada em um DB"""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cnpjs (
            cnae_fiscal_principal TEXT,
            cnae_fiscal_secundaria TEXT,
            cnpj_completo TEXT,
            razao_social TEXT,
            natureza_juridica TEXT,
            qualificacao_responsavel TEXT,
            capital_social TEXT,
            porte_empresa TEXT,
            ente_federativo_responsavel TEXT,
            matriz_filial TEXT,
            situacao_cadastral TEXT,
            data_situacao_cadastral TEXT,
            nome_cidade_exterior TEXT,
            pais TEXT,
            data_inicio_atividade TEXT,
            tipo_logradouro TEXT,
            logradouro TEXT,
            numero TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            uf TEXT,
            municipio TEXT,
            situacao_especial TEXT,
            data_situacao_especial TEXT
        );
    """)
    conn.commit()
    
    cols = ["cnae_fiscal_principal", "cnae_fiscal_secundaria", "cnpj_completo", "razao_social", "natureza_juridica", "qualificacao_responsavel", 
            "capital_social", "porte_empresa", "ente_federativo_responsavel", "matriz_filial", "situacao_cadastral", "data_situacao_cadastral", 
            "nome_cidade_exterior", "pais", "data_inicio_atividade", "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", 
            "cep", "uf", "municipio", "situacao_especial", "data_situacao_especial"]
    
    chunk_iter = pd.read_csv(csv_file, sep=';', encoding='UTF-8', header=None, names=cols, chunksize=100000, dtype=str)
    
    for chunk in tqdm(chunk_iter, desc = "Passando do .csv para um Banco de Dados"):
        chunk.to_sql("cnpjs", conn, if_exists='append', index=False)

    conn.close()

def places(DB_PATH_entrada, DB_PATH_saida, uf):
    """Filtra os dados pelos seus locais"""
    conn = obter_conexao(DB_PATH_entrada)
    cursor = conn.cursor()

    cursor.execute(f"ATTACH DATABASE '{DB_PATH_saida}' AS filtered_db")
    
    cursor.execute(f"""
        CREATE TABLE filtered_db.cnpj_{uf} AS
        SELECT *
        FROM view_cnpj_completo 
        WHERE uf = '{uf}'
    """) # AJUSTAR view_cnpj_completo SE NECESSÁRIO
    conn.commit()

    csv_filtrado = "cnpj_uf.csv" # AJUSTAR SE NECESSÁRIO

    chunks = pd.read_sql_query(f"SELECT * FROM filtered_db.cnpj_{uf}", conn, chunksize = 100000)

    first_chunk = True

    for chunk in tqdm(chunks, desc = "Filtrando os dados por UF"):
        chunk.to_csv(csv_filtrado, index=False, sep=';', encoding='utf-8', mode='a', header=first_chunk)
        first_chunk = False

    conn.close()