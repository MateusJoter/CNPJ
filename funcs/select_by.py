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

def UF(DB_PATH, ufs, export_csv = False):
    """Filtra os dados pelos seus locais"""

    if type(ufs) == list:
        ufs = ", ".join([f'{uf}' for uf in ufs])

    elif type(ufs) == str:
        ufs = f"'{ufs}'"

    else:
        print("Os locais devem entrar como str ou list.")
        return
    
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"DROP VIEW IF EXISTS cnpj_UFs_selecionados")

    cursor.execute(f"""
        CREATE VIEW IF NOT EXISTS cnpj_UFs_selecionados AS
        SELECT *
        FROM view_cnpj_completo 
        WHERE uf in ({ufs})
    """) 
    conn.commit()

    if export_csv:
        csv_filtrado = "cnpj_UFs_selecionados.csv"
    
        chunks = pd.read_sql_query(f"SELECT * FROM cnpj_UFs_selecionados", conn, chunksize = 100000)
    
        first_chunk = True
    
        for chunk in tqdm(chunks, desc = "Filtrando os dados por UF"):
            chunk.to_csv(csv_filtrado, index=False, sep=';', encoding='utf-8', mode='a', header=first_chunk)
            first_chunk = False

    conn.close()

def main_CNAE(DB_PATH, CNAEs, export_csv = False):
    """Filtra os dados pelos seus CNAEs principais"""

    if type(CNAEs) == list:
        CNAEs = ", ".join([f'{CNAE}' for CNAE in CNAEs])

    elif type(CNAEs) == str:
        CNAEs = f"'{CNAEs}'"

    else:
        print("Os locais devem entrar como str ou list.")
        return
    
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"DROP VIEW IF EXISTS cnpj_CNAEs_principais_selecionados")

    cursor.execute(f"""
        CREATE VIEW IF NOT EXISTS cnpj_CNAEs_principais_selecionados AS
        SELECT *
        FROM view_cnpj_completo 
        WHERE cnae_fiscal_principal in ({CNAEs})
    """)
    conn.commit()

    if export_csv:
        csv_filtrado = "cnpj_CNAEs_principais_selecionados.csv" 
    
        chunks = pd.read_sql_query(f"SELECT * FROM cnpj_CNAEs_principais_selecionados", conn, chunksize = 100000)
    
        first_chunk = True
    
        for chunk in tqdm(chunks, desc = "Filtrando os dados por CNAE princial"):
            chunk.to_csv(csv_filtrado, index=False, sep=';', encoding='utf-8', mode='a', header=first_chunk)
            first_chunk = False

    conn.close()