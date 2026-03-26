import sqlite3
import pandas as pd
import os
import zipfile
import io

def obter_conexao(DB_PATH):
    return sqlite3.connect(DB_PATH)

def configurar_schema(DB_PATH):
    """Cria as tabelas principais e as tabelas de dicionário/referência."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico TEXT, 
            razao_social TEXT, 
            natureza_juridica TEXT,
            qualificacao_responsavel TEXT, 
            capital_social REAL, 
            porte_empresa TEXT,
            ente_federativo_responsavel TEXT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estabelecimentos (
            cnpj_completo TEXT,
            cnpj_basico TEXT, 
            cnpj_ordem TEXT, 
            cnpj_dv TEXT, 
            identificador_matriz_filial TEXT,
            nome_fantasia TEXT, 
            situacao_cadastral INTEGER, 
            data_situacao_cadastral TEXT,
            motivo_situacao_cadastral TEXT, 
            nome_cidade_exterior TEXT, 
            pais TEXT,
            data_inicio_atividade TEXT, 
            cnae_fiscal_principal TEXT, 
            cnae_fiscal_secundaria TEXT,
            tipo_logradouro TEXT, 
            logradouro TEXT, 
            numero TEXT, 
            complemento TEXT,
            bairro TEXT, 
            cep TEXT, 
            uf TEXT, 
            municipio TEXT, 
            ddd_1 TEXT, 
            telefone_1 TEXT, 
            ddd_2 TEXT, 
            telefone_2 TEXT, 
            ddd_fax TEXT, 
            fax TEXT, 
            correio_eletronico TEXT,
            situacao_especial TEXT, 
            data_situacao_especial TEXT
        );
    """)
    for tab in ["cnae", "natju", "quals", "pais", "munic"]:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {tab} (codigo TEXT, descricao TEXT);")
    conn.commit()
    conn.close()

def criar_indices(DB_PATH):
    """Cria índices para acelerar os JOINs das tabelas principais e dicionários."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    print("Criando índices de performance...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emp_cnpj ON empresas (cnpj_basico);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_est_cnpj ON estabelecimentos (cnpj_basico);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_est_cnpj_full ON estabelecimentos (cnpj_completo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_natju_cod ON natju (codigo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_quals_cod ON quals (codigo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pais_cod ON pais (codigo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_munic_cod ON munic (codigo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cnae_cod ON cnae (codigo);")
    conn.commit()
    conn.close()

def formatar_datas_estabelecimentos(DB_PATH):
    """Transforma as datas de AAAAMMDD para DD/MM/AAAA."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    print("Formatando campos de data (AAAAMMDD -> DD/MM/AAAA)...")
    colunas_data = ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']
    for col in colunas_data:
        cursor.execute(f"""
            UPDATE estabelecimentos 
            SET {col} = substr({col}, 7, 2) || '/' || substr({col}, 5, 2) || '/' || substr({col}, 1, 4)
            WHERE length({col}) = 8 AND {col} NOT LIKE '%/%';
        """)
    conn.commit()
    conn.close()

def corrigir_numeros_estabelecimentos(DB_PATH):
    """Corrige números de imóveis (0, 00, 000 para vazio e SN para S/N)."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    print("Corrigindo números de imóveis...")
    cursor.execute("""
        UPDATE estabelecimentos
        SET numero = CASE
            WHEN numero IN ('0', '00', '000') THEN ''
            WHEN numero = 'SN' THEN 'S/N'
            ELSE numero
        END
        WHERE numero IN ('0', '00', '000', 'SN');
    """)
    conn.commit()
    conn.close()

def padronizar_logradouros(DB_PATH):
    """Padroniza o campo tipo_logradouro para RUA, AVENIDA ou TRAVESSA."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    print("Padronizando tipos de logradouro...")
    cursor.execute("""
        UPDATE estabelecimentos
        SET tipo_logradouro = CASE
            WHEN tipo_logradouro LIKE '%RUA%' THEN 'RUA'
            WHEN tipo_logradouro LIKE '%AVENIDA%' THEN 'AVENIDA'
            WHEN tipo_logradouro LIKE '%TRAVESSA%' THEN 'TRAVESSA'
            ELSE tipo_logradouro
        END
        WHERE tipo_logradouro LIKE '%RUA%' 
           OR tipo_logradouro LIKE '%AVENIDA%' 
           OR tipo_logradouro LIKE '%TRAVESSA%';
    """)
    conn.commit()
    conn.close()

def traduzir_codigos(DB_PATH):
    """Traduz códigos de Matriz/Filial e Porte da Empresa e renomeia coluna."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    print("Traduzindo códigos de Matriz/Filial e Porte...")
    cursor.execute("""
        UPDATE estabelecimentos
        SET identificador_matriz_filial = CASE
            WHEN identificador_matriz_filial IN ('1', 1) THEN 'Matriz'
            WHEN identificador_matriz_filial IN ('2', 2) THEN 'Filial'
            ELSE 'outro'
        END;
    """)
    try:
        cursor.execute("ALTER TABLE estabelecimentos RENAME COLUMN identificador_matriz_filial TO matriz_filial;")
    except sqlite3.OperationalError: pass 

    cursor.execute("""
        UPDATE empresas
        SET porte_empresa = CASE
            WHEN porte_empresa = '00' THEN 'NÃO INFORMADO'
            WHEN porte_empresa = '01' THEN 'MICRO EMPRESA'
            WHEN porte_empresa = '03' THEN 'EMPRESA DE PEQUENO PORTE'
            WHEN porte_empresa = '05' THEN 'DEMAIS'
            ELSE porte_empresa
        END;
    """)
    conn.commit()
    conn.close()

def criar_view_consolidada(DB_PATH):
    """Cria a VIEW consolidada final com a ordem exata das colunas solicitadas."""
    conn = obter_conexao(DB_PATH)
    cursor = conn.cursor()
    print("Criando VIEW consolidada final...")
    cursor.execute("DROP VIEW IF EXISTS view_cnpj_completo;")
    cursor.execute("""
        CREATE VIEW view_cnpj_completo AS
        SELECT 
            t.cnae_fiscal_principal,
            t.cnae_fiscal_secundaria,
            t.cnpj_completo,
            e.razao_social,
            nj.descricao AS natureza_juridica,
            q.descricao AS qualificacao_responsavel,
            e.capital_social,
            e.porte_empresa,
            e.ente_federativo_responsavel,
            t.matriz_filial,
            t.situacao_cadastral,
            t.data_situacao_cadastral,
            t.nome_cidade_exterior,
            p.descricao AS pais,
            t.data_inicio_atividade,
            t.tipo_logradouro,
            t.logradouro,
            t.numero,
            t.complemento,
            t.bairro,
            t.cep,
            t.uf,
            m.descricao AS municipio,
            t.situacao_especial,
            t.data_situacao_especial
        FROM estabelecimentos t
        LEFT JOIN empresas e ON t.cnpj_basico = e.cnpj_basico
        LEFT JOIN natju nj ON e.natureza_juridica = nj.codigo
        LEFT JOIN quals q ON e.qualificacao_responsavel = q.codigo
        LEFT JOIN pais p ON t.pais = p.codigo
        LEFT JOIN munic m ON t.municipio = m.codigo;
    """)
    conn.commit()
    conn.close()

def exportar_csv_final(DB_PATH):
    """Exporta todos os dados para arquivos CSV (sem limite de linhas)."""
    conn = obter_conexao(DB_PATH)
    
    # Exportação 1: Dados Consolidados
    csv_consolidado = "dados_cnpj_consolidado.csv"
    print(f"Exportando todos os dados para {csv_consolidado} (em blocos)...")
    
    # Usamos chunks para não sobrecarregar a memória RAM na leitura/escrita de milhões de linhas
    chunks = pd.read_sql_query("SELECT * FROM view_cnpj_completo", conn, chunksize=500000)
    
    first_chunk = True
    for chunk in chunks:
        # Modo 'a' (append) para ir adicionando ao arquivo
        chunk.to_csv(csv_consolidado, index=False, sep=';', encoding='utf-8', mode='a', header=first_chunk)
        first_chunk = False
        print(".", end="", flush=True)

    # Exportação 2: Dicionário CNAE
    csv_cnae = "dicionario_cnaes.csv"
    print(f"\nExportando {csv_cnae}...")
    df_cnae = pd.read_sql_query("SELECT * FROM cnae", conn)
    df_cnae.to_csv(csv_cnae, index=False, sep=';', encoding='utf-8')
    
    conn.close()
    print("\nExportação concluída com sucesso!")

def processar_zip_principal(caminho_zip_master, DB_PATH):
    """Lida com a estrutura de ZIP dentro de ZIP sem extração física."""
    if not os.path.exists(caminho_zip_master):
        print(f"Erro: Arquivo {caminho_zip_master} não encontrado.")
        return False
    with zipfile.ZipFile(caminho_zip_master, 'r') as master_zip:
        nomes_sub_zips = [nome for nome in master_zip.namelist() if nome.endswith('.zip')]
        for nome_sub_zip in nomes_sub_zips:
            with master_zip.open(nome_sub_zip) as sub_zip_data:
                content = io.BytesIO(sub_zip_data.read())
                with zipfile.ZipFile(content, 'r') as sub_zip:
                    nome_csv = sub_zip.namelist()[0]
                    tabela = ""
                    n = nome_csv.upper()
                    if "EMPRE" in n: tabela = "empresas"
                    elif "ESTABELE" in n: tabela = "estabelecimentos"
                    elif "CNAE" in n: tabela = "cnae"
                    elif "NATJU" in n: tabela = "natju"
                    elif "QUALS" in n: tabela = "quals"
                    elif "PAIS" in n: tabela = "pais"
                    elif "MUNIC" in n: tabela = "munic"
                    if tabela:
                        print(f"Processando {nome_csv}...")
                        with sub_zip.open(nome_csv) as csv_file:
                            carregar_dataframe_sqlite(csv_file, tabela, DB_PATH)
    return True

def carregar_dataframe_sqlite(file_object, tabela, DB_PATH):
    """Insere dados no SQLite em chunks concatenando o CNPJ completo."""
    conn = obter_conexao(DB_PATH)
    cols_emp = ['cnpj_basico','razao_social','natureza_juridica','qualificacao_responsavel','capital_social','porte_empresa','ente_federativo_responsavel']
    cols_est = ['cnpj_basico','cnpj_ordem','cnpj_dv','identificador_matriz_filial','nome_fantasia','situacao_cadastral','data_situacao_cadastral','motivo_situacao_cadastral','nome_cidade_exterior','pais','data_inicio_atividade','cnae_fiscal_principal','cnae_fiscal_secundaria','tipo_logradouro','logradouro','numero','complemento','bairro','cep','uf','municipio','ddd_1','telefone_1','ddd_2','telefone_2','ddd_fax','fax','correio_eletronico','situacao_especial','data_situacao_especial']
    cols_ref = ['codigo', 'descricao']
    colunas = cols_emp if tabela == 'empresas' else (cols_est if tabela == 'estabelecimentos' else cols_ref)
    
    chunk_iter = pd.read_csv(file_object, sep=';', encoding='latin-1', header=None, names=colunas, chunksize=100000, dtype=str)
    
    for chunk in chunk_iter:
        if tabela == 'estabelecimentos':
            chunk['cnpj_completo'] = chunk['cnpj_basico'] + chunk['cnpj_ordem'] + chunk['cnpj_dv']
        if 'capital_social' in chunk.columns:
            chunk['capital_social'] = chunk['capital_social'].str.replace(',', '.').astype(float)
        chunk.to_sql(tabela, conn, if_exists='append', index=False)
    conn.close()

def aglutinar_endereco(db_path, db_name):
    """
    Cria uma VIEW no SQLite que exclui as colunas individuais de endereço 
    e mantém apenas a coluna aglutinada junto aos metadados do CNPJ.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    sql_view = f"""
    CREATE VIEW IF NOT EXISTS view_endereco_simplificado AS
    SELECT 
        cnae_fiscal_principal,
        cnae_fiscal_secundaria,
        cnpj_completo,
        razao_social,
        natureza_juridica,
        qualificacao_responsavel,
        capital_social,
        porte_empresa,
        ente_federativo_responsavel,
        matriz_filial,
        situacao_cadastral,
        data_situacao_cadastral,
        nome_cidade_exterior,
        pais,
        data_inicio_atividade,
        complemento,
        situacao_especial,
        data_situacao_especial,
        (
            COALESCE(tipo_logradouro, '') || ' ' || 
            COALESCE(logradouro, '') || ', ' || 
            COALESCE(numero, 'S/N') || ' - ' || 
            COALESCE(bairro, '') || ', ' || 
            COALESCE(municipio, '') || ' - ' || 
            COALESCE(uf, '') || ', ' || 
            COALESCE(cep, '') || ', BRASIL'
        ) AS endereco_completo
    FROM {db_name}
    """

    try:
        cursor.execute("DROP VIEW IF EXISTS view_endereco_simplificado") 
        cursor.execute(sql_view)
        conn.commit()
        print("✓ VIEW 'view_endereco_simplificado' atualizada. Colunas redundantes removidas.")
    except sqlite3.Error as e:
        print(f"Erro ao configurar a VIEW: {e}")
    finally:
        conn.close()

def validacao(db_path, db_name, limite=5):
    """
    Validação visual das colunas tratadas, exibindo um recorte das primeiras linhas.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Permite acessar colunas pelo nome
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {db_name} LIMIT ?", (limite,))
        rows = cursor.fetchall()
        
        if rows:
            # Exibe os nomes das colunas uma única vez
            colunas = list(rows[0].keys())
            print(f"\n{'='*100}")
            print(f"COLUNAS PRESENTES: {colunas}")
            print(f"{'='*100}")
            
            # Exibe o conteúdo de cada linha formatado como dicionário para facilitar a leitura
            print(f"\nRECORTE DAS PRIMEIRAS {len(rows)} LINHAS DA TABELA '{db_name}':")
            for idx, row in enumerate(rows, 1):
                print(f"\n[Registro {idx}]")
                for coluna in colunas:
                    print(f"  {coluna.ljust(25)}: {row[coluna]}")
            print(f"\n{'='*100}")
        else:
            print(f"A tabela ou view '{db_name}' está vazia.")
            
    except sqlite3.Error as e:
        print(f"Erro na validação: {e}")
    finally:
        conn.close()