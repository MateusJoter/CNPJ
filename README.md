# Processamento de Dados de CNPJ - Receita Federal

Este repositório contém um pipeline robusto para a extração, tratamento e análise dos dados públicos de CNPJ disponibilizados mensalmente pela Receita Federal do Brasil. O objetivo principal é transformar os arquivos brutos (zipados) em bancos de dados relacionais SQLite (.db) otimizados para consulta.

## Entradas

A única entrada necessária é o grande arquivo zipado dos .csv vindo do site da [**Receita Federal do Brasil**](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9). 
Acesse o link, selecione o mês e baixe o arquivo através do botão "*↓ Baixar*"

## 🚀 Funcionalidades

O projeto é dividido em três workflows principais que garantem a integridade e a utilidade dos dados:

### 1. Workflow de Tratamento - tratamento.ipynb

O ponto de entrada do pipeline. Ele é responsável por:

**Consumo**: Receber o pacote completo de arquivos .zip de um determinado mês.

```python
ZIP_COMPLETO = r"C:\seu_arquivo_baixado.zip"

DB_PATH = "dados_receita.db"

DB_NAME_IN = "view_cnpj_completo"

exportar_csv = False
```

**Processamento**: Realizar o parsing dos arquivos de layout fixo da Receita.

**Unificação**: Consolidar todas as tabelas (Empresas, Estabelecimentos, Sócios, CNAEs, etc.) num único banco de dados SQLite.

**Saída**: Uma VIEW completa que permite visualizar os dados de forma integrada sem a necessidade de múltiplos JOINs manuais.

### 2. Workflow de Unificação de Endereços - aglutinar_enderecos.ipynb

Focado na qualidade dos dados geográficos:

**Entrada**: Banco de dados gerado no workflow de tratamento.

```python
DB_PATH = "sua_db_de_cnpjs.db"

DB_NAME_IN = "nome_tabela_entrada"

DB_NAME_OUT = "nome_tabela_saida"
```

**Ação**: Normaliza campos de logradouro, número, bairro e CEP.

**Saída**: Uma VIEW otimizada com endereços unificados, facilitando processos de geocodificação ou análise regional.

### 3. Workflow de Filtragem - filtragem.ipynb

Permite a extração de subconjuntos específicos de dados:

**Segmentação**: Cria views baseadas em filtros customizáveis.

**Filtros Atuais**: Seleção por UF (Unidade da Federação) e CNAE (Classificação Nacional de Atividades Económicas).

**Utilidade**: Ideal para quem precisa de analisar apenas um setor específico da economia ou uma região geográfica sem carregar o banco de dados completo.

## 🛠️ Tecnologias Utilizadas

**Python**: Linguagem base para a lógica de extração e automação.

**SQLite**: Engine de banco de dados para armazenamento eficiente em arquivos .db.

**SQL**: Para criação de views complexas e otimização de consultas.

## 📂 Como Utilizar

**Download dos Dados**: Descarregue os arquivos zipados do site oficial da Receita Federal.

**Execução do Tratamento**: Aponte o script para a pasta dos zips para gerar o banco consolidado.

**Unificação/Filtragem**: Execute os workflows subsequentes conforme a sua necessidade de análise.

**Nota**: Este projeto é destinado a investigadores, analistas de dados e programadores que procuram agilidade no manuseio da base de dados de CNPJs, que originalmente possui um volume massivo de informações.
