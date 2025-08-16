# Importa as bibliotecas necessárias
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import psycopg2
from psycopg2 import sql
import re  # Nova importação para expressões regulares

# ---- PARÂMETROS DE CONEXÃO COM O BANCO DE DADOS ----
DB_HOST = "localhost"
DB_NAME = "vagas_db"
DB_USER = "postgres"  # <-- SUBSTITUA
DB_PASS = "sua_nova_senha"   # <-- SUBSTITUA

# Lista para armazenar os dados das vagas
lista_vagas = []

# Define a URL base para a pesquisa de vagas
url_base = 'https://www.vagas.com.br/vagas-de-engenheiro-de-dados?pagina='

# Função para limpar o texto e remover caracteres problemáticos


def clean_text(text):
    if not isinstance(text, str):
        return text
    # Apenas mantém os caracteres ASCII, ou seja, letras, números e pontuação básica.
    return text.encode("ascii", "ignore").decode()


print("Iniciando a raspagem de dados...")

# Loop para percorrer as primeiras 5 páginas de resultados
for pagina in range(1, 6):
    print(f'Raspando página {pagina}...')
    url_pagina = f'{url_base}{pagina}'

    try:
        response = requests.get(url_pagina, headers={
                                'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()

        # ---- SOLUÇÃO FINAL E ABSOLUTA PARA CODIFICAÇÃO ----
        # Acessa o conteúdo binário da resposta e decodifica manualmente
        html_content = response.content.decode('latin-1')
        sopa = BeautifulSoup(html_content, 'html.parser')

        vagas = sopa.find_all('li', class_='vaga')

        if not vagas:
            print("Nenhuma vaga encontrada nesta página. Fim da raspagem.")
            break

        for vaga in vagas:
            titulo_element = vaga.find('h2', class_='cargo').find(
                'a', class_='link-detalhes-vaga')
            titulo = titulo_element.get_text(
                strip=True) if titulo_element else 'N/A'
            link = "https://www.vagas.com.br" + \
                titulo_element['href'] if titulo_element else 'N/A'

            empresa_element = vaga.find('span', class_='empr Vaga')
            empresa = empresa_element.get_text(
                strip=True) if empresa_element else 'N/A'

            localizacao_element = vaga.find('span', class_='localizacao')
            localizacao = localizacao_element.get_text(
                strip=True) if localizacao_element else 'N/A'

            dados_vaga = {
                'titulo': titulo,
                'empresa': empresa,
                'localizacao': localizacao,
                'link': link
            }
            lista_vagas.append(dados_vaga)

        time.sleep(2)

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a página {url_pagina}: {e}")
        continue

print(f"\nRaspagem concluída. Total de vagas coletadas: {len(lista_vagas)}")


# ---- ETAPA DE INGESTÃO DOS DADOS NO SQLITE ----
if lista_vagas:
    print("Iniciando a ingestão de dados...")

    # Cria o DataFrame e aplica a limpeza
    df_vagas = pd.DataFrame(lista_vagas)
    df_vagas['titulo'] = df_vagas['titulo'].apply(clean_text)
    df_vagas['empresa'] = df_vagas['empresa'].apply(clean_text)
    df_vagas['localizacao'] = df_vagas['localizacao'].apply(clean_text)

    # Conecta ao banco de dados SQLite (o arquivo será criado se não existir)
    import sqlite3
    conn = sqlite3.connect('vagas_db.sqlite')

    try:
        # Insere o DataFrame diretamente na tabela 'vagas'
        df_vagas.to_sql('vagas', conn, if_exists='replace', index=False)
        print(f"Ingestão de {len(df_vagas)} linhas concluída com sucesso!")
    except Exception as e:
        print(f"Erro na ingestão de dados: {e}")
    finally:
        conn.close()
        print("Conexão com o banco de dados fechada.")
else:
    print("Nenhum dado para ingerir.")
