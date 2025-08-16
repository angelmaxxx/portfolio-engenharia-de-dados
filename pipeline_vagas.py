# Importa as bibliotecas necessárias
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd

# Função para limpar o texto e lidar com caracteres problemáticos


def clean_text(text):
    if not isinstance(text, str):
        return text
    # Substitui o traço (en dash) por um hífen simples
    text = text.replace('\u2013', '-')
    # Substitui outros caracteres problemáticos
    text = text.replace('‘', "'").replace('’', "'")
    return text


# Lista para armazenar os dados das vagas
lista_vagas = []

# Define a URL base para a pesquisa de vagas
url_base = 'https://www.vagas.com.br/vagas-de-engenheiro-de-dados?pagina='

print("Iniciando a raspagem de dados...")

# Loop para percorrer as primeiras 5 páginas de resultados
for pagina in range(1, 6):
    print(f'Raspando página {pagina}...')
    url_pagina = f'{url_base}{pagina}'

    try:
        response = requests.get(url_pagina, headers={
                                'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        sopa = BeautifulSoup(response.text, 'html.parser')
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

# Converte a lista para um DataFrame do pandas
if lista_vagas:
    df_vagas = pd.DataFrame(lista_vagas)

    # ---- ETAPA DE LIMPEZA DOS DADOS ----
    # Aplica a função de limpeza nas colunas de texto
    df_vagas['titulo'] = df_vagas['titulo'].apply(clean_text)
    df_vagas['empresa'] = df_vagas['empresa'].apply(clean_text)
    df_vagas['localizacao'] = df_vagas['localizacao'].apply(clean_text)

    # Salva os dados em um arquivo CSV usando UTF-8, que é a codificação padrão e mais robusta
    df_vagas.to_csv('vagas_brutas.csv', index=False, encoding='utf-8')
    print("Dados salvos em 'vagas_brutas.csv'")
else:
    print("Nenhum dado coletado para ser salvo.")
