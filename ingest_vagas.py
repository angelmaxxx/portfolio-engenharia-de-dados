# Importa as bibliotecas necessárias
import pandas as pd
import psycopg2
from psycopg2 import sql

# ---- PARÂMETROS DE CONEXÃO COM O BANCO DE DADOS ----
DB_HOST = "localhost"
DB_NAME = "vagas_db"
DB_USER = "postgres"  # <-- SUBSTITUA PELA SUA NOVA SENHA
DB_PASS = "sua_nova_senha"   # <-- SUBSTITUA PELA SUA NOVA SENHA

# ---- ETAPA DE LEITURA DO ARQUIVO CSV ----
try:
    df_vagas = pd.read_csv('vagas_brutas.csv', encoding='utf-8')
    print("Arquivo 'vagas_brutas.csv' lido com sucesso.")
except FileNotFoundError:
    print("Erro: O arquivo 'vagas_brutas.csv' não foi encontrado.")
    exit()

# ---- ETAPA DE INGESTÃO DOS DADOS NO POSTGRESQL ----
if not df_vagas.empty:
    conn = None
    try:
        # Conecta ao banco de dados
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        # SQL para inserir os dados
        insert_sql = """
        INSERT INTO vagas (titulo, empresa, localizacao, link)
        VALUES (%s, %s, %s, %s);
        """

        # Itera sobre cada linha do DataFrame e insere na tabela
        print("Iniciando a ingestão de dados...")
        for index, row in df_vagas.iterrows():
            cur.execute(
                insert_sql, (row['titulo'], row['empresa'], row['localizacao'], row['link']))

        # Confirma as alterações no banco de dados
        conn.commit()
        print(f"Ingestão de {len(df_vagas)} linhas concluída com sucesso!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro na ingestão de dados: {error}")
        if conn:
            conn.rollback()  # Desfaz as alterações em caso de erro
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Conexão com o banco de dados fechada.")
else:
    print("O DataFrame está vazio. Nenhum dado para ingerir.")
