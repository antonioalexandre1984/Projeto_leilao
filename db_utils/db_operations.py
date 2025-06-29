import psycopg2
from psycopg2 import sql
from psycopg2.errors import DuplicateTable, UndefinedTable

# Importa as configurações do banco de dados do arquivo db_config.py
from .db_config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

def connect_db():
    """Conecta ao banco de dados PostgreSQL e retorna o objeto de conexão."""
    conn = None
    try:
        # Adicione este print para verificar o valor de DB_HOST no runtime.
        # Ele deve mostrar 'db' quando executado via docker-compose.
        print(f"[DEBUG] Tentando conectar ao PostgreSQL em: host={DB_HOST}, dbname={DB_NAME}, user={DB_USER}")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("[INFO] Conexão com o PostgreSQL estabelecida com sucesso!")
        return conn
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar ao PostgreSQL: {e}")
        return None

def create_table_leilao_data(conn):
    """Cria a tabela 'leilao_data' se ela não existir, com as colunas apropriadas para o scraper 'leilo'."""
    try:
        with conn.cursor() as cur:
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS leilao_data (
                    id SERIAL PRIMARY KEY,
                    titulo VARCHAR(500),
                    link TEXT,
                    imagem TEXT,
                    uf VARCHAR(10),
                    ano_basico VARCHAR(10),
                    km_basico VARCHAR(100),
                    valor_lance VARCHAR(100),
                    situacao VARCHAR(255),
                    data_leilao VARCHAR(100),
                    leiloeiro VARCHAR(255),
                    ano_veiculo VARCHAR(10),
                    combustivel VARCHAR(100),
                    km_veiculo VARCHAR(100),
                    valor_mercado_fipe VARCHAR(100),
                    cor_veiculo VARCHAR(100),
                    veiculo_possui_chave VARCHAR(50),
                    tipo_retomada VARCHAR(255),
                    localizacao VARCHAR(500),
                    tipo_veiculo VARCHAR(100),
                    data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute(create_table_query)
            conn.commit()
            print("[INFO] Tabela 'leilao_data' verificada/criada com sucesso.")
    except DuplicateTable:
        print("[INFO] Tabela 'leilao_data' já existe.")
        conn.rollback()
    except Exception as e:
        print(f"[ERRO] Erro ao criar ou verificar a tabela 'leilao_data': {e}")
        if conn:
            conn.غرب()

def create_table_consolidado(conn):
    """Cria a tabela 'consolidado' se ela não existir, com as colunas especificadas."""
    try:
        with conn.cursor() as cur:
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS consolidado (
                    id SERIAL PRIMARY KEY,
                    titulo VARCHAR(500),
                    link TEXT,
                    imagem TEXT,
                    lote_numero VARCHAR(100),
                    data_inicio VARCHAR(100),
                    data_termino VARCHAR(100),
                    lance_inicial NUMERIC(15, 2),
                    lance_atual NUMERIC(15, 2),
                    visualizacoes INTEGER,
                    total_lances INTEGER,
                    situacao VARCHAR(255),
                    marca VARCHAR(255),
                    km NUMERIC(15, 2),
                    ano_fabricacao VARCHAR(10),
                    ano_modelo VARCHAR(10),
                    chaves VARCHAR(50),
                    condicao_motor VARCHAR(255),
                    tabela_fipe NUMERIC(15, 2),
                    final_placa VARCHAR(20),
                    combustivel VARCHAR(100),
                    procedencia VARCHAR(255),
                    valor_mercado NUMERIC(15, 2),
                    localizacao_detalhe VARCHAR(500),
                    descricao_detalhada TEXT,
                    data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute(create_table_query)
            conn.commit()
            print("[INFO] Tabela 'consolidado' verificada/criada com sucesso.")
    except DuplicateTable:
        print("[INFO] Tabela 'consolidado' já existe.")
        conn.rollback()
    except Exception as e:
        print(f"[ERRO] Erro ao criar ou verificar a tabela 'consolidado': {e}")
        if conn:
            conn.rollback()

def insert_data_leilao_data(conn, data):
    """Insere um dicionário de dados na tabela 'leilao_data'."""
    try:
        with conn.cursor() as cur:
            # Mapeamento das chaves do dicionário para os nomes das colunas no banco de dados
            db_column_map = {
                "Título": "titulo",
                "Link": "link",
                "Imagem": "imagem",
                "UF": "uf",
                "Ano": "ano_basico",
                "KM": "km_basico",
                "Valor do Lance": "valor_lance",
                "Situação": "situacao",
                "Data Leilao": "data_leilao",
                "Leiloeiro": "leiloeiro",
                "ano_veiculo": "ano_veiculo",
                "combustivel": "combustivel",
                "km_veiculo": "km_veiculo",
                "valor_mercado_fipe": "valor_mercado_fipe",
                "cor_veiculo": "cor_veiculo",
                "veiculo_possui_chave": "veiculo_possui_chave",
                "tipo_retomada": "tipo_retomada",
                "localizacao": "localizacao",
                "tipo_veiculo": "tipo_veiculo"
            }

            columns = []
            placeholders = []
            values = {}

            for key, db_col_name in db_column_map.items():
                columns.append(db_col_name)
                placeholders.append(sql.Placeholder(db_col_name))
                values[db_col_name] = data.get(key)

            insert_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({})"
            ).format(
                sql.Identifier("leilao_data"),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(placeholders)
            )
            
            cur.execute(insert_query, values)
            conn.commit()
            print(f"[INFO] Lote '{data.get('Título', 'N/A')}' inserido com sucesso na tabela 'leilao_data'.")
    except Exception as e:
        print(f"[ERRO] Erro ao inserir dados na tabela 'leilao_data' para o lote '{data.get('Título', 'N/A')}': {e}")
        if conn:
            conn.rollback()
