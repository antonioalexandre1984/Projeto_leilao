import psycopg2
from psycopg2 import sql
from psycopg2.errors import DuplicateTable, UndefinedTable
from datetime import datetime

# Importação RELATIVA CORRETA para db_config
from .db_config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

def connect_db():
    """Conecta ao banco de dados PostgreSQL e retorna o objeto de conexão."""
    conn = None
    try:
        print(f"[DEBUG] Tentando conectar ao PostgreSQL em: host={DB_HOST}, port={DB_PORT}, dbname={DB_NAME}, user={DB_USER}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = False # Garante que as transações são controladas manualmente
        print("[INFO] Conexão com o PostgreSQL estabelecida com sucesso!")
        return conn
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar ao PostgreSQL: {e}")
        return None

# --- Funções para a Tabela 'parque_leiloes_oficial' ---
def create_parque_leiloes_oficial_table(conn):
    """
    Cria a tabela 'parque_leiloes_oficial' se ela não existir,
    com as colunas consistentes com os dados coletados pelo scraper
    e com os tipos de dados ajustados.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "parque_leiloes_oficial"
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                veiculo_titulo VARCHAR(500), -- Ajustado para minúsculas
                veiculo_link_lote TEXT, -- Ajustado para minúsculas
                veiculo_imagem TEXT, -- Ajustado para minúsculas
                veiculo_km NUMERIC(15, 2), -- Ajustado para minúsculas
                veiculo_lance_inicial NUMERIC(15, 2), -- Ajustado para minúsculas
                veiculo_valor_lance_atual NUMERIC(15, 2), -- Ajustado para minúsculas
                veiculo_data_leilao VARCHAR(100), -- Ajustado para minúsculas
                veiculo_fabricante VARCHAR(255), -- Ajustado para minúsculas
                veiculo_final_placa VARCHAR(50), -- Ajustado para minúsculas
                veiculo_ano_fabricacao INTEGER, -- Ajustado para minúsculas
                veiculo_ano_modelo INTEGER, -- Ajustado para minúsculas
                veiculo_possui_chave VARCHAR(100), -- Ajustado para minúsculas
                veiculo_condicao_motor VARCHAR(100), -- Ajustado para minúsculas
                veiculo_valor_fipe NUMERIC(15, 2), -- Ajustado para minúsculas
                veiculo_tipo_combustivel VARCHAR(100), -- Ajustado para minúsculas
                veiculo_tipo_retomada VARCHAR(100), -- Ajustado para minúsculas
                veiculo_total_lances INTEGER, -- Ajustado para minúsculas
                veiculo_modelo VARCHAR(255), -- Ajustado para minúsculas
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """).format(sql.Identifier(table_name))
        cursor.execute(create_table_query)
        conn.commit()
        print(f"[DB] Tabela '{table_name}' verificada/criada com sucesso.")
    except DuplicateTable:
        print(f"[DB] Tabela '{table_name}' já existe.")
        conn.rollback()
    except Exception as e:
        print(f"[DB ERROR] Erro ao criar ou verificar a tabela: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def insert_data_parque_leiloes_oficial(conn, data_row_dict):
    """
    Insere um dicionário de dados na tabela 'parque_leiloes_oficial'.
    O dicionário 'data_row_dict' deve conter chaves com os nomes das colunas 'PARA' do mapeamento.
    Inclui logs para comparar as colunas esperadas com as recebidas e ajusta a conversão de tipos.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "parque_leiloes_oficial"
        
        # Lista de colunas no banco de dados (nomes 'PARA' do mapeamento)
        # Esta lista DEVE ser idêntica às colunas definidas em create_parque_leiloes_oficial_table,
        # excluindo 'id' e 'data_extracao' que são geradas automaticamente.
        columns_to_insert = [
            "veiculo_titulo", # Ajustado para minúsculas
            "veiculo_link_lote", # Ajustado para minúsculas
            "veiculo_imagem", # Ajustado para minúsculas
            "veiculo_km", # Ajustado para minúsculas
            "veiculo_lance_inicial", # Ajustado para minúsculas
            "veiculo_valor_lance_atual", # Ajustado para minúsculas
            "veiculo_data_leilao", # Ajustado para minúsculas
            "veiculo_fabricante", # Ajustado para minúsculas
            "veiculo_final_placa", # Ajustado para minúsculas
            "veiculo_ano_fabricacao", # Ajustado para minúsculas
            "veiculo_ano_modelo", # Ajustado para minúsculas
            "veiculo_possui_chave", # Ajustado para minúsculas
            "veiculo_condicao_motor", # Ajustado para minúsculas
            "veiculo_valor_fipe", # Ajustado para minúsculas
            "veiculo_tipo_combustivel", # Ajustado para minúsculas
            "veiculo_tipo_retomada", # Ajustado para minúsculas
            "veiculo_total_lances", # Ajustado para minúsculas
            "veiculo_modelo" # Ajustado para minúsculas
        ]
        
        # Imprime as colunas que serão usadas na inserção
        print(f"[DB DEBUG] Colunas para inserção na tabela '{table_name}': {columns_to_insert}")

        # --- Lógica de verificação e log de colunas ---
        data_keys = set(data_row_dict.keys())
        expected_columns_set = set(columns_to_insert)

        missing_in_data = expected_columns_set - data_keys
        extra_in_data = data_keys - expected_columns_set

        if missing_in_data:
            print(f"[DB WARN] Colunas esperadas no banco de dados, mas ausentes nos dados recebidos: {missing_in_data}")
            # Para evitar erro, preenche com None para as colunas ausentes
            for col in missing_in_data:
                data_row_dict[col] = None 
        
        if extra_in_data:
            print(f"[DB WARN] Colunas presentes nos dados recebidos, mas não esperadas no banco de dados: {extra_in_data}")
            # Estas colunas serão ignoradas automaticamente pelo insert_query, mas o log é útil.

        values_to_insert = []
        for col_name in columns_to_insert:
            val = data_row_dict.get(col_name) # Obtém o valor diretamente usando o nome 'PARA' como chave

            # Lógica para converter valores numéricos (moeda e KM)
            if col_name in ["veiculo_lance_inicial", "veiculo_valor_lance_atual", "veiculo_valor_fipe", "veiculo_km"]:
                try:
                    # Remove 'R$', espaços, e substitui vírgula por ponto para float
                    cleaned_val = str(val).replace('R$', '').replace('.', '').replace(',', '.').strip()
                    values_to_insert.append(float(cleaned_val) if cleaned_val and cleaned_val != "N/A" else None)
                except (ValueError, TypeError):
                    values_to_insert.append(None) # Insere None se a conversão falhar
            # Lógica para converter anos e total de lances para INTEGER
            elif col_name in ["veiculo_total_lances", "veiculo_ano_fabricacao", "veiculo_ano_modelo"]:
                try:
                    # Converte para inteiro, lidando com "N/A" ou strings não numéricas
                    values_to_insert.append(int(val) if str(val).isdigit() and str(val) != "N/A" else None)
                except (ValueError, TypeError):
                    values_to_insert.append(None)
            else:
                values_to_insert.append(val if val != "N/A" and val is not None else None) # Insere None para 'N/A' ou None

        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))
        )
        
        cursor.execute(insert_query, values_to_insert)
        conn.commit()
        print(f"[DB] Dados inseridos para o lote: {data_row_dict.get('veiculo_titulo', 'N/A')[:50]}...") # Ajustado para minúsculas
    except Exception as e:
        print(f"[DB ERROR] Erro ao inserir dados na tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

# --- Funções para a Tabela 'leilo' (mantidas como estavam) ---
# Estas funções foram mantidas inalteradas conforme sua solicitação.
def create_leilo_table(conn):
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "leilo"
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                Veiculo_Ano_Fabricacao VARCHAR(10),
                Veiculo_Data_Leilao VARCHAR(100),
                Veiculo_Tipo_Combustivel VARCHAR(100),
                Veiculo_Cor VARCHAR(100),
                Veiculo_Possui_Chave VARCHAR(100),
                Veiculo_Tipo_Retomada VARCHAR(100),
                Veiculo_Tipo VARCHAR(100),
                Veiculo_Valor_Fipe NUMERIC(15, 2),
                Veiculo_Fabricante VARCHAR(255),
                Veiculo_Imagem TEXT,
                Veiculo_KM VARCHAR(100),
                Veiculo_Link_Lote TEXT,
                Veiculo_Modelo VARCHAR(255),
                Veiculo_Situacao VARCHAR(255),
                Veiculo_Titulo VARCHAR(500),
                Veiculo_Patio_UF VARCHAR(10),
                Veiculo_Valor_Lance_Atual NUMERIC(15, 2),
                Veiculo_Patio_UF_Localizacao VARCHAR(255),
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """).format(sql.Identifier(table_name))
        cursor.execute(create_table_query)
        conn.commit()
        print(f"[DB] Tabela '{table_name}' verificada/criada com sucesso.")
    except DuplicateTable:
        print(f"[DB] Tabela '{table_name}' já existe.")
        conn.rollback()
    except Exception as e:
        print(f"[DB ERROR] Erro ao criar ou verificar a tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def insert_data_leilo(conn, data_row_dict):
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "leilo"
        
        columns_to_insert = [
            "Veiculo_Titulo", "Veiculo_Link_Lote", "Veiculo_Imagem", "Veiculo_Patio_UF",
            "Veiculo_Ano_Fabricacao", "Veiculo_KM", "Veiculo_Valor_Lance_Atual",
            "Veiculo_Situacao", "Veiculo_Data_Leilao", "Veiculo_Tipo_Combustivel",
            "Veiculo_Cor", "Veiculo_Possui_Chave", "Veiculo_Tipo_Retomada",
            "Veiculo_Tipo", "Veiculo_Valor_Fipe", "Veiculo_Fabricante",
            "Veiculo_Modelo", "Veiculo_Patio_UF_Localizacao"
        ]
        
        values_to_insert = []
        for col_name in columns_to_insert:
            val = data_row_dict.get(col_name)

            if col_name in ["Veiculo_Valor_Lance_Atual", "Veiculo_Valor_Fipe"]:
                try:
                    cleaned_val = str(val).replace('R$', '').replace('.', '').replace(',', '.').strip()
                    values_to_insert.append(float(cleaned_val) if cleaned_val and cleaned_val != "N/A" else None)
                except (ValueError, TypeError):
                    values_to_insert.append(None)
            else:
                values_to_insert.append(val if val != "N/A" and val is not None else None)

        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))
        )
        
        cursor.execute(insert_query, values_to_insert)
        conn.commit()
        print(f"[DB] Dados inseridos para o lote: {data_row_dict.get('Veiculo_Titulo', 'N/A')[:50]}...")
    except Exception as e:
        print(f"[DB ERROR] Erro ao inserir dados na tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def test_insert_mock_data():
    """
    Função para testar a inserção de um registro mock na tabela 'parque_leiloes_oficial'.
    Utiliza dados extraídos do log de erro fornecido, ajustados para minúsculas.
    """
    mock_data = {
        "veiculo_titulo": "MERCEDES-BENZ ATEGO 2730K 22/22", # Ajustado para minúsculas
        "veiculo_link_lote": "https://parquedosleiloesoficial.com/lote/MERCEDES-BENZ-ATEGO-2730K-22-22-/858/", # Ajustado para minúsculas
        "veiculo_imagem": "https://parquedosleiloesoficial.com/web/fotos/lotes_858_mercedes-benz-atego-2730k-22-22_parquedosleilao.com_zz9e98fbc1c5.png", # Ajustado para minúsculas
        "veiculo_km": None, # Ajustado para minúsculas
        "veiculo_lance_inicial": 23260000.0, # Ajustado para minúsculas
        "veiculo_valor_lance_atual": 0.0, # Ajustado para minúsculas
        "veiculo_data_leilao": "27/06/2025", # Ajustado para minúsculas
        "veiculo_fabricante": None, # Ajustado para minúsculas
        "veiculo_final_placa": None, # Ajustado para minúsculas
        "veiculo_ano_fabricacao": None, # Ajustado para minúsculas
        "veiculo_ano_modelo": None, # Ajustado para minúsculas
        "veiculo_possui_chave": None, # Ajustado para minúsculas
        "veiculo_condicao_motor": None, # Ajustado para minúsculas
        "veiculo_valor_fipe": None, # Ajustado para minúsculas
        "veiculo_tipo_combustivel": None, # Ajustado para minúsculas
        "veiculo_tipo_retomada": None, # Ajustado para minúsculas
        "veiculo_total_lances": None, # Ajustado para minúsculas
        "veiculo_modelo": None # Ajustado para minúsculas
    }

    conn = None
    try:
        conn = connect_db()
        if conn:
            print("\n[TESTE] Conexão com o banco de dados estabelecida para teste.")
            create_parque_leiloes_oficial_table(conn) # Garante que a tabela está criada com a estrutura mais recente
            print("[TESTE] Tentando inserir registro mock...")
            insert_data_parque_leiloes_oficial(conn, mock_data)
            print("[TESTE] Inserção de registro mock concluída (verifique logs acima para sucesso/erro).")
        else:
            print("[TESTE ERRO] Não foi possível estabelecer conexão com o banco de dados para teste.")
    except Exception as e:
        print(f"[TESTE ERRO] Erro geral durante o teste de inserção: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("[TESTE] Conexão com o banco de dados fechada após teste.")

# Exemplo de como você pode chamar a função de teste (descomente para usar)
if __name__ == "__main__":
    test_insert_mock_data()