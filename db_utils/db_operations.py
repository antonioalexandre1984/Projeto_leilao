import psycopg2
from psycopg2 import sql
from psycopg2.errors import DuplicateTable, UndefinedTable
from datetime import datetime
import re 

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
                veiculo_link_lote TEXT,
                veiculo_titulo VARCHAR(500),
                veiculo_fabricante VARCHAR(255),
                veiculo_modelo VARCHAR(255),       
                veiculo_ano_fabricacao INTEGER,         
                veiculo_ano_modelo INTEGER,
                veiculo_valor_fipe NUMERIC,              
                veiculo_possui_chave VARCHAR(100),
                veiculo_condicao_motor VARCHAR(100),
                veiculo_tipo_combustivel VARCHAR(100),
                veiculo_km NUMERIC,                     
                veiculo_total_lances INTEGER,
                veiculo_data_leilao VARCHAR(100),
                veiculo_imagem TEXT,
                veiculo_lance_inicial NUMERIC,
                veiculo_valor_lance_atual NUMERIC,
                veiculo_final_placa VARCHAR(50),
                veiculo_tipo_retomada VARCHAR(100),
                veiculo_tipo VARCHAR(100),
                veiculo_valor_vendido NUMERIC,
                veiculo_patio_uf VARCHAR (100),          
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
        columns_to_insert = [
            "veiculo_titulo",
            "veiculo_link_lote",
            "veiculo_imagem",
            "veiculo_km",
            "veiculo_lance_inicial",
            "veiculo_valor_lance_atual",
            "veiculo_data_leilao",
            "veiculo_fabricante",
            "veiculo_final_placa",
            "veiculo_ano_fabricacao",
            "veiculo_ano_modelo",
            "veiculo_possui_chave",
            "veiculo_condicao_motor",
            "veiculo_valor_fipe",
            "veiculo_tipo_combustivel",
            "veiculo_tipo_retomada",
            "veiculo_tipo",
            "veiculo_total_lances",
            "veiculo_modelo",
            "veiculo_valor_vendido",
            "veiculo_patio_uf"
        ]
        
        print(f"[DB DEBUG] Colunas para inserção na tabela '{table_name}': {columns_to_insert}")

        data_keys = set(data_row_dict.keys())
        expected_columns_set = set(columns_to_insert)

        missing_in_data = expected_columns_set - data_keys
        extra_in_data = data_keys - expected_columns_set

        if missing_in_data:
            print(f"[DB WARN] Colunas esperadas no banco de dados, mas ausentes nos dados recebidos: {missing_in_data}")
            for col in missing_in_data:
                data_row_dict[col] = None 
        
        if extra_in_data:
            print(f"[DB WARN] Colunas presentes nos dados recebidos, mas não esperadas no banco de dados: {extra_in_data}")

        values_to_insert = []
        for col_name in columns_to_insert:
            val = data_row_dict.get(col_name)

            if col_name in ["veiculo_lance_inicial", "veiculo_valor_lance_atual", "veiculo_valor_fipe", "veiculo_valor_vendido", "veiculo_km"]:
                try:
                    if isinstance(val, (int, float)):
                        values_to_insert.append(float(val))
                    else:
                        s_val = str(val).replace('R$', '').strip()
                        s_val = s_val.replace('.', '') 
                        s_val = s_val.replace(',', '.') 
                        values_to_insert.append(float(s_val) if s_val and s_val.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao converter valor '{val}' para numérico na coluna '{col_name}'. Definindo como None.")
                    values_to_insert.append(None)
            elif col_name in ["veiculo_total_lances", "veiculo_ano_fabricacao", "veiculo_ano_modelo"]:
                try:
                    numeric_val = re.sub(r'\D', '', str(val))
                    values_to_insert.append(int(numeric_val) if numeric_val and numeric_val.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao converter valor '{val}' para inteiro na coluna '{col_name}'. Definindo como None.")
                    values_to_insert.append(None)
            else:
                values_to_insert.append(val if val is not None and str(val).lower() != "n/a" else None)

        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))
        )
        
        cursor.execute(insert_query, values_to_insert)
        conn.commit()
        print(f"[DB] Dados inseridos para o lote: {data_row_dict.get('veiculo_titulo', 'N/A')[:50]}...")
    except Exception as e:
        print(f"[DB ERROR] Erro ao inserir dados na tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

# --- Funções para a Tabela 'leilo' ---
def create_leilo_table(conn):
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "leilo"
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                veiculo_ano_fabricacao VARCHAR(10),
                veiculo_data_leilao VARCHAR(100),
                veiculo_tipo_combustivel VARCHAR(100),
                veiculo_cor VARCHAR(100),
                veiculo_possui_chave VARCHAR(100),
                veiculo_tipo_retomada VARCHAR(100),
                veiculo_tipo VARCHAR(100),
                veiculo_valor_fipe NUMERIC,
                veiculo_fabricante VARCHAR(255),
                veiculo_imagem TEXT,
                veiculo_km VARCHAR(100),
                veiculo_link_lote TEXT,
                veiculo_modelo VARCHAR(255),
                veiculo_situacao VARCHAR(255),
                veiculo_titulo VARCHAR(500),
                veiculo_patio_uf VARCHAR(30),
                veiculo_valor_lance_atual NUMERIC,
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
    """
    Insere um dicionário de dados na tabela 'leilo'.
    O dicionário 'data_row_dict' DEVE conter chaves com os nomes das colunas 'PARA' do mapeamento.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "leilo"
        
        # Lista de colunas esperadas para inserção na tabela 'leilo' (NOMES 'PARA')
        columns_to_insert = [
            "veiculo_titulo",
            "veiculo_link_lote",
            "veiculo_imagem",
            "veiculo_patio_uf",
            "veiculo_ano_fabricacao",
            "veiculo_km",
            "veiculo_valor_lance_atual",
            "veiculo_situacao",
            "veiculo_data_leilao",
            "veiculo_tipo_combustivel",
            "veiculo_cor",
            "veiculo_possui_chave",
            "veiculo_tipo_retomada",
            "veiculo_tipo",
            "veiculo_valor_fipe",
            "veiculo_fabricante",
            "veiculo_modelo"
        ]
        
        print(f"[DB DEBUG] Colunas para inserção na tabela '{table_name}': {columns_to_insert}")

        data_keys = set(data_row_dict.keys())
        expected_columns_set = set(columns_to_insert)

        missing_in_data = expected_columns_set - data_keys
        extra_in_data = data_keys - expected_columns_set

        if missing_in_data:
            print(f"[DB WARN] Colunas esperadas na tabela '{table_name}', mas ausentes nos dados recebidos: {missing_in_data}")
            for col in missing_in_data:
                data_row_dict[col] = None 
        
        if extra_in_data:
            print(f"[DB WARN] Colunas presentes nos dados recebidos, mas não esperadas na tabela '{table_name}': {extra_in_data}")

        values_to_insert = []
        for col_name in columns_to_insert:
            val = data_row_dict.get(col_name)

            if col_name in ["veiculo_valor_lance_atual", "veiculo_valor_fipe"]:
                try:
                    if isinstance(val, (int, float)):
                        values_to_insert.append(float(val))
                    else:
                        s_val = str(val).replace('R$', '').strip()
                        s_val = s_val.replace('.', '')
                        s_val = s_val.replace(',', '.')
                        values_to_insert.append(float(s_val) if s_val and s_val.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao converter valor '{val}' para numérico na coluna '{col_name}'. Definindo como None.")
                    values_to_insert.append(None)
            elif col_name in ["veiculo_ano_fabricacao", "veiculo_ano_modelo", "veiculo_total_lances", "veiculo_numero_visualizacoes"]:
                try:
                    numeric_val = re.sub(r'\D', '', str(val)) 
                    values_to_insert.append(int(numeric_val) if numeric_val and numeric_val.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao converter valor '{val}' para inteiro na coluna '{col_name}'. Definindo como None.")
                    values_to_insert.append(None)
            elif col_name == "veiculo_km":
                try:
                    if isinstance(val, (int, float)):
                        values_to_insert.append(str(val))
                    else:
                        cleaned_km = str(val).lower().replace('km', '').strip()
                        numeric_km = re.sub(r'[^\d,.]', '', cleaned_km).replace('.', '').replace(',', '.')
                        values_to_insert.append(numeric_km if numeric_km and numeric_km.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao processar valor '{val}' para 'veiculo_km'. Definindo como None.")
                    values_to_insert.append(None)
            else:
                values_to_insert.append(val if val is not None and str(val).lower() != "n/a" else None)

        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))
        )
        
        cursor.execute(insert_query, values_to_insert)
        conn.commit()
        print(f"[DB] Dados inseridos para o lote: {data_row_dict.get('veiculo_titulo', 'N/A')[:50]}...")
    except Exception as e:
        print(f"[DB ERROR] Erro ao inserir dados na tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()


### Funções para a Tabela 'loop' (Corrigidas e Consistentes)

def create_loop_table(conn):
    """
    Cria a tabela 'loop' se ela não existir,
    com as colunas consistentes com o mapeamento fornecido, usando 'veiculo_condicao_motor'.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "loop"
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                veiculo_link_lote TEXT UNIQUE NOT NULL,
                veiculo_titulo VARCHAR(500),
                veiculo_fabricante VARCHAR(255),
                veiculo_modelo VARCHAR(255),
                veiculo_versao VARCHAR(255),
                veiculo_ano_fabricacao INTEGER,
                veiculo_ano_modelo INTEGER,
                veiculo_valor_fipe NUMERIC(15, 2),
                veiculo_blindado VARCHAR(50),
                veiculo_chave VARCHAR(50),
                veiculo_condicao_motor VARCHAR(50), -- Nomenclatura mantida: veiculo_condicao_motor
                veiculo_tipo_combustivel VARCHAR(100),
                veiculo_km VARCHAR(100),
                veiculo_total_lances INTEGER,
                veiculo_numero_visualizacoes INTEGER,
                veiculo_data_leilao VARCHAR(100),
                veiculo_horario_leilao VARCHAR(50),
                veiculo_lance_atual NUMERIC(15, 2),
                veiculo_situacao_lote VARCHAR(100),
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

def insert_data_loop(conn, data_row_dict):
    """
    Insere um dicionário de dados na tabela 'loop'.
    O dicionário 'data_row_dict' DEVE conter chaves com os nomes das colunas 'PARA' do mapeamento,
    usando 'veiculo_condicao_motor'.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        table_name = "loop"
        
        columns_to_insert = [
            "veiculo_link_lote",
            "veiculo_titulo",
            "veiculo_fabricante",
            "veiculo_modelo",
            "veiculo_versao",
            "veiculo_ano_fabricacao",
            "veiculo_ano_modelo",
            "veiculo_valor_fipe",
            "veiculo_blindado",
            "veiculo_chave",
            "veiculo_condicao_motor", # Nomenclatura mantida: veiculo_condicao_motor
            "veiculo_tipo_combustivel",
            "veiculo_km",
            "veiculo_total_lances",
            "veiculo_numero_visualizacoes",
            "veiculo_data_leilao",
            "veiculo_horario_leilao",
            "veiculo_lance_atual",
            "veiculo_situacao_lote"
        ]
        
        print(f"[DB DEBUG] Colunas para inserção na tabela '{table_name}': {columns_to_insert}")

        data_keys = set(data_row_dict.keys())
        expected_columns_set = set(columns_to_insert)

        missing_in_data = expected_columns_set - data_keys
        extra_in_data = data_keys - expected_columns_set

        if missing_in_data:
            print(f"[DB WARN] Colunas esperadas na tabela '{table_name}', mas ausentes nos dados recebidos: {missing_in_data}")
            for col in missing_in_data:
                data_row_dict[col] = None 
        
        if extra_in_data:
            print(f"[DB WARN] Colunas presentes nos dados recebidos, mas não esperadas na tabela '{table_name}': {extra_in_data}")

        values_to_insert = []
        for col_name in columns_to_insert:
            val = data_row_dict.get(col_name)

            if col_name in ["veiculo_valor_fipe", "veiculo_lance_atual"]:
                try:
                    if isinstance(val, (int, float)):
                        values_to_insert.append(float(val))
                    else:
                        s_val = str(val).replace('R$', '').strip()
                        s_val = s_val.replace('.', '')
                        s_val = s_val.replace(',', '.')
                        values_to_insert.append(float(s_val) if s_val and s_val.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao converter valor '{val}' para numérico na coluna '{col_name}'. Definindo como None.")
                    values_to_insert.append(None)
            elif col_name in ["veiculo_ano_fabricacao", "veiculo_ano_modelo", "veiculo_total_lances", "veiculo_numero_visualizacoes"]:
                try:
                    numeric_val = re.sub(r'\D', '', str(val)) 
                    values_to_insert.append(int(numeric_val) if numeric_val and numeric_val.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao converter valor '{val}' para inteiro na coluna '{col_name}'. Definindo como None.")
                    values_to_insert.append(None)
            elif col_name == "veiculo_km":
                try:
                    if isinstance(val, (int, float)):
                        values_to_insert.append(str(val))
                    else:
                        cleaned_km = str(val).lower().replace('km', '').strip()
                        numeric_km = re.sub(r'[^\d,.]', '', cleaned_km).replace('.', '').replace(',', '.')
                        values_to_insert.append(numeric_km if numeric_km and numeric_km.lower() != "n/a" else None)
                except (ValueError, TypeError):
                    print(f"[DB ERROR] Falha ao processar valor '{val}' para 'veiculo_km'. Definindo como None.")
                    values_to_insert.append(None)
            else:
                values_to_insert.append(val if val is not None and str(val).lower() != "n/a" else None)

        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (veiculo_link_lote) DO UPDATE SET "
            "veiculo_titulo = EXCLUDED.veiculo_titulo, "
            "veiculo_fabricante = EXCLUDED.veiculo_fabricante, "
            "veiculo_modelo = EXCLUDED.veiculo_modelo, "
            "veiculo_versao = EXCLUDED.veiculo_versao, "
            "veiculo_ano_fabricacao = EXCLUDED.veiculo_ano_fabricacao, "
            "veiculo_ano_modelo = EXCLUDED.veiculo_ano_modelo, "
            "veiculo_valor_fipe = EXCLUDED.veiculo_valor_fipe, "
            "veiculo_blindado = EXCLUDED.veiculo_blindado, "
            "veiculo_chave = EXCLUDED.veiculo_chave, "
            "veiculo_condicao_motor = EXCLUDED.veiculo_condicao_motor, " # Nomenclatura mantida: veiculo_condicao_motor
            "veiculo_tipo_combustivel = EXCLUDED.veiculo_tipo_combustivel, "
            "veiculo_km = EXCLUDED.veiculo_km, "
            "veiculo_total_lances = EXCLUDED.veiculo_total_lances, "
            "veiculo_numero_visualizacoes = EXCLUDED.veiculo_numero_visualizacoes, "
            "veiculo_data_leilao = EXCLUDED.veiculo_data_leilao, "
            "veiculo_horario_leilao = EXCLUDED.veiculo_horario_leilao, "
            "veiculo_lance_atual = EXCLUDED.veiculo_lance_atual, "
            "veiculo_situacao_lote = EXCLUDED.veiculo_situacao_lote, "
            "data_extracao = EXCLUDED.data_extracao"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns_to_insert)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))
        )
        
        cursor.execute(insert_query, values_to_insert)
        conn.commit()
        print(f"[DB] Dados inseridos/atualizados para o lote: {data_row_dict.get('veiculo_link_lote', 'N/A')}")
    except Exception as e:
        print(f"[DB ERROR] Erro ao inserir dados na tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def test_insert_mock_data():
    """
    Função para testar a inserção de um registro mock nas tabelas.
    """
    mock_data_leilo = {
        "veiculo_titulo": "CARRO DE TESTE LEILÃO",
        "veiculo_link_lote": "https://example.com/leilo/test-lote-123",
        "veiculo_imagem": "https://placehold.co/600x400/png?text=LeiloTest",
        "veiculo_patio_uf": "SP",
        "veiculo_ano_fabricacao": "2020", 
        "veiculo_km": "50000", 
        "veiculo_valor_lance_atual": "15.000,00",
        "veiculo_situacao": "ABERTO",
        "veiculo_data_leilao": "10/07/2025",
        "veiculo_tipo_combustivel": "GASOLINA",
        "veiculo_cor": "PRETO",
        "veiculo_possui_chave": "SIM",
        "veiculo_tipo_retomada": "FINANCEIRA",
        "veiculo_tipo": "PASSEIO",
        "veiculo_valor_fipe": "20.000,00",
        "veiculo_fabricante": "FIAT",
        "veiculo_modelo": "ARGO",
    }

    mock_data_loop = {
        'veiculo_link_lote': 'https://loopbrasil.net/lote/JEEP-RENEGADE-20-20-/99999/',
        'veiculo_titulo': 'JEEP RENEGADE 20/20',
        'veiculo_fabricante': 'JEEP',
        'veiculo_modelo': 'RENEGADE',
        'veiculo_versao': 'SPORT',
        'veiculo_ano_fabricacao': '2020',
        'veiculo_ano_modelo': '2020',
        'veiculo_valor_fipe': 'R$ 75.000,00',
        'veiculo_blindado': 'NÃO',
        'veiculo_chave': 'SIM',
        'veiculo_condicao_motor': 'SIM', # Nomenclatura mantida: veiculo_condicao_motor
        'veiculo_tipo_combustivel': 'FLEX',
        'veiculo_km': '35.000 km',
        'veiculo_total_lances': '50',
        'veiculo_numero_visualizacoes': '500',
        'veiculo_data_leilao': '10/07/2025',
        'veiculo_horario_leilao': '09:30h',
        'veiculo_lance_atual': 'R$ 30.000,00',
        'veiculo_situacao_lote': 'Aberto para Lances'
    }

    conn = None
    try:
        conn = connect_db()
        if conn:
            print("\n[TESTE] Conexão com o banco de dados estabelecida para teste.")
            
            # Teste para a tabela 'leilo'
            create_leilo_table(conn) 
            print("[TESTE] Tentando inserir registro mock na tabela 'leilo'...")
            insert_data_leilo(conn, mock_data_leilo) 
            print("[TESTE] Inserção de registro mock na tabela 'leilo' concluída (verifique logs acima para sucesso/erro).")

            # Teste para a tabela 'loop'
            create_loop_table(conn)
            print("\n[TESTE] Tentando inserir registro mock na tabela 'loop'...")
            insert_data_loop(conn, mock_data_loop)
            print("[TESTE] Inserção de registro mock na tabela 'loop' concluída (verifique logs acima para sucesso/erro).")
            
        else:
            print("[TESTE ERRO] Não foi possível estabelecer conexão com o banco de dados para teste.")
    except Exception as e:
        print(f"[TESTE ERRO] Erro geral durante o teste de inserção: {e}")
    finally:
        if conn:
            conn.close()
            print("[TESTE] Conexão com o banco de dados fechada após teste.")

if __name__ == "__main__":
    test_insert_mock_data()