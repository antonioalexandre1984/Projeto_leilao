import psycopg2
import random
# Importa a função de conexão do seu módulo db_operations
from db_utils.db_operations import connect_db, create_table_leilao_data, create_table_consolidado

def run_seed_data():
    """
    Script para criar a tabela 'lotes' e inserir dados fictícios.
    Agora utiliza a função connect_db do db_utils.
    """
    print("Iniciando processo de inserção de dados fictícios...")

    # Tenta conectar ao banco de dados usando a função de db_operations
    conn = connect_db()

    if conn is None:
        print("[ERRO] Não foi possível conectar ao banco de dados. Abortando seed de dados.")
        return

    try:
        cur = conn.cursor()

        # Cria a tabela 'lotes' se ela não existir
        # Nota: As funções create_table_leilao_data e create_table_consolidado
        # já cuidam da criação de suas respectivas tabelas.
        # Se 'lotes' for uma tabela separada que você precisa, mantenha este CREATE TABLE.
        # Se 'lotes' deve ser 'leilao_data' ou 'consolidado', você pode remover este bloco.
        # Mantenho aqui assumindo que 'lotes' é uma tabela adicional para este script.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lotes (
                id SERIAL PRIMARY KEY,
                modelo TEXT NOT NULL,
                ano INTEGER NOT NULL,
                km INTEGER NOT NULL,
                preco_lote FLOAT NOT NULL
            )
        """)
        conn.commit()
        print("[INFO] Tabela 'lotes' verificada/criada com sucesso.")

        modelos = ["Onix", "HB20", "Civic", "Corolla", "Gol", "Ka", "Fox", "Cruze", "Argo", "Toro"]
        anos = list(range(2012, 2023))

        for _ in range(100):
            modelo = random.choice(modelos)
            ano = random.choice(anos)
            km = random.randint(20000, 180000)
            preco_lote = round(random.uniform(10000, 70000), 2)
            cur.execute(
                "INSERT INTO lotes (modelo, ano, km, preco_lote) VALUES (%s, %s, %s, %s)",
                (modelo, ano, km, preco_lote)
            )
        
        conn.commit()
        print("✅ 100 dados fictícios inseridos com sucesso na tabela 'lotes'.")

    except Exception as e:
        print(f"[ERRO] Erro durante a inserção de dados: {e}")
        if conn:
            conn.rollback() # Reverte a transação em caso de erro
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("[INFO] Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    run_seed_data()
