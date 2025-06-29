# analyzer/test_db_connection.py
# Remova as linhas sys e os.path.join, pois o PYTHONPATH no Docker Compose já deve lidar com isso.

# A importação agora se baseia no PYTHONPATH e na estrutura de pacote (com __init__.py em db_utils)
from db_utils.db_operations import connect_db

if __name__ == "__main__":
    print("Iniciando teste de conexão ao banco de dados a partir do container 'analyzer'...")
    conn = connect_db()
    if conn:
        print("Teste de conexão BEM-SUCEDIDO!")
        conn.close()
    else:
        print("Teste de conexão FALHOU. Verifique os logs de erro acima para mais detalhes.")
