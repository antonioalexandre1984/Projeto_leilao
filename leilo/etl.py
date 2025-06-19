import pandas as pd
import os
import re # Importa o módulo re para expressões regulares
from datetime import datetime, timedelta # Importa o módulo datetime para gerar timestamps e timedelta para diferença de tempo

# Define o caminho do diretório onde o scraper gera os arquivos CSV.
# Este caminho assume que o scraper salvou o CSV em /app dentro do Docker,
# e que este diretório está mapeado para 'C:\Users\anton\Desktop\parque_leiloes_scraper\app'
# no sistema de arquivos do seu host Windows.
CSV_DIRECTORY = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\leilo\etl"

def find_latest_csv(directory):
    """
    Encontra o arquivo CSV mais recente com o padrão 'output_YYYYMMDD_HHMMSS.csv'
    em um dado diretório e retorna o caminho completo e seu timestamp.
    Retorna (caminho_do_arquivo, timestamp_do_arquivo)
    """
    latest_csv = None
    latest_timestamp = None
    
    # Regex para encontrar arquivos com o padrão 'output_YYYYMMDD_HHMMSS.csv'
    # Captura o timestamp para comparação
    csv_pattern = re.compile(r"leilo_(\d{8}_\d{6})\.csv")

    if not os.path.exists(directory):
        print(f"[ERRO] O diretório '{directory}' para os arquivos CSV não foi encontrado.")
        return None, None

    try:
        files = os.listdir(directory)
        for filename in files:
            match = csv_pattern.match(filename)
            if match:
                current_timestamp_str = match.group(1)
                try:
                    # Converte o timestamp do nome do arquivo para um objeto datetime para comparação
                    current_timestamp = datetime.strptime(current_timestamp_str, "%Y%m%d_%H%M%S")
                    if latest_timestamp is None or current_timestamp > latest_timestamp:
                        latest_timestamp = current_timestamp
                        latest_csv = os.path.join(directory, filename)
                except ValueError:
                    print(f"[WARN] Impossível parsear timestamp do arquivo: {filename}")
        return latest_csv, latest_timestamp
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao listar arquivos no diretório {directory}: {e}")
        return None, None

def process_and_display_data():
    """
    Carrega os dados do CSV mais recente, os exibe em formato de tabela
    e os exporta para um arquivo Excel com um ID único no nome,
    salvo em um caminho específico, e com a coluna 'titulo' desmembrada,
    e a coluna 'situação' duplicada e formatada como 'data leilão'.
    Também compara a data/hora atual com a do arquivo mais recente.
    """
    print("--- Iniciando processamento de dados ETL ---")

    # Encontra o arquivo CSV mais recente gerado pelo scraper e seu timestamp
    CSV_FILE_PATH, latest_file_timestamp = find_latest_csv(CSV_DIRECTORY)

    if not CSV_FILE_PATH:
        print(f"[ERRO] Nenhum arquivo CSV com o padrão 'output_YYYYMMDD_HHMMSS.csv' encontrado em '{CSV_DIRECTORY}'.")
        print("Certifique-se de que o scraper foi executado com sucesso e o CSV foi gerado.")
        print("Verifique também o mapeamento de volumes no seu docker-compose.yml, se aplicável.")
        return

    print(f"[INFO] Processando o arquivo CSV mais recente: '{CSV_FILE_PATH}'.")

    # --- Comparação da data e hora atual com o arquivo mais recente ---
    current_time = datetime.now()
    if latest_file_timestamp:
        time_difference = current_time - latest_file_timestamp
        print(f"[INFO] Hora atual: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Hora do arquivo mais recente: {latest_file_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Diferença de tempo desde o arquivo mais recente: {time_difference}")
        # Opcional: Você pode adicionar lógica aqui para verificar se a diferença é maior que um certo limite
        # Por exemplo, se o arquivo for mais antigo que 24 horas:
        # if time_difference > timedelta(days=1):
        #     print("[ALERTA] O arquivo CSV mais recente tem mais de 24 horas!")
    else:
        print("[WARN] Não foi possível determinar o timestamp do arquivo CSV mais recente para comparação.")
    # --- Fim da Comparação ---

    try:
        # Carrega os dados do CSV para um DataFrame do pandas
        df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8-sig")
        print(f"[INFO] Dados carregados com sucesso do '{CSV_FILE_PATH}'.")
        print(f"[INFO] Total de {len(df)} registros encontrados.")

        # --- Desmembrar a coluna 'titulo' ---
        # Aparentemente a coluna 'titulo' contém informações como 'MARCA/MODELO'
        # Vamos tentar desmembrá-la em 'Marca' e 'Modelo'
        if 'Título' in df.columns: # Alterado de 'titulo' para 'Título' para corresponder ao scraper
            # Usa .str.split() para dividir a string pelo '/'
            # expand=True cria novas colunas a partir dos elementos divididos
            # .fillna('') para preencher valores ausentes com string vazia, se a divisão não gerar 2 partes
            split_title = df['Título'].str.split('/', n=1, expand=True).fillna('')

            # Atribui as novas colunas
            df['Marca'] = split_title[0].str.strip() # Remove espaços em branco
            # Verifica se a segunda parte existe antes de atribuir ao 'Modelo'
            if len(split_title.columns) > 1:
                df['Modelo'] = split_title[1].str.strip()
            else:
                df['Modelo'] = '' # Se não houver segunda parte, define como vazio

            print("[INFO] Coluna 'Título' desmembrada em 'Marca' e 'Modelo'.")
        else:
            print("[WARN] Coluna 'Título' não encontrada no CSV. Não foi possível desmembrar.")

        # --- Duplicar e formatar a coluna 'situação' para 'data leilão' ---
        # No scraper, a coluna 'Data Leilão' já é extraída e formatada.
        # Agora o ETL pode usá-la diretamente.
        if 'Data Leilão' in df.columns:
            # Garante que a coluna 'Data Leilão' esteja no formato string
            df['data leilão'] = df['Data Leilão'].astype(str).str.strip()
            print("[INFO] Coluna 'Data Leilão' usada para 'data leilão'.")
        elif 'Situação' in df.columns: # Fallback para 'Situação' se 'Data Leilão' não existir
            df['data leilão'] = df['Situação'].astype(str)
            df['data leilão'] = df['data leilão'].str.replace("Leilão ao vivo em: ", "", regex=False).str.strip()
            try:
                # O ano precisa ser inferido corretamente para a conversão de data/hora
                # Uma abordagem mais robusta seria tentar diferentes formatos ou usar bibliotecas de parse mais inteligentes
                # Para este exemplo, vamos assumir que a data é do ano atual se apenas a hora é fornecida
                df['data leilão'] = df['data leilão'].apply(lambda x: pd.to_datetime(
                    f"{datetime.now().year}-{datetime.now().month}-{datetime.now().day} {x.replace('h', ':').replace('m', ':').replace('s', '')}",
                    format="%Y-%m-%d %H:%M:%S"
                ).strftime("%d/%m/%Y") if pd.notna(x) and x.strip() else '')
                print("[INFO] Coluna 'Situação' duplicada e formatada para 'data leilão' (dd/mm/aaaa) como fallback.")
            except Exception as e:
                print(f"[ERRO] Ocorreu um erro ao formatar a coluna 'data leilão' do fallback: {e}.")
        else:
            print("[WARN] Nenhuma coluna 'Data Leilão' ou 'Situação' encontrada. Não foi possível criar 'data leilão'.")


        # Exibe a tabela completa no console
        # Configura pandas para exibir todas as colunas e mais linhas se necessário
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None) # Exibe todas as linhas, ajuste se houver muitos dados

        print("\n--- Tabela de Dados Extraídos ---")
        print(df.to_string()) # Usa to_string() para melhor formatação no console
        print("\n--- Fim da Tabela ---")

        # Gera um timestamp para o nome do arquivo Excel, garantindo unicidade
        timestamp_excel = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Define o nome do arquivo Excel
        EXCEL_FILE_NAME = f"leilo_tratado_{timestamp_excel}.xlsx"

        # Define o caminho completo para o arquivo Excel de saída
        OUTPUT_DIR = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\leilo\etl_tratado"
        EXCEL_FILE_PATH = os.path.join(OUTPUT_DIR, EXCEL_FILE_NAME)

        # Garante que o diretório de saída exista
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Exporta o DataFrame para um arquivo Excel
        try:
            df.to_excel(EXCEL_FILE_PATH, index=False) # index=False para não incluir o índice do DataFrame como uma coluna no Excel
            print(f"[INFO] Dados exportados com sucesso para '{EXCEL_FILE_PATH}'.")
        except Exception as excel_err:
            print(f"[ERRO] Ocorreu um erro ao exportar para Excel: {excel_err}")
            print("Verifique as permissões de escrita para o caminho de saída ou se o arquivo já está aberto.")

    except pd.errors.EmptyDataError:
        print(f"[WARN] O arquivo CSV '{CSV_FILE_PATH}' está vazio. Nenhum dado para processar.")
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao ler ou processar o CSV: {e}")

if __name__ == "__main__":
    process_and_display_data()