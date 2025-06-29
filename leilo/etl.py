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
    Encontra o arquivo CSV mais recente com o padrão 'leilo_YYYYMMDD_HHMMSS.csv'
    em um dado diretório e retorna o caminho completo e seu timestamp.
    Retorna (caminho_do_arquivo, timestamp_do_arquivo)
    """
    latest_csv = None
    latest_timestamp = None
    
    # Regex para encontrar arquivos com o padrão 'leilo_YYYYMMDD_HHMMSS.csv'
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
    a coluna 'modelo' desmembrada,
    e a coluna 'situação' duplicada e formatada como 'data leilão'.
    Também compara a data/hora atual com a do arquivo mais recente.
    """
    print("--- Iniciando processamento de dados ETL ---")

    # Encontra o arquivo CSV mais recente gerado pelo scraper e seu timestamp
    CSV_FILE_PATH, latest_file_timestamp = find_latest_csv(CSV_DIRECTORY)

    if not CSV_FILE_PATH:
        print(f"[ERRO] Nenhum arquivo CSV com o padrão 'leilo_YYYYMMDD_HHMMSS.csv' encontrado em '{CSV_DIRECTORY}'.")
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
    else:
        print("[WARN] Não foi possível determinar o timestamp do arquivo CSV mais recente para comparação.")
    # --- Fim da Comparação ---

    try:
        # Carrega os dados do CSV para um DataFrame do pandas
        df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8-sig")
        print(f"[INFO] Dados carregados com sucesso do '{CSV_FILE_PATH}'.")
        print(f"[INFO] Total de {len(df)} registros encontrados.")

        
        ### Limpeza da Coluna 'KM'
        
        #Caso a coluna 'KM' exista, ela será processada para **remover apenas o texto 'km'**, mantendo a formatação numérica e a pontuação (como `.`, `,`) intactas. O tipo de dado da coluna permanecerá como **string** (`object`) para que a pontuação seja preservada visualmente.

    
        if 'KM' in df.columns:
            # Converte para string para garantir operações de string
            df['KM'] = df['KM'].astype(str)
            # Remove 'km' (case-insensitive) e espaços extras antes/depois
            df['KM'] = df['KM'].str.replace(r'km', '', flags=re.IGNORECASE).str.strip()
            # A coluna 'KM' permanecerá como tipo de dado 'object' (string) para preservar a pontuação.
            print("[INFO] Coluna 'KM' limpa: 'km' removido, numeração e pontuação mantidas como strings.")
        else:
            print("[WARN] Coluna 'KM' não encontrada. Pulando a limpeza da coluna.")
      

        
        ### Desmembramento e Ajuste de Colunas
      
        # --- Desmembrar a coluna 'Título' e extrair 'Fabricante_Veiculo' ---
        if 'Título' in df.columns:
            split_title = df['Título'].str.split('/', n=1, expand=True).fillna('')
            # Criando 'Fabricante_Veiculo'
            df['Fabricante_Veiculo'] = split_title[0].str.strip() 
            
            # --- AJUSTE NA COLUNA 'Fabricante_Veiculo' ---
            if 'Fabricante_Veiculo' in df.columns:
                df['Fabricante_Veiculo'] = df['Fabricante_Veiculo'].replace({
                    'VOLKSWAGEN': 'VW - VolksWagen',
                    'CHEVROLET': 'GM - Chevrolet'
                })
                print("[INFO] Coluna 'Fabricante_Veiculo' ajustada para 'VW - VolksWagen' e 'GM - Chevrolet'.")
            else:
                print("[WARN] Coluna 'Fabricante_Veiculo' não encontrada após extração. Não foi possível realizar o ajuste.")
            # --- FIM DO AJUSTE ---

            if len(split_title.columns) > 1:
                df['Modelo'] = split_title[1].str.strip()
            else:
                df['Modelo'] = ''
            print("[INFO] Coluna 'Título' desmembrada, 'Fabricante_Veiculo' extraído e 'Modelo' criado/atualizado.")
        else:
            print("[WARN] Coluna 'Título' não encontrada no CSV. Não foi possível desmembrar e extrair Fabricante_Veiculo/Modelo.")

        # --- Extrair 'Modelo_Veiculo' da coluna 'Modelo' ---
        if 'Modelo' in df.columns:
            split_model = df['Modelo'].str.split(' ', n=1, expand=True).fillna('')
            df['Modelo_Veiculo'] = split_model[0].str.strip()
            
            if len(split_model.columns) > 1:
                df['Modelo'] = split_model[1].str.strip()
            else:
                df['Modelo'] = '' 
            print("[INFO] Coluna 'Modelo' desmembrada, 'Modelo_Veiculo' extraído.")
        else:
            print("[WARN] Coluna 'Modelo' não encontrada. Não foi possível extrair 'Modelo_Veiculo'.")


        # --- Duplicar e formatar a coluna 'Data Leilão' para 'data leilão' ---
        if 'Data Leilão' in df.columns:
            df['data leilão'] = df['Data Leilão'].astype(str).str.strip()
            print("[INFO] Coluna 'Data Leilão' usada para 'data leilão'.")
        elif 'Situação' in df.columns:
            df['data leilão'] = df['Situação'].astype(str)
            df['data leilão'] = df['data leilão'].str.replace("Leilão ao vivo em: ", "", regex=False).str.strip()
            try:
                def format_date_from_situacao(text):
                    if pd.isna(text) or not text.strip():
                        return ''
                    
                    date_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                    if date_match:
                        return date_match.group(1)
                    
                    time_match = re.search(r'(\d{2}h\d{2}m\d{2}s)', text)
                    if time_match:
                        current_date = datetime.now().strftime("%d/%m/%Y")
                        return current_date
                    
                    return ''
                
                df['data leilão'] = df['data leilão'].apply(format_date_from_situacao)
                print("[INFO] Coluna 'Situação' usada como fallback e formatada para 'data leilão' (dd/mm/aaaa).")
            except Exception as e:
                print(f"[ERRO] Ocorreu um erro ao formatar a coluna 'data leilão' do fallback: {e}.")
        else:
            print("[WARN] Nenhuma coluna 'Data Leilão' ou 'Situação' encontrada. Não foi possível criar 'data leilão'.")


        # Exibe a tabela completa no console
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None)

        print("\n--- Tabela de Dados Extraídos ---")
        print(df.to_string())
        print("\n--- Fim da Tabela ---")

        # Gera um timestamp para o nome do arquivo Excel, garantindo unicidade
        timestamp_excel = datetime.now().strftime("%Y%m%d_%H%M%S")
        EXCEL_FILE_NAME = f"leilo_tratado_{timestamp_excel}.xlsx"

        OUTPUT_DIR = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\leilo\etl_tratado"
        EXCEL_FILE_PATH = os.path.join(OUTPUT_DIR, EXCEL_FILE_NAME)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        try:
            df.to_excel(EXCEL_FILE_PATH, index=False)
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