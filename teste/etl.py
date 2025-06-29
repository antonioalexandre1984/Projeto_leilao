import pandas as pd
import os
import re
from datetime import datetime, timedelta

# Define o caminho do diretório onde o scraper gera os arquivos CSV.
CSV_DIRECTORY = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\teste"

def find_latest_csv(directory):
    latest_csv = None
    latest_timestamp = None
    csv_pattern = re.compile(r"leilo_com_br_carros_(\d{8}_\d{6})\.csv")

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
                    current_timestamp = datetime.strptime(current_timestamp_str, "%Y%m%d_%H%M%S")
                    if latest_timestamp is None or current_timestamp > latest_timestamp:
                        latest_timestamp = current_timestamp
                        latest_csv = os.path.join(directory, filename)
                except ValueError:
                    print(f"[AVISO] Impossível analisar o timestamp do arquivo: {filename}")
        return latest_csv, latest_timestamp
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao listar arquivos no diretório {directory}: {e}")
        return None, None

def process_and_display_data():
    print("--- Iniciando o processamento de dados ETL ---")

    csv_file_path, latest_file_timestamp = find_latest_csv(CSV_DIRECTORY)

    if not csv_file_path:
        print(f"[ERRO] Nenhum arquivo CSV com o padrão 'leilao_parque_data_YYYYMMDD_HHMMSS.csv' encontrado em '{CSV_DIRECTORY}'.")
        print("Certifique-se de que o scraper foi executado com sucesso e o CSV foi gerado.")
        print("Verifique também o mapeamento de volumes no seu docker-compose.yml, se aplicável.")
        return

    print(f"[INFO] Processando o arquivo CSV mais recente: '{csv_file_path}'.")

    current_time = datetime.now()
    if latest_file_timestamp:
        time_difference = current_time - latest_file_timestamp
        print(f"[INFO] Hora atual do ETL: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Hora de geração do arquivo CSV (baseado no nome): {latest_file_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if time_difference.total_seconds() < 0:
            abs_time_difference = abs(time_difference)
            print(f"[AVISO] O arquivo CSV foi gerado no futuro em relação à hora atual do ETL: {abs_time_difference}")
            print("[AVISO] Isso pode indicar um problema de sincronização de relógio ou fuso horário no ambiente do scraper.")
        else:
            print(f"[INFO] Diferença de tempo desde a geração do arquivo CSV: {time_difference}")
    else:
        print("[AVISO] Impossível determinar o timestamp do arquivo CSV mais recente para comparação.")

    try:
        df = pd.read_csv(csv_file_path, encoding="utf-8-sig")
        print(f"[INFO] Dados carregados com sucesso de '{csv_file_path}'.")
        print(f"[INFO] Total de {len(df)} registros encontrados.")

        print("\n[DEBUG] Colunas presentes no DataFrame após carregamento:")
        print(df.columns.tolist())
        print("\n[DEBUG] Tipos de dados das colunas:")
        print(df.dtypes)
        print("\n[DEBUG] Primeiras 5 linhas do DataFrame (antes do processamento):")
        print(df.head().to_string())

        # --- Passo 1: Extrair e popular colunas da 'Descricao Detalhada' primeiro ---
        if 'Descricao Detalhada' in df.columns:
            # Garante que a coluna 'Descricao Detalhada' é string e preenche NaNs com string vazia
            df['Descricao Detalhada'] = df['Descricao Detalhada'].fillna('').astype(str)

            # Define os padrões regex para cada campo a ser extraído da 'Descricao Detalhada'
            patterns_desc = {
                'Marca Veiculo': r"Marca:\s*(.+?)\n",
                'Modelo Veiculo': r"Modelo:\s*(.+?)\n",
                'Versao Veiculo': r"Versão:\s*(.+?)\n",
                'KM Veiculo': r"KM:\s*(.+?)\n",
                'Ano Fabricacao Veiculo': r"Ano de Fabricação:\s*(.+?)\n",
                'Ano Modelo Veiculo': r"Ano Modelo:\s*(.+?)\n",
                'Chaves Veiculo': r"Chaves:\s*(.+?)\n",
                'Condicao Motor Veiculo': r"Condição do Motor:\s*(.+?)(?:\s*\n|$)".replace(' ', r'[\s\xA0]'),
                'Tabela FIPE Veiculo': r"Tabela FIPE R\$?\s*(.+?)\n",
                'Final da Placa Veiculo': r"Final da Placa:\s*(.+?)\n",
                'Combustivel Veiculo': r"Combustível:\s*(.+?)\n",
                'Procedencia Veiculo': r"Procedência:\s*(.+)",
            }

            for col_name, pattern in patterns_desc.items():
                # Aplica a regex na coluna já tratada como string
                extracted_data = df['Descricao Detalhada'].str.extract(pattern, flags=re.IGNORECASE|re.MULTILINE)
                # Preenche a nova coluna com os valores extraídos, usando '' para NaNs, e remove espaços extras
                df[col_name] = extracted_data.fillna('').iloc[:, 0].str.strip()
            
            print("[INFO] Detalhes do veículo extraídos da 'Descricao Detalhada'.")
            df = df.drop(columns=['Descricao Detalhada'], errors='ignore')
            print("[INFO] Coluna original 'Descricao Detalhada' removida.")
        else:
            print("[AVISO] Coluna 'Descricao Detalhada' não encontrada. Detalhes do veículo podem estar incompletos.")
            for col_name in ['Marca Veiculo', 'Modelo Veiculo', 'Versao Veiculo', 'KM Veiculo',
                             'Ano Fabricacao Veiculo', 'Ano Modelo Veiculo', 'Chaves Veiculo',
                             'Condicao Motor Veiculo', 'Tabela FIPE Veiculo', 'Final da Placa Veiculo',
                             'Combustivel Veiculo', 'Procedencia Veiculo']:
                if col_name not in df.columns:
                    df[col_name] = 'N/A'

        # --- Passo 2: Priorizar 'Ano Fabricacao Veiculo' e 'Ano Modelo Veiculo' do 'Título' ---
        if 'Título' in df.columns:
            # Garante que a coluna 'Título' é string e preenche NaNs com string vazia
            df['Título'] = df['Título'].fillna('').astype(str)

            year_pattern_title = r"^(.*?)\s*(\d{2})\/(\d{2})(?:$|\s.*)"

            extracted_years_df = df['Título'].str.extract(year_pattern_title, flags=re.IGNORECASE)

            if not extracted_years_df.empty and len(extracted_years_df.columns) >= 3:
                df['Título Limpo'] = extracted_years_df[0].str.strip()
                
                temp_fabrication_year = extracted_years_df[1].apply(
                    lambda x: f"20{x}" if pd.notna(x) and len(str(x)) == 2 else x
                ).fillna('')

                temp_model_year = extracted_years_df[2].apply(
                    lambda x: f"20{x}" if pd.notna(x) and len(str(x)) == 2 else x
                ).fillna('')

                df['Ano Fabricacao Veiculo'] = temp_fabrication_year.mask(
                    temp_fabrication_year == '', df['Ano Fabricacao Veiculo']
                ).replace('', 'N/A')

                df['Ano Modelo Veiculo'] = temp_model_year.mask(
                    temp_model_year == '', df['Ano Modelo Veiculo']
                ).replace('', 'N/A')

                print("[INFO] 'Ano Fabricacao Veiculo' e 'Ano Modelo Veiculo' atualizados a partir da coluna 'Título'.")
            else:
                print("[INFO] Formato 'XX/XX' não encontrado na coluna 'Título' para extração de anos. Mantendo valores de 'Descricao Detalhada' ou 'N/A'.")
            
            df['Título'] = df['Título Limpo'].fillna(df['Título'])
            df = df.drop(columns=['Título Limpo'], errors='ignore')
        else:
            print("[AVISO] Coluna 'Título' não encontrada. Anos do veículo podem estar incompletos.")

        # --- Duplicar e formatar a coluna 'Data Término' para 'data leilão' ---
        if 'Data Término' in df.columns:
            # Garante que a coluna 'Data Término' é string e preenche NaNs com string vazia
            df['Data Término'] = df['Data Término'].fillna('').astype(str)
            df['data leilão'] = df['Data Término'].str.strip()
            print("[INFO] Coluna 'Data Término' utilizada para 'data leilão'.")
        elif 'Situação' in df.columns:
            # Garante que a coluna 'Situação' é string e preenche NaNs com string vazia
            df['Situação'] = df['Situação'].fillna('').astype(str)
            df['data leilão'] = df['Situação']
            df['data leilão'] = df['data leilão'].str.replace("Leilão ao vivo em: ", "", regex=False).str.strip()
            try:
                df['data leilão'] = df['data leilão'].apply(lambda x: pd.to_datetime(
                    f"{datetime.now().year}-{datetime.now().month}-{datetime.now().day} {x.replace('h', ':').replace('m', ':').replace('s', '')}",
                    format="%Y-%m-%d %H:%M:%S"
                ).strftime("%d/%m/%Y") if pd.notna(x) and x.strip() else '')
                print("[INFO] Coluna 'Situação' duplicada e formatada para 'data leilão' (dd/mm/aaaa) como fallback.")
            except Exception as e:
                print(f"[ERRO] Ocorreu um erro ao formatar a coluna 'data leilão' do fallback: {e}.")
        else:
            print("[AVISO] Nenhuma coluna 'Data Término' ou 'Situação' encontrada. 'data leilão' não pôde ser criada.")
            df['data leilão'] = 'N/A' # Garante que a coluna exista

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None)

        print("\n--- Tabela de Dados Extraídos e Processados ---")
        print(df.to_string())
        print("\n--- Fim da Tabela ---")

        timestamp_excel = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file_name = f"dados_leilao_processado_{timestamp_excel}.xlsx"

        OUTPUT_DIR = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\teste\etl_tratado"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        excel_file_path = os.path.join(OUTPUT_DIR, excel_file_name)

        try:
            df.to_excel(excel_file_path, index=False)
            print(f"[INFO] Dados exportados com sucesso para '{excel_file_path}'.")
        except Exception as excel_err:
            print(f"[ERRO] Ocorreu um erro ao exportar para Excel: {excel_err}")
            print("Verifique as permissões de escrita para o caminho de saída ou se o arquivo já está aberto.")

    except pd.errors.EmptyDataError:
        print(f"[AVISO] O arquivo CSV '{csv_file_path}' está vazio. Nenhum dado para processar.")
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao ler ou processar o CSV: {e}")

if __name__ == "__main__":
    process_and_display_data()