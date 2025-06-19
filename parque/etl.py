import pandas as pd
import os
import re # Importa o módulo 're' para usar expressões regulares
from datetime import datetime, timedelta # Importa 'datetime' para datas/horas e 'timedelta' para diferenças de tempo

# Define o caminho do diretório onde o scraper gera os arquivos CSV.
# Este caminho deve corresponder ao local onde seus arquivos CSV de saída do scraper estão sendo salvos.
CSV_DIRECTORY = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\parque\etl"

def find_latest_csv(directory):
    """
    Encontra o arquivo CSV mais recente que segue o padrão 'leilao_parque_data_YYYYMMDD_HHMMSS.csv'
    em um dado diretório e retorna seu caminho completo e o timestamp.
    Retorna uma tupla: (caminho_do_arquivo, timestamp_do_arquivo).
    """
    latest_csv = None
    latest_timestamp = None
    
    # Regex para encontrar arquivos com o padrão 'leilao_parque_data_YYYYMMDD_HHMMSS.csv'
    # Captura o timestamp (o grupo de dígitos) para comparação
    csv_pattern = re.compile(r"leilao_parque_data_(\d{8}_\d{6})\.csv")

    # Verifica se o diretório existe
    if not os.path.exists(directory):
        print(f"[ERRO] O diretório '{directory}' para os arquivos CSV não foi encontrado.")
        return None, None

    try:
        # Lista todos os arquivos no diretório
        files = os.listdir(directory)
        for filename in files:
            # Tenta casar o nome do arquivo com o padrão regex
            match = csv_pattern.match(filename)
            if match:
                current_timestamp_str = match.group(1) # Pega a string do timestamp
                try:
                    # Converte a string do timestamp para um objeto datetime para comparação
                    current_timestamp = datetime.strptime(current_timestamp_str, "%Y%m%d_%H%M%S")
                    # Se for o primeiro arquivo encontrado ou mais recente, atualiza
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
    """
    Carrega os dados do CSV mais recente, os exibe em formato de tabela,
    e os exporta para um arquivo Excel com um ID único no nome.
    Este processo prioriza a extração de 'Ano Fabricacao Veiculo' e 'Ano Modelo Veiculo'
    da coluna 'Título', usando 'Descricao Detalhada' como fallback.
    Também compara a data/hora atual com a do arquivo mais recente.
    """
    print("--- Iniciando o processamento de dados ETL ---")

    # Encontra o arquivo CSV mais recente gerado pelo scraper e seu timestamp
    csv_file_path, latest_file_timestamp = find_latest_csv(CSV_DIRECTORY)

    # Verifica se um arquivo CSV foi encontrado
    if not csv_file_path:
        print(f"[ERRO] Nenhum arquivo CSV com o padrão 'leilao_parque_data_YYYYMMDD_HHMMSS.csv' encontrado em '{CSV_DIRECTORY}'.")
        print("Certifique-se de que o scraper foi executado com sucesso e o CSV foi gerado.")
        print("Verifique também o mapeamento de volumes no seu docker-compose.yml, se aplicável.")
        return

    print(f"[INFO] Processando o arquivo CSV mais recente: '{csv_file_path}'.")

    # --- Comparação da data e hora atual com o arquivo mais recente ---
    current_time = datetime.now()
    if latest_file_timestamp:
        time_difference = current_time - latest_file_timestamp
        print(f"[INFO] Hora atual: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Hora do arquivo mais recente: {latest_file_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Diferença de tempo desde o arquivo mais recente: {time_difference}")
    else:
        print("[AVISO] Impossível determinar o timestamp do arquivo CSV mais recente para comparação.")
    # --- Fim da Comparação ---

    try:
        # Carrega os dados do CSV para um DataFrame do pandas
        df = pd.read_csv(csv_file_path, encoding="utf-8-sig")
        print(f"[INFO] Dados carregados com sucesso de '{csv_file_path}'.")
        print(f"[INFO] Total de {len(df)} registros encontrados.")

        # --- Passo 1: Extrair e popular colunas da 'Descricao Detalhada' primeiro ---
        # Esta etapa serve como população inicial ou fallback para essas colunas.
        if 'Descricao Detalhada' in df.columns:
            # Define os padrões regex para cada campo a ser extraído da 'Descricao Detalhada'
            patterns_desc = {
                'Marca Veiculo': r"Marca:\s*(.+?)\n",
                'Modelo Veiculo': r"Modelo:\s*(.+?)\n",
                'Versao Veiculo': r"Versão:\s*(.+?)\n",
                'KM Veiculo': r"KM:\s*(.+?)\n",
                # As colunas de ano serão tratadas especificamente para prioridade mais tarde
                'Ano Fabricacao Veiculo': r"Ano de Fabricação:\s*(.+?)\n",
                'Ano Modelo Veiculo': r"Ano Modelo:\s*(.+?)\n",
                'Chaves Veiculo': r"Chaves:\s*(.+?)\n",
                'Condicao Motor Veiculo': r"Condição do Motor:\s*(.+?)(?:\s*\n|$)".replace(' ', r'[\s\xA0]'), # Lida com espaços normais e non-breaking spaces
                'Tabela FIPE Veiculo': r"Tabela FIPE R\$?\s*(.+?)\n",
                'Final da Placa Veiculo': r"Final da Placa:\s*(.+?)\n",
                'Combustivel Veiculo': r"Combustível:\s*(.+?)\n",
                'Procedencia Veiculo': r"Procedência:\s*(.+)",
            }

            for col_name, pattern in patterns_desc.items():
                # Garante que a coluna exista ou a cria, então extrai o valor.
                # Substitui non-breaking spaces (\xa0) por espaços normais antes de aplicar a regex.
                df[col_name] = df['Descricao Detalhada'].astype(str).str.replace(u'\xa0', u' ').str.extract(pattern, flags=re.IGNORECASE|re.MULTILINE).fillna('').str.strip()
            
            print("[INFO] Detalhes do veículo extraídos da 'Descricao Detalhada'.")
            # Remove a coluna original 'Descricao Detalhada' após a extração
            df = df.drop(columns=['Descricao Detalhada'], errors='ignore')
            print("[INFO] Coluna original 'Descricao Detalhada' removida.")
        else:
            print("[AVISO] Coluna 'Descricao Detalhada' não encontrada. Detalhes do veículo podem estar incompletos.")
            # Garante que todas as colunas de detalhe existam, mesmo que com 'N/A', se 'Descricao Detalhada' estiver faltando
            for col_name in ['Marca Veiculo', 'Modelo Veiculo', 'Versao Veiculo', 'KM Veiculo',
                             'Ano Fabricacao Veiculo', 'Ano Modelo Veiculo', 'Chaves Veiculo',
                             'Condicao Motor Veiculo', 'Tabela FIPE Veiculo', 'Final da Placa Veiculo',
                             'Combustivel Veiculo', 'Procedencia Veiculo']:
                if col_name not in df.columns:
                    df[col_name] = 'N/A'


        # --- Passo 2: Priorizar 'Ano Fabricacao Veiculo' e 'Ano Modelo Veiculo' do 'Título' ---
        if 'Título' in df.columns:
            # Padrão para encontrar "XX/XX" no final do título
            # Captura o texto antes dos anos (Grupo 1), o ano de fabricação (Grupo 2) e o ano modelo (Grupo 3)
            year_pattern_title = r"^(.*?)\s*(\d{2})\/(\d{2})(?:$|\s.*)" 

            # Tenta extrair os anos da coluna 'Título'
            # 'str.extract' lida com valores NaN para não correspondências
            extracted_years_df = df['Título'].astype(str).str.extract(year_pattern_title, flags=re.IGNORECASE)

            # Verifica se a extração foi bem-sucedida antes de tentar atribuir
            if not extracted_years_df.empty and len(extracted_years_df.columns) >= 3:
                # Armazena o título "limpo" (sem os anos)
                df['Título Limpo'] = extracted_years_df[0].str.strip()
                
                # Cria Series temporárias para os anos do Título, convertendo "YY" para "20YY"
                temp_fabrication_year = extracted_years_df[1].apply(
                    lambda x: f"20{x}" if pd.notna(x) and len(str(x)) == 2 else x
                ).fillna('') # Preenche NaNs com string vazia

                temp_model_year = extracted_years_df[2].apply(
                    lambda x: f"20{x}" if pd.notna(x) and len(str(x)) == 2 else x
                ).fillna('') # Preenche NaNs com string vazia

                # Atualiza as colunas existentes 'Ano Fabricacao Veiculo' e 'Ano Modelo Veiculo',
                # priorizando os valores extraídos do 'Título'.
                # Usa .mask(): se temp_fabrication_year for vazio, mantém o valor atual da coluna;
                # caso contrário, usa o valor de temp_fabrication_year. Finalmente, substitui vazios por 'N/A'.
                df['Ano Fabricacao Veiculo'] = temp_fabrication_year.mask(
                    temp_fabrication_year == '', df['Ano Fabricacao Veiculo']
                ).replace('', 'N/A')

                df['Ano Modelo Veiculo'] = temp_model_year.mask(
                    temp_model_year == '', df['Ano Modelo Veiculo']
                ).replace('', 'N/A')

                print("[INFO] 'Ano Fabricacao Veiculo' e 'Ano Modelo Veiculo' atualizados a partir da coluna 'Título'.")
            else:
                print("[INFO] Formato 'XX/XX' não encontrado na coluna 'Título' para extração de anos. Mantendo valores de 'Descricao Detalhada' ou 'N/A'.")
            
            # Atualiza a coluna 'Título' com a versão "limpa" (sem os anos)
            # Se 'Título Limpo' for vazio (nenhum ano encontrado), mantém o 'Título' original.
            df['Título'] = df['Título Limpo'].fillna(df['Título'])
            # Remove a coluna temporária 'Título Limpo'
            df = df.drop(columns=['Título Limpo'], errors='ignore')


        # --- Duplicar e formatar a coluna 'Data Término' para 'data leilão' ---
        # Prioriza 'Data Término' para ser a data do leilão
        if 'Data Término' in df.columns:
            df['data leilão'] = df['Data Término'].astype(str).str.strip()
            print("[INFO] Coluna 'Data Término' utilizada para 'data leilão'.")
        # Se 'Data Término' não existir, usa 'Situação' como fallback e tenta formatar
        elif 'Situação' in df.columns:
            df['data leilão'] = df['Situação'].astype(str)
            df['data leilão'] = df['data leilão'].str.replace("Leilão ao vivo em: ", "", regex=False).str.strip()
            try:
                # Tenta formatar a data, inferindo o ano atual se necessário.
                df['data leilão'] = df['data leilão'].apply(lambda x: pd.to_datetime(
                    f"{datetime.now().year}-{datetime.now().month}-{datetime.now().day} {x.replace('h', ':').replace('m', ':').replace('s', '')}",
                    format="%Y-%m-%d %H:%M:%S"
                ).strftime("%d/%m/%Y") if pd.notna(x) and x.strip() else '')
                print("[INFO] Coluna 'Situação' duplicada e formatada para 'data leilão' (dd/mm/aaaa) como fallback.")
            except Exception as e:
                print(f"[ERRO] Ocorreu um erro ao formatar a coluna 'data leilão' do fallback: {e}.")
        else:
            print("[AVISO] Nenhuma coluna 'Data Término' ou 'Situação' encontrada. 'data leilão' não pôde ser criada.")

        # --- Configurações de exibição do Pandas para o console ---
        pd.set_option('display.max_columns', None) # Exibe todas as colunas
        pd.set_option('display.width', 1000) # Define a largura de exibição da tabela
        pd.set_option('display.max_rows', None) # Exibe todas as linhas

        print("\n--- Tabela de Dados Extraídos e Processados ---")
        print(df.to_string()) # Usa to_string() para uma melhor formatação no console
        print("\n--- Fim da Tabela ---")

        # --- Exportação para Excel ---
        # Gera um timestamp para garantir um nome de arquivo Excel único
        timestamp_excel = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Define o nome do arquivo Excel de saída
        excel_file_name = f"dados_leilao_processado_{timestamp_excel}.xlsx"

        # Define o caminho completo para o arquivo Excel de saída
        OUTPUT_DIR = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\parque\etl_tratado"
        os.makedirs(OUTPUT_DIR, exist_ok=True) # Garante que o diretório de saída exista
        excel_file_path = os.path.join(OUTPUT_DIR, excel_file_name)

        # Exporta o DataFrame para um arquivo Excel
        try:
            df.to_excel(excel_file_path, index=False) # 'index=False' para não incluir o índice do DataFrame como uma coluna
            print(f"[INFO] Dados exportados com sucesso para '{excel_file_path}'.")
        except Exception as excel_err:
            print(f"[ERRO] Ocorreu um erro ao exportar para Excel: {excel_err}")
            print("Verifique as permissões de escrita para o caminho de saída ou se o arquivo já está aberto.")

    except pd.errors.EmptyDataError:
        print(f"[AVISO] O arquivo CSV '{csv_file_path}' está vazio. Nenhum dado para processar.")
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao ler ou processar o CSV: {e}")

# Executa a função principal quando o script é iniciado
if __name__ == "__main__":
    process_and_display_data()