import pandas as pd
import os
import re
from datetime import datetime, timedelta

# Define o caminho do diretório onde o scraper gera os arquivos CSV.
# O caminho está em ambiente Docker, então /app/loop/webscraping é o correto.
CSV_DIRECTORY = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\loop\webscraping"

def find_latest_csv(directory):
    latest_csv = None
    latest_timestamp = None
    csv_pattern = re.compile(r"loopbrasil_(\d{8}_\d{6})\.csv")

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
        print(f"[ERRO] Nenhum arquivo CSV com o padrão 'loopbrasil_YYYYMMDD_HHMMSS.csv' encontrado em '{CSV_DIRECTORY}'.")
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

        # --- Etapa 1: Renomear colunas para os nomes padronizados ---
        # Este é o mapeamento dos nomes de coluna que vêm do CSV
        # para os novos nomes desejados.
        # Ajuste este dicionário se os nomes das colunas no CSV mudarem.
        column_rename_map = {
            'URL do Lote': 'URL do Lote', # Mantido igual, mas para clareza
            'Nome do Veículo (Header)': 'Nome do Veículo',
            'Marca': 'Marca',
            'Modelo': 'Modelo',
            'Versão': 'Versão',
            'Ano de Fabricação': 'Ano de Fabricação',
            'Ano Modelo': 'Ano Modelo',
            'Fipe': 'Tabela FIPE', # Renomeando de Fipe para Tabela FIPE
            'Blindado': 'Blindado',
            'Chave': 'Chaves', # Renomeando de Chave para Chaves
            'Funcionando': 'Funcionando',
            'Combustível': 'Combustível',
            'Km': 'KM', # Renomeando de Km para KM
            'Número de Lances': 'Numero de Lances',
            'Número de Visualizações': 'Numero de Visualizacoes',
            'Data do Leilão': 'Data do Leilao',
            'Horário do Leilão': 'Horario do Leilao',
            'Lance Atual': 'Lance Atual',
            'Situação do Lote': 'Situacao do Lote'
        }

        # Renomeia as colunas que existem no DataFrame
        df = df.rename(columns=column_rename_map)
        print("[INFO] Colunas renomeadas para os nomes padronizados.")
        print("[DEBUG] Colunas após renomeação:")
        print(df.columns.tolist())


        # --- Etapa 2: Definir a ordem final e quais colunas manter ---
        final_columns_order = [
            'URL do Lote',
            'Nome do Veículo',
            'Marca',
            'Modelo',
            'Versão',
            'Ano de Fabricação',
            'Ano Modelo',
            'Tabela FIPE',
            'Blindado',
            'Chaves',
            'Funcionando',
            'Combustível',
            'KM',
            'Numero de Lances',
            'Numero de Visualizacoes',
            'Data do Leilao',
            'Horario do Leilao',
            'Lance Atual',
            'Situacao do Lote'
        ]

        # Filtra as colunas do DataFrame para conter apenas as da lista `final_columns_order`
        # e as coloca na ordem especificada.
        # Quaisquer colunas no DataFrame que não estejam em `final_columns_order` serão removidas.
        # Quaisquer colunas em `final_columns_order` que não estejam no DataFrame serão adicionadas com NaN.
        # Para evitar adicionar com NaN e apenas remover as não existentes, faremos um loop.
        
        # Primeiro, identifique as colunas presentes no DF que estão na lista final
        columns_to_keep = [col for col in final_columns_order if col in df.columns]

        # Em seguida, crie o DataFrame final contendo apenas essas colunas e na ordem desejada
        df = df[columns_to_keep]

        print(f"[INFO] DataFrame ajustado para conter apenas as {len(columns_to_keep)} colunas desejadas e na ordem especificada.")
        print("[DEBUG] Colunas finais no DataFrame:")
        print(df.columns.tolist())


        # --- Não há necessidade de extrair de "Descricao Detalhada" ou "Título"
        #     porque o scraper já está extraindo isso diretamente e o CSV já vem com as colunas certas.
        #     Seus dados já devem vir com 'Marca', 'Modelo', 'Versão', etc., diretamente do scraper.
        #     Removi o código original que fazia essa extração no ETL.


        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None)

        print("\n--- Tabela de Dados Extraídos e Processados ---")
        print(df.to_string())
        print("\n--- Fim da Tabela ---")

        timestamp_excel = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file_name = f"dados_loop_processado_{timestamp_excel}.xlsx"

        # Caminho de saída para o Excel, agora dentro do volume mapeado do Docker
        # Mude para um caminho dentro do contêiner que está mapeado para o seu host,
        # como /app/output ou o diretório raiz do projeto se ele estiver montado.
        # Se você quer que ele salve no mesmo local do CSV, use CSV_DIRECTORY
        OUTPUT_DIR = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\loop\etl_tratado" # Assumindo que você tem um volume mapeado para isso
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