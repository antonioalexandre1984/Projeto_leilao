import pandas as pd
import os
import re
from datetime import datetime
import requests
import time

# --- Configurações de Caminho ---
CSV_DIRECTORY = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\leilo\etl_tratado"
EXCEL_OUTPUT_DIR = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\leilo"


# --- Configurações da API FIPE ---
FIPE_API_BASE_URL = "https://parallelum.com.br/fipe/api/v1"

class FipeApiClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def _make_request(self, endpoint):
        try:
            response = requests.get(f"{self.base_url}{endpoint}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[ERRO API FIPE] Não foi possível conectar ou obter dados de {endpoint}: {e}")
            return None

    def get_brands(self, vehicle_type):
        return self._make_request(f"/{vehicle_type}/marcas")

    def get_models(self, vehicle_type, brand_id):
        return self._make_request(f"/{vehicle_type}/marcas/{brand_id}/modelos")

    def get_years(self, vehicle_type, brand_id, model_id):
        return self._make_request(f"/{vehicle_type}/marcas/{brand_id}/modelos/{model_id}/anos")

    def get_vehicle_value(self, vehicle_type, brand_id, model_id, year_id):
        data = self._make_request(f"/{vehicle_type}/marcas/{brand_id}/modelos/{model_id}/anos/{year_id}")
        if data and 'Valor' in data:
            return data['Valor']
        return None

def determine_vehicle_type(brand_name, model_name):
    brand_name_lower = brand_name.lower() if brand_name else ''
    model_name_lower = model_name.lower() if model_name else ''

    if "moto" in model_name_lower or "honda" in brand_name_lower or "yamaha" in brand_name_lower:
        return "motos"
    elif "caminhao" in model_name_lower or "volkswagen caminhao" in brand_name_lower or "iveco" in brand_name_lower:
        return "caminhoes"
    return "carros"


def clean_and_normalize_name(name):
    if not isinstance(name, str):
        return ""
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip().lower()
    return cleaned

def find_latest_excel(directory):
    latest_excel = None
    latest_timestamp = None
    
    excel_pattern = re.compile(r"leilo_tratado_(\d{8}_\d{6})\.xlsx")

    if not os.path.exists(directory):
        print(f"[ERRO] O diretório '{directory}' para os arquivos Excel não foi encontrado.")
        return None

    try:
        files = os.listdir(directory)
        for filename in files:
            match = excel_pattern.match(filename)
            if match:
                current_timestamp_str = match.group(1)
                try:
                    current_timestamp = datetime.strptime(current_timestamp_str, "%Y%m%d_%H%M%S")
                    if latest_timestamp is None or current_timestamp > latest_timestamp:
                        latest_timestamp = current_timestamp
                        latest_excel = os.path.join(directory, filename)
                except ValueError:
                    print(f"[WARN] Impossível parsear timestamp do arquivo: {filename}")
        return latest_excel
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao listar arquivos no diretório {directory}: {e}")
        return None

def process_and_display_data():
    print("--- Iniciando processamento de dados ETL e consulta FIPE ---")

    EXCEL_FILE_PATH = find_latest_excel(CSV_DIRECTORY)

    if not EXCEL_FILE_PATH:
        print(f"[ERRO] Nenhum arquivo Excel com o padrão 'leilo_tratado_YYYYMMDD_HHMMSS.xlsx' encontrado em '{CSV_DIRECTORY}'.")
        print("Certifique-se de que o scraper foi executado com sucesso e o arquivo Excel foi gerado.")
        print("Verifique também o mapeamento de volumes no seu docker-compose.yml, se aplicável.")
        return

    print(f"[INFO] Processando o arquivo Excel mais recente: '{EXCEL_FILE_PATH}'.")

    try:
        df = pd.read_excel(EXCEL_FILE_PATH)
        print(f"[INFO] Dados carregados com sucesso do '{EXCEL_FILE_PATH}'.")
        print(f"[INFO] Total de {len(df)} registros encontrados.")

        if 'Título' in df.columns:
            split_title = df['Título'].str.split('/', n=1, expand=True).fillna('')
            df['Marca'] = split_title[0].str.strip()
            if len(split_title.columns) > 1:
                # O modelo raspado já vem limpo para a primeira palavra
                df['Modelo'] = split_title[1].str.split(' ', n=1, expand=True)[0].str.strip() 
            else:
                df['Modelo'] = ''
            print("[INFO] Coluna 'Título' desmembrada em 'Marca' e 'Modelo' (modelo extraído até o primeiro espaço).")
        else:
            print("[WARN] Coluna 'Título' não encontrada no Excel. Não foi possível desmembrar.")
            df['Marca'] = ''
            df['Modelo'] = ''

        if 'Data Leilão' in df.columns:
            df['data leilão'] = df['Data Leilão'].astype(str).str.strip()
            print("[INFO] Coluna 'Data Leilão' usada para 'data leilão'.")
        elif 'Situação' in df.columns:
            df['data leilão'] = df['Situação'].astype(str)
            df['data leilão'] = df['data leilão'].str.replace("Leilão ao vivo em: ", "", regex=False).str.strip()
            try:
                df['data leilão'] = df['data leilão'].apply(lambda x: pd.to_datetime(
                    f"{datetime.now().strftime('%Y-%m-%d')} {x.replace('h', ':').replace('m', ':').replace('s', '')}",
                    format="%Y-%m-%d %H:%M:%S"
                ).strftime("%d/%m/%Y") if pd.notna(x) and x.strip() else '')
                print("[INFO] Coluna 'Situação' duplicada e formatada para 'data leilão' (dd/mm/aaaa) como fallback.")
            except Exception as e:
                print(f"[ERRO] Ocorreu um erro ao formatar a coluna 'data leilão' do fallback: {e}.")
        else:
            print("[WARN] Nenhuma coluna 'Data Leilão' ou 'Situação' encontrada. Não foi possível criar 'data leilão'.")
            df['data leilão'] = ''

        fipe_client = FipeApiClient(FIPE_API_BASE_URL)
        
        df['FIPE_Marca_Correspondente'] = None
        df['FIPE_Modelo_Correspondente'] = None
        df['Diferenca_Valor (%)'] = None
        df['Status_FIPE'] = "Não Processado"
        df['valor_fipe'] = None

        for index, row in df.iterrows():
            marca_scraped = row['Marca']
            modelo_scraped_original = row['Modelo'] # Modelo já "limpo" (primeira palavra)
            ano_scraped = str(row['Ano'])

            print(f"\nConsulta FIPE para: Marca='{marca_scraped}', Modelo='{modelo_scraped_original}', Ano='{ano_scraped}'")
            
            vehicle_type = determine_vehicle_type(marca_scraped, modelo_scraped_original)
            if not vehicle_type:
                print(f"[WARN] Tipo de veículo não determinado para Marca: '{marca_scraped}', Modelo: '{modelo_scraped_original}'. Pulando FIPE.")
                df.at[index, 'Status_FIPE'] = "Tipo não determinado"
                continue

            fipe_brand_name = None
            fipe_model_name = None
            fipe_price = None
            fipe_status = "Não Encontrado"

            brands_data = fipe_client.get_brands(vehicle_type)
            if brands_data:
                normalized_scraped_brand = clean_and_normalize_name(marca_scraped)
                matched_brand = next((b for b in brands_data if normalized_scraped_brand == clean_and_normalize_name(b['nome'])), None)
                
                if matched_brand:
                    fipe_brand_name = matched_brand['nome']
                    brand_id = matched_brand['codigo']
                    
                    models_data = fipe_client.get_models(vehicle_type, brand_id)
                    if models_data and 'modelos' in models_data:
                        normalized_scraped_model = clean_and_normalize_name(modelo_scraped_original)
                        matched_model = None

                        # --- TENTATIVA 1: Procurar pelo modelo completo já extraído ---
                        for m in models_data['modelos']:
                            normalized_fipe_model = clean_and_normalize_name(m['nome'])
                            if normalized_scraped_model == normalized_fipe_model or \
                               (normalized_scraped_model in normalized_fipe_model and len(normalized_scraped_model) > 3):
                                matched_model = m
                                break
                        
                        # --- TENTATIVA 2: Se não encontrar, tentar com a primeira palavra do modelo ---
                        if not matched_model and ' ' in modelo_scraped_original: # Verifica se há mais de uma palavra originalmente
                            first_word_of_model = modelo_scraped_original.split(' ')[0].strip()
                            normalized_first_word_model = clean_and_normalize_name(first_word_of_model)
                            
                            # Evita tentar novamente se o modelo original já era uma única palavra
                            if normalized_first_word_model != normalized_scraped_model: 
                                print(f"    - [INFO] Modelo completo não encontrado. Tentando buscar modelo usando apenas a primeira palavra: '{first_word_of_model}'")
                                for m in models_data['modelos']:
                                    normalized_fipe_model = clean_and_normalize_name(m['nome'])
                                    if normalized_first_word_model == normalized_fipe_model or \
                                       (normalized_first_word_model in normalized_fipe_model and len(normalized_first_word_model) > 3):
                                        matched_model = m
                                        fipe_status = "Sucesso (Modelo parcial)"
                                        break

                        if matched_model:
                            fipe_model_name = matched_model['nome']
                            model_id = matched_model['codigo']

                            years_data = fipe_client.get_years(vehicle_type, brand_id, model_id)
                            if years_data:
                                matched_year_code = None
                                for y in years_data:
                                    fipe_year_part = y['codigo'].split('-')[0] 
                                    if ano_scraped == fipe_year_part:
                                        matched_year_code = y['codigo']
                                        break

                                if not matched_year_code and "32000-1" in [y['codigo'] for y in years_data]:
                                    matched_year_code = "32000-1"
                                
                                if matched_year_code:
                                    fipe_value_raw = fipe_client.get_vehicle_value(vehicle_type, brand_id, model_id, matched_year_code)
                                    if fipe_value_raw:
                                        fipe_value_numeric = float(fipe_value_raw.replace("R$", "").replace(".", "").replace(",", ".").strip())
                                        fipe_price = fipe_value_numeric
                                        if fipe_status != "Sucesso (Modelo parcial)":
                                            fipe_status = "Sucesso"
                                        print(f"    - Valor FIPE encontrado: {fipe_value_raw}")
                                    else:
                                        fipe_status = "Valor FIPE não encontrado"
                                        print(f"    - [WARN] Valor FIPE não encontrado para Ano: {ano_scraped}, Marca: {fipe_brand_name}, Modelo: {fipe_model_name}")
                                else:
                                    fipe_status = "Ano FIPE não encontrado"
                                    print(f"    - [WARN] Ano FIPE '{ano_scraped}' não encontrado para Marca: {fipe_brand_name}, Modelo: {fipe_model_name}")
                            else:
                                fipe_status = "Anos FIPE não encontrados"
                                print(f"    - [WARN] Anos FIPE não encontrados para Marca: {fipe_brand_name}, Modelo: {fipe_model_name}")
                        else:
                            fipe_status = "Modelo FIPE não encontrado"
                            print(f"    - [WARN] Modelo FIPE não encontrado para Marca: '{marca_scraped}', Modelo: '{modelo_scraped_original}'")
                    else:
                        fipe_status = "Modelos FIPE não encontrados"
                        print(f"    - [WARN] Modelos FIPE não encontrados para Marca: '{fipe_brand_name}'")
                else:
                    fipe_status = "Marca FIPE não encontrada"
                    print(f"    - [WARN] Marca FIPE não encontrada para '{marca_scraped}'")
            else:
                fipe_status = "Marcas FIPE não encontradas"
                print(f"    - [WARN] Marcas FIPE não encontradas para Tipo: {vehicle_type}")

            df.at[index, 'valor_fipe'] = fipe_price
            df.at[index, 'FIPE_Marca_Correspondente'] = fipe_brand_name
            df.at[index, 'FIPE_Modelo_Correspondente'] = fipe_model_name
            df.at[index, 'Status_FIPE'] = fipe_status

            scraped_valor_str = str(row.get('Valor do Lance', '0')).replace("R$", "").replace(".", "").replace(",", ".").strip()
            try:
                scraped_valor = float(scraped_valor_str)
                if fipe_price is not None and fipe_price > 0:
                    diferenca_percentual = ((scraped_valor - fipe_price) / fipe_price) * 100
                    df.at[index, 'Diferenca_Valor (%)'] = f"{diferenca_percentual:.2f}%"
                    print(f"    - Valor Raspado: R$ {scraped_valor:.2f}, Valor FIPE: R$ {fipe_price:.2f}, Diferença: {diferenca_percentual:.2f}%")
                else:
                    print(f"    - Não é possível calcular diferença para Lote {index}: Valor FIPE não disponível ou zero.")
            except ValueError:
                print(f"    - [WARN] Não foi possível converter 'Valor do Lance' raspado '{row.get('Valor do Lance', 'N/A')}' para número.")
                df.at[index, 'Diferenca_Valor (%)'] = "Erro de Valor Raspado"

            time.sleep(0.1)

        df['valor_fipe'] = df['valor_fipe'].apply(
            lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(x) else None
        )
        print("[INFO] Coluna 'valor_fipe' formatada para o padrão 'R$ X.XXX,XX'.")

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None)

        print("\n--- Tabela de Dados Processados (com FIPE) ---")
        print(df.to_string())
        print("\n--- Fim da Tabela ---")

        timestamp_excel_output = datetime.now().strftime("%Y%m%d_%H%M%S")
        EXCEL_OUTPUT_FILE_NAME = f"comparacao_fipe_{timestamp_excel_output}.xlsx"
        EXCEL_FULL_PATH = os.path.join(EXCEL_OUTPUT_DIR, EXCEL_OUTPUT_FILE_NAME)

        os.makedirs(EXCEL_OUTPUT_DIR, exist_ok=True)

        try:
            df.to_excel(EXCEL_FULL_PATH, index=False)
            print(f"\n--- Comparação com FIPE concluída e salva em '{EXCEL_FULL_PATH}' ---")
        except Exception as excel_err:
            print(f"[ERRO] Ocorreu um erro ao exportar os resultados para Excel: {excel_err}")
            print("Verifique as permissões de escrita para o caminho de saída ou se o arquivo já está aberto.")

    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao ler ou processar o arquivo Excel para comparação com FIPE: {e}")

if __name__ == "__main__":
    process_and_display_data()