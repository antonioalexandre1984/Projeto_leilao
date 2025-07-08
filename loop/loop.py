import csv
import time
import re
import os
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Importar as funções de banco de dados
try:
    from db_utils.db_operations import connect_db, create_loop_table, insert_data_loop
    print("[INFO] Módulos de banco de dados importados com sucesso.")
except ImportError as e:
    print(f"[ERRO] Falha ao importar db_manager: {e}. Certifique-se de que db_manager.py está acessível.")
    # import sys
    # sys.exit(1)


def safe_get_element_text(element, css_selector, wait_time=0):
    """
    Tenta obter o texto de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o texto for vazio.
    Inclui tratamento para StaleElementReferenceException e tempo de espera opcional.
    """
    try:
        if isinstance(element, webdriver.remote.webdriver.WebDriver):
            found_element = WebDriverWait(element, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            ) if wait_time > 0 else element.find_element(By.CSS_SELECTOR, css_selector)
        else:
            found_element = element.find_element(By.CSS_SELECTOR, css_selector)

        text = found_element.text.strip()
        cleaned_text = text.replace('\xa0', ' ').strip()
        return cleaned_text if cleaned_text else "N/A"
    except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
        return "N/A"
    except Exception as e:
        return "N/A"

def safe_get_element_attribute(element, css_selector, attribute, wait_time=0):
    """
    Tenta obter um atributo de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o atributo não existir.
    Inclui tratamento para StaleElementReferenceException e tempo de espera opcional.
    """
    try:
        if isinstance(element, webdriver.remote.webdriver.WebDriver):
            found_element = WebDriverWait(element, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            ) if wait_time > 0 else element.find_element(By.CSS_SELECTOR, css_selector)
        else:
            found_element = element.find_element(By.CSS_SELECTOR, css_selector)
            
        attr_value = found_element.get_attribute(attribute)
        return attr_value.strip() if attr_value else "N/A"
    except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
        return "N/A"
    except Exception as e:
        return "N/A"

def extract_data_from_lot_detail_page(driver_instance, lot_url):
    """
    Navega para a URL de detalhes do lote e extrai as informações,
    usando as chaves "DE" do mapeamento fornecido.
    """
    print(f"    Acessando página de detalhes do lote: {lot_url}")
    # Usando 'URL do Lote' como a chave DE do mapeamento
    data = {'URL do Lote': lot_url} 

    try:
        driver_instance.get(lot_url)
        WebDriverWait(driver_instance, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.editor.taj")) 
        )
        print(f"    Página de detalhes '{driver_instance.title}' carregada.")

        # --- Extração de dados do bloco "div.editor.taj" usando RegEx ---
        details_block_text = safe_get_element_text(driver_instance, "div.editor.taj")
        
        if details_block_text != "N/A":
            def get_regex_value(text, label):
                match = re.search(fr'{label}:\s*\n(.+)', text, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
                match = re.search(fr'{label}:\s*([^\n]+)', text, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    if re.search(r'\w+:', value): 
                        value = re.split(r'\w+:', value)[0].strip()
                    return value.strip()
                return "N/A"

            # Ajustando as chaves de acordo com o mapeamento DE
            data['Marca'] = get_regex_value(details_block_text, "Marca")
            data['Modelo'] = get_regex_value(details_block_text, "Modelo")
            data['Versão'] = get_regex_value(details_block_text, "Versão")
            data['Ano de Fabricação'] = get_regex_value(details_block_text, "Ano de Fabricação")
            data['Ano Modelo'] = get_regex_value(details_block_text, "Ano Modelo")
            data['Fipe'] = get_regex_value(details_block_text, "Fipe")
            data['Blindado'] = get_regex_value(details_block_text, "Blindado")
            data['Chave'] = get_regex_value(details_block_text, "Chave")
            data['Funcionando'] = get_regex_value(details_block_text, "Funcionando")
            data['Combustível'] = get_regex_value(details_block_text, "Combustível") # Mantém com acento para extração
            data['Km'] = get_regex_value(details_block_text, "Km")
        else:
            print("    [WARN] Bloco de detalhes 'div.editor.taj' não encontrado na página de detalhes.")
            data['Marca'] = "N/A"
            data['Modelo'] = "N/A"
            data['Versão'] = "N/A"
            data['Ano de Fabricação'] = "N/A"
            data['Ano Modelo'] = "N/A"
            data['Fipe'] = "N/A"
            data['Blindado'] = "N/A"
            data['Chave'] = "N/A"
            data['Funcionando'] = "N/A"
            data['Combustível'] = "N/A"
            data['Km'] = "N/A"

        # --- Extração de dados adicionais (ajustando chaves para o mapeamento DE) ---
        data['Nome do Veículo (Header)'] = safe_get_element_text(driver_instance, "h1.LL_carname") 
        data['Data do Leilão'] = safe_get_element_text(driver_instance, "p.datalote + p.cor_969696") 
        data['Horário do Leilão'] = safe_get_element_text(driver_instance, "div.datalote2 p.cor_969696")

        data['Lance Atual'] = safe_get_element_text(driver_instance, "p.LL_lance_atual")
        
        lances_views_text = safe_get_element_text(driver_instance, "p.contagem") 
        lances_match = re.search(r'(\d+)\s*Lances', lances_views_text)
        views_match = re.search(r'(\d+)\s*Visualizações', lances_views_text)
        data['Número de Lances'] = lances_match.group(1) if lances_match else "N/A"
        data['Número de Visualizações'] = views_match.group(1) if views_match else "N/A"

        data['Situação do Lote'] = safe_get_element_text(driver_instance, "ul.LL_situacao li p")

        print("    Dados extraídos da página de detalhes.")
        return data

    except TimeoutException:
        print(f"⚠️ Timeout ao carregar a página de detalhes do lote em {lot_url}.")
        return data 
    except Exception as e:
        print(f"❌ ERRO ao extrair dados da página de detalhes {lot_url}: {e}")
        return data

def save_to_csv(data_list):
    """
    Salva uma lista de dicionários em um arquivo CSV no caminho especificado.
    Usa as chaves "DE" do mapeamento.
    """
    if not data_list:
        print("[INFO] Nenhuns dados para salvar no CSV.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_directory = r"C:\Users\anton\Desktop\parque_leiloes_scraper\app\loop\webscraping"
    os.makedirs(output_directory, exist_ok=True) 
    
    output_filename = os.path.join(output_directory, f"lotes_loopbrasil_{timestamp}.csv")

    # Os fieldnames devem corresponder às chaves retornadas por extract_data_from_lot_detail_page
    fieldnames = [
        'URL do Lote',
        'Nome do Veículo (Header)', 
        'Marca',
        'Modelo',
        'Versão',
        'Ano de Fabricação',
        'Ano Modelo',
        'Fipe',
        'Blindado',
        'Chave',
        'Funcionando',
        'Combustível', # Mantém com acento para CSV
        'Km',
        'Número de Lances',
        'Número de Visualizações',
        'Data do Leilão',
        'Horário do Leilão',
        'Lance Atual',
        'Situação do Lote'
    ]

    try:
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data_list)
        print(f"[SUCESSO] Dados salvos em '{output_filename}' com sucesso!")
    except IOError as e:
        print(f"[ERRO] Não foi possível salvar o arquivo CSV: {e}")
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro inesperado ao salvar o CSV: {e}")

def save_to_database(data_list, db_connection):
    """
    Salva uma lista de dicionários no banco de dados 'loop',
    mapeando as chaves do scraper ('DE') para as chaves do banco de dados ('PARA').
    """
    if not data_list:
        print("[INFO] Nenhuns dados para salvar no banco de dados.")
        return

    # Mapeamento EXATO das chaves "DE" para as chaves "PARA" do banco de dados
    db_mapping = {
        'URL do Lote': 'veiculo_link_lote',
        'Nome do Veículo (Header)': 'veiculo_titulo',
        'Marca': 'veiculo_fabricante', # Mapeado para fabricante
        'Modelo': 'veiculo_modelo',
        'Versão': 'veiculo_versao',
        'Ano de Fabricação': 'veiculo_ano_fabricacao',
        'Ano Modelo': 'veiculo_ano_modelo',
        'Fipe': 'veiculo_valor_fipe', # Mapeado para valor_fipe
        'Blindado': 'veiculo_blindado',
        'Chave': 'veiculo_chave',
        'Funcionando': 'veiculo_funcionando',
        'Combustível': 'veiculo_tipo_combustivel', # Mapeado para tipo_combustivel
        'Km': 'veiculo_km',
        'Número de Lances': 'veiculo_total_lances', # Mapeado para total_lances
        'Número de Visualizações': 'veiculo_numero_visualizacoes',
        'Data do Leilão': 'veiculo_data_leilao',
        'Horário do Leilão': 'veiculo_horario_leilao',
        'Lance Atual': 'veiculo_lance_atual', # Mapeado para lance_atual
        'Situação do Lote': 'veiculo_situacao_lote'
    }

    print("[INFO] Iniciando salvamento dos dados no banco de dados...")
    for data_row_scraper in data_list:
        data_row_db = {}
        for scraper_key, db_key in db_mapping.items():
            # Pega o valor da chave original do scraper e atribui à chave mapeada para o DB
            data_row_db[db_key] = data_row_scraper.get(scraper_key)
        
        # Insere a linha processada no banco de dados
        insert_data_loop(db_connection, data_row_db)
    print("[SUCESSO] Todos os dados foram processados para inserção no banco de dados.")


# --- Configuração e Inicialização do Selenium ---
print("[INFO] Iniciando a configuração do Selenium...")
options = Options()
# options.add_argument("--headless")    # Rodar em modo headless para mais velocidade e menor uso de recursos
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_argument("--start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

# --- CONEXÃO COM O SELENIUM ---
SELENIUM_URL = "http://selenium:4444/wd/hub"

driver = None
MAX_TRIES = 2

for attempt in range(MAX_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_TRIES})...")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        driver.implicitly_wait(1)
        print("[INFO] Conectado ao Selenium com sucesso!")
        break
    except WebDriverException as e:
        print(f"[WARN] Selenium ainda não está pronto: {e}. Tentando novamente em 2 segundos...")
        time.sleep(2)
else:
    raise Exception("❌ Não foi possível conectar ao Selenium após várias tentativas.")

# --- Conexão com o Banco de Dados ---
db_conn = None
try:
    db_conn = connect_db()
    if db_conn:
        print("[INFO] Conexão com o banco de dados estabelecida. Verificando/Criando tabela 'loop'...")
        create_loop_table(db_conn)
    else:
        print("[WARN] Não foi possível conectar ao banco de dados. Os dados serão salvos APENAS em CSV.")
except Exception as e:
    print(f"[ERRO] Erro ao conectar ou criar tabela no banco de dados: {e}. Os dados serão salvos APENAS em CSV.")
    db_conn = None

all_lotes_data = []
base_url = "https://loopbrasil.net"
initial_list_page_url = f"{base_url}/lotes/?&cate[]=3"

current_page_url = initial_list_page_url
page_counter = 0

try:
    while current_page_url:
        page_counter += 1
        print(f"\n[INFO] Navegando para a página de listagem: {current_page_url} (Página {page_counter})")
        driver.get(current_page_url)
        print(f"[INFO] Página de listagem carregada: {driver.title}")

        # --- Lógica para fechar pop-up de cookies ou outros banners ---
        print("[INFO] Tentando fechar pop-ups/aceitar cookies (se houver)...")
        try:
            cookie_banner_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.cc-nb-main-container"))
            )
            cookie_accept_button = WebDriverWait(cookie_banner_container, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-nb-okcookie"))
            )
            
            driver.execute_script("arguments[0].click();", cookie_accept_button)
            print("[INFO] Pop-up de cookies/aceitação clicado com sucesso.")
            time.sleep(0.5)
        except TimeoutException:
            print("[INFO] Nenhum pop-up de cookies/aceitação identificado ou apareceu no tempo limite. Prosseguindo.")
        except NoSuchElementException:
            print("[INFO] Contêiner de cookies encontrado, mas o botão de aceitar não. Prosseguindo.")
        except Exception as e:
            print(f"[WARN] Erro inesperado ao tentar lidar com pop-ups: {e}. Prosseguindo.")

        # --- Encontra e processa todos os lotes na página principal ---
        print(f"\n--- Localizando e extraindo URLs dos lotes na página de listagem (Página {page_counter}) ---")
        
        WebDriverWait(driver, 20).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "section[id^='lote'] > a.card"))
        )
        
        lot_card_elements = driver.find_elements(By.CSS_SELECTOR, "section[id^='lote'] > a.card")

        if lot_card_elements:
            print(f"[INFO] Encontrados {len(lot_card_elements)} lotes na página de listagem.")
            
            lot_urls_to_visit = []
            for i, card_element in enumerate(lot_card_elements):
                try:
                    lot_url = card_element.get_attribute("href")
                    if lot_url and not lot_url.startswith("http"):
                        lot_url = base_url + lot_url
                    
                    if lot_url:
                        lot_urls_to_visit.append(lot_url)
                    else:
                        print(f"[WARN] URL vazia ou nula para o card {i+1}.")
                except Exception as e:
                    print(f"[WARN] Não foi possível obter a URL para o card {i+1} devido a um erro: {e}")

            if not lot_urls_to_visit:
                print("[INFO] Nenhuma URL de lote válida encontrada para processar nesta página.")
            else:
                print(f"[INFO] Coletadas {len(lot_urls_to_visit)} URLs de lotes para visitar.")
                
                for i, lot_detail_url in enumerate(lot_urls_to_visit):
                    print(f"\n--- Processando lote {i+1}/{len(lot_urls_to_visit)} (da Página {page_counter}) ---")
                    
                    lote_data = extract_data_from_lot_detail_page(driver, lot_detail_url)
                    all_lotes_data.append(lote_data)
                    
                    for key, value in lote_data.items():
                        print(f"    {key}: {value}")
                    
                    time.sleep(1)

        else:
            print("[INFO] Nenhum lote encontrado nesta página para análise.")

        # --- Lógica de Paginação ---
        print("\n--- Verificando Paginação ---")
        next_page_link = None
        try:
            pagination_div = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.pagg"))
            )
            
            next_page_element = pagination_div.find_element(By.XPATH, ".//a[contains(text(), 'Próximo') and @href]")
            
            next_page_link = next_page_element.get_attribute("href")
            if next_page_link and not next_page_link.startswith("http"):
                next_page_link = base_url + next_page_link
            
            print(f"[INFO] Próxima página encontrada: {next_page_link}")

        except NoSuchElementException:
            print("[INFO] Link 'Próximo' não encontrado ou não tem href válido. Fim da paginação.")
            current_page_url = None
        except TimeoutException:
            print("[INFO] Timeout ao tentar encontrar a paginação. Fim da paginação.")
            current_page_url = None
        except Exception as e:
            print(f"[WARN] Erro ao processar paginação: {e}. Assumindo fim da paginação.")
            current_page_url = None
        
        current_page_url = next_page_link
        if current_page_url:
            time.sleep(2)

    # --- Salva os dados após processar todas as páginas ---
    save_to_csv(all_lotes_data)
    if db_conn:
        save_to_database(all_lotes_data, db_conn)

except TimeoutException:
    print("[WARN] Timeout ao carregar a página inicial ou elementos de lote.")
except Exception as e:
    print(f"❌ ERRO geral no processo de raspagem: {e}")

finally:
    if driver:
        print("[INFO] Fechando o navegador Selenium.")
        driver.quit()
    if db_conn:
        print("[INFO] Fechando a conexão com o banco de dados.")
        db_conn.close()
    print("[INFO] Processo concluído.")