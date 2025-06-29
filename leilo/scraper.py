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

def safe_get_element_text(element, css_selector):
    """
    Tenta obter o texto de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o texto for vazio.
    """
    try:
        found_element = element.find_element(By.CSS_SELECTOR, css_selector)
        text = found_element.text.strip()
        # Replace non-breaking space with regular space and ensure it's not just whitespace
        cleaned_text = text.replace('\xa0', ' ').strip()
        return cleaned_text if cleaned_text else "N/A"
    except NoSuchElementException:
        return "N/A"
    except StaleElementReferenceException:
        # If the element becomes stale here, try to re-find it or just return N/A
        try:
            # Attempt to re-find the element if it became stale
            re_found_element = WebDriverWait(element, 1).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            text = re_found_element.text.strip()
            cleaned_text = text.replace('\xa0', ' ').strip()
            return cleaned_text if cleaned_text else "N/A"
        except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
            return "N/A"
        except Exception as e:
            print(f"DEBUG: Erro ao re-encontrar elemento em safe_get_element_text para seletor '{css_selector}': {e}")
            return "N/A"
    except Exception as e:
        # Catch any other unexpected error during text retrieval
        print(f"DEBUG: Erro em safe_get_element_text para seletor '{css_selector}': {e}")
        return "N/A"

def safe_get_element_attribute(element, css_selector, attribute):
    """
    Tenta obter um atributo de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o atributo não existir.
    """
    try:
        found_element = element.find_element(By.CSS_SELECTOR, css_selector)
        attr_value = found_element.get_attribute(attribute)
        return attr_value.strip() if attr_value else "N/A"
    except NoSuchElementException:
        return "N/A"
    except StaleElementReferenceException:
        # If the element becomes stale here, try to re-find it or just return N/A
        try:
            re_found_element = WebDriverWait(element, 1).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            attr_value = re_found_element.get_attribute(attribute)
            return attr_value.strip() if attr_value else "N/A"
        except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
            return "N/A"
        except Exception as e:
            print(f"DEBUG: Erro ao re-encontrar elemento em safe_get_element_attribute para seletor '{css_selector}' e atributo '{attribute}': {e}")
            return "N/A"
    except Exception as e:
        # Catch any other unexpected error during attribute retrieval
        print(f"DEBUG: Erro em safe_get_element_attribute para seletor '{css_selector}' e atributo '{attribute}': {e}")
        return "N/A"

# Configura opções do Chrome
options = Options()
# options.add_argument("--headless")   # Descomente para rodar sem interface gráfica (modo headless)
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
# Adicionando opções para simular melhor um navegador real e evitar detecções
options.add_argument("--window-size=1920,1080")
options.add_argument("--start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

# URL do servidor Selenium remoto no container
SELENIUM_URL = "http://selenium:4444/wd/hub"

# Conecta ao Selenium remoto
driver = None
MAX_TRIES = 10 # Número máximo de tentativas de conexão

for attempt in range(MAX_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_TRIES})...")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        print("[INFO] Conectado ao Selenium!")
        break
    except WebDriverException as e:
        print(f"[WARN] Selenium ainda não está pronto: {e}. Tentando novamente em 2 segundos...")
        time.sleep(2)
else:
    raise Exception("❌ Não foi possível conectar ao Selenium após várias tentativas.")

dados = [] # Lista para armazenar os dados de todos os lotes

try:
    url = "https://leilo.com.br/leilao/carros?" # URL da página de leilões
    driver.get(url)
    print(f"[INFO] Página carregada: {driver.title}")

    # --- Lógica para fechar pop-up de cookies ou outros banners (CRÍTICO) ---
    print("[INFO] Tentando fechar pop-ups/aceitar cookies (se houver)...")
    try:
        # Aguarda o banner principal
        cookie_banner_container = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.cc-nb-main-container"))
        )
        # Tenta encontrar e clicar no botão de aceitar cookies
        cookie_accept_button = WebDriverWait(cookie_banner_container, 3).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-nb-okcookie"))
        )
        
        driver.execute_script("arguments[0].click();", cookie_accept_button)
        print("[INFO] Pop-up de cookies/aceitação clicado com sucesso.")
        time.sleep(2) # Pausa para o pop-up desaparecer
    except TimeoutException:
        print("[INFO] Nenhum pop-up de cookies/aceitação identificado ou apareceu no tempo limite. Prosseguindo.")
    except NoSuchElementException:
        print("[INFO] Contêiner de cookies encontrado, mas o botão de aceitar não. Prosseguindo.")
    except Exception as e:
        print(f"[WARN] Erro inesperado ao tentar lidar com pop-ups: {e}. Prosseguindo.")
    # --- FIM da lógica de pop-up ---

    # Variáveis de controle de paginação
    total_paginas = 1
    current_page = 1

    # Loop principal para iterar por todas as páginas
    while current_page <= total_paginas:
        print(f"\n--- Processando Página {current_page} de {total_paginas} ---")

        # Espera até que os elementos dos lotes (cards) estejam presentes na página.
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
            )
            time.sleep(2) 
        except TimeoutException:
            print(f"[WARN] Timeout ao carregar lotes na Página {current_page}. Pode não haver lotes nesta página ou carregamento lento. Fim da extração.")
            break 

        lotes = driver.find_elements(By.CSS_SELECTOR, ".sessao.cursor-pointer")
        print(f"[INFO] {len(lotes)} lotes encontrados na Página {current_page}.")

        if not lotes: 
            print(f"[INFO] Nenhum lote encontrado na Página {current_page}. Fim da extração.")
            break

        # Extração de dados dos lotes da página atual
        for i, lote in enumerate(lotes, start=1):
            try:
                # Re-locate the lot element to avoid StaleElementReferenceException for the lot cards
                # This is important if the page reloads or elements shift during pagination
                lote_element_on_page = WebDriverWait(driver, 10).until(EC.visibility_of(lotes[i-1]))
                
                print(f"🔍 Extraindo dados básicos do Lote {i} na Página {current_page}:")

                titulo = safe_get_element_text(lote_element_on_page, "div.header-card h3")
                link = safe_get_element_attribute(lote_element_on_page, "a.img-card", "href")
                if link == "N/A":
                    link = safe_get_element_attribute(lote_element_on_page, "div.header-card a", "href")
                if link and not link.startswith("http"):
                    link = "https://leilo.com.br" + link
                imagem_style = safe_get_element_attribute(lote_element_on_page, "div.q-img__image", "style")
                imagem = "N/A"
                if imagem_style != "N/A" and "url(" in imagem_style:
                    match = re.search(r'url\("?\'?([^"\')]+)"?\'?\)', imagem_style)
                    if match:
                        imagem = match.group(1)
                
                # Extração da UF
                uf = safe_get_element_text(lote_element_on_page, "div.codigo-anuncio span")
                # print(f"   - UF: {uf}") # Comentado para reduzir logs detalhados aqui

                ano_raw = safe_get_element_text(lote_element_on_page, "p.text-ano")
                ano = "N/A"
                if ano_raw != "N/A":
                    # Tenta extrair 4 dígitos para o ano (ex: 2010)
                    match_ano = re.search(r'\d{4}', ano_raw)
                    if match_ano:
                        ano = match_ano.group(0)
                    else: # Se não encontrar 4 dígitos, tenta 2 (ex: '10' para 2010)
                         match_ano = re.search(r'\d{2}', ano_raw)
                         if match_ano:
                             ano = "20" + match_ano.group(0)
                
                km = safe_get_element_text(lote_element_on_page, "p.text-km")
                valor = safe_get_element_text(lote_element_on_page, "li.valor-atual")
                situacao = "N/A"
                tempo_restante_span = safe_get_element_text(lote_element_on_page, "a.tempo-restante div > div span.text-weight-medium")
                if tempo_restante_span != "N/A" and tempo_restante_span.strip() != "":
                    situacao = "Leilão ao vivo em: " + tempo_restante_span
                else:
                    tag_finalizado = safe_get_element_text(lote_element_on_page, "div.tag-finalizado")
                    if tag_finalizado != "N/A" and tag_finalizado.strip() != "":
                        situacao = tag_finalizado
                    else:
                        data_e_hora_leilao_raw = safe_get_element_text(lote_element_on_page, "p.q-mb-none.text-grey-7")
                        if data_e_hora_leilao_raw != "N/A":
                            situacao = "Leilão: " + data_e_hora_leilao_raw.replace('\n', ' ').strip()
                data_leilao_str = "N/A"
                full_date_text = safe_get_element_text(lote_element_on_page, "p.q-mb-none.text-grey-7")
                if full_date_text != "N/A":
                    match_date = re.search(r'(\d{2}/\d{2}/\d{4})', full_date_text)
                    if match_date:
                        data_leilao_str = match_date.group(1)

                dados.append({
                    "Título": titulo, "Link": link, "Imagem": imagem, "UF": uf, "Ano": ano,
                    "KM": km, "Valor do Lance": valor, "Situação": situacao, "Data Leilao": data_leilao_str
                })
            except StaleElementReferenceException:
                print(f"[WARN] StaleElementReferenceException no Lote {i} da Página {current_page}. Re-localizando lotes e tentando novamente esta página.")
                # If a stale element is encountered, break this inner loop and re-fetch 'lotes' in the next outer loop iteration
                # This ensures we are always working with fresh elements
                break 
            except Exception as e:
                print(f"[ERRO] Erro inesperado ao extrair dados do Lote {i} na Página {current_page}: {e}")

        # Lógica de Paginação: Obter o total de páginas e navegar
        if current_page == 1: 
            try:
                total_pages_element = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'text-grey-7') and contains(., 'Página')]/span[2]"))
                )
                total_paginas_str = total_pages_element.text.strip()
                if total_paginas_str.isdigit():
                    total_paginas = int(total_paginas_str)
                    print(f"[INFO] Total de páginas identificado: {total_paginas}")
                else:
                    print(f"[WARN] Não foi possível extrair o total de páginas numérico. Usando 1 como padrão. Texto encontrado: '{total_paginas_str}'")
                    total_paginas = 1 
            except TimeoutException:
                print("❌ ERRO: Elemento 'Total de Páginas' não encontrado para determinar o loop. Assumindo 1 página.")
                total_paginas = 1
            except Exception as e:
                print(f"❌ ERRO Inesperado ao obter total de páginas: {e}. Assumindo 1 página.")
                total_paginas = 1

        # Avançar para a próxima página, se não for a última
        if current_page < total_paginas:
            try:
                next_page_to_click = current_page + 1
                print(f"[INFO] Tentando clicar no botão da Página {next_page_to_click}...")
                
                next_page_button_xpath = f"//div[contains(@class, 'q-pagination__middle')]/button[@aria-label='{next_page_to_click}']"
                
                button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.XPATH, next_page_button_xpath))
                )
                
                if 'disabled' not in button.get_attribute('class'):
                    driver.execute_script("arguments[0].click();", button)
                    print(f"✅ SUCESSO: Clicado no botão da Página {next_page_to_click}.")
                    current_page += 1
                    print("[INFO] Aguardando o carregamento dos novos lotes na próxima página...")
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
                    )
                    time.sleep(2) 
                else:
                    print(f"[WARN] Botão da Página {next_page_to_click} está desabilitado. Fim da paginação esperada.")
                    break 
            except TimeoutException:
                print(f"❌ ERRO: Botão da Página {next_page_to_click} NÃO encontrado ou não clicável (Timeout). Fim da paginação.")
                break 
            except ElementClickInterceptedException as e:
                print(f"❌ ERRO: Clique no botão da Página {next_page_to_click} interceptado: {e}. Fim da paginação.")
                break 
            except Exception as e:
                print(f"❌ ERRO Inesperado ao clicar no botão da próxima página: {e}. Fim da paginação.")
                break
        else:
            print("[INFO] Última página alcançada. Fim da paginação.")
            break 

    # --- INÍCIO DA NOVA FUNCIONALIDADE: BUSCA DE DETALHES PÓS-PAGINAÇÃO ---
    print("\n--- INICIANDO EXTRAÇÃO DE DETALHES DE CADA LOTE ---")

    def extract_lot_details(driver_instance, lot_data_list):
        """
        Navega para o link de cada lote e extrai informações detalhadas.
        Atualiza a lista lot_data_list com os novos dados.
        """
        main_window_handle = driver_instance.current_window_handle
        
        # Cria uma cópia da lista de dados para iterar, pois vamos modificá-la
        links_to_visit = [(index, item['Link']) for index, item in enumerate(lot_data_list)]

        for index, link in links_to_visit:
            if link != "N/A" and link:
                print(f"\n🔄 Processando detalhes para o lote {index + 1} (Link: {link})...")
                
                # Inicializa as variáveis de detalhe para cada lote para garantir que existam
                detalhe_4ano_veiculo = "N/A"
                detalhe_combustivel = "N/A"
                detalhe_km_veiculo = "N/A"
                detalhe_valor_mercado = "N/A"
                detalhe_cor = "N/A"
                detalhe_possui_chave = "N/A"
                detalhe_tipo_retomada = "N/A"
                detalhe_localizacao = "N/A"
                detalhe_tipo_veiculo = "N/A"

                try:
                    # Open the link in a new tab
                    driver_instance.execute_script("window.open(arguments[0]);", link)
                    # Wait until a new tab is opened (2 tabs in total)
                    WebDriverWait(driver_instance, 10).until(EC.number_of_windows_to_be(2))
                    driver_instance.switch_to.window(driver_instance.window_handles[-1]) # Switch to the new tab
                    
                    # Espera por um elemento específico da página de detalhes para garantir que carregou
                    # e faz um scroll para que o Selenium foque na área de dados.
                    detail_container = WebDriverWait(driver_instance, 20).until( # Increased timeout
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.categorias-veiculo"))
                    )
                    driver_instance.execute_script("arguments[0].scrollIntoView(true);", detail_container)
                    time.sleep(3) # Aumentei a pausa após o scroll para garantir o carregamento dinâmico

                    # Function to get detail by label using XPath for GT-SM structure
                    def get_detail_gt_sm_by_label(driver_obj, label_text):
                        try:
                            # The value is inside a <span> within an <a>, which is a sibling of a <div>
                            # containing the label <span>.
                            xpath = f"//div[contains(@class, 'gt-sm')]//span[contains(@class, 'label-categoria') and text()='{label_text}']/ancestor::div[contains(@class, 'col-md-4') or contains(@class, 'col-sm-6')]/a/span"
                            element = driver_obj.find_element(By.XPATH, xpath)
                            text = element.text.strip().replace('\xa0', ' ')
                            return text if text else "N/A"
                        except NoSuchElementException:
                            return "N/A"
                        except StaleElementReferenceException:
                            print(f"   ❗ StaleElementReferenceException ao buscar detalhe '{label_text}' em gt-sm. Ignorando.")
                            return "N/A"
                        except Exception as e:
                            print(f"   ❗ Erro inesperado ao buscar detalhe '{label_text}' em gt-sm: {e}")
                            return "N/A"

                    # Function to get detail by direct CSS selector for LT-MD structure
                    def get_detail_lt_md_direct(driver_obj, css_selector):
                        try:
                            element = driver_obj.find_element(By.CSS_SELECTOR, css_selector)
                            text = element.text.strip().replace('\xa0', ' ')
                            return text if text else "N/A"
                        except NoSuchElementException:
                            return "N/A"
                        except StaleElementReferenceException:
                            # Try to re-find if stale
                            try:
                                re_found_element = WebDriverWait(driver_obj, 1).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                                )
                                text = re_found_element.text.strip().replace('\xa0', ' ')
                                return text if text else "N/A"
                            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                                return "N/A"
                            except Exception as e:
                                print(f"DEBUG: Erro ao re-encontrar elemento direto lt-md: {e}")
                                return "N/A"
                        except Exception as e:
                            print(f"DEBUG: Erro ao buscar detalhe direto lt-md: {e}")
                            return "N/A"

                    # --- Tentativa de extração para gt-sm (prioritário) ---
                    print("   -> Tentando extrair detalhes do bloco gt-sm (layout desktop)...")
                    
                    detalhe_ano_veiculo = get_detail_gt_sm_by_label(driver_instance, "Ano")
                    # Special parsing for Year if it's "YYYY/YYYY"
                    if detalhe_ano_veiculo != "N/A" and "/" in detalhe_ano_veiculo:
                        detalhe_ano_veiculo = detalhe_ano_veiculo.split('/')[0].strip()

                    detalhe_combustivel = get_detail_gt_sm_by_label(driver_instance, "Combustivel")
                    detalhe_km_veiculo = get_detail_gt_sm_by_label(driver_instance, "Km")
                    detalhe_valor_mercado = get_detail_gt_sm_by_label(driver_instance, "Valor Mercado")
                    if detalhe_valor_mercado != "N/A":
                        detalhe_valor_mercado = detalhe_valor_mercado.replace('R$', '').strip()
                        
                    detalhe_cor = get_detail_gt_sm_by_label(driver_instance, "Cor")
                    detalhe_possui_chave = get_detail_gt_sm_by_label(driver_instance, "Possui Chave")
                    detalhe_tipo_retomada = get_detail_gt_sm_by_label(driver_instance, "Tipo Retomada")
                    detalhe_localizacao = get_detail_gt_sm_by_label(driver_instance, "Localização")
                    detalhe_tipo_veiculo = get_detail_gt_sm_by_label(driver_instance, "Tipo")

                    # --- Fallback para lt-md se gt-sm falhou para os campos principais ---
                    # Check if ANY of the primary fields from GT-SM are still N/A
                    if (detalhe_ano_veiculo == "N/A" or detalhe_combustivel == "N/A" or detalhe_km_veiculo == "N/A" or
                        detalhe_valor_mercado == "N/A" or detalhe_cor == "N/A" or detalhe_possui_chave == "N/A"):

                        print("   -> GT-SM não forneceu todos os detalhes. Tentando extrair detalhes do bloco lt-md (layout mobile)...")
                        
                        # LT-MD has a different structure for Year, Fuel, KM (direct p.text-categoria)
                        temp_ano = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center.q-pb-sm div.col-4:nth-child(1) p.text-categoria")
                        if temp_ano != "N/A":
                            # Use the first part if it's "YYYY/YYYY" or just the year
                            if "/" in temp_ano:
                                detalhe_ano_veiculo = temp_ano.split('/')[0].strip()
                            else:
                                detalhe_ano_veiculo = temp_ano.strip()

                        temp_combustivel = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center.q-pb-sm div.col-4:nth-child(2) p.text-categoria")
                        if temp_combustivel != "N/A":
                            detalhe_combustivel = temp_combustivel
                        
                        temp_km = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center.q-pb-sm div.col-4:nth-child(3) p.text-categoria")
                        if temp_km != "N/A":
                            detalhe_km_veiculo = temp_km

                        # For other fields, LT-MD might use the `label-categoria` span + p.text-categoria pattern
                        if detalhe_valor_mercado == "N/A":
                            temp_valor_mercado = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center div.col-4:nth-child(1) p.text-categoria")
                            if temp_valor_mercado != "N/A":
                                detalhe_valor_mercado = temp_valor_mercado.replace('R$', '').strip()

                        if detalhe_cor == "N/A":
                            temp_cor = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center div.col-4:nth-child(2) p.text-categoria")
                            if temp_cor != "N/A":
                                detalhe_cor = temp_cor

                        if detalhe_possui_chave == "N/A":
                            temp_chave = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center div.col-4:nth-child(3) p.text-categoria")
                            if temp_chave != "N/A":
                                detalhe_possui_chave = temp_chave

                        # For Tipo Retomada, Localização, Tipo Veículo, LT-MD has a different structure:
                        # <div> with label-categoria then <a>/<span>
                        if detalhe_tipo_retomada == "N/A":
                            detalhe_tipo_retomada = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(1) a.text-categoria > span")
                        if detalhe_localizacao == "N/A":
                            # Location in LT-MD has a <p> inside the <span>, need to handle that
                            temp_location_ltmd = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(2) a.text-categoria > span > p")
                            if temp_location_ltmd != "N/A":
                                detalhe_localizacao = temp_location_ltmd
                            else: # Fallback if there's no <p> inside <span>
                                detalhe_localizacao = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(2) a.text-categoria > span")
                        if detalhe_tipo_veiculo == "N/A":
                            detalhe_tipo_veiculo = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(3) a.text-categoria > span")


                    print(f"   ✔️ Detalhes extraídos para {link}.")
                    print(f"   -> Ano: {detalhe_ano_veiculo}")
                    print(f"   -> Combustível: {detalhe_combustivel}")
                    print(f"   -> KM: {detalhe_km_veiculo}")
                    print(f"   -> Valor Mercado: {detalhe_valor_mercado}")
                    print(f"   -> Cor: {detalhe_cor}")
                    print(f"   -> Possui Chave: {detalhe_possui_chave}")
                    print(f"   -> Tipo Retomada: {detalhe_tipo_retomada}")
                    print(f"   -> Localização: {detalhe_localizacao}")
                    print(f"   -> Tipo Veículo: {detalhe_tipo_veiculo}")

                except TimeoutException:
                    print(f"   ⚠️ Timeout ao carregar detalhes do lote em {link}. Informações adicionais podem estar incompletas ou a página não carregou corretamente.")
                except Exception as e:
                    print(f"   ❌ ERRO geral ao extrair detalhes da página {link}: {e}")
                
                finally:
                    # Fecha a aba do lote e volta para a aba principal
                    driver_instance.close()
                    driver_instance.switch_to.window(main_window_handle)
                    print(f"   ⬅️ Voltando para a página principal.")
            else:
                print(f"   ❗ Link não disponível para o lote {index + 1}. Pulando extração de detalhes.")
            
            # Atualiza o dicionário correspondente na lista `dados`
            # É importante garantir que o índice seja válido e que o dicionário já exista
            if index < len(lot_data_list):
                lot_data_list[index].update({
                    "Detalhe_Ano_Veiculo": detalhe_ano_veiculo,
                    "Detalhe_Combustivel": detalhe_combustivel,
                    "Detalhe_KM_Veiculo": detalhe_km_veiculo,
                    "Detalhe_Valor_Mercado": detalhe_valor_mercado,
                    "Detalhe_Cor": detalhe_cor,
                    "Detalhe_Possui_Chave": detalhe_possui_chave,
                    "Detalhe_Tipo_Retomada": detalhe_tipo_retomada,
                    "Detalhe_Localizacao": detalhe_localizacao,
                    "Detalhe_Tipo_Veiculo": detalhe_tipo_veiculo
                })
            else:
                print(f"[ERROR] Índice {index} fora dos limites para atualizar dados.")


    # Chama a função para extrair os detalhes de cada lote APÓS a paginação completa
    extract_lot_details(driver, dados)

    print("\n--- EXTRAÇÃO DE DETALHES CONCLUÍDA ---")
    # --- FIM DA NOVA FUNCIONALIDADE ---

    # --- Geração do CSV ---
    if dados: # Verifica se a lista de dados não está vazia
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = f"leilo_{timestamp}.csv"
        output_path = os.path.join("etl/", output_file_name) 

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        try:
            with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
                if dados:
                    # Garante que todos os dicionários tenham as mesmas chaves para o cabeçalho
                    # Pega todas as chaves de todos os dicionários e remove duplicatas
                    all_keys = list(set(key for d in dados for key in d.keys()))
                    writer = csv.DictWriter(f, fieldnames=all_keys)
                    writer.writeheader()
                    writer.writerows(dados)
            print(f"\n[INFO] Dados salvos com sucesso em {output_path}")
        except IOError as e:
            print(f"\n[ERRO] Não foi possível salvar o arquivo CSV em {output_path}.")
            print(f"Causa do erro: {e}")
            print("Isso pode ser devido a permissões de escrita ou o caminho do arquivo não ser acessível.")
        except Exception as e:
            print(f"\n[ERRO] Ocorreu um erro inesperado ao tentar salvar o CSV: {e}")
    else:
        print("\n[WARN] Nenhum lote foi encontrado em todas as páginas. O arquivo CSV não será gerado.")
    
finally:
    if driver: # Garante que o driver seja fechado mesmo se ocorrer um erro
        driver.quit()
    print("[INFO] Navegador fechado.")
    print("[INFO] Scraping e paginação concluídos.")