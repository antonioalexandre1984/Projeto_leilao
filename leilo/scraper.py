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

# Importa as funções de banco de dados do seu módulo db_utils
from db_utils.db_operations import (
    connect_db,
    create_leilo_table, # Nome da função ajustado conforme o db_operations_py
    insert_data_leilo    # Nome da função ajustado conforme o db_operations_py
)

def safe_get_element_text(element, css_selector):
    """
    Tenta obter o texto de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o texto for vazio.
    Inclui tratamento para StaleElementReferenceException e tempo de espera opcional.
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
    Inclui tratamento para StaleElementReferenceException e tempo de espera opcional.
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

# --- Funções auxiliares para extração de detalhes (agora no escopo global) ---
def get_detail_gt_sm_by_label(driver_obj, label_text):
    try:
        xpath = f"//div[contains(@class, 'gt-sm')]//span[contains(@class, 'label-categoria') and text()='{label_text}']/ancestor::div[contains(@class, 'col-md-4') or contains(@class, 'col-sm-6')]/a/span"
        element = driver_obj.find_element(By.XPATH, xpath)
        text = element.text.strip().replace('\xa0', ' ')
        return text if text else "N/A"
    except NoSuchElementException:
        return "N/A"
    except StaleElementReferenceException:
        print(f"    ❗ StaleElementReferenceException ao buscar detalhe '{label_text}' em gt-sm. Ignorando.")
        return "N/A"
    except Exception as e:
        print(f"    ❗ Erro inesperado ao buscar detalhe '{label_text}' em gt-sm: {e}")
        return "N/A"

""" def get_detail_lt_md_direct(driver_obj, css_selector):
    try:
        element = driver_obj.find_element(By.CSS_SELECTOR, css_selector)
        text = element.text.strip().replace('\xa0', ' ')
        return text if text else "N/A"
    except NoSuchElementException:
        return "N/A"
    except StaleElementReferenceException:
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
        return "N/A" """

# --- FUNÇÃO DE EXTRAÇÃO DE DETALHES (MOVIDA PARA O ESCOPO GLOBAL) ---
def extract_lot_details(driver_instance, lot_data_list):
    """
    Navega para o link de cada lote e extrai informações detalhadas.
    Atualiza a lista lot_data_list com os novos dados.
    """
    main_window_handle = driver_instance.current_window_handle
    
    # Cria uma cópia da lista de dados para iterar, pois vamos modificá-la
    # As chaves aqui devem ser as mesmas usadas no dicionário 'dados' (minúsculas)
    links_to_visit = [(index, item['veiculo_link_lote']) for index, item in enumerate(lot_data_list)] 

    for index, link in links_to_visit:
        if link != "N/A" and link:
            print(f"\n🔄 Processando detalhes para o lote {index + 1} (Link: {link})...")
            
            # Inicializa as variáveis de detalhe para cada lote para garantir que existam
            ano_veiculo = "N/A"
            combustivel = "N/A"
            km_veiculo = "N/A"
            valor_mercado_fipe = "N/A"
            cor_veiculo = "N/A"
            veiculo_possui_chave = "N/A"
            tipo_retomada = "N/A"
            localizacao_detalhe = "N/A" 
            tipo_veiculo = "N/A"
            veiculo_versao = "N/A" # Inicializa a nova variável
            # fabricante_veiculo e modelo_veiculo já vêm do título, mas são inicializados aqui para segurança
            fabricante_veiculo = lot_data_list[index].get("veiculo_fabricante", "N/A")
            modelo_veiculo = lot_data_list[index].get("veiculo_modelo", "N/A")

            # --- Extração da versão do veículo a partir do modelo ---
            if modelo_veiculo != "N/A" and " " in modelo_veiculo:
                # Divide a string no primeiro espaço e pega o restante
                veiculo_versao = modelo_veiculo.split(' ', 1)[1].strip()
                # O modelo_veiculo agora será apenas a primeira parte
                modelo_veiculo = modelo_veiculo.split(' ', 1)[0].strip()
            else:
                veiculo_versao = "N/A" # Se não houver espaço, não há versão específica

            try:
                # Open the link in a new tab
                driver_instance.execute_script("window.open(arguments[0]);", link)
                # Wait until a new tab is opened (2 tabs in total)
                WebDriverWait(driver_instance, 10).until(EC.number_of_windows_to_be(2))
                driver_instance.switch_to.window(driver_instance.window_handles[-1]) # Switch to the new tab
                
                # **REVISADO**: Espera por um contêiner mais geral e rola a página
                # Espera que o contêiner principal de categorias esteja presente
                detail_container = WebDriverWait(driver_instance, 25).until( # Aumentei o timeout para 25s
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.categorias-veiculo"))
                )
                
                # Rola para o final para garantir o carregamento de elementos dinâmicos (lazy loading)
                driver_instance.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2) # Pequena pausa para renderização após o scroll
                
                # Rola de volta para o topo (opcional, para consistência de visualização do Selenium)
                driver_instance.execute_script("window.scrollTo(0, 0);")
                time.sleep(2) # Pausa para o scroll se ajustar

                # --- Tentativa de extração para gt-sm (prioritário) ---
                print("    -> Tentando extrair detalhes do bloco gt-sm (layout desktop)...")
                
                ano_veiculo = get_detail_gt_sm_by_label(driver_instance, "Ano")
                # Special parsing for Year if it's "YYYY/YYYY"
                if ano_veiculo != "N/A" and "/" in ano_veiculo:
                    ano_veiculo = ano_veiculo.split('/')[0].strip()

                combustivel = get_detail_gt_sm_by_label(driver_instance, "Combustivel")
                km_veiculo = get_detail_gt_sm_by_label(driver_instance, "Km")
                
                # --- Ajuste para extração de valor de mercado (FIPE) ---
                valor_mercado_fipe_raw = get_detail_gt_sm_by_label(driver_instance, "Valor Mercado")
                # Limpa a string para um formato numérico compatível com float (ex: "20000.00")
                if valor_mercado_fipe_raw != "N/A":
                    valor_mercado_fipe = valor_mercado_fipe_raw.replace('R$', '').replace('.', '').replace(',', '.').strip()
                else:
                    valor_mercado_fipe = "N/A" # Garante que se não encontrar, seja N/A
                        
                cor_veiculo = get_detail_gt_sm_by_label(driver_instance, "Cor")
                veiculo_possui_chave = get_detail_gt_sm_by_label(driver_instance, "Possui Chave")
                tipo_retomada = get_detail_gt_sm_by_label(driver_instance, "Tipo Retomada")
                localizacao_detalhe = get_detail_gt_sm_by_label(driver_instance, "Localização") 
                tipo_veiculo = get_detail_gt_sm_by_label(driver_instance, "Tipo")
                
                # Fabricante e Modelo não serão mais extraídos aqui, pois já vêm do título
                # mas as variáveis são mantidas para o log e para o update final

                # **NOVO**: Logs de debug para GT-SM
                print(f"        DEBUG GT-SM - Ano: '{ano_veiculo}'")
                print(f"        DEBUG GT-SM - Combustivel: '{combustivel}'")
                print(f"        DEBUG GT-SM - KM: '{km_veiculo}'")
                print(f"        DEBUG GT-SM - Valor Mercado: '{valor_mercado_fipe}'")
                print(f"        DEBUG GT-SM - Cor: '{cor_veiculo}'")
                print(f"        DEBUG GT-SM - Possui Chave: '{veiculo_possui_chave}'")
                print(f"        DEBUG GT-SM - Tipo Retomada: '{tipo_retomada}'")
                print(f"        DEBUG GT-SM - Localização: '{localizacao_detalhe}'")
                print(f"        DEBUG GT-SM - Tipo: '{tipo_veiculo}'")


                # --- Fallback para lt-md se gt-sm falhou para os campos principais ---
                # Check if ANY of the primary fields from GT-SM are still N/A
                if (ano_veiculo == "N/A" or combustivel == "N/A" or km_veiculo == "N/A" or
                    valor_mercado_fipe == "N/A" or cor_veiculo == "N/A" or veiculo_possui_chave == "N/A" or
                    tipo_retomada == "N/A" or localizacao_detalhe == "N/A" or tipo_veiculo == "N/A"):

                    print("    -> GT-SM não forneceu todos os detalhes. Tentando extrair detalhes do bloco lt-md (layout mobile)...")
                    
                    # LT-MD has a different structure for Year, Fuel, KM (direct p.text-categoria)
                    temp_ano = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center.q-pb-sm div.col-4:nth-child(1) p.text-categoria")
                    if temp_ano != "N/A":
                        if "/" in temp_ano:
                            ano_veiculo = temp_ano.split('/')[0].strip()
                        else:
                            ano_veiculo = temp_ano.strip()

                    temp_combustivel = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center.q-pb-sm div.col-4:nth-child(2) p.text-categoria")
                    if temp_combustivel != "N/A":
                        combustivel = temp_combustivel
                    
                    temp_km = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-sm.text-center.q-pb-sm div.col-4:nth-child(3) p.text-categoria")
                    if temp_km != "N/A":
                        km_veiculo = temp_km

                    # --- Ajustes para LT-MD com base no HTML fornecido ---
                    # Valor Mercado (FIPE)
                    if valor_mercado_fipe == "N/A": # Só tenta novamente se ainda for N/A
                        temp_valor_mercado = get_detail_lt_md_direct(driver_instance, "div.lt-md div.btn-rounded-custom > div.row.q-col-gutter-sm.text-center:nth-of-type(2) div.col-4:nth-child(1) p.text-categoria")
                        if temp_valor_mercado != "N/A":
                            valor_mercado_fipe = temp_valor_mercado.replace('R$', '').replace('.', '').replace(',', '.').strip()

                    # Cor
                    if cor_veiculo == "N/A":
                        temp_cor = get_detail_lt_md_direct(driver_instance, "div.lt-md div.btn-rounded-custom > div.row.q-col-gutter-sm.text-center:nth-of-type(2) div.col-4:nth-child(2) p.text-categoria")
                        if temp_cor != "N/A":
                            cor_veiculo = temp_cor

                    # Possui Chave
                    if veiculo_possui_chave == "N/A":
                        temp_chave = get_detail_lt_md_direct(driver_instance, "div.lt-md div.btn-rounded-custom > div.row.q-col-gutter-sm.text-center:nth-of-type(2) div.col-4:nth-child(3) p.text-categoria")
                        if temp_chave != "N/A":
                            veiculo_possui_chave = temp_chave

                    # Tipo Retomada
                    if tipo_retomada == "N/A":
                        # O seletor original para tipo_retomada no lt-md estava correto
                        tipo_retomada = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(1) a.text-categoria > span")
                    
                    # Localização (mantido o seletor original, pois parece estar correto)
                    if localizacao_detalhe == "N/A":
                        temp_location_ltmd = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(2) a.text-categoria > span > p")
                        if temp_location_ltmd != "N/A":
                            localizacao_detalhe = temp_location_ltmd
                        else: # Fallback if there's no <p> inside <span>
                            localizacao_detalhe = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(2) a.text-categoria > span")
                    
                    # Tipo Veículo (mantido o seletor original, pois parece estar correto)
                    if tipo_veiculo == "N/A":
                        tipo_veiculo = get_detail_lt_md_direct(driver_instance, "div.lt-md div.row.q-col-gutter-md div.col-md-4:nth-child(3) a.text-categoria > span")
                    
                    # Fabricante e Modelo não serão extraídos aqui, pois já vêm do título

                    # **NOVO**: Logs de debug para LT-MD FALLBACK
                    print(f"        DEBUG LT-MD FALLBACK - Ano: '{ano_veiculo}'")
                    print(f"        DEBUG LT-MD FALLBACK - Combustivel: '{combustivel}'")
                    print(f"        DEBUG LT-MD FALLBACK - KM: '{km_veiculo}'")
                    print(f"        DEBUG LT-MD FALLBACK - Valor Mercado: '{valor_mercado_fipe}'")
                    print(f"        DEBUG LT-MD FALLBACK - Cor: '{cor_veiculo}'")
                    print(f"        DEBUG LT-MD FALLBACK - Possui Chave: '{veiculo_possui_chave}'")
                    print(f"        DEBUG LT-MD FALLBACK - Tipo Retomada: '{tipo_retomada}'")
                    print(f"        DEBUG LT-MD FALLBACK - Localização: '{localizacao_detalhe}'")
                    print(f"        DEBUG LT-MD FALLBACK - Tipo: '{tipo_veiculo}'")


                print(f"    ✔️ Detalhes extraídos para {link}.")
                print(f"    -> Ano Fabricação: {ano_veiculo}")
                print(f"    -> Combustível: {combustivel}")
                print(f"    -> KM Detalhe: {km_veiculo}")
                print(f"    -> Valor Mercado (FIPE): {valor_mercado_fipe}")
                print(f"    -> Cor: {cor_veiculo}")
                print(f"    -> Possui Chave: {veiculo_possui_chave}")
                print(f"    -> Tipo Retomada: {tipo_retomada}")
                print(f"    -> Localização Detalhe: {localizacao_detalhe}")
                print(f"    -> Tipo Veículo Detalhe: {tipo_veiculo}")
                print(f"    -> Fabricante: {fabricante_veiculo}") # Valor do título
                print(f"    -> Modelo: {modelo_veiculo}") # Valor do título
                print(f"    -> Versão: {veiculo_versao}") # Novo campo

            except TimeoutException:
                print(f"    ⚠️ Timeout ao carregar detalhes do lote em {link}. Informações adicionais podem estar incompletas ou a página não carregou corretamente.")
            except Exception as e:
                print(f"    ❌ ERRO geral ao extrair detalhes da página {link}: {e}")
            
            finally:
                # Fecha a aba do lote e volta para a aba principal
                driver_instance.close()
                driver_instance.switch_to.window(main_window_handle)
                print(f"    ⬅️ Voltando para a página principal.")
        else:
            print(f"    ❗ Link não disponível para o lote {index + 1}. Pulando extração de detalhes.")
        
        # Atualiza o dicionário correspondente na lista `dados`
        if index < len(lot_data_list):
            lot_data_list[index].update({
                "veiculo_ano_fabricacao": ano_veiculo, 
                "veiculo_tipo_combustivel": combustivel,
                "veiculo_km": km_veiculo, 
                "veiculo_valor_fipe": valor_mercado_fipe, 
                "veiculo_cor": cor_veiculo,
                "veiculo_possui_chave": veiculo_possui_chave,
                "veiculo_tipo_retomada": tipo_retomada,
                "veiculo_patio_uf": localizacao_detalhe, # Atualizado para receber localizacao_detalhe
                "veiculo_tipo": tipo_veiculo,
                "veiculo_fabricante": fabricante_veiculo, 
                "veiculo_modelo": modelo_veiculo,
                "veiculo_versao": veiculo_versao # Adicionado o novo campo
            })
        else:
            print(f"[ERROR] Índice {index} fora dos limites para atualizar dados.")

# Configura opções do Chrome
options = Options()
# options.add_argument("--headless")    # Descomente para rodar sem interface gráfica (modo headless)
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
MAX_TRIES = 2 # Número máximo de tentativas de conexão

for attempt in range(MAX_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_TRIES})...")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        print("[INFO] Conectado ao Selenium!")
        break
    except WebDriverException as e:
        print(f"[WARN] Selenium ainda não está pronto: {e}. Tentando novamente em 2 segundos...")
        time.sleep(1)
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
            WebDriverWait(driver, 20).until(
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
            # Inicializa todas as variáveis que serão usadas no dicionário 'dados'
            # para garantir que estejam sempre definidas, mesmo em caso de falha na extração.
            titulo = "N/A"
            link = "N/A"
            imagem = "N/A"
            uf = "N/A" # Será sobrescrito por localizacao_detalhe se encontrado
            ano = "N/A"
            km = "N/A"
            valor_lance_atual = "N/A" 
            situacao = "N/A"
            data_leilao_str = "N/A"
            
            # Variáveis para detalhes que serão preenchidos depois, mas inicializadas aqui para consistência
            veiculo_tipo_combustivel = "N/A"
            veiculo_cor = "N/A"
            veiculo_possui_chave = "N/A"
            veiculo_tipo_retomada = "N/A"
            veiculo_tipo = "N/A"
            veiculo_valor_fipe = "N/A"
            veiculo_modelo = "N/A"
            veiculo_fabricante = "N/A"
            veiculo_versao = "N/A" # Inicializa a nova variável aqui também
            localizacao_detalhe = "N/A" # Inicializa a variável de detalhe de localização

            try:
                # Re-locate the lot element to avoid StaleElementReferenceException for the lot cards
                # This is important if the page reloads or elements shift during pagination
                lote_element_on_page = WebDriverWait(driver, 10).until(EC.visibility_of(lotes[i-1]))
                
                print(f"🔍 Extraindo dados básicos do Lote {i} na Página {current_page}:")

                titulo = safe_get_element_text(lote_element_on_page, "div.header-card h3")
                
                # --- Extração de Fabricante e Modelo do Título ---
                if titulo != "N/A" and "/" in titulo:
                    parts = titulo.split('/', 1) # Split only on the first '/'
                    veiculo_fabricante = parts[0].strip()
                    veiculo_modelo = parts[1].strip()
                else:
                    veiculo_fabricante = titulo # If no '/', the whole title is the manufacturer
                    veiculo_modelo = "N/A" # No specific model found
                # --- Fim da Extração de Fabricante e Modelo ---

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
                
                # Extração da UF (inicial, será sobrescrita por localização detalhada)
                uf = safe_get_element_text(lote_element_on_page, "div.codigo-anuncio span")

                ano_raw = safe_get_element_text(lote_element_on_page, "p.text-ano")
                ano = "N/A"
                if ano_raw != "N/A":
                    match_ano = re.search(r'\d{4}', ano_raw)
                    if match_ano:
                        ano = match_ano.group(0)
                    else: 
                        match_ano = re.search(r'\d{2}', ano_raw)
                        if match_ano:
                            ano = "20" + match_ano.group(0)
                
                km = safe_get_element_text(lote_element_on_page, "p.text-km") 
                valor_lance_atual_raw = safe_get_element_text(lote_element_on_page, "li.valor-atual")
                if valor_lance_atual_raw != "N/A":
                    valor_lance_atual = valor_lance_atual_raw.replace('R$', '').replace('.', '').replace(',', '.').strip()


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

                # --- Campos mapeados para minúsculas com underscores ---
                dados.append({
                    "veiculo_titulo": titulo,
                    "veiculo_link_lote": link,
                    "veiculo_imagem": imagem,
                    "veiculo_patio_uf": uf, # Este será atualizado com localizacao_detalhe
                    "veiculo_ano_fabricacao": ano,
                    "veiculo_km": km,
                    "veiculo_valor_lance_atual": valor_lance_atual, 
                    "veiculo_situacao": situacao,
                    "veiculo_data_leilao": data_leilao_str,
                    # Adicionar placeholders para os campos de detalhe que serão preenchidos depois
                    "veiculo_tipo_combustivel": veiculo_tipo_combustivel, 
                    "veiculo_cor": veiculo_cor, 
                    "veiculo_possui_chave": veiculo_possui_chave, 
                    "veiculo_tipo_retomada": veiculo_tipo_retomada, 
                    "veiculo_tipo": veiculo_tipo, 
                    "veiculo_valor_fipe": veiculo_valor_fipe, 
                    "veiculo_modelo": veiculo_modelo, 
                    "veiculo_fabricante": veiculo_fabricante,
                    "veiculo_versao": veiculo_versao # Adicionado o novo campo
                })
            except StaleElementReferenceException:
                print(f"[WARN] StaleElementReferenceException no Lote {i} da Página {current_page}. Re-localizando lotes e tentando novamente esta página.")
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

    # --- CHAMADA PARA A FUNÇÃO DE EXTRAÇÃO DE DETALHES ---
    print("\n--- INICIANDO EXTRAÇÃO DE DETALHES DE CADA LOTE ---")
    extract_lot_details(driver, dados)


finally:
    # --- Conexão e Inserção no PostgreSQL (para a tabela 'leilo') ---
    if dados:
        conn = None
        db_connection_retries = 5 
        db_retry_delay = 5 

        for i in range(db_connection_retries):
            print(f"[INFO] Tentando conectar ao banco de dados para salvar resultados ({i+1}/{db_connection_retries})...")
            conn = connect_db()
            if conn:
                print("[INFO] Conexão com o banco de dados estabelecida para salvar dados.")
                break
            print(f"[WARN] Falha na conexão com o banco de dados. Tentando novamente em {db_retry_delay}s...")
            time.sleep(db_retry_delay)

        if conn:
            print("[INFO] Criando ou verificando a tabela 'Leilo'...")
            create_leilo_table(conn) 
            print("[INFO] Iniciando inserção de dados na tabela 'Leilo'...")
            for lote_data in dados:
                # Adicionado: Imprime as colunas que estão sendo enviadas para o banco de dados
                print(f"[INFO] Colunas enviadas para o DB para este lote: {list(lote_data.keys())}")
                try:
                    insert_data_leilo(conn, lote_data) # Passando lote_data diretamente
                except Exception as e:
                    print(f"[ERRO] Erro ao inserir registro no banco: {lote_data.get('veiculo_titulo', 'N/A')[:50]}... Erro: {e}")
            print(f"[INFO] {len(dados)} registros processados para inserção na tabela 'Leilo'.")
            
            try:
                conn.close()
                print("[INFO] Conexão com o banco de dados fechada.")
            except Exception as e:
                print(f"[ERRO] Erro ao fechar conexão com o banco de dados: {e}")
        else:
            print("[ERRO] Não foi possível estabelecer conexão com o banco de dados. Os dados não serão salvos no DB.")
            
    # --- Geração e diagnóstico do arquivo CSV (mantido) ---
    if dados:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = f"leilao_leilo_data_{timestamp}.csv"
        output_dir = "/app/etl"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_file_name)

        # Define os cabeçalhos do CSV estritamente com base na coluna "DE"
        # As chaves aqui devem ser as mesmas usadas no dicionário 'dados'
        csv_fieldnames = [
            "veiculo_titulo",
            "veiculo_link_lote",
            "veiculo_imagem",
            "veiculo_patio_uf", # Agora representa a localização detalhada
            "veiculo_ano_fabricacao",
            "veiculo_km",
            "veiculo_valor_lance_atual",
            "veiculo_situacao",
            "veiculo_data_leilao",
            "veiculo_tipo_combustivel",
            "veiculo_cor",
            "veiculo_possui_chave",
            "veiculo_tipo_retomada",
            "veiculo_tipo",
            "veiculo_valor_fipe",
            "veiculo_fabricante",
            "veiculo_modelo",
            "veiculo_versao", # Adicionado o novo campo
        ]

        # O dicionário 'dados' já está formatado com as chaves "DE", então
        # podemos usá-lo diretamente para escrever no CSV.
        try:
            with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
                writer.writeheader()
                writer.writerows(dados)
            print(f"\n[INFO] Dados salvos com sucesso em CSV: {output_path}")
        except IOError as e:
            print(f"\n[ERRO] Não foi possível salvar o arquivo CSV em {output_path}.")
            print(f"Causa do erro: {e}")
            print("Isso pode ser devido a permissões de escrita ou o caminho do arquivo não ser acessível.")
        except Exception as e:
            print(f"\n[ERRO] Ocorreu um erro inesperado ao tentar salvar o CSV: {e}")
    else:
        print("\n[AVISO] Nenhum lote foi processado. O arquivo CSV não será gerado.")

    if driver:
        print("[INFO] Fechando navegador.")
        driver.quit()
    print("[INFO] Raspagem concluída.")
    print("✅ Extração de dados do Leilão concluída com sucesso!")
