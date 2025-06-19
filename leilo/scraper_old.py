import csv
import time
import re
import os
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def safe_get_element_text(element, by_method, selector):
    """
    Tenta obter o texto de um elemento da web.
    Retorna "N/A" se o elemento não for encontrado ou se tornar obsoleto.
    """
    try:
        # Tenta encontrar o elemento dentro do 'element' pai
        found_element = element.find_element(by_method, selector)
        return found_element.text.strip()
    except (NoSuchElementException, StaleElementReferenceException):
        return "N/A"

def safe_get_element_attribute(element, by_method, selector, attribute):
    """
    Tenta obter o valor de um atributo (ex: 'href', 'style') de um elemento da web.
    Retorna "N/A" se o elemento não for encontrado, se tornar obsoleto ou se o atributo não existir.
    """
    try:
        # Tenta encontrar o elemento dentro do 'element' pai
        found_element = element.find_element(by_method, selector)
        return found_element.get_attribute(attribute)
    except (NoSuchElementException, StaleElementReferenceException):
        return "N/A"

# --- Configuração do Driver Selenium ---
options = Options()
# options.add_argument("--headless") # DESCOMENTE PARA VER O NAVEGADOR DURANTE O DESENVOLVIMENTO
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080") # Define um tamanho de janela consistente
options.add_argument("--start-maximized") # Maximiza a janela para evitar problemas de layout
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") # Adiciona user-agent para simular navegador real

SELENIUM_URL = "http://selenium:4444/wd/hub"
driver = None
MAX_CONNECTION_TRIES = 10

for attempt in range(MAX_CONNECTION_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_CONNECTION_TRIES})...")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        print("[INFO] Conectado ao Selenium com sucesso!")
        break
    except WebDriverException as e:
        print(f"[WARN] Selenium ainda não está pronto: {e}. Tentando novamente em 2 segundos...")
        time.sleep(2)
else:
    raise Exception("❌ Não foi possível conectar ao Selenium após várias tentativas. Encerrando.")

# --- Lógica Principal de Raspagem ---
dados = []
url_main_page = "https://leilo.com.br/leilao/carros?" # Mantive essa URL mais específica

try:
    driver.get(url_main_page)
    print(f"[INFO] Página carregada: {driver.title}")

    # --- Esperar por elementos da página principal (os cards dos lotes) ---
    # É crucial que este seletor esteja correto e que os elementos estejam visíveis/presentes
    try:
        # Aumenta o tempo de espera e usa 'visibility_of_all_elements_located'
        WebDriverWait(driver, 30).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
        )
        print("[INFO] Elementos da página principal (.sessao.cursor-pointer) visíveis e carregados.")
    except TimeoutException:
        print("❌ ERRO CRÍTICO: Os elementos dos lotes (.sessao.cursor-pointer) NÃO foram encontrados na página principal dentro do tempo limite.")
        print("   Isso pode indicar que o seletor CSS está incorreto para a página carregada ou que há um pop-up/carregador bloqueando.")
        driver.save_screenshot("erro_lotes_nao_encontrados.png") # Salva um print para depuração
        raise Exception("Falha ao carregar os lotes da página principal. Encerrando.") # Encerra o script pois não há dados para raspar

    # Coleta a lista inicial de links e títulos dos lotes
    lot_infos = []
    try:
        # Espera que os links específicos dentro dos cards estejam presentes e visíveis
        WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer a.img-card"))
        )
        temp_lot_elements = driver.find_elements(By.CSS_SELECTOR, ".sessao.cursor-pointer a.img-card")

        for element in temp_lot_elements:
            try:
                # Tenta obter o título do atributo 'title' do link, ou de um h6 dentro do card
                lot_title = element.get_attribute("title")
                if not lot_title or lot_title == "N/A":
                    lot_title = safe_get_element_text(element.find_element(By.XPATH, ".."), By.CSS_SELECTOR, "p.text-h6")
                    if not lot_title or lot_title == "N/A":
                        lot_title = "Título Não Encontrado"

                lot_href = element.get_attribute("href")
                if lot_href and not lot_href.startswith("http"):
                    lot_href = "https://leilo.com.br" + lot_href

                lot_infos.append({"title": lot_title, "link": lot_href})
            except StaleElementReferenceException:
                print("⚠️ Aviso: StaleElementReferenceException ao coletar links iniciais dos lotes. Ignorando elemento obsoleto.")
                continue
            except Exception as inner_e:
                print(f"❌ Erro ao processar um elemento de lote individual: {inner_e}")
                continue

    except TimeoutException:
        print("❌ ERRO: Nenhum link de lote (a.img-card) encontrado nos cards da página principal. Verifique o seletor.")
    except Exception as e:
        print(f"❌ Erro ao coletar links de lotes iniciais: {e}")

    print(f"[INFO] Encontrados {len(lot_infos)} lotes para processar.")

    MAX_LOT_RETRIES = 3

    for i, lot_data in enumerate(lot_infos):
        title = lot_data["title"]
        link = lot_data["link"]

        image_url = "N/A"
        uf = "N/A"
        year = "N/A"
        km = "N/A"
        bid_value = "N/A"
        status = "N/A"
        auction_date_str = "N/A"
        market_value = "N/A"
        detail_location = "N/A"

        print(f"\n🔍 Processando Lote {i+1} - Título: {title}")
        print(f"   - Link: {link}")

        current_retry = 0
        while current_retry < MAX_LOT_RETRIES:
            try:
                driver.get(link)
                print(f"   - Navegando para a página de detalhes de '{title}'...")

                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(., 'Valor Mercado')]"))
                )
                time.sleep(1.5)

                market_value = safe_get_element_text(driver, By.XPATH, "//div[contains(@class, 'q-mb-none') and .//i[contains(text(), 'attach_money')]]//a/span")
                if market_value == "N/A":
                    print("   - [AVISO] Elemento 'Valor Mercado' não encontrado na página de detalhes.")

                detail_location = safe_get_element_text(driver, By.XPATH, "//div[contains(@class, 'q-mb-none') and .//i[contains(text(), 'place')]]//a//p")
                if detail_location == "N/A":
                    print("   - [AVISO] Elemento 'Localização Detalhe' não encontrado na página de detalhes.")

                year = safe_get_element_text(driver, By.XPATH, "//span[contains(text(), 'Ano:')]/following-sibling::span")
                if year == "N/A":
                    print("   - [AVISO] Elemento 'Ano' não encontrado na página de detalhes.")

                km = safe_get_element_text(driver, By.XPATH, "//span[contains(text(), 'Km:')]/following-sibling::span")
                if km == "N/A":
                    print("   - [AVISO] Elemento 'KM' não encontrado na página de detalhes.")

                uf_match = re.search(r'([A-Z]{2})$', detail_location)
                if uf_match:
                    uf = uf_match.group(1)
                else:
                    uf = safe_get_element_text(driver, By.XPATH, "//span[contains(text(), 'Estado:')]/following-sibling::span")

                image_element = safe_get_element_attribute(driver, By.CSS_SELECTOR, "img.q-img__image", "src")
                if image_element != "N/A":
                    image_url = image_element
                else:
                    print("   - [AVISO] Elemento de Imagem não encontrado na página de detalhes.")

                dados.append({
                    "Título": title,
                    "Link": link,
                    "Imagem": image_url,
                    "UF": uf,
                    "Ano": year,
                    "KM": km,
                    "Valor do Lance": bid_value,
                    "Situação": status,
                    "Data Leilão": auction_date_str,
                    "Valor Mercado": market_value,
                    "Localização Detalhe": detail_location
                })
                print(f"   - Valor Mercado (Detalhe): {market_value}")
                print(f"   - Localização (Detalhe): {detail_location}")
                print(f"   - Ano: {year}")
                print(f"   - KM: {km}")
                print(f"   - UF: {uf}")
                print(f"   - Imagem: {image_url}")

                break

            except TimeoutException:
                print(f"   - [ERRO] Tempo esgotado esperando elementos na página de detalhes para '{title}'. Tentando novamente... ({current_retry + 1}/{MAX_LOT_RETRIES})")
                current_retry += 1
                time.sleep(3)
            except Exception as e:
                print(f"   - [ERRO] Ocorreu um erro inesperado para o lote '{title}': {e}. Tentando novamente... ({current_retry + 1}/{MAX_LOT_RETRIES})")
                current_retry += 1
                if current_retry == MAX_LOT_RETRIES:
                    dados.append({
                        "Título": title, "Link": link, "Imagem": image_url, "UF": uf, "Ano": year,
                        "KM": "ERRO", "Valor do Lance": bid_value, "Situação": status, "Data Leilão": auction_date_str,
                        "Valor Mercado": f"ERRO: {e}", "Localização Detalhe": f"ERRO: {e}"
                    })
                time.sleep(3)

        else:
            print(f"   - [ERRO CRÍTICO] Falha ao processar o lote '{title}' após {MAX_LOT_RETRIES} tentativas. Pulando.")
            if not any(d["Link"] == link and d.get("KM") == "FALHA_TOTAL" for d in dados):
                dados.append({
                    "Título": title, "Link": link, "Imagem": image_url, "UF": uf, "Ano": year,
                    "KM": "FALHA_TOTAL", "Valor do Lance": bid_value, "Situação": status, "Data Leilão": auction_date_str,
                    "Valor Mercado": "FALHA_TOTAL", "Localização Detalhe": "FALHA_TOTAL"
                })

finally:
    if dados:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = f"leilao_data_{timestamp}.csv"
        output_dir = "scraped_data"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_file_name)

        try:
            all_keys = sorted(list(set(key for d in dados for key in d.keys())))

            with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=all_keys)
                writer.writeheader()
                writer.writerows(dados)
            print(f"\n[INFO] Dados salvos com sucesso em {output_path}")
        except IOError as e:
            print(f"\n[ERRO] Não foi possível salvar o arquivo CSV em {output_path}.")
            print(f"Causa: {e}")
            print("Isso pode ser devido a permissões de escrita ou o caminho do arquivo não ser acessível.")
        except Exception as e:
            print(f"\n[ERRO] Ocorreu um erro inesperado ao tentar salvar o CSV: {e}")
    else:
        print("\n[AVISO] Nenhum lote foi processado. O arquivo CSV não será gerado.")

    if driver:
        print("[INFO] Fechando navegador.")
        driver.quit()
    print("[INFO] Raspagem concluída.")