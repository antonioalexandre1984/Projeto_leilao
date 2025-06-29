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
    Inclui tratamento para StaleElementReferenceException.
    """
    try:
        # Tenta encontrar o elemento imediatamente
        found_element = element.find_element(By.CSS_SELECTOR, css_selector)
        text = found_element.text.strip()
        cleaned_text = text.replace('\xa0', ' ').strip()
        return cleaned_text if cleaned_text else "N/A"
    except NoSuchElementException:
        return "N/A"
    except StaleElementReferenceException:
        # Tenta re-encontrar o elemento se ele ficou "stale"
        try:
            # Reduzindo o tempo de espera para 0.5s para re-tentativa rápida
            re_found_element = WebDriverWait(element, 0.5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            text = re_found_element.text.strip()
            cleaned_text = text.replace('\xa0', ' ').strip()
            return cleaned_text if cleaned_text else "N/A"
        except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
            return "N/A"
        except Exception: # Captura qualquer outra exceção e retorna N/A
            return "N/A"
    except Exception: # Captura qualquer outra exceção e retorna N/A
        return "N/A"

def safe_get_element_attribute(element, css_selector, attribute):
    """
    Tenta obter um atributo de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o atributo não existir.
    Inclui tratamento para StaleElementReferenceException.
    """
    try:
        # Tenta encontrar o elemento imediatamente
        found_element = element.find_element(By.CSS_SELECTOR, css_selector)
        attr_value = found_element.get_attribute(attribute)
        return attr_value.strip() if attr_value else "N/A"
    except NoSuchElementException:
        return "N/A"
    except StaleElementReferenceException:
        # Tenta re-encontrar o elemento se ele ficou "stale"
        try:
            # Reduzindo o tempo de espera para 0.5s para re-tentativa rápida
            re_found_element = WebDriverWait(element, 0.5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            attr_value = re_found_element.get_attribute(attribute)
            return attr_value.strip() if attr_value else "N/A"
        except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
            return "N/A"
        except Exception: # Captura qualquer outra exceção e retorna N/A
            return "N/A"
    except Exception: # Captura qualquer outra exceção e retorna N/A
        return "N/A"

def list_all_html_elements(driver_instance, lot_url):
    """
    Navega para a URL do lote e lista todos os elementos HTML encontrados,
    incluindo seus atributos.
    """
    print(f"\n--- Acessando o lote em: {lot_url} para listar todos os elementos HTML ---")
    try:
        driver_instance.get(lot_url)
        # Espera pelo carregamento do corpo da página ou de um elemento significativo
        WebDriverWait(driver_instance, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        print(f"Página do lote '{driver_instance.title}' carregada.")

        all_elements = driver_instance.find_elements(By.XPATH, "//*")
        print(f"\n--- {len(all_elements)} elementos HTML encontrados na página do lote: ---")

        for i, element in enumerate(all_elements):
            try:
                tag_name = element.tag_name
                attributes = driver_instance.execute_script(
                    'var items = {}; for (index = 0; index < arguments[0].attributes.length; ++index) { items[arguments[0].attributes[index].name] = arguments[0].attributes[index].value }; return items;',
                    element
                )
                print(f"[{i+1}] Tag: <{tag_name}>")
                if attributes:
                    for attr_name, attr_value in attributes.items():
                        print(f"    Atributo: {attr_name} = '{attr_value}'")
                # Opcional: imprimir o texto do elemento se não for muito longo
                # text_content = element.text.strip()
                # if text_content and len(text_content) < 100:
                #     print(f"    Texto: {text_content}")
            except StaleElementReferenceException:
                print(f"[{i+1}] Elemento se tornou obsoleto, pulando.")
                continue
            except Exception as e:
                print(f"[{i+1}] Erro ao processar elemento: {e}")
                continue

    except TimeoutException:
        print(f"⚠️ Timeout ao carregar a página do lote em {lot_url}.")
    except Exception as e:
        print(f"❌ ERRO ao acessar ou processar a página do lote {lot_url}: {e}")

# --- Configuração e Inicialização do Selenium ---
print("[INFO] Iniciando a configuração do Selenium...")
options = Options()
#options.add_argument("--headless")   # Rodar em modo headless para mais velocidade e menor uso de recursos
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_argument("--start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

# Adicionando opções para otimizar o desempenho do navegador
#options.add_argument("--blink-settings=imagesEnabled=false") # Desabilita o carregamento de imagens
#options.add_argument("--disable-extensions") # Desabilita extensões
#options.add_argument("--dns-prefetch-disable") # Desabilita pré-busca DNS
#options.add_argument("--disable-browser-side-navigation") # Pode acelerar a navegação
#options.add_argument("--disable-features=NetworkService") # Desabilita o serviço de rede (cuidado com isso, pode quebrar sites complexos)
#options.add_argument("--disable-background-networking") # Desabilita atividades de rede em segundo plano

# --- CONEXÃO COM O SELENIUM ---
SELENIUM_URL = "http://selenium:4444/wd/hub"

driver = None
MAX_TRIES = 10

for attempt in range(MAX_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_TRIES})...")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        driver.implicitly_wait(1) # Tempo de espera implícita baixo
        print("[INFO] Conectado ao Selenium com sucesso!")
        break
    except WebDriverException as e:
        print(f"[WARN] Selenium ainda não está pronto: {e}. Tentando novamente em 2 segundos...")
        time.sleep(2)
else:
    raise Exception("❌ Não foi possível conectar ao Selenium após várias tentativas.")

try:
    url = "https://leilo.com.br/leilao/carros?" # URL da página de leilões
    driver.get(url)
    print(f"[INFO] Página carregada: {driver.title}")

    # --- Lógica para fechar pop-up de cookies ou outros banners (CRÍTICO) ---
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

    # --- Encontra o primeiro lote e acessa seu link ---
    print("\n--- Localizando o primeiro lote para análise detalhada ---")
    try:
        # Espera pelos elementos dos lotes (cards) estarem presentes na página
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
        )
        lotes = driver.find_elements(By.CSS_SELECTOR, ".sessao.cursor-pointer")

        if lotes:
            first_lote_element = lotes[0]
            link_first_lot = safe_get_element_attribute(first_lote_element, "a.img-card", "href")
            
            if link_first_lot == "N/A":
                link_first_lot = safe_get_element_attribute(first_lote_element, "div.header-card a", "href")
            
            if link_first_lot and not link_first_lot.startswith("http"):
                link_first_lot = "https://leilo.com.br" + link_first_lot

            if link_first_lot != "N/A" and link_first_lot:
                print(f"[INFO] Link do primeiro lote encontrado: {link_first_lot}")
                list_all_html_elements(driver, link_first_lot)
            else:
                print("[WARN] Não foi possível encontrar um link válido para o primeiro lote.")
        else:
            print("[INFO] Nenhum lote encontrado na página inicial para análise.")

    except TimeoutException:
        print("[WARN] Timeout ao carregar lotes na página inicial. Pode não haver lotes ou carregamento lento.")
    except Exception as e:
        print(f"❌ ERRO geral ao processar o primeiro lote: {e}")

finally:
    if driver:
        print("[INFO] Fechando o navegador Selenium.")
        driver.quit()
    print("[INFO] Processo concluído.")