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

# Aumentar o tempo de espera implícita para dar mais tempo ao navegador,
# mas ainda permitindo waits explícitos para elementos críticos.
# Embora waits explícitos sejam preferíveis, um tempo implícito pode ajudar
# em situações onde elementos podem aparecer com um pequeno atraso.
# IMPORTANTE: Use com moderação, pois pode adicionar atrasos se elementos não existirem.
# driver.implicitly_wait(5) # Pode ser definido após a inicialização do driver

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

def extract_lot_details(driver_instance, lot_data_list):
    """
    Navega para o link de cada lote na 'lot_data_list' e extrai informações detalhadas da página do lote.
    Atualiza os dicionários existentes na 'lot_data_list' com os novos dados detalhados.
    Também registra quais elementos foram encontrados ou falharam.
    """
    main_window_handle = driver_instance.current_window_handle
    
    def get_detail_by_label_internal(driver_obj, label_text, parent_selector=""):
        try:
            xpaths_to_try = [
                f"{parent_selector}//span[contains(@class, 'label-categoria') and normalize-space(text())='{label_text}']/following-sibling::p",
                f"{parent_selector}//span[contains(@class, 'label-categoria') and normalize-space(text())='{label_text}']/following-sibling::span",
                f"{parent_selector}//span[contains(@class, 'label-categoria') and normalize-space(text())='{label_text}']/following-sibling::a/*[self::span or self::p]",
                f"{parent_selector}//span[contains(@class, 'label-categoria') and normalize-space(text())='{label_text}']/following-sibling::a",
                f"{parent_selector}//div[contains(@class, 'col-4')]//span[normalize-space(text())='{label_text}']/parent::div/following-sibling::div//p",
                f"{parent_selector}//div[contains(@class, 'col-4')]//span[normalize-space(text())='{label_text}']/following-sibling::p"
            ]
            
            for xpath_attempt in xpaths_to_try:
                try:
                    element = driver_obj.find_element(By.XPATH, xpath_attempt)
                    text = element.text.strip().replace('\xa0', ' ')
                    if text:
                        return text, True
                except NoSuchElementException:
                    continue
            return "N/A", False
        except StaleElementReferenceException:
            # print(f"  ❗ StaleElementReferenceException ao buscar detalhe '{label_text}'. Ignorando.")
            return "N/A", False
        except Exception:
            # print(f"  ❗ Erro inesperado ao buscar detalhe '{label_text}': {e}")
            return "N/A", False
            
    def get_direct_detail_internal(driver_obj, css_selectors_list):
        for css_selector in css_selectors_list:
            try:
                element = driver_obj.find_element(By.CSS_SELECTOR, css_selector)
                text = element.text.strip().replace('\xa0', ' ')
                if text:
                    return text, True
            except NoSuchElementException:
                continue
            except StaleElementReferenceException:
                try:
                    # Reduzindo o tempo de espera para 0.5s para re-tentativa rápida
                    re_found_element = WebDriverWait(driver_obj, 0.5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                    )
                    text = re_found_element.text.strip().replace('\xa0', ' ')
                    if text:
                        return text, True
                except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                    continue
                except Exception:
                    continue
            except Exception:
                continue
        return "N/A", False

    for index, item in enumerate(lot_data_list):
        link = item['Link']
        if link != "N/A" and link:
            print(f"\n🔄 Processando detalhes para o lote {index + 1} (Link: {link})...")
            
            # Inicializa com N/A e False para cada campo
            details = {
                "Detalhe Ano Veículo": "N/A", "found_Ano_Veiculo": False,
                "Detalhe Combustível": "N/A", "found_Combustivel": False,
                "Detalhe KM Veículo": "N/A", "found_KM_Veiculo": False,
                "Detalhe Valor Mercado": "N/A", "found_Valor_Mercado": False,
                "Detalhe Cor": "N/A", "found_Cor": False,
                "Detalhe Possui Chave": "N/A", "found_Possui_Chave": False,
                "Detalhe Tipo Retomada": "N/A", "found_Tipo_Retomada": False,
                "Detalhe Localização": "N/A", "found_Localizacao": False,
                "Detalhe Tipo Veículo": "N/A", "found_Tipo_Veiculo": False,
            }

            try:
                # Abrir link na mesma aba, se possível, para evitar overhead de múltiplas abas
                # Ou usar driver.execute_script("window.location.href = arguments[0]", link)
                # O ideal aqui é navegar diretamente, não abrir nova aba, a menos que seja um requisito
                driver_instance.get(link) # Altera para navegar na mesma aba
                
                # Aguarda um elemento chave da página de detalhes
                detail_container = WebDriverWait(driver_instance, 15).until( # Reduzindo o tempo de espera
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.categorias-veiculo, div.q-card__section--vert.q-card__section.q-pa-md.text-detail-veiculo-container"))
                )
                # Não é necessário scrollIntoView se os elementos estiverem no DOM e você estiver buscando por eles
                # driver_instance.execute_script("arguments[0].scrollIntoView(true);", detail_container)
                # time.sleep(0.5) # Pausas mínimas ou zero, dependendo da necessidade real de renderização

                print("  -> Tentando extrair detalhes do bloco gt-sm (layout desktop)...")
                
                # Extração otimizada com atribuição direta e verificação de found
                details["Detalhe Ano Veículo"], details["found_Ano_Veiculo"] = get_direct_detail_internal(driver_instance, [
                    "div.gt-sm div.row.q-pb-sm div.col-4:nth-child(1) p.text-categoria",
                    "div.gt-sm div.q-card__section--vert.q-card__section.q-pa-md div.row div.col-4:nth-child(1) p.text-categoria"
                ])
                if details["found_Ano_Veiculo"]:
                    match_ano = re.search(r'\d{4}', details["Detalhe Ano Veículo"])
                    if match_ano:
                        details["Detalhe Ano Veículo"] = match_ano.group(0)
                    elif "/" in details["Detalhe Ano Veículo"]:
                        details["Detalhe Ano Veículo"] = details["Detalhe Ano Veículo"].split('/')[0].strip()

                details["Detalhe Combustível"], details["found_Combustivel"] = get_direct_detail_internal(driver_instance, [
                    "div.gt-sm div.row.q-pb-sm div.col-4:nth-child(2) p.text-categoria",
                    "div.gt-sm div.q-card__section--vert.q-card__section.q-pa-md div.row div.col-4:nth-child(2) p.text-categoria"
                ])
                details["Detalhe KM Veículo"], details["found_KM_Veiculo"] = get_direct_detail_internal(driver_instance, [
                    "div.gt-sm div.row.q-pb-sm div.col-4:nth-child(3) p.text-categoria",
                    "div.gt-sm div.q-card__section--vert.q-card__section.q-pa-md div.row div.col-4:nth-child(3) p.text-categoria"
                ])

                details["Detalhe Valor Mercado"], details["found_Valor_Mercado"] = get_detail_by_label_internal(driver_instance, "Valor Mercado", parent_selector="//div[contains(@class, 'gt-sm')]")
                if details["found_Valor_Mercado"]:
                    details["Detalhe Valor Mercado"] = details["Detalhe Valor Mercado"].replace('R$', '').replace('.', '').replace(',', '.').strip()

                details["Detalhe Cor"], details["found_Cor"] = get_detail_by_label_internal(driver_instance, "Cor", parent_selector="//div[contains(@class, 'gt-sm')]")
                details["Detalhe Possui Chave"], details["found_Possui_Chave"] = get_detail_by_label_internal(driver_instance, "Possui Chave", parent_selector="//div[contains(@class, 'gt-sm')]")
                details["Detalhe Tipo Retomada"], details["found_Tipo_Retomada"] = get_detail_by_label_internal(driver_instance, "Tipo Retomada", parent_selector="//div[contains(@class, 'gt-sm')]")
                details["Detalhe Localização"], details["found_Localizacao"] = get_detail_by_label_internal(driver_instance, "Localização", parent_selector="//div[contains(@class, 'gt-sm')]")
                details["Detalhe Tipo Veículo"], details["found_Tipo_Veiculo"] = get_detail_by_label_internal(driver_instance, "Tipo", parent_selector="//div[contains(@class, 'gt-sm')]")

                # Fallback para layout mobile (lt-md) se os dados do desktop não foram encontrados
                if not details["found_Ano_Veiculo"]:
                    temp_ano, found = get_direct_detail_internal(driver_instance, [
                        "div.lt-md div.row.q-pb-sm div.col-4:nth-child(1) p.text-categoria",
                        "div.lt-md div.q-card__section--vert.q-card__section.q-pa-md div.row div.col-4:nth-child(1) p.text-categoria"
                    ])
                    if found:
                        match_ano = re.search(r'\d{4}', temp_ano)
                        if match_ano:
                            details["Detalhe Ano Veículo"] = match_ano.group(0)
                        elif "/" in temp_ano:
                            details["Detalhe Ano Veículo"] = temp_ano.split('/')[0].strip()
                        else:
                            details["Detalhe Ano Veículo"] = temp_ano.strip()
                        details["found_Ano_Veiculo"] = True # Atualiza o status

                if not details["found_Combustivel"]:
                    temp_combustivel, found = get_direct_detail_internal(driver_instance, [
                        "div.lt-md div.row.q-pb-sm div.col-4:nth-child(2) p.text-categoria",
                        "div.lt-md div.q-card__section--vert.q-card__section.q-pa-md div.row div.col-4:nth-child(2) p.text-categoria"
                    ])
                    if found:
                        details["Detalhe Combustivel"] = temp_combustivel
                        details["found_Combustivel"] = True
                        
                if not details["found_KM_Veiculo"]:
                    temp_km, found = get_direct_detail_internal(driver_instance, [
                        "div.lt-md div.row.q-pb-sm div.col-4:nth-child(3) p.text-categoria",
                        "div.lt-md div.q-card__section--vert.q-card__section.q-pa-md div.row div.col-4:nth-child(3) p.text-categoria"
                    ])
                    if found:
                        details["Detalhe KM Veículo"] = temp_km
                        details["found_KM_Veiculo"] = True

                if not details["found_Valor_Mercado"]:
                    temp_valor_mercado, found = get_detail_by_label_internal(driver_instance, "Valor Mercado", parent_selector="//div[contains(@class, 'lt-md')]")
                    if found:
                        details["Detalhe Valor Mercado"] = temp_valor_mercado.replace('R$', '').replace('.', '').replace(',', '.').strip()
                        details["found_Valor_Mercado"] = True

                if not details["found_Cor"]:
                    temp_cor, found = get_detail_by_label_internal(driver_instance, "Cor", parent_selector="//div[contains(@class, 'lt-md')]")
                    if found:
                        details["Detalhe Cor"] = temp_cor
                        details["found_Cor"] = True

                if not details["found_Possui_Chave"]:
                    temp_chave, found = get_detail_by_label_internal(driver_instance, "Possui Chave", parent_selector="//div[contains(@class, 'lt-md')]")
                    if found:
                        details["Detalhe Possui Chave"] = temp_chave
                        details["found_Possui_Chave"] = True

                if not details["found_Tipo_Retomada"]:
                    temp_tipo_retomada, found = get_detail_by_label_internal(driver_instance, "Tipo Retomada", parent_selector="//div[contains(@class, 'lt-md')]")
                    if found:
                        details["Detalhe Tipo Retomada"] = temp_tipo_retomada
                        details["found_Tipo_Retomada"] = True

                if not details["found_Localizacao"]:
                    temp_localizacao, found = get_detail_by_label_internal(driver_instance, "Localização", parent_selector="//div[contains(@class, 'lt-md')]")
                    if found:
                        details["Detalhe Localizacao"] = temp_localizacao
                        details["found_Localizacao"] = True

                if not details["found_Tipo_Veiculo"]:
                    temp_tipo_veiculo, found = get_detail_by_label_internal(driver_instance, "Tipo", parent_selector="//div[contains(@class, 'lt-md')]")
                    if found:
                        details["Detalhe Tipo Veiculo"] = temp_tipo_veiculo
                        details["found_Tipo_Veiculo"] = True
                
                print(f"  ✔️ Detalhes extraídos para {link}.")
                print("  Status de extração dos campos de detalhe:")
                for field_name in [
                    "Ano Veículo", "Combustível", "KM Veículo", "Valor Mercado",
                    "Cor", "Possui Chave", "Tipo Retomada", "Localização", "Tipo Veículo"
                ]:
                    status_icon = "✅" if details[f"found_{field_name.replace(' ', '_')}"] else "❌"
                    print(f"    {status_icon} {field_name}")


                lot_data_list[index].update({
                    "Detalhe Ano Veículo": details["Detalhe Ano Veículo"],
                    "Detalhe Combustível": details["Detalhe Combustivel"],
                    "Detalhe KM Veículo": details["Detalhe KM Veiculo"],
                    "Detalhe Valor Mercado": details["Detalhe Valor Mercado"],
                    "Detalhe Cor": details["Detalhe Cor"],
                    "Detalhe Possui Chave": details["Detalhe Possui Chave"],
                    "Detalhe Tipo Retomada": details["Detalhe Tipo Retomada"],
                    "Detalhe Localização": details["Detalhe Localizacao"],
                    "Detalhe Tipo Veículo": details["Detalhe Tipo Veiculo"]
                })

            except TimeoutException:
                print(f"  ⚠️ Timeout ao carregar detalhes do lote em {link}. Informações adicionais podem estar incompletas ou a página não carregou corretamente.")
            except Exception as e:
                print(f"  ❌ ERRO geral ao extrair detalhes da página {link}: {e}")
            
            finally:
                # Não é necessário fechar e reabrir abas se estiver navegando na mesma aba
                # Se houver múltiplas abas, você precisaria mudar de volta para a principal.
                # No entanto, ao usar driver.get(link), você substitui a página atual,
                # então não há necessidade de gerenciamento de janela aqui.
                pass # Não faz nada no finally se estiver navegando na mesma aba.

# --- Configuração e Inicialização do Selenium ---
print("[INFO] Iniciando a configuração do Selenium...")
options = Options()
options.add_argument("--headless")   # Rodar em modo headless para mais velocidade e menor uso de recursos
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_argument("--start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

# Adicionando opções para otimizar o desempenho do navegador
options.add_argument("--blink-settings=imagesEnabled=false") # Desabilita o carregamento de imagens
options.add_argument("--disable-extensions") # Desabilita extensões
options.add_argument("--dns-prefetch-disable") # Desabilita pré-busca DNS
options.add_argument("--disable-browser-side-navigation") # Pode acelerar a navegação
options.add_argument("--disable-features=NetworkService") # Desabilita o serviço de rede (cuidado com isso, pode quebrar sites complexos)
options.add_argument("--disable-background-networking") # Desabilita atividades de rede em segundo plano

# --- CONEXÃO COM O SELENIUM ---
# Você precisará ter um Selenium Grid ou Standalone Server rodando, por exemplo, via Docker.
# Exemplo de URL para um container Docker chamado 'selenium':
SELENIUM_URL = "http://selenium:4444/wd/hub" 
# Se você estiver executando o Selenium localmente sem Docker, pode ser "http://localhost:4444/wd/hub"

driver = None
MAX_TRIES = 10 

for attempt in range(MAX_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_TRIES})...")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        # Definir espera implícita aqui, após o driver ser inicializado
        driver.implicitly_wait(1) # Tempo de espera implícita baixo
        print("[INFO] Conectado ao Selenium com sucesso!")
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
        # Aumentar o tempo do WebDriverWait para o pop-up, pois ele pode demorar
        cookie_banner_container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.cc-nb-main-container"))
        )
        cookie_accept_button = WebDriverWait(cookie_banner_container, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-nb-okcookie"))
        )
        
        driver.execute_script("arguments[0].click();", cookie_accept_button)
        print("[INFO] Pop-up de cookies/aceitação clicado com sucesso.")
        time.sleep(0.5) # Pausa mínima para o pop-up desaparecer
    except TimeoutException:
        print("[INFO] Nenhum pop-up de cookies/aceitação identificado ou apareceu no tempo limite. Prosseguindo.")
    except NoSuchElementException:
        print("[INFO] Contêiner de cookies encontrado, mas o botão de aceitar não. Prosseguindo.")
    except Exception as e:
        print(f"[WARN] Erro inesperado ao tentar lidar com pop-ups: {e}. Prosseguindo.")

    # --- Variáveis de controle de paginação ---
    total_paginas = 1
    current_page = 1

    # --- Loop principal para iterar por todas as páginas de listagem ---
    while current_page <= total_paginas:
        print(f"\n--- Processando Página {current_page} de {total_paginas} ---")

        try:
            # Espera pelos elementos dos lotes (cards) estarem presentes na página
            WebDriverWait(driver, 20).until( # Reduzindo o tempo de espera aqui
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
            )
            # time.sleep(0.5) # Pequena pausa para garantir que todos os elementos renderizem - Tente remover se possível
        except TimeoutException:
            print(f"[WARN] Timeout ao carregar lotes na Página {current_page}. Pode não haver lotes nesta página ou carregamento lento. Fim da extração de lista.")
            break

        lotes = driver.find_elements(By.CSS_SELECTOR, ".sessao.cursor-pointer")
        print(f"[INFO] {len(lotes)} lotes encontrados na Página {current_page}.")

        if not lotes: # Se não encontrar lotes na página, encerra o loop de paginação
            print(f"[INFO] Nenhum lote encontrado na Página {current_page}. Fim da extração de lista.")
            break

        # --- Extração de dados básicos dos lotes da página atual ---
        for i, lote in enumerate(lotes, start=1):
            try:
                # Re-localiza o elemento do lote para evitar StaleElementReferenceException
                # Aumentar o tempo aqui para ter certeza que o elemento está visível antes de interagir
                lote_element_on_page = WebDriverWait(driver, 10).until(EC.visibility_of(lotes[i-1])) 
                
                print(f"🔍 Extraindo dados básicos do Lote {i} na Página {current_page}:")

                titulo = safe_get_element_text(lote_element_on_page, "div.header-card h3")
                link = safe_get_element_attribute(lote_element_on_page, "a.img-card", "href")
                if link == "N/A": # Tenta um seletor alternativo para o link se o primeiro falhar
                    link = safe_get_element_attribute(lote_element_on_page, "div.header-card a", "href")
                if link and not link.startswith("http"): # Garante que o link é absoluto
                    link = "https://leilo.com.br" + link
                
                imagem_style = safe_get_element_attribute(lote_element_on_page, "div.q-img__image", "style")
                imagem = "N/A"
                if imagem_style != "N/A" and "url(" in imagem_style:
                    match = re.search(r'url\("?\'?([^"\')]+)"?\'?\)', imagem_style)
                    if match:
                        imagem = match.group(1)
                
                uf = safe_get_element_text(lote_element_on_page, "div.codigo-anuncio span")

                ano_raw = safe_get_element_text(lote_element_on_page, "p.text-ano")
                ano = "N/A"
                if ano_raw != "N/A":
                    match_ano = re.search(r'\d{4}', ano_raw)
                    if match_ano:
                        ano = match_ano.group(0)
                    else: # Fallback para anos com 2 dígitos
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
                    "KM": km, "Valor do Lance": valor, "Situação": situacao, "Data Leilão": data_leilao_str
                })
            except StaleElementReferenceException:
                print(f"[WARN] StaleElementReferenceException no Lote {i} da Página {current_page}. Este lote pode ser pulado para evitar lentidão.")
                # Em vez de tentar re-localizar, podemos optar por pular o lote se a performance for crítica.
                # Ou re-tentar apenas este lote, se for essencial.
            except Exception as e:
                print(f"[ERRO] Erro inesperado ao extrair dados do Lote {i} na Página {current_page}: {e}")

        # --- Lógica de Paginação ---
        if current_page == 1: # Só obtém o total de páginas na primeira vez
            try:
                # Localiza o elemento que contém o número total de páginas
                total_pages_element = WebDriverWait(driver, 10).until( # Reduzindo o tempo de espera
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

        # Avança para a próxima página, se não for a última
        if current_page < total_paginas:
            try:
                next_page_to_click = current_page + 1
                print(f"[INFO] Tentando clicar no botão da Página {next_page_to_click}...")
                
                # XPath para o botão da próxima página (baseado no 'aria-label')
                next_page_button_xpath = f"//div[contains(@class, 'q-pagination__middle')]/button[@aria-label='{next_page_to_click}']"
                
                # Reduzindo o tempo de espera para o botão de próxima página
                button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, next_page_button_xpath))
                )
                
                if 'disabled' not in button.get_attribute('class'):
                    driver.execute_script("arguments[0].click();", button)
                    print(f"✅ SUCESSO: Clicado no botão da Página {next_page_to_click}.")
                    current_page += 1
                    # Não há time.sleep(2) fixo aqui. A espera é feita pelo WebDriverWait abaixo.
                    print("[INFO] Aguardando o carregamento dos novos lotes na próxima página...")
                    WebDriverWait(driver, 20).until( # Reduzindo o tempo de espera para novos lotes
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
                    )
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
    extract_lot_details(driver, dados) # Chama a função para extrair detalhes

    # --- Salvamento dos Dados em CSV ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"leilo_com_br_carros_{timestamp}.csv"

    fieldnames = [
        "Título", "Link", "Imagem", "UF", "Ano", "KM", "Valor do Lance", "Situação", "Data Leilão",
        "Detalhe Ano Veículo", "Detalhe Combustível", "Detalhe KM Veículo", "Detalhe Detalhe Valor Mercado",
        "Detalhe Cor", "Detalhe Possui Chave", "Detalhe Tipo Retomada", "Detalhe Localização",
        "Detalhe Tipo Veículo"
    ]

    print(f"\n--- Salvando dados em '{output_filename}' ---")
    try:
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dados)
        print(f"✅ SUCESSO: Dados salvos em '{output_filename}'. Total de {len(dados)} registros.")
    except IOError as e:
        print(f"❌ ERRO: Não foi possível escrever no arquivo CSV '{output_filename}': {e}")
    except Exception as e:
        print(f"❌ ERRO Inesperado ao salvar o arquivo CSV: {e}")

except Exception as e:
    print(f"❌ Ocorreu um erro crítico durante a execução principal: {e}")
finally:
    if driver:
        print("[INFO] Fechando o navegador...")
        driver.quit()
    print("[INFO] Script finalizado.")