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
    create_parque_leiloes_oficial_table,
    insert_data_parque_leiloes_oficial
)

def format_currency_brl(value, include_symbol=False):
    """
    Formata um valor numérico para o formato de moeda brasileiro (BRL).
    Ex: 12345.67 -> "12.345,67" ou "R$ 12.345,67"
    Retorna "N/A" se o valor não for numérico ou for None.
    """
    if value is None or value == "N/A":
        return "N/A"
    
    # Tenta converter para float, se for uma string numérica
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return "N/A" # Não é um número válido

    if isinstance(value, (int, float)):
        # Formata para duas casas decimais, usa vírgula como separador decimal e ponto como separador de milhar
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if include_symbol:
            return f"R$ {formatted_value}"
        else:
            return formatted_value
    return "N/A"


def safe_get_element_text(element, css_selector, wait_time=2):
    """
    Tenta obter o texto de um elemento usando um seletor CSS.
    'element' pode ser o driver (para elementos globais) ou um WebElement (para elementos aninhados).
    Retorna "N/A" se o elemento não for encontrado dentro do tempo limite.
    Prioriza .text, mas tenta .innerHTML se .text estiver vazio.
    """
    try:
        # Se 'element' for o driver, usa presence_of_element_located
        if element == driver:
            found_element = WebDriverWait(element, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
        # Se 'element' for um WebElement, busca dentro dele
        else:
            found_element = WebDriverWait(element, wait_time).until(
                lambda p_elem: p_elem.find_element(By.CSS_SELECTOR, css_selector)
            )
        
        # Espera adicional para que o texto do elemento não seja vazio ou apenas espaços
        WebDriverWait(element, wait_time).until(
            lambda driver_or_elem: found_element.text.strip() != "" or found_element.get_attribute('innerHTML').strip() != ""
        )
        
        text_content = found_element.text.strip()
        if not text_content: # Se .text estiver vazio, tenta .innerHTML
            text_content = found_element.get_attribute('innerHTML').strip()
            # Substituir <br> por nova linha para regex funcionar melhor com innerHTML
            if text_content:
                text_content = re.sub(r'<br\s*\/?>', '\n', text_content) # Converte <br> para nova linha
                text_content = re.sub(r'\s+', ' ', text_content).strip() # Normaliza espaços novamente
                text_content = text_content.replace(u'\xa0', u' ') # Remove non-breaking space explicitly

        return text_content if text_content else "N/A"

    except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
        return "N/A"
    except Exception as e:
        print(f"DEBUG: Erro inesperado em safe_get_element_text para seletor '{css_selector}': {e}")
        return "N/A"

def safe_get_element_attribute(element, css_selector, attribute, wait_time=2):
    """
    Tenta obter o valor de um atributo de um elemento da web usando um seletor CSS.
    'element' pode ser o driver ou um WebElement.
    Retorna "N/A" se o elemento não for encontrado, se tornar obsoleto ou se o atributo não existir.
    """
    try:
        # Se 'element' for o driver, usa presence_of_element_located
        if element == driver:
            found_element = WebDriverWait(element, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
        # Se 'element' for um WebElement, busca dentro dele
        else:
            found_element = WebDriverWait(element, wait_time).until(
                lambda p_elem: p_elem.find_element(By.CSS_SELECTOR, css_selector)
            )
        attr_value = found_element.get_attribute(attribute)
        return attr_value.strip() if attr_value else "N/A"
    except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
        return "N/A"
    except Exception as e:
        print(f"DEBUG: Erro inesperado em safe_get_element_attribute para seletor '{css_selector}' (atributo '{attribute}'): {e}")
        return "N/A"

# Configura as opções para o navegador Chrome.
options = Options()
# options.add_argument("--headless") # Descomente esta linha para rodar o navegador em segundo plano.
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080") # Define um tamanho de janela consistente
options.add_argument("--start-maximized") # Maximiza a janela
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") # User-agent

SELENIUM_URL = "http://selenium:4444/wd/hub"

driver = None
MAX_CONNECTION_TRIES = 10 # Aumentado para 10 tentativas
RETRY_DELAY = 5 # Aumentado para 5 segundos de espera

for attempt in range(MAX_CONNECTION_TRIES):
    try:
        print(f"[INFO] Tentando conectar ao Selenium ({attempt+1}/{MAX_CONNECTION_TRIES})....")
        driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
        print("[INFO] Conectado ao Selenium com sucesso!")
        break
    except WebDriverException as e:
        print(f"[WARN] Selenium ainda não está pronto: {e}. Tentando novamente em {RETRY_DELAY} segundos...")
        time.sleep(RETRY_DELAY)
else:
    raise Exception("❌ Não foi possível conectar ao Selenium após várias tentativas. Encerrando.")

# Lista para armazenar todos os dados coletados de todas as URLs
dados = []

# URLs das categorias a serem raspadas
urls_categorias = [
    "https://parquedosleiloesoficial.com/lotes/veiculos",
    "https://parquedosleiloesoficial.com/lotes/motocicletas",
    "https://parquedosleiloesoficial.com/lotes/utilitarios"
]

try:
    for url_main_page in urls_categorias:
        # Determina o tipo de veículo com base na URL
        veiculo_tipo = "N/A"
        if "veiculos" in url_main_page:
            veiculo_tipo = "carros"
        elif "motocicletas" in url_main_page:
            veiculo_tipo = "motocicleta"
        elif "utilitarios" in url_main_page:
            veiculo_tipo = "utilitarios"
        
        print(f"\n--- Iniciando raspagem para a URL: {url_main_page} (Tipo: {veiculo_tipo}) ---")
        driver.get(url_main_page)
        print(f"[INFO] Página carregada: {driver.title}")

        # Espera inicial para a página carregar os primeiros lotes
        try:
            WebDriverWait(driver, 20).until( # Aumentado timeout
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.wr3[class*='LL_box_']"))
            )
            print("[INFO] Primeiros lotes da página principal encontrados.")
        except TimeoutException:
            print("❌ ERRO CRÍTICO: Nenhum lote encontrado na página principal dentro do tempo limite inicial. Verifique o seletor ou a URL.")
            driver.save_screenshot(f"erro_inicial_lotes_{url_main_page.split('/')[-1]}.png")
            continue # Pula para a próxima URL na lista

        time.sleep(3) # Pausa adicional para estabilidade

        # --- Lógica para rolar a página e carregar todos os lotes (rolagem infinita) ---
        last_num_lotes = 0
        scroll_attempts = 0
        MAX_SCROLL_ATTEMPTS = 50 # Aumentado para mais tentativas de rolagem

        print("[INFO] Iniciando rolagem da página para carregar todos os lotes...")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(6) # Tempo maior para permitir o carregamento de novos lotes

            new_lotes_on_page = driver.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")
            current_num_lotes = len(new_lotes_on_page)

            if current_num_lotes == last_num_lotes:
                print(f"[INFO] Nenhum lote novo carregado na última rolagem. Total: {current_num_lotes}.")
                if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
                    print(f"[INFO] Limite de {MAX_SCROLL_ATTEMPTS} tentativas de rolagem atingido. Parando.")
                    break
                scroll_attempts += 1
                print(f"[INFO] Tentando rolar novamente... ({scroll_attempts}/{MAX_SCROLL_ATTEMPTS})")
                time.sleep(3)
            else:
                print(f"[INFO] Rolando página. Total de lotes encontrados até agora: {current_num_lotes}")
                last_num_lotes = current_num_lotes
                scroll_attempts = 0

        current_lotes_on_page = driver.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")
        num_lotes = len(current_lotes_on_page)
        print(f"[INFO] {num_lotes} lotes coletados na página principal após rolagem completa para {url_main_page}.")

        for index in range(num_lotes):
            try:
                lote_element = WebDriverWait(driver, 10).until(
                    lambda d: d.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")[index]
                )
            except (StaleElementReferenceException, TimeoutException):
                print(f"[WARN] StaleElementReferenceException ou Timeout para o lote {index+1}. Re-tentando obter o elemento.")
                try:
                    current_lotes_on_page = driver.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")
                    if index < len(current_lotes_on_page):
                        lote_element = WebDriverWait(driver, 10).until(
                            lambda d: d.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")[index]
                        )
                        print(f"[INFO] Lote {index+1} re-obtido com sucesso após StaleElementReferenceException.")
                    else:
                        print(f"[ERRO] Lote {index+1} não encontrado após re-tentativa. Pode ter sido removido ou o índice está fora do limite. Pulando este lote.")
                        continue
                except Exception as e:
                    print(f"[ERRO] Falha crítica ao re-obter lote {index+1}: {e}. Pulando.")
                    continue

            # Inicializa todas as variáveis para garantir que "N/A" seja atribuído se não encontrado
            titulo = "N/A"
            link = "N/A"
            imagem = "N/A"
            km_veiculo = "N/A"
            lance_inicial = "N/A"
            valor_do_lance = "N/A"
            data_leilao = "N/A"
            marca_veiculo = "N/A"
            final_da_placa_veiculo = "N/A"
            ano_fabricacao_veiculo = "N/A"
            ano_modelo_veiculo = "N/A"
            chaves_veiculo = "N/A"
            condicao_motor_veiculo = "N/A"
            tabela_fipe_veiculo = "N/A"
            combustivel_veiculo = "N/A"
            procedencia_veiculo = "N/A"
            total_lances = "N/A" 
            modelo_veiculo = "N/A" 
            veiculo_patio_uf = "Brasilia-DF (AGUAS CLARAS/DF)" 
            veiculo_valor_vendido = "N/A" # Nova variável inicializada

            print(f"\n🔍 Extraindo dados do Lote {index+1} de {num_lotes} na URL: {url_main_page}:")

            # Extração de dados da visualização inicial do lote
            titulo = safe_get_element_text(lote_element, "li.LL_nome")
            link = safe_get_element_attribute(lote_element, "a.posr.db.m10", "href")
            if link == "N/A":
                link = safe_get_element_attribute(lote_element, "div.header-card a", "href")
            if link and not link.startswith("http"):
                link = "https://parquedosleiloesoficial.com" + link

            imagem = safe_get_element_attribute(lote_element, "img#phfotos_resposive", "src")
            if imagem and not imagem.startswith("http"):
                imagem = "https://parquedosleiloesoficial.com" + imagem 

            data_leilao_raw = safe_get_element_text(lote_element, "li.LL_data_fim data.dib")
            data_leilao = data_leilao_raw if data_leilao_raw != "N/A" else "N/A"
            
            lance_inicial_raw = safe_get_element_text(lote_element, "div.LL_lance_ini b.fz15")
            lance_inicial = re.sub(r'[^\d,]', '', lance_inicial_raw).replace(',', '.') if lance_inicial_raw != "N/A" else "N/A"

            valor_do_lance_raw = safe_get_element_text(lote_element, "div.LL_lance_atual b.fz15")
            
            # Aplica a formatação de moeda para valor_do_lance
            if valor_do_lance_raw != "N/A":
                cleaned_value = re.sub(r'[^\d,]', '', valor_do_lance_raw).replace(',', '.')
                try:
                    valor_do_lance = format_currency_brl(float(cleaned_value), include_symbol=True)
                except ValueError:
                    valor_do_lance = "N/A"
            else:
                valor_do_lance = "N/A"

            situacao = safe_get_element_text(lote_element, "div.LL_situacao p.itm-statusname")

            # Lógica para preencher veiculo_valor_vendido
            if situacao == "ARREMATADO" and valor_do_lance != "N/A":
                veiculo_valor_vendido = valor_do_lance
            elif situacao == "RECEBENDO LANCES": # Nova condição para "RECEBENDO LANCES"
                veiculo_valor_vendido = format_currency_brl(0.00, include_symbol=True) # Define como "R$ 0,00"
            else:
                veiculo_valor_vendido = "N/A"


            print(f"    - Título: {titulo}")
            print(f"    - Situação: {situacao}")
            print(f"    - Link: {link}")

            # Condição para navegar para a página de detalhes
            if situacao == "RECEBENDO LANCES" and link != "N/A":
                print("    - Situação é 'RECEBENDO LANCES'. Navegando para mais detalhes...")
                try:
                    driver.get(link) # Navega diretamente para o link do lote
                    
                    # --- LÓGICA PARA EXTRAIR DA ABA "DESCRIÇÃO" ---
                    descricao_tab_selector = (By.XPATH, "//ul[@id='box_info_leilao']/li/a[contains(., 'DESCRIÇÃO')]")
                    try:
                        descricao_tab = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable(descricao_tab_selector)
                        )
                        if "back_F5F5F5" not in descricao_tab.get_attribute("class"):
                            descricao_tab.click()
                            print("    - Clicado na aba 'DESCRIÇÃO'.")
                            time.sleep(2)
                        else:
                            print("    - Aba 'DESCRIÇÃO' já estava ativa.")

                        seletor_conteudo_descricao = "li.box__1 div.editor.taj p"
                        descricao_detalhada_raw = safe_get_element_text(driver, seletor_conteudo_descricao, wait_time=5)
                        
                        if descricao_detalhada_raw != "N/A" and descricao_detalhada_raw.strip() != "":
                            print("    - Conteúdo da DESCRIÇÃO detalhada encontrado e extraído. Processando...")
                            
                            # Ajustando os regex para os nomes de variáveis "DE"
                            marca_match = re.search(r"Marca: (.+?)(?=\nModelo:|$)", descricao_detalhada_raw)
                            km_match = re.search(r"KM: (.+?)(?=\nAno de Fabricação:|$)", descricao_detalhada_raw)
                            ano_fabricacao_match = re.search(r"Ano de Fabricação: (.+?)(?=\nAno Modelo:|$)", descricao_detalhada_raw)
                            ano_modelo_match = re.search(r"Ano Modelo: (.+?)(?=\nChaves:|$)", descricao_detalhada_raw)
                            chaves_match = re.search(r"Chaves: (.+?)(?=\nCondição do Motor:|$)", descricao_detalhada_raw)
                            condicao_motor_match = re.search(r"Condição do Motor: (.+?)(?=\nTabela FIPE R\$|$)", descricao_detalhada_raw.replace(u'\xa0', u' '))
                            
                            # Ajuste do regex para 'tabela_fipe_veiculo'
                            tabela_fipe_match = re.search(r"Tabela FIPE R\$ ([0-9.,]+)", descricao_detalhada_raw) 
                            
                            final_placa_match = re.search(r"Final da Placa: (.+?)(?=\nCombustível:|$)", descricao_detalhada_raw)
                            combustivel_match = re.search(r"Combustível: (.+?)(?=\nProcedência:|$)", descricao_detalhada_raw)
                            procedencia_match = re.search(r"Procedência: (.+)", descricao_detalhada_raw)
                            total_lances_match = re.search(r"Total Lances: (\d+)", descricao_detalhada_raw) 

                            marca_veiculo = marca_match.group(1).strip() if marca_match else "N/A"
                            km_veiculo = km_match.group(1).strip() if km_match else "N/A"
                            ano_fabricacao_veiculo = ano_fabricacao_match.group(1).strip() if ano_fabricacao_match else "N/A"
                            ano_modelo_veiculo = ano_modelo_match.group(1).strip() if ano_modelo_match else "N/A"
                            chaves_veiculo = chaves_match.group(1).strip() if chaves_match else "N/A"
                            condicao_motor_veiculo = condicao_motor_match.group(1).strip() if condicao_motor_match else "N/A"
                            
                            # Limpeza e formatação do valor da Tabela FIPE
                            if tabela_fipe_match:
                                cleaned_value = tabela_fipe_match.group(1).replace('.', '').replace(',', '.')
                                try:
                                    tabela_fipe_veiculo = format_currency_brl(float(cleaned_value), include_symbol=True)
                                except ValueError:
                                    tabela_fipe_veiculo = "N/A"
                            else:
                                tabela_fipe_veiculo = "N/A"
                                
                            final_da_placa_veiculo = final_placa_match.group(1).strip() if final_placa_match else "N/A"
                            combustivel_veiculo = combustivel_match.group(1).strip() if combustivel_match else "N/A"
                            procedencia_veiculo = procedencia_match.group(1).strip() if procedencia_match else "N/A"
                            total_lances = total_lances_match.group(1).strip() if total_lances_match else "N/A"

                            print(f"        - Marca Veiculo: {marca_veiculo}")
                            print(f"        - KM Veiculo: {km_veiculo}")
                            print(f"        - Ano Fabricacao Veiculo: {ano_fabricacao_veiculo}")
                            print(f"        - Ano Modelo Veiculo: {ano_modelo_veiculo}")
                            print(f"        - Chaves Veiculo: {chaves_veiculo}")
                            print(f"        - Condicao Motor Veiculo: {condicao_motor_veiculo}")
                            print(f"        - Tabela FIPE Veiculo: {tabela_fipe_veiculo}")
                            print(f"        - Final da Placa Veiculo: {final_da_placa_veiculo}")
                            print(f"        - Combustivel Veiculo: {combustivel_veiculo}")
                            print(f"        - Procedencia Veiculo: {procedencia_veiculo}")
                            print(f"        - Total Lances: {total_lances}")
                        else:
                            print("    - Conteúdo da DESCRIÇÃO do veículo não encontrado ou está vazia.")

                    except (NoSuchElementException, TimeoutException, ElementClickInterceptedException) as e:
                        print(f"[WARN] Não foi possível clicar na aba 'DESCRIÇÃO' ou encontrar seu conteúdo: {e}. Detalhes do veículo permanecerão 'N/A'.")

                except (NoSuchElementException, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException) as e:
                    print(f"[WARN] Erro ao tentar acessar detalhes do lote {index+1} ({link}): {e}. Pulando detalhes e marcando como 'N/A'.")
                    driver.save_screenshot(f"erro_detalhes_lote_{index+1}_{url_main_page.split('/')[-1]}.png")
                finally:
                    # Sempre retorna para a página principal da categoria atual para continuar o loop de lotes
                    driver.get(url_main_page)
                    try:
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "li.wr3[class*='LL_box_']"))
                        )
                        time.sleep(2)
                    except TimeoutException:
                        print(f"[WARN] Lotes não re-carregados na página principal da categoria {url_main_page} após retorno. Pode afetar a continuidade.")
                        driver.save_screenshot(f"erro_retorno_pagina_principal_{index+1}_{url_main_page.split('/')[-1]}.png")
                    except Exception as e:
                        print(f"[WARN] Erro inesperado ao retornar à página principal da categoria {url_main_page}: {e}")
            else:
                print("    - Situação não é 'RECEBENDO LANCES' ou link é 'N/A'. Não navegando para detalhes.")

            # Extração de modelo_veiculo a partir do titulo
            if titulo != "N/A":
                words_in_title = titulo.split(' ')
                if len(words_in_title) > 1:
                    modelo_veiculo = words_in_title[1].strip()
                else:
                    modelo_veiculo = "N/A"
            else:
                modelo_veiculo = "N/A"
            print(f"    - Modelo Veiculo (do título): {modelo_veiculo}") # Print após a atribuição

            # Adiciona os dados coletados à lista, incluindo os novos campos
            dados.append({
                "titulo": titulo,
                "link": link,
                "imagem": imagem,
                "km_veiculo": km_veiculo,
                "lance_inicial": lance_inicial,
                "valor_do_lance": valor_do_lance,
                "data_leilao": data_leilao,
                "marca_veiculo": marca_veiculo,
                "final_da_placa_veiculo": final_da_placa_veiculo,
                "ano_fabricacao_veiculo": ano_fabricacao_veiculo,
                "ano_modelo_veiculo": ano_modelo_veiculo,
                "chaves_veiculo": chaves_veiculo,
                "condicao_motor_veiculo": condicao_motor_veiculo,
                "tabela_fipe_veiculo": tabela_fipe_veiculo,
                "combustivel_veiculo": combustivel_veiculo,
                "procedencia_veiculo": procedencia_veiculo,
                "total_lances": total_lances,
                "modelo_veiculo": modelo_veiculo,
                "veiculo_tipo": veiculo_tipo,
                "veiculo_patio_uf": veiculo_patio_uf,
                "veiculo_valor_vendido": veiculo_valor_vendido, # Nova coluna adicionada
            })

finally:
    # --- Conexão e Inserção no PostgreSQL (para a tabela 'Parque_Leiloes_Oficial') ---
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
            print("[INFO] Criando ou verificando a tabela 'Parque_Leiloes_Oficial'...")
            create_parque_leiloes_oficial_table(conn) 
            print("[INFO] Iniciando inserção de dados na tabela 'Parque_Leiloes_Oficial'...")
            for lote_data in dados:
                # Mapear as chaves do dicionário `lote_data` (que estão no padrão "DE")
                # para as chaves esperadas pela função `insert_data_parque_leiloes_oficial`
                # (que correspondem aos nomes das colunas "PARA" no banco de dados, agora em minúsculas).
                transformed_data = {
                    "veiculo_titulo": lote_data.get("titulo", "N/A"),
                    "veiculo_link_lote": lote_data.get("link", "N/A"),
                    "veiculo_imagem": lote_data.get("imagem", "N/A"),
                    "veiculo_km": lote_data.get("km_veiculo", "N/A"),
                    "veiculo_lance_inicial": lote_data.get("lance_inicial", "N/A"),
                    "veiculo_valor_lance_atual": lote_data.get("valor_do_lance", "N/A"),
                    "veiculo_data_leilao": lote_data.get("data_leilao", "N/A"),
                    "veiculo_fabricante": lote_data.get("marca_veiculo", "N/A"),
                    "veiculo_final_placa": lote_data.get("final_da_placa_veiculo", "N/A"),
                    "veiculo_ano_fabricacao": lote_data.get("ano_fabricacao_veiculo", "N/A"),
                    "veiculo_ano_modelo": lote_data.get("ano_modelo_veiculo", "N/A"),
                    "veiculo_possui_chave": lote_data.get("chaves_veiculo", "N/A"),
                    "veiculo_condicao_motor": lote_data.get("condicao_motor_veiculo", "N/A"),
                    "veiculo_valor_fipe": lote_data.get("tabela_fipe_veiculo", "N/A"),
                    "veiculo_tipo_combustivel": lote_data.get("combustivel_veiculo", "N/A"),
                    "veiculo_tipo_retomada": lote_data.get("procedencia_veiculo", "N/A"),
                    "veiculo_total_lances": lote_data.get("total_lances", "N/A"),
                    "veiculo_modelo": lote_data.get("modelo_veiculo", "N/A"),
                    "veiculo_tipo": lote_data.get("veiculo_tipo", "N/A"),
                    "veiculo_patio_uf": lote_data.get("veiculo_patio_uf", "N/A"),
                    "veiculo_valor_vendido": lote_data.get("veiculo_valor_vendido", "N/A"), # Nova coluna para o DB
                }
                print(f"[INFO] Colunas enviadas para o DB para este lote: {list(transformed_data.keys())}")
                try:
                    insert_data_parque_leiloes_oficial(conn, transformed_data)
                except Exception as e:
                    print(f"[ERRO] Erro ao inserir registro no banco: {lote_data.get('titulo', 'N/A')[:50]}... Erro: {e}")
            print(f"[INFO] {len(dados)} registros processados para inserção na tabela 'Parque_Leiloes_Oficial'.")
            
            try:
                conn.close()
            except Exception as e:
                print(f"[ERRO] Erro ao fechar conexão com o banco de dados: {e}")
        else:
            print("[ERRO] Não foi possível estabelecer conexão com o banco de dados. Os dados não serão salvos no DB.")
            
    # --- Geração e diagnóstico do arquivo CSV (mantido) ---
    if dados:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = f"leilao_parque_data_{timestamp}.csv"
        output_dir = "/app/etl"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_file_name)

        # Define os cabeçalhos do CSV estritamente com base na coluna "DE"
        csv_fieldnames = [
            "titulo",
            "link",
            "imagem",
            "km_veiculo",
            "lance_inicial",
            "valor_do_lance",
            "data_leilao",
            "marca_veiculo",
            "final_da_placa_veiculo",
            "ano_fabricacao_veiculo",
            "ano_modelo_veiculo",
            "chaves_veiculo",
            "condicao_motor_veiculo",
            "tabela_fipe_veiculo",
            "combustivel_veiculo",
            "procedencia_veiculo",
            "total_lances",
            "modelo_veiculo",
            "veiculo_tipo",
            "veiculo_patio_uf",
            "veiculo_valor_vendido" # Nova coluna para o CSV
        ]

        # O dicionário 'dados' já está formatado com as chaves "DE", então
        # podemos usá-o diretamente para escrever no CSV.
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
    print("✅ Extração de dados do Parque dos Leilões concluída com sucesso!")