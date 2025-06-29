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

# Importa as funções de banco de dados e configurações do seu módulo db_utils
from db_utils.db_operations import connect_db, create_table_consolidado, insert_data_consolidado

def safe_get_element_text(parent_element, by_method, selector, wait_time=5):
    """
    Tenta obter o texto de um elemento da web usando um método By e um seletor específicos.
    'parent_element' pode ser o driver (para elementos globais) ou um WebElement (para elementos aninhados).
    Retorna "N/A" se o elemento não for encontrado dentro do tempo limite.
    Prioriza .text, mas tenta .innerHTML se .text estiver vazio.
    """
    try:
        # Espera pelo elemento ficar presente/visível E com algum texto
        found_element = WebDriverWait(parent_element, wait_time).until(
            EC.presence_of_element_located((by_method, selector))
        )
        
        # Espera adicional para que o texto do elemento não seja vazio ou apenas espaços
        WebDriverWait(parent_element, wait_time).until(
            lambda driver: found_element.text.strip() != "" or found_element.get_attribute('innerHTML').strip() != ""
        )
        
        text_content = found_element.text.strip()
        if not text_content: # Se .text estiver vazio, tenta .innerHTML
            text_content = found_element.get_attribute('innerHTML').strip()
            # Substituir <br> por nova linha para regex funcionar melhor com innerHTML
            if text_content:
                text_content = re.sub(r'<br\s*\/?>', '\n', text_content) # Converte <br> para nova linha
                text_content = re.sub(r'\s+', ' ', text_content).strip() # Normaliza espaços novamente
                text_content = text_content.replace(u'\xa0', u' ') # Remove non-breaking space explicitly
                print(f"DEBUG: Obtido innerHTML para '{selector}'. Conteúdo: {text_content[:100]}...") # Print de depuração

        return text_content if text_content else "N/A"

    except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
        print(f"DEBUG: Elemento não encontrado ou com timeout para seletor '{selector}'.")
        return "N/A"
    except Exception as e:
        print(f"DEBUG: Erro inesperado em safe_get_element_text para seletor '{selector}': {e}")
        return "N/A"

def safe_get_element_attribute(parent_element, by_method, selector, attribute, wait_time=5):
    """
    Tenta obter o valor de um atributo de um elemento da web usando um método By e um seletor.
    'parent_element' pode ser o driver ou um WebElement.
    Retorna "N/A" se o elemento não for encontrado, se tornar obsoleto ou se o atributo não existir.
    """
    try:
        found_element = WebDriverWait(parent_element, wait_time).until(
            EC.presence_of_element_located((by_method, selector))
        )
        attr_value = found_element.get_attribute(attribute)
        return attr_value.strip() if attr_value else "N/A"
    except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
        return "N/A"
    except Exception as e:
        print(f"DEBUG: Erro inesperado em safe_get_element_attribute para seletor '{selector}' (atributo '{attribute}'): {e}")
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

dados = []

try:
    url_main_page = "https://parquedosleiloesoficial.com/lotes/veiculos"
    driver.get(url_main_page)
    print(f"[INFO] Página carregada: {driver.title}")

    # Espera inicial para a página carregar os primeiros lotes
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li.wr3[class*='LL_box_']"))
        )
        print("[INFO] Primeiros lotes da página principal encontrados.")
    except TimeoutException:
        print("❌ ERRO CRÍTICO: Nenhum lote encontrado na página principal dentro do tempo limite inicial. Verifique o seletor ou a URL.")
        driver.save_screenshot("erro_inicial_lotes.png")
        raise Exception("Falha ao carregar a página principal de lotes. Encerrando.")

    time.sleep(3) # Pausa adicional para estabilidade

    # --- Lógica para rolar a página e carregar todos os lotes (rolagem infinita) ---
    last_num_lotes = 0
    scroll_attempts = 0
    MAX_SCROLL_ATTEMPTS = 50 # Limite de tentativas para evitar loops infinitos

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
            # Dê mais algumas tentativas caso o carregamento seja lento
            scroll_attempts += 1
            print(f"[INFO] Tentando rolar novamente... ({scroll_attempts}/{MAX_SCROLL_ATTEMPTS})")
            time.sleep(3) # Pequena pausa extra antes da próxima tentativa
        else:
            print(f"[INFO] Rolando página. Total de lotes encontrados até agora: {current_num_lotes}")
            last_num_lotes = current_num_lotes
            scroll_attempts = 0 # Reinicia o contador de tentativas se novos lotes forem encontrados

    # Re-obtenha a lista final de lotes após a rolagem completa
    current_lotes_on_page = driver.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")
    num_lotes = len(current_lotes_on_page)
    print(f"[INFO] {num_lotes} lotes coletados na página principal após rolagem completa.")

    for index in range(num_lotes):
        try:
            # Re-encontrar o elemento do lote a cada iteração para evitar StaleElementReferenceException
            # Use WebDriverWait para garantir que o elemento esteja acessível
            lote_element = WebDriverWait(driver, 10).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "li.wr3[class*='LL_box_']")[index]
            )
        except (StaleElementReferenceException, TimeoutException):
            print(f"[WARN] StaleElementReferenceException ou Timeout para o lote {index+1}. Re-tentando obter o elemento.")
            # Se o elemento ficar obsoleto, tentamos re-obter a lista completa e o elemento específico
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
        numero_lote = "N/A"
        data_inicio = "N/A"
        data_termino = "N/A"
        lance_inicial = "N/A"
        lance_atual = "N/A"
        visualizacoes = "N/A"
        total_lances = "N/A"
        situacao = "N/A"
        valor_mercado = "N/A"
        localizacao_detalhe = "N/A"
        marca = "N/A"
        km = "N/A"
        ano_fabricacao = "N/A"
        ano_modelo = "N/A"
        chaves = "N/A"
        condicao_motor = "N/A"
        tabela_fipe = "N/A"
        final_placa = "N/A"
        combustivel = "N/A"
        procedencia = "N/A"
        descricao_detalhada = "N/A" # Campo para a descrição detalhada do veículo

        print(f"\n🔍 Extraindo dados do Lote {index+1} de {num_lotes}:")

        # Extração de dados da visualização inicial do lote
        titulo = safe_get_element_text(lote_element, By.CSS_SELECTOR, "li.LL_nome")
        link = safe_get_element_attribute(lote_element, By.CSS_SELECTOR, "a.posr.db.m10", "href")
        if link and not link.startswith("http"):
            link = "https://parquedosleiloesoficial.com" + link

        imagem = safe_get_element_attribute(lote_element, By.CSS_SELECTOR, "img#phfotos_resposive", "src")
        if imagem and not imagem.startswith("http"):
            imagem = "https://parquedosleiloesoficial.com" + imagem

        numero_lote = safe_get_element_text(lote_element, By.CSS_SELECTOR, "li span span[style*='font-size: 17px;']")
        data_inicio = safe_get_element_text(lote_element, By.CSS_SELECTOR, "li.LL_data_ini data.dib")
        data_termino = safe_get_element_text(lote_element, By.CSS_SELECTOR, "li.LL_data_fim data.dib")
        lance_inicial = safe_get_element_text(lote_element, By.CSS_SELECTOR, "div.LL_lance_ini b.fz15")
        lance_atual = safe_get_element_text(lote_element, By.CSS_SELECTOR, "div.LL_lance_atual b.fz15")
        visualizacoes = safe_get_element_text(lote_element, By.CSS_SELECTOR, "span.views-total span.LL_count")
        total_lances = safe_get_element_text(lote_element, By.CSS_SELECTOR, "span.bids-total span.LL_count_lances")
        situacao = safe_get_element_text(lote_element, By.CSS_SELECTOR, "div.LL_situacao p.itm-statusname")

        print(f"    - Título: {titulo}")
        print(f"    - Situação: {situacao}")
        print(f"    - Link: {link}")

        # Condição para navegar para a página de detalhes
        if situacao == "RECEBENDO LANCES" and link != "N/A":
            print("    - Situação é 'RECEBENDO LANCES'. Navegando para mais detalhes...")
            try:
                driver.get(link) # Navega diretamente para o link do lote
                
                # --- LÓGICA PARA EXTRAIR DA ABA "DESCRIÇÃO" (AGORA MAIS PRECISA) ---
                # 1. Clicar na aba "DESCRIÇÃO" se ela não for a ativa por padrão
                descricao_tab_selector = (By.XPATH, "//ul[@id='box_info_leilao']/li/a[contains(., 'DESCRIÇÃO')]")
                try:
                    descricao_tab = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(descricao_tab_selector)
                    )
                    # Verifica se a aba já está ativa (pela classe 'back_F5F5F5' no HTML fornecido)
                    if "back_F5F5F5" not in descricao_tab.get_attribute("class"):
                        descricao_tab.click()
                        print("    - Clicado na aba 'DESCRIÇÃO'.")
                        time.sleep(2) # Pausa para o conteúdo da aba carregar
                    else:
                        print("    - Aba 'DESCRIÇÃO' já estava ativa.")

                    # 2. Extrair o conteúdo da descrição da área revelada
                    # Com base no HTML fornecido, a descrição detalhada está dentro de:
                    # <li class="box__ box__1 ..."><div class="editor taj"><p>...</p></div></li>
                    seletor_conteudo_descricao = (By.CSS_SELECTOR, "li.box__1 div.editor.taj p") 

                    descricao_detalhada = safe_get_element_text(driver, seletor_conteudo_descricao[0], seletor_conteudo_descricao[1], wait_time=10)
                    
                    # --- NOVO PRINT PARA DEPURAR (CONTEÚDO DA DESCRIÇÃO) ---
                    print(f"    --- CONTEÚDO BRUTO DA DESCRIÇÃO (li.box__1 div.editor.taj p): START ---")
                    print(f"    Raw Text Length: {len(descricao_detalhada) if descricao_detalhada != 'N/A' else 'N/A'}")
                    print(descricao_detalhada)
                    print(f"    --- CONTEÚDO BRUTO DA DESCRIÇÃO (li.box__1 div.editor.taj p): END ---")
                    # --- FIM DO PRINT DEPURAR ---


                    if descricao_detalhada != "N/A" and descricao_detalhada.strip() != "":
                        print("    - Conteúdo da DESCRIÇÃO detalhada encontrado e extraído. Processando...")
                        
                        # Agora, os regex serão aplicados em 'descricao_detalhada'
                        # As expressões regulares já foram ajustadas para lidar com espaços e novas linhas
                        # (A função safe_get_element_text já cuida das tags <br> convertendo-as para \n)
                        marca_match = re.search(r"Marca: (.+?)\nModelo:", descricao_detalhada)
                        km_match = re.search(r"KM: (.+?)\nAno de Fabricação:", descricao_detalhada)
                        ano_fabricacao_match = re.search(r"Ano de Fabricação: (.+?)\nAno Modelo:", descricao_detalhada)
                        ano_modelo_match = re.search(r"Ano Modelo: (.+?)\nChaves:", descricao_detalhada)
                        chaves_match = re.search(r"Chaves: (.+?)\nCondição do Motor:", descricao_detalhada)
                        condicao_motor_match = re.search(r"Condição do Motor: (.+?)\n(?=Tabela FIPE|$)", descricao_detalhada.replace(u'\xa0', u' ')) # Usar lookahead para pegar até Tabela FIPE ou fim da string. Cuidado com non-breaking space
                        tabela_fipe_match = re.search(r"Tabela FIPE R\$ (.+?)\nFinal da Placa:", descricao_detalhada) 
                        final_placa_match = re.search(r"Final da Placa: (.+?)\nCombustível:", descricao_detalhada)
                        combustivel_match = re.search(r"Combustível: (.+?)\nProcedência:", descricao_detalhada)
                        procedencia_match = re.search(r"Procedência: (.+)", descricao_detalhada)

                        # Atribuir valores
                        marca = marca_match.group(1).strip() if marca_match else "N/A"
                        km = km_match.group(1).strip() if km_match else "N/A"
                        ano_fabricacao = ano_fabricacao_match.group(1).strip() if ano_fabricacao_match else "N/A"
                        ano_modelo = ano_modelo_match.group(1).strip() if ano_modelo_match else "N/A"
                        chaves = chaves_match.group(1).strip() if chaves_match else "N/A"
                        condicao_motor = condicao_motor_match.group(1).strip() if condicao_motor_match else "N/A"
                        tabela_fipe = tabela_fipe_match.group(1).strip() if tabela_fipe_match else "N/A"
                        final_placa = final_placa_match.group(1).strip() if final_placa_match else "N/A"
                        combustivel = combustivel_match.group(1).strip() if combustivel_match else "N/A"
                        procedencia = procedencia_match.group(1).strip() if procedencia_match else "N/A"

                        print(f"        - Marca: {marca}")
                        print(f"        - KM: {km}")
                        print(f"        - Ano de Fabricação: {ano_fabricacao}")
                        print(f"        - Ano Modelo: {ano_modelo}")
                        print(f"        - Chaves: {chaves}")
                        print(f"        - Condição do Motor: {condicao_motor}")
                        print(f"        - Tabela FIPE: {tabela_fipe}")
                        print(f"        - Final da Placa: {final_placa}")
                        print(f"        - Combustível: {combustivel}")
                        print(f"        - Procedência: {procedencia}")
                    else:
                        print("    - Conteúdo da DESCRIÇÃO do veículo não encontrado ou está vazia.")

                    # Valor de Mercado e Localização Detalhe - MANTENHA O SELETOR ORIGINAL ATÉ INSPECIONAR!
                    # Se eles não estão dentro da mesma <p> ou <div>, o seletor precisa ser diferente.
                    # Mantenho esses seletores antigos, pois o HTML fornecido não os continha, mas eles podem estar em outro lugar na página de detalhes.
                    valor_mercado = safe_get_element_text(driver, By.CSS_SELECTOR, "span.price-market", wait_time=5)
                    localizacao_detalhe = safe_get_element_text(driver, By.CSS_SELECTOR, "span.location-detail", wait_time=5)
                    print(f"        - Valor Mercado: {valor_mercado}")
                    print(f"        - Localização Detalhe: {localizacao_detalhe}")


                except (NoSuchElementException, TimeoutException, ElementClickInterceptedException) as e:
                    print(f"[WARN] Não foi possível clicar na aba 'DESCRIÇÃO' ou encontrar seu conteúdo: {e}. Detalhes do veículo permanecerão 'N/A'.")
                # --- FIM DA LÓGICA DA DESCRIÇÃO ---


            except (NoSuchElementException, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException) as e:
                print(f"[WARN] Erro ao tentar acessar detalhes do lote {index+1} ({link}): {e}. Pulando detalhes e marcando como 'N/A'.")
                driver.save_screenshot(f"erro_detalhes_lote_{index+1}.png")
            finally:
                # Sempre retorna para a página principal para continuar o loop de lotes
                driver.get(url_main_page)
                try:
                    # Espera que os lotes da página principal estejam visíveis novamente
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "li.wr3[class*='LL_box_']"))
                    )
                    time.sleep(2) # Pequena pausa após o retorno e espera
                except TimeoutException:
                    print("[WARN] Lotes não re-carregados na página principal após retorno. Pode afetar a continuidade.")
                    driver.save_screenshot(f"erro_retorno_pagina_principal_{index+1}.png")
                except Exception as e:
                    print(f"[WARN] Erro inesperado ao retornar à página principal: {e}")
        else:
            print("    - Situação não é 'RECEBENDO LANCES' ou link é 'N/A'. Não navegando para detalhes.")

        # Adiciona os dados coletados à lista, incluindo os "N/A" para campos não encontrados
        dados.append({
            "Título": titulo,
            "Link": link,
            "Imagem": imagem,
            "Lote Número": numero_lote,
            "Data Início": data_inicio,
            "Data Término": data_termino,
            "Lance Inicial": lance_inicial,
            "Lance Atual": lance_atual,
            "Visualizações": visualizacoes,
            "Total Lances": total_lances,
            "Situação": situacao,
            "Marca": marca,
            "KM": km,
            "Ano de Fabricacao": ano_fabricacao,
            "Ano Modelo": ano_modelo,
            "Chaves": chaves,
            "Condicao do Motor": condicao_motor,
            "Tabela FIPE": tabela_fipe,
            "Final da Placa": final_placa,
            "Combustivel": combustivel,
            "Procedencia": procedencia,
            "Valor Mercado": valor_mercado,
            "Localizacao Detalhe": localizacao_detalhe,
            "Descricao Detalhada": descricao_detalhada, # Esta é a nova coluna para a descrição completa
        })

finally:
    # --- Conexão e Inserção no PostgreSQL (para a tabela 'consolidado') ---
    if dados:
        conn = connect_db()
        if conn:
            create_table_consolidado(conn) # Cria a tabela 'consolidado'
            for lote_data in dados:
                insert_data_consolidado(conn, lote_data) # Insere dados na tabela 'consolidado'
            conn.close()
            print("[INFO] Dados inseridos na tabela 'consolidado' no PostgreSQL e conexão fechada.")
        else:
            print("[ERRO] Não foi possível conectar ao banco de dados, os dados não serão salvos no PostgreSQL.")
            
    # --- Geração e diagnóstico do arquivo CSV (mantido) ---
    if dados:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = f"leilao_parque_data_{timestamp}.csv"
        output_dir = "/app/etl"
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