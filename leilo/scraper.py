import csv
import time
import re # Importa o módulo re para expressões regulares
import os # Importa o módulo os para manipulação de caminhos
from datetime import datetime # Importa o módulo datetime para gerar timestamps

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def safe_get_element_text(element, css_selector):
    """
    Tenta obter o texto de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado.
    """
    try:
        return element.find_element(By.CSS_SELECTOR, css_selector).text.strip()
    except NoSuchElementException:
        return "N/A"

def safe_get_element_attribute(element, css_selector, attribute):
    """
    Tenta obter um atributo de um elemento usando um seletor CSS.
    Retorna "N/A" se o elemento não for encontrado ou o atributo não existir.
    """
    try:
        return element.find_element(By.CSS_SELECTOR, css_selector).get_attribute(attribute)
    except NoSuchElementException:
        return "N/A"

# Configura opções do Chrome
options = Options()
# options.add_argument("--headless")   # Descomente para rodar sem interface gráfica (modo headless)
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

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
    url = "https://leilo.com.br/leilao/carros?" # URL da página de leilões em Brasília
    driver.get(url)
    print(f"[INFO] Página carregada: {driver.title}")

    # Espera até que os elementos dos lotes (cards) estejam presentes na página.
    # Usamos o seletor mais específico para os cards de lote.
    WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sessao.cursor-pointer"))
    )

    # Encontra todos os elementos que representam um lote de leilão
    lotes = driver.find_elements(By.CSS_SELECTOR, ".sessao.cursor-pointer")
    print(f"[INFO] {len(lotes)} lotes encontrados.")

    for i, lote in enumerate(lotes, start=1):
        print(f"\n🔍 Extraindo dados do Lote {i}:")

        # 1. Título do Lote
        # Seletor: h3 dentro de div.header-card
        titulo = safe_get_element_text(lote, "div.header-card h3")
        print(f"   - Título: {titulo}")

        # 2. Link do Lote
        # Seletor: a.img-card (link principal da imagem)
        link = safe_get_element_attribute(lote, "a.img-card", "href")
        if link == "N/A":
            # Fallback para o link dentro do header-card se o principal não for encontrado
            link = safe_get_element_attribute(lote, "div.header-card a", "href")
        
        # Garante que o link seja absoluto
        if link and not link.startswith("http"):
            link = "https://leilo.com.br" + link
        print(f"   - Link: {link}")

        # 3. Imagem do Lote
        # Seletor: div.q-img__image (onde a URL da imagem está no estilo background-image)
        imagem_style = safe_get_element_attribute(lote, "div.q-img__image", "style")
        imagem = "N/A"
        if imagem_style != "N/A" and "url(" in imagem_style:
            # Extrai a URL usando regex
            match = re.search(r'url\("?\'?([^"\')]+)"?\'?\)', imagem_style)
            if match:
                imagem = match.group(1)
        print(f"   - Imagem URL: {imagem}")

        # 4. UF (Estado) do Lote
        # Seletor: span que vem depois do ícone de localização dentro de lote-codigo
        uf = safe_get_element_text(lote, "div.lote-codigo i.location_on + span")
        print(f"   - UF: {uf}")

        # 5. Ano do Lote
        # Seletor: p.text-ano
        ano_raw = safe_get_element_text(lote, "p.text-ano")
        ano = "N/A"
        if ano_raw != "N/A":
            # Limpa o texto "13 /13" para "2013"
            match_ano = re.search(r'\d{2}', ano_raw) # Busca dois dígitos
            if match_ano:
                ano = "20" + match_ano.group(0) # Assume que são anos 20xx
        print(f"   - Ano: {ano}")

        # 6. KM (Quilometragem) do Lote
        # Seletor: p.text-km
        km = safe_get_element_text(lote, "p.text-km")
        print(f"   - KM: {km}")

        # 7. Valor do Lance Atual
        # Seletor: li.valor-atual (o texto do li é o valor)
        valor = safe_get_element_text(lote, "li.valor-atual")
        print(f"   - Valor do Lance: {valor}")

        # 8. Situação do Lote (Ex: tempo restante, finalizado, retirado)
        situacao = "N/A"
        
        # Tenta obter o tempo restante (Leilão ao vivo em...)
        tempo_restante_span = safe_get_element_text(lote, "a.tempo-restante div > div span.text-weight-medium")
        if tempo_restante_span != "N/A" and tempo_restante_span.strip() != "":
            situacao = "Leilão ao vivo em: " + tempo_restante_span
        else:
            # Tenta obter "Finalizado" ou "Retirado"
            tag_finalizado = safe_get_element_text(lote, "div.tag-finalizado")
            if tag_finalizado != "N/A" and tag_finalizado.strip() != "":
                situacao = tag_finalizado
            else:
                # Tenta obter a data e hora do leilão no rodapé para 'situação'
                data_e_hora_leilao_raw = safe_get_element_text(lote, "p.q-mb-none.text-grey-7")
                if data_e_hora_leilao_raw != "N/A":
                    situacao = "Leilão: " + data_e_hora_leilao_raw.replace('\n', ' ').strip()
        print(f"   - Situação: {situacao}")

        # 9. Data do Leilão (NOVA COLUNA)
        data_leilao_str = "N/A"
        # O seletor para a data do leilão é a tag <p> com as classes q-mb-none e text-grey-7
        full_date_text = safe_get_element_text(lote, "p.q-mb-none.text-grey-7")
        if full_date_text != "N/A":
            # Usa regex para extrair a data no formato dd/mm/aaaa
            match_date = re.search(r'(\d{2}/\d{2}/\d{4})', full_date_text)
            if match_date:
                data_leilao_str = match_date.group(1)
        print(f"   - Data Leilão: {data_leilao_str}")


        dados.append({
            "Título": titulo,
            "Link": link,
            "Imagem": imagem,
            "UF": uf,
            "Ano": ano,
            "KM": km,
            "Valor do Lance": valor,
            "Situação": situacao,
            "Data Leilão": data_leilao_str # Adiciona a nova coluna
        })

    # --- Correção e diagnóstico para a geração do CSV ---
    if dados: # Verifica se a lista de dados não está vazia
        # Gera um timestamp para o nome do arquivo, garantindo unicidade
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Define o nome do arquivo CSV com o timestamp
        output_file_name = f"leilo_{timestamp}.csv"
        output_path = os.path.join("etl/", output_file_name) # Caminho completo no container Docker

        try:
            # Salvar os dados em um arquivo CSV
            # O arquivo será salvo no diretório /app dentro do container Docker
            with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=dados[0].keys())
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
        print("\n[WARN] Nenhum lote foi encontrado na página. O arquivo CSV não será gerado.")
    
    print("[INFO] Navegador fechado.")
    print("[INFO] Scraping concluído.")

finally:
    if driver: # Garante que o driver seja fechado mesmo se ocorrer um erro
        driver.quit()