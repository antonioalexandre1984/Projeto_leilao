import streamlit as st
import pandas as pd
import psycopg2
from sklearn.linear_model import LinearRegression
import google.generativeai as genai
from dotenv import load_dotenv
import os

# Carregar variáveis do .env (para chaves de API, etc.).
# As variáveis definidas no docker-compose.yml terão prioridade, o que é ideal para o ambiente Docker.
load_dotenv()

# Variáveis de ambiente para conexão com o banco de dados e API Gemini
DB_HOST = os.getenv("PG_HOST") 
DB_NAME = os.getenv("PG_DATABASE")
DB_USER = os.getenv("PG_USER")
DB_PASS = os.getenv("PG_PASSWORD")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Inicializa o estado da sessão para armazenar a resposta do Gemini
if 'gemini_response' not in st.session_state:
    st.session_state.gemini_response = ""
if 'selected_lote_data' not in st.session_state:
    st.session_state.selected_lote_data = None

# Configurar conexão com banco de dados
@st.cache_data
def carregar_dados():
    """
    Carrega dados da tabela 'lotes' do banco de dados PostgreSQL.
    Usa st.cache_data para cachear os dados e evitar recarregar desnecessariamente.
    Nota: Este dashboard está configurado para a tabela 'lotes'.
    Se seus scrapers populam 'leilao_data' ou 'consolidado', ajuste a query SQL aqui.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port="5432" # Porta padrão do PostgreSQL dentro da rede Docker
        )
        df = pd.read_sql("SELECT modelo, ano, km, preco_lote FROM lotes", conn)
        conn.close()
        st.success("Dados carregados do banco de dados com sucesso!")
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados ou carregar dados: {e}")
        st.warning(f"Detalhes da conexão (verificados no container 'analyzer'): Host={DB_HOST}, DB={DB_NAME}, User={DB_USER}")
        return pd.DataFrame()

# Estimar valor de mercado
def estimar_valor(df):
    """
    Estima o valor de mercado dos veículos usando Regressão Linear
    e calcula o percentual de desconto e oportunidade.
    Considerações para melhoria: Para dados reais, considere mais features (e.g., tipo_combustível)
    e modelos de ML mais complexos (e.g., Random Forest) para maior precisão.
    """
    # Certifica-se de que os tipos de dados estão corretos para o modelo
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce')
    df['km'] = pd.to_numeric(df['km'], errors='coerce')
    df['preco_lote'] = pd.to_numeric(df['preco_lote'], errors='coerce')

    # Remove linhas com valores NaN resultantes da conversão, se houver
    df.dropna(subset=['ano', 'km', 'preco_lote'], inplace=True)

    if df.empty or len(df) < 2: # Necessita de pelo menos 2 amostras para regressão
        st.warning("Dados insuficientes para estimar o valor de mercado. Necessita de pelo menos 2 amostras válidas.")
        df['preco_estimado'] = 0.0
        df['desconto_percentual'] = 0.0
        df['oportunidade'] = False
        return df
    
    modelo = LinearRegression()
    X = df[['ano', 'km']]
    y = df['preco_lote']
    
    try:
        modelo.fit(X, y)
        df['preco_estimado'] = modelo.predict(X)
        df['desconto_percentual'] = ((df['preco_estimado'] - df['preco_lote']) / df['preco_estimado']) * 100
        df['oportunidade'] = df['desconto_percentual'] > 20 # Define oportunidade se o desconto for > 20%
    except Exception as e:
        st.error(f"Erro ao estimar valores de mercado: {e}. Verifique se há variância suficiente nos dados para a regressão.")
        df['preco_estimado'] = 0.0
        df['desconto_percentual'] = 0.0
        df['oportunidade'] = False

    return df

# Função para consultar Gemini, agora com um botão de ação
def call_gemini_api(modelo, ano, km, preco_lote, preco_estimado):
    """
    Função auxiliar para chamar a API do Gemini.
    """
    if not GEMINI_API_KEY:
        st.session_state.gemini_response = "Erro: Chave de API do Gemini não configurada. Por favor, defina GEMINI_API_KEY."
        return

    # ALTERAÇÃO AQUI: Chave de API configurada globalmente.
    # Removendo 'api_key' do construtor GenerativeModel
    genai.configure(api_key=GEMINI_API_KEY)
    
    preco_lote_formatado = f"{preco_lote:.2f}" if isinstance(preco_lote, (int, float)) else str(preco_lote)
    preco_estimado_formatado = f"{preco_estimado:.2f}" if isinstance(preco_estimado, (int, float)) else str(preco_estimado)

    prompt = f"""
    Analise a seguinte oportunidade de compra de veículo em leilão:
    Modelo: {modelo}
    Ano: {ano}
    Quilometragem: {km}
    Preço no leilão: R$ {preco_lote_formatado}
    Valor estimado de mercado: R$ {preco_estimado_formatado}

    Isso representa uma boa oportunidade de compra? Qual é a sua justificativa?
    Considere o preço do leilão em comparação com o valor de mercado estimado, bem como a idade (ano) e a quilometragem do veículo.
    Se o preço do leilão for significativamente menor que o valor de mercado, é provável que seja uma boa oportunidade.
    """
    try:
        # Removendo 'api_key' do construtor GenerativeModel
        model = genai.GenerativeModel("gemini-2.0-flash") 
        with st.spinner("Consultando a inteligência artificial do Gemini..."):
            response = model.generate_content(prompt)
            st.session_state.gemini_response = response.text.strip()
    except Exception as e:
        st.session_state.gemini_response = f"Erro ao consultar Gemini: {e}"

# Interface do Streamlit
st.title("🚗 Análise de Leilões de Carros com IA")
st.markdown("Bem-vindo ao seu painel de análise de leilões! Conectamos ao seu banco de dados para identificar as melhores oportunidades de compra de veículos.")

df = carregar_dados()

if not df.empty:
    df = estimar_valor(df)
    
    # Filtra e exibe lotes com oportunidade
    st.subheader("📊 Lotes com Oportunidade de Compra")
    top_lotes = df[df['oportunidade']].copy() # Usar .copy() para evitar SettingWithCopyWarning

    if not top_lotes.empty:
        top_lotes = top_lotes.sort_values(by='desconto_percentual', ascending=False)
        st.dataframe(top_lotes[['modelo', 'ano', 'km', 'preco_lote', 'preco_estimado', 'desconto_percentual', 'oportunidade']])
        st.info(f"Foram encontradas {len(top_lotes)} oportunidades de compra.")
    else:
        st.info("Nenhum lote identificado como oportunidade de compra no momento (desconto percentual inferior a 20%).")

    # Avaliação Detalhada com IA
    st.subheader("🔎 Avaliação Detalhada com IA")
    
    if not top_lotes.empty:
        # Seleção do modelo
        modelos_disponiveis = top_lotes['modelo'].unique()
        modelo_selecionado = st.selectbox(
            "Escolha um modelo para análise detalhada com IA:", 
            modelos_disponiveis, 
            key="model_selector"
        )
        
        # Armazena os dados do lote selecionado no session_state para uso no callback do botão
        lote_selecionado_data = top_lotes[top_lotes['modelo'] == modelo_selecionado].iloc[0]
        st.session_state.selected_lote_data = lote_selecionado_data

        # Botão para consultar a IA
        if st.button("Consultar IA para este Lote", key="consult_gemini_button"):
            call_gemini_api(
                modelo=st.session_state.selected_lote_data['modelo'],
                ano=st.session_state.selected_lote_data['ano'],
                km=st.session_state.selected_lote_data['km'],
                preco_lote=st.session_state.selected_lote_data['preco_lote'],
                preco_estimado=st.session_state.selected_lote_data['preco_estimado']
            )
        
        # Exibe a resposta do Gemini se já tiver sido gerada
        if st.session_state.gemini_response:
            st.markdown("### 🤖 Resposta da Análise de IA (Gemini):")
            st.write(st.session_state.gemini_response)

    else:
        st.info("Não há lotes com oportunidade para análise detalhada no momento.")
else:
    st.warning("Nenhum dado encontrado para análise. Verifique a conexão com o banco de dados e se há dados na tabela 'lotes'.")

st.markdown("---")
st.markdown("Desenvolvido para análise de leilões de veículos.")